/*
Copyright The Velero Contributors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package csi

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	jsonpatch "github.com/evanphx/json-patch/v5"
	snapshotv1api "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	snapshotter "github.com/kubernetes-csi/external-snapshotter/client/v8/clientset/versioned/typed/volumesnapshot/v1"
	"github.com/sirupsen/logrus"
	corev1api "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"

	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/util/boolptr"
	"github.com/vmware-tanzu/velero/pkg/util/stringptr"
	"github.com/vmware-tanzu/velero/pkg/util/stringslice"
)

const (
	waitInternal                          = 2 * time.Second
	volumeSnapshotContentProtectFinalizer = "velero.io/volume-snapshot-content-protect-finalizer"
)

// WaitVolumeSnapshotReady waits a VS to become ready to use until the timeout reaches
func WaitVolumeSnapshotReady(
	ctx context.Context,
	snapshotClient snapshotter.SnapshotV1Interface,
	volumeSnapshot string,
	volumeSnapshotNS string,
	timeout time.Duration,
	log logrus.FieldLogger,
) (*snapshotv1api.VolumeSnapshot, error) {
	var updated *snapshotv1api.VolumeSnapshot
	errMessage := sets.NewString()

	err := wait.PollUntilContextTimeout(
		ctx,
		waitInternal,
		timeout,
		true,
		func(ctx context.Context) (bool, error) {
			tmpVS, err := snapshotClient.VolumeSnapshots(volumeSnapshotNS).Get(
				ctx, volumeSnapshot, metav1.GetOptions{})
			if err != nil {
				return false, errors.Wrapf(
					err,
					"error to get VolumeSnapshot %s/%s",
					volumeSnapshotNS, volumeSnapshot,
				)
			}

			if tmpVS.Status == nil {
				return false, nil
			}

			if tmpVS.Status.Error != nil {
				errMessage.Insert(stringptr.GetString(tmpVS.Status.Error.Message))
			}

			if !boolptr.IsSetToTrue(tmpVS.Status.ReadyToUse) {
				return false, nil
			}

			updated = tmpVS
			return true, nil
		},
	)

	if wait.Interrupted(err) {
		err = errors.Errorf(
			"volume snapshot is not ready until timeout, errors: %v",
			errMessage.List(),
		)
	}

	if errMessage.Len() > 0 {
		log.Warnf("Some errors happened during waiting for ready snapshot, errors: %v",
			errMessage.List())
	}

	return updated, err
}

// GetVolumeSnapshotContentForVolumeSnapshot returns the VolumeSnapshotContent
// object associated with the VolumeSnapshot.
func GetVolumeSnapshotContentForVolumeSnapshot(
	volSnap *snapshotv1api.VolumeSnapshot,
	snapshotClient snapshotter.SnapshotV1Interface,
) (*snapshotv1api.VolumeSnapshotContent, error) {
	if volSnap.Status == nil || volSnap.Status.BoundVolumeSnapshotContentName == nil {
		return nil, errors.Errorf("invalid snapshot info in volume snapshot %s", volSnap.Name)
	}

	vsc, err := snapshotClient.VolumeSnapshotContents().Get(
		context.TODO(),
		*volSnap.Status.BoundVolumeSnapshotContentName,
		metav1.GetOptions{},
	)
	if err != nil {
		return nil, errors.Wrap(err, "error getting volume snapshot content from API")
	}

	return vsc, nil
}

// RetainVSC updates the VSC's deletion policy to Retain and then return the update VSC
func RetainVSC(ctx context.Context, snapshotClient snapshotter.SnapshotV1Interface,
	vsc *snapshotv1api.VolumeSnapshotContent) (*snapshotv1api.VolumeSnapshotContent, error) {
	if vsc.Spec.DeletionPolicy == snapshotv1api.VolumeSnapshotContentRetain {
		return vsc, nil
	}

	return patchVSC(ctx, snapshotClient, vsc, func(updated *snapshotv1api.VolumeSnapshotContent) {
		updated.Spec.DeletionPolicy = snapshotv1api.VolumeSnapshotContentRetain
	})
}

// DeleteVolumeSnapshotContentIfAny deletes a VSC by name if it exists,
// and log an error when the deletion fails.
func DeleteVolumeSnapshotContentIfAny(
	ctx context.Context,
	snapshotClient snapshotter.SnapshotV1Interface,
	vscName string, log logrus.FieldLogger,
) {
	err := snapshotClient.VolumeSnapshotContents().Delete(ctx, vscName, metav1.DeleteOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.WithError(err).Debugf("Abort deleting VSC, it doesn't exist %s", vscName)
		} else {
			log.WithError(err).Errorf("Failed to delete volume snapshot content %s", vscName)
		}
	}
}

// EnsureDeleteVS asserts the existence of a VS by name, deletes it and waits for its
// disappearance and returns errors on any failure.
func EnsureDeleteVS(ctx context.Context, snapshotClient snapshotter.SnapshotV1Interface,
	vsName string, vsNamespace string, timeout time.Duration) error {
	err := snapshotClient.VolumeSnapshots(vsNamespace).Delete(ctx, vsName, metav1.DeleteOptions{})
	if err != nil {
		return errors.Wrap(err, "error to delete volume snapshot")
	}

	var updated *snapshotv1api.VolumeSnapshot
	err = wait.PollUntilContextTimeout(ctx, waitInternal, timeout, true, func(ctx context.Context) (bool, error) {
		vs, err := snapshotClient.VolumeSnapshots(vsNamespace).Get(ctx, vsName, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				return true, nil
			}

			return false, errors.Wrapf(err, "error to get VolumeSnapshot %s", vsName)
		}

		updated = vs
		return false, nil
	})

	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return errors.Errorf("timeout to assure VolumeSnapshot %s is deleted, finalizers in VS %v", vsName, updated.Finalizers)
		} else {
			return errors.Wrapf(err, "error to assure VolumeSnapshot is deleted, %s", vsName)
		}
	}

	return nil
}

func RemoveVSCProtect(ctx context.Context, snapshotClient snapshotter.SnapshotV1Interface, vscName string, timeout time.Duration) error {
	err := wait.PollUntilContextTimeout(ctx, waitInternal, timeout, true, func(ctx context.Context) (bool, error) {
		vsc, err := snapshotClient.VolumeSnapshotContents().Get(ctx, vscName, metav1.GetOptions{})
		if err != nil {
			return false, errors.Wrapf(err, "error to get VolumeSnapshotContent %s", vscName)
		}

		vsc.Finalizers = stringslice.Except(vsc.Finalizers, volumeSnapshotContentProtectFinalizer)

		_, err = snapshotClient.VolumeSnapshotContents().Update(ctx, vsc, metav1.UpdateOptions{})
		if err == nil {
			return true, nil
		}

		if !apierrors.IsConflict(err) {
			return false, errors.Wrapf(err, "error to update VolumeSnapshotContent %s", vscName)
		}

		return false, nil
	})

	return err
}

// EnsureDeleteVSC asserts the existence of a VSC by name, deletes it and waits for its
// disappearance and returns errors on any failure.
func EnsureDeleteVSC(ctx context.Context, snapshotClient snapshotter.SnapshotV1Interface,
	vscName string, timeout time.Duration) error {
	err := snapshotClient.VolumeSnapshotContents().Delete(ctx, vscName, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return errors.Wrap(err, "error to delete volume snapshot content")
	}

	var updated *snapshotv1api.VolumeSnapshotContent
	err = wait.PollUntilContextTimeout(ctx, waitInternal, timeout, true, func(ctx context.Context) (bool, error) {
		vsc, err := snapshotClient.VolumeSnapshotContents().Get(ctx, vscName, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				return true, nil
			}

			return false, errors.Wrapf(err, "error to get VolumeSnapshotContent %s", vscName)
		}

		updated = vsc
		return false, nil
	})

	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return errors.Errorf("timeout to assure VolumeSnapshotContent %s is deleted, finalizers in VSC %v", vscName, updated.Finalizers)
		} else {
			return errors.Wrapf(err, "error to assure VolumeSnapshotContent is deleted, %s", vscName)
		}
	}

	return nil
}

// DeleteVolumeSnapshotIfAny deletes a VS by name if it exists,
// and log an error when the deletion fails
func DeleteVolumeSnapshotIfAny(
	ctx context.Context,
	snapshotClient snapshotter.SnapshotV1Interface,
	vsName string,
	vsNamespace string,
	log logrus.FieldLogger,
) {
	err := snapshotClient.VolumeSnapshots(vsNamespace).Delete(ctx, vsName, metav1.DeleteOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.WithError(err).Debugf(
				"Abort deleting volume snapshot, it doesn't exist %s/%s",
				vsNamespace, vsName)
		} else {
			log.WithError(err).Errorf(
				"Failed to delete volume snapshot %s/%s", vsNamespace, vsName)
		}
	}
}

func patchVSC(
	ctx context.Context,
	snapshotClient snapshotter.SnapshotV1Interface,
	vsc *snapshotv1api.VolumeSnapshotContent,
	updateFunc func(*snapshotv1api.VolumeSnapshotContent),
) (*snapshotv1api.VolumeSnapshotContent, error) {
	origBytes, err := json.Marshal(vsc)
	if err != nil {
		return nil, errors.Wrap(err, "error marshaling original VSC")
	}

	updated := vsc.DeepCopy()
	updateFunc(updated)

	updatedBytes, err := json.Marshal(updated)
	if err != nil {
		return nil, errors.Wrap(err, "error marshaling updated VSC")
	}

	patchBytes, err := jsonpatch.CreateMergePatch(origBytes, updatedBytes)
	if err != nil {
		return nil, errors.Wrap(err, "error creating json merge patch for VSC")
	}

	patched, err := snapshotClient.VolumeSnapshotContents().Patch(ctx, vsc.Name, types.MergePatchType, patchBytes, metav1.PatchOptions{})
	if err != nil {
		return nil, errors.Wrap(err, "error patching VSC")
	}

	return patched, nil
}

func GetVolumeSnapshotClass(
	provisioner string,
	backup *velerov1api.Backup,
	pvc *corev1api.PersistentVolumeClaim,
	log logrus.FieldLogger,
	crClient crclient.Client,
) (*snapshotv1api.VolumeSnapshotClass, error) {
	snapshotClasses := new(snapshotv1api.VolumeSnapshotClassList)
	err := crClient.List(context.TODO(), snapshotClasses)
	if err != nil {
		return nil, errors.Wrap(err, "error listing VolumeSnapshotClass")
	}

	// 1. Check if there is a mapping for the PVC's StorageClass in the Backup annotations
	snapshotClass, err := GetVolumeSnapshotClassFromBackupMapping(backup, pvc, provisioner, snapshotClasses)
	if err != nil {
		log.Debugf("Didn't find VolumeSnapshotClass from Backup StorageClass mapping: %v", err)
	}
	if snapshotClass != nil {
		log.Infof("Found VolumeSnapshotClass %s from Backup StorageClass mapping for PVC %s", snapshotClass.Name, pvc.Name)
		return snapshotClass, nil
	}

	// 2. If a snapshot class is set for provider in PVC annotations, use that
	snapshotClass, err = GetVolumeSnapshotClassFromPVCAnnotationsForDriver(
		pvc, provisioner, snapshotClasses,
	)
	if err != nil {
		log.Debugf("Didn't find VolumeSnapshotClass from PVC annotations: %v", err)
	}
	if snapshotClass != nil {
		return snapshotClass, nil
	}

	// 3. If there is no annotation in PVC, attempt to fetch it from backup annotations (Driver based)
	snapshotClass, err = GetVolumeSnapshotClassFromBackupAnnotationsForDriver(
		backup, provisioner, snapshotClasses)
	if err != nil {
		log.Debugf("Didn't find VolumeSnapshotClass from Backup annotations: %v", err)
	}
	if snapshotClass != nil {
		return snapshotClass, nil
	}

	// Drivers listed in the Backup's SnapshotClassRequiredDriversAnnotation must
	// have a VolumeSnapshotClass configured explicitly (tiers 1-3 above, or a VSC
	// the user labeled as the default in tier 4). For these drivers we do NOT
	// allow the implicit selections that follow -- picking a lone unlabeled VSC
	// (tier 4) or auto-creating one (tier 5) -- because the result would lack the
	// driver-specific parameters the snapshot needs and would only time out.
	isRequiredDriver := snapshotClassDriverRequired(backup, provisioner)

	// 4. Fallback to default behavior of fetching snapshot class based on label.
	// For required drivers the single-match fallback is disabled, so only a
	// user-labeled VSC qualifies here.
	snapshotClass, err = GetVolumeSnapshotClassForStorageClass(
		provisioner, snapshotClasses, !isRequiredDriver)
	if err == nil && snapshotClass != nil {
		return snapshotClass, nil
	}

	// For a required driver, none of the explicit methods matched, so fail this
	// PVC's snapshot immediately with an actionable message instead of falling
	// through to the implicit auto-create.
	if isRequiredDriver {
		return nil, snapshotClassRequiredError(provisioner, pvc)
	}

	log.Debugf("No VolumeSnapshotClass found via standard methods: %v", err)

	// 5. Absolute last resort fallback.
	return GetFallbackVolumeSnapshotClass(provisioner, log, crClient)
}

// snapshotClassRequiredError builds the actionable error returned when a CSI
// driver requires a manually-configured VolumeSnapshotClass but none was found
// through the explicit selection methods.
func snapshotClassRequiredError(
	provisioner string,
	pvc *corev1api.PersistentVolumeClaim,
) error {
	pvcRef := "<unknown>"
	storageClassName := "<unknown>"
	if pvc != nil {
		pvcRef = fmt.Sprintf("%s/%s", pvc.Namespace, pvc.Name)
		if pvc.Spec.StorageClassName != nil && *pvc.Spec.StorageClassName != "" {
			storageClassName = *pvc.Spec.StorageClassName
		}
	}
	return errors.Errorf(
		"No VolumeSnapshotClass configured for CSI driver %q (PVC %s, StorageClass %q). "+
			"This driver requires custom parameters in its VolumeSnapshotClass, so CloudCasa "+
			"will not create one automatically. Configure a VolumeSnapshotClass "+
			"as described in the documentation, then retry the backup.",
		provisioner, pvcRef, storageClassName,
	)
}

// GetSnapshotClassRequiredDrivers parses the comma-separated list of CSI driver
// names from the Backup's SnapshotClassRequiredDriversAnnotation. These drivers
// require a manually-configured VolumeSnapshotClass, so the plugin must not
// auto-create one for them. Returns an empty set when the annotation is absent
// or empty, in which case the feature is a no-op.
//
// Names are lower-cased so membership checks are case-insensitive against a
// StorageClass provisioner / VolumeSnapshotClass driver; callers must lower-case
// the value they test with Has (see snapshotClassDriverRequired).
func GetSnapshotClassRequiredDrivers(backup *velerov1api.Backup) sets.String {
	drivers := sets.NewString()
	if backup == nil {
		return drivers
	}
	raw := backup.Annotations[velerov1api.SnapshotClassRequiredDriversAnnotation]
	for _, d := range strings.Split(raw, ",") {
		if trimmed := strings.TrimSpace(d); trimmed != "" {
			drivers.Insert(strings.ToLower(trimmed))
		}
	}
	return drivers
}

// snapshotClassDriverRequired reports whether the given CSI driver/provisioner
// is in the Backup's required-drivers list, matching case-insensitively to stay
// consistent with the driver comparisons used by the other selection tiers.
func snapshotClassDriverRequired(backup *velerov1api.Backup, provisioner string) bool {
	return GetSnapshotClassRequiredDrivers(backup).Has(strings.ToLower(provisioner))
}

// Checks for StorageClass -> VolumeSnapshotClass mapping
func GetVolumeSnapshotClassFromBackupMapping(
	backup *velerov1api.Backup,
	pvc *corev1api.PersistentVolumeClaim,
	provisioner string,
	snapshotClasses *snapshotv1api.VolumeSnapshotClassList,
) (*snapshotv1api.VolumeSnapshotClass, error) {
	if pvc.Spec.StorageClassName == nil || *pvc.Spec.StorageClassName == "" {
		// If no StorageClass is specified, we cannot use the SC-based mapping.
		// Log this and return nil to fall through to other methods.
		// (An empty string check is also good practice, though nil is the primary concern).
		return nil, nil
	}
	targetStorageClass := *pvc.Spec.StorageClassName

	// Iterate over ALL backup annotations to find our specific prefix
	var targetVSCName string
	found := false

	for k, v := range backup.Annotations {
		if strings.HasPrefix(k, velerov1api.VolumeSnapshotClassStorageClassBackupAnnotationPrefix) {
			// Value format is "StorageClass:VolumeSnapshotClass"
			parts := strings.SplitN(v, ":", 2)
			if len(parts) != 2 {
				continue // Malformed value, skip
			}

			scName := parts[0]
			vscName := parts[1]

			// Check if this mapping matches the PVC's StorageClass
			if scName == targetStorageClass {
				targetVSCName = vscName
				found = true
				break
			}
		}
	}

	if !found {
		return nil, nil // No mapping found for this storage class
	}

	// Verify the mapped VolumeSnapshotClass exists and matches the driver
	for _, sc := range snapshotClasses.Items {
		if strings.EqualFold(targetVSCName, sc.ObjectMeta.Name) {
			if !strings.EqualFold(sc.Driver, provisioner) {
				return nil, errors.Errorf(
					"Mapped VolumeSnapshotClass %s (from SC %s) is not for driver %s",
					sc.ObjectMeta.Name, targetStorageClass, provisioner,
				)
			}
			return &sc, nil
		}
	}

	return nil, errors.Errorf(
		"Mapped VolumeSnapshotClass %s not found in cluster for StorageClass %s",
		targetVSCName, targetStorageClass,
	)
}

func GetVolumeSnapshotClassFromPVCAnnotationsForDriver(
	pvc *corev1api.PersistentVolumeClaim,
	provisioner string,
	snapshotClasses *snapshotv1api.VolumeSnapshotClassList,
) (*snapshotv1api.VolumeSnapshotClass, error) {
	annotationKey := velerov1api.VolumeSnapshotClassDriverPVCAnnotation
	snapshotClassName, ok := pvc.ObjectMeta.Annotations[annotationKey]
	if !ok {
		return nil, nil
	}
	for _, sc := range snapshotClasses.Items {
		if strings.EqualFold(snapshotClassName, sc.ObjectMeta.Name) {
			if !strings.EqualFold(sc.Driver, provisioner) {
				return nil, errors.Errorf(
					"Incorrect VolumeSnapshotClass %s is not for driver %s",
					sc.ObjectMeta.Name, provisioner,
				)
			}
			return &sc, nil
		}
	}
	return nil, errors.Errorf(
		"No CSI VolumeSnapshotClass found with name %s for provisioner %s for PVC %s",
		snapshotClassName, provisioner, pvc.Name,
	)
}

// GetVolumeSnapshotClassFromAnnotationsForDriver returns a
// VolumeSnapshotClass for the supplied volume provisioner/driver
// name from the annotation of the backup.
func GetVolumeSnapshotClassFromBackupAnnotationsForDriver(
	backup *velerov1api.Backup,
	provisioner string,
	snapshotClasses *snapshotv1api.VolumeSnapshotClassList,
) (*snapshotv1api.VolumeSnapshotClass, error) {
	annotationKey := fmt.Sprintf(
		"%s_%s",
		velerov1api.VolumeSnapshotClassDriverBackupAnnotationPrefix,
		strings.ToLower(provisioner),
	)
	snapshotClassName, ok := backup.ObjectMeta.Annotations[annotationKey]
	if !ok {
		return nil, nil
	}
	for _, sc := range snapshotClasses.Items {
		if strings.EqualFold(snapshotClassName, sc.ObjectMeta.Name) {
			if !strings.EqualFold(sc.Driver, provisioner) {
				return nil, errors.Errorf(
					"Incorrect VolumeSnapshotClass %s is not for driver %s for backup %s",
					sc.ObjectMeta.Name, provisioner, backup.Name,
				)
			}
			return &sc, nil
		}
	}
	return nil, errors.Errorf(
		"No CSI VolumeSnapshotClass found with name %s for driver %s for backup %s",
		snapshotClassName, provisioner, backup.Name,
	)
}

// GetVolumeSnapshotClassForStorageClass returns a VolumeSnapshotClass
// for the supplied volume provisioner/ driver name.
//
// A VSC carrying the selector label is always preferred. When
// allowSingleMatchFallback is true and no labeled VSC exists, a lone VSC for the
// driver is returned as an implicit default. Callers pass false to disable that
// implicit fallback (e.g. drivers that require an explicitly-configured VSC), in
// which case only a labeled VSC qualifies.
func GetVolumeSnapshotClassForStorageClass(
	provisioner string,
	snapshotClasses *snapshotv1api.VolumeSnapshotClassList,
	allowSingleMatchFallback bool,
) (*snapshotv1api.VolumeSnapshotClass, error) {
	n := 0
	var vsClass snapshotv1api.VolumeSnapshotClass
	// We pick the VolumeSnapshotClass that matches the CSI driver name
	// and has a 'cloudcasa.io/csi-volumesnapshot-class' label. This allows
	// multiple VolumeSnapshotClasses for the same driver with different
	// values for the other fields in the spec.
	for _, sc := range snapshotClasses.Items {
		_, hasLabelSelector := sc.Labels[velerov1api.VolumeSnapshotClassSelectorLabel]
		if sc.Driver == provisioner {
			n += 1
			vsClass = sc
			if hasLabelSelector {
				return &sc, nil
			}
		}
	}
	// If there's only one volumesnapshotclass for the driver, return it,
	// unless the caller disabled this implicit fallback.
	if allowSingleMatchFallback && n == 1 {
		return &vsClass, nil
	}
	return nil, fmt.Errorf(
		"failed to get VolumeSnapshotClass for provisioner %s, "+
			"ensure that the desired VolumeSnapshot class is configured in Cloudcasa.",
		provisioner)
}

// GetFallbackVolumeSnapshotClass attempts to find a usable class or creates a new one
// if strict matching failed.
//
// Drivers that require a manually-configured VolumeSnapshotClass never reach this
// function: GetVolumeSnapshotClass fails them fast at the explicit-methods
// boundary (see snapshotClassRequiredError), so this fallback only runs for
// drivers where auto-selection/creation is acceptable.
func GetFallbackVolumeSnapshotClass(
	provisioner string,
	log logrus.FieldLogger,
	client crclient.Client,
) (*snapshotv1api.VolumeSnapshotClass, error) {
	log.Infof("Attempting to find or create a fallback VolumeSnapshotClass for driver: %s", provisioner)

	// 1. List all VSCs
	vscList := new(snapshotv1api.VolumeSnapshotClassList)
	if err := client.List(context.TODO(), vscList); err != nil {
		return nil, errors.Wrap(err, "failed to list VolumeSnapshotClasses for fallback selection")
	}

	// 2. Filter by Driver/Provisioner
	var matches []snapshotv1api.VolumeSnapshotClass
	for _, vsc := range vscList.Items {
		if vsc.Driver == provisioner {
			matches = append(matches, vsc)
		}
	}

	// 3. Logic: If exactly one exists, use it (Relaxed requirements)
	if len(matches) == 1 {
		log.Infof("Found exactly one existing VolumeSnapshotClass for driver %s: %s. Using it.", provisioner, matches[0].Name)
		return &matches[0], nil
	}

	// 4. Logic: If 0 or >1, we create a Cloudcasa specific fallback class.
	// If >1, we create a new one to ensure we don't pick a class with unknown/undesirable parameters (e.g. Gold vs Bronze).
	return CreateFallbackVolumeSnapshotClass(provisioner, log, client)
}

func CreateFallbackVolumeSnapshotClass(
	provisioner string,
	log logrus.FieldLogger,
	client crclient.Client,
) (*snapshotv1api.VolumeSnapshotClass, error) {

	// Sanitize provisioner name
	safeName := strings.ReplaceAll(provisioner, ".", "-")
	vscName := fmt.Sprintf("cloudcasa-%s", safeName)

	log.Infof("Creating new fallback VolumeSnapshotClass: %s", vscName)

	// Define Parameters
	parameters := make(map[string]string)
	switch provisioner {
	case "driver.longhorn.io":
		parameters["type"] = "snap"
	case "cinder.csi.openstack.org":
		parameters["force-create"] = "true"
	}

	// Define Object
	newVSC := &snapshotv1api.VolumeSnapshotClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: vscName,
			Labels: map[string]string{
				velerov1api.VolumeSnapshotClassSelectorLabel: "true",
				"created-by": "cloudcasa",
			},
		},
		Driver:         provisioner,
		DeletionPolicy: snapshotv1api.VolumeSnapshotContentRetain,
		Parameters:     parameters,
	}

	// Attempt Create
	err := client.Create(context.TODO(), newVSC)
	if err == nil {
		log.Infof("Successfully created fallback VolumeSnapshotClass: %s", vscName)
		return newVSC, nil
	}

	if !apierrors.IsAlreadyExists(err) {
		return nil, errors.Wrapf(err, "failed to create fallback VolumeSnapshotClass %s", vscName)
	}

	// --- Handling Race Condition / Cache Lag ---
	log.Infof("Fallback VolumeSnapshotClass %s already exists, waiting for cache sync...", vscName)

	existingVSC := new(snapshotv1api.VolumeSnapshotClass)
	key := crclient.ObjectKey{Name: vscName}

	// Retry loop: Poll every 500ms, up to 5 seconds.
	// This handles the case where API says "Created" but client cache says "NotFound".
	pollErr := wait.PollUntilContextTimeout(context.Background(), 500*time.Millisecond, 5*time.Second, true, func(ctx context.Context) (bool, error) {
		// Use the context passed into the closure
		if err := client.Get(ctx, key, existingVSC); err != nil {
			if apierrors.IsNotFound(err) {
				// Object exists on server but not in cache yet. Retry.
				return false, nil
			}
			// If we get a permission error or connection refused, stop retrying.
			return false, err
		}
		// Success: Found the object
		return true, nil
	})

	if pollErr != nil {
		return nil, errors.Wrapf(pollErr, "failed to retrieve existing fallback VolumeSnapshotClass %s after creation conflict", vscName)
	}

	return existingVSC, nil
}

// IsVolumeSnapshotClassHasListerSecret returns whether a volumesnapshotclass has a snapshotlister secret
func IsVolumeSnapshotClassHasListerSecret(vc *snapshotv1api.VolumeSnapshotClass) bool {
	// https://github.com/kubernetes-csi/external-snapshotter/blob/master/pkg/utils/util.go#L59-L60
	// There is no release w/ these constants exported. Using the strings for now.
	_, nameExists := vc.Annotations[velerov1api.PrefixedListSecretNameAnnotation]
	_, nsExists := vc.Annotations[velerov1api.PrefixedListSecretNamespaceAnnotation]
	return nameExists && nsExists
}

// IsVolumeSnapshotContentHasDeleteSecret returns whether a volumesnapshotcontent has a deletesnapshot secret
func IsVolumeSnapshotContentHasDeleteSecret(vsc *snapshotv1api.VolumeSnapshotContent) bool {
	// https://github.com/kubernetes-csi/external-snapshotter/blob/master/pkg/utils/util.go#L56-L57
	// use exported constants in the next release
	_, nameExists := vsc.Annotations[velerov1api.PrefixedSecretNameAnnotation]
	_, nsExists := vsc.Annotations[velerov1api.PrefixedSecretNamespaceAnnotation]
	return nameExists && nsExists
}

// IsVolumeSnapshotExists returns whether a specific volumesnapshot object exists.
func IsVolumeSnapshotExists(
	ns,
	name string,
	crClient crclient.Client,
) bool {
	vs := new(snapshotv1api.VolumeSnapshot)
	err := crClient.Get(
		context.TODO(),
		crclient.ObjectKey{Namespace: ns, Name: name},
		vs,
	)

	return err == nil
}

func SetVolumeSnapshotContentDeletionPolicy(
	vscName string,
	crClient crclient.Client,
	policy snapshotv1api.DeletionPolicy,
) (*snapshotv1api.VolumeSnapshotContent, error) {
	vsc := new(snapshotv1api.VolumeSnapshotContent)
	if err := crClient.Get(context.TODO(), crclient.ObjectKey{Name: vscName}, vsc); err != nil {
		return nil, err
	}

	originVSC := vsc.DeepCopy()
	vsc.Spec.DeletionPolicy = policy

	return vsc, crClient.Patch(context.TODO(), vsc, crclient.MergeFrom(originVSC))
}

// CleanupVolumeSnapshot deletes the VolumeSnapshot and the associated VolumeSnapshotContent.  It will make sure the
// physical snapshot is also deleted.
func CleanupVolumeSnapshot(
	volSnap *snapshotv1api.VolumeSnapshot,
	crClient crclient.Client,
	log logrus.FieldLogger,
) {
	log.Infof("Deleting Volumesnapshot %s/%s", volSnap.Namespace, volSnap.Name)
	vs := new(snapshotv1api.VolumeSnapshot)
	err := crClient.Get(
		context.TODO(),
		crclient.ObjectKey{Name: volSnap.Name, Namespace: volSnap.Namespace},
		vs,
	)
	if err != nil {
		log.Debugf("Failed to get volumesnapshot %s/%s", volSnap.Namespace, volSnap.Name)
		return
	}

	if vs.Status != nil && vs.Status.BoundVolumeSnapshotContentName != nil {
		// we patch the DeletionPolicy of the VolumeSnapshotContent to set it to Delete.
		// This ensures that the volume snapshot in the storage provider is also deleted.
		_, err := SetVolumeSnapshotContentDeletionPolicy(
			*vs.Status.BoundVolumeSnapshotContentName,
			crClient,
			snapshotv1api.VolumeSnapshotContentDelete,
		)
		if err != nil {
			log.Debugf("Failed to patch DeletionPolicy of volume snapshot %s/%s",
				vs.Namespace, vs.Name)
		}
	}
	err = crClient.Delete(context.TODO(), vs)
	if err != nil {
		log.Debugf("Failed to delete volumesnapshot %s/%s: %v", vs.Namespace, vs.Name, err)
	} else {
		log.Infof("Deleted volumesnapshot with volumesnapshotContent %s/%s",
			vs.Namespace, vs.Name)
	}
}

func DeleteReadyVolumeSnapshot(
	vs snapshotv1api.VolumeSnapshot,
	client crclient.Client,
	logger logrus.FieldLogger,
) {
	logger.Infof("Deleting Volumesnapshot %s/%s", vs.Namespace, vs.Name)
	if vs.Status == nil ||
		vs.Status.BoundVolumeSnapshotContentName == nil ||
		len(*vs.Status.BoundVolumeSnapshotContentName) <= 0 {
		logger.Errorf("VolumeSnapshot %s/%s is not ready. This is not expected.",
			vs.Namespace, vs.Name)
		return
	}

	var vsc *snapshotv1api.VolumeSnapshotContent

	if vs.Status != nil && vs.Status.BoundVolumeSnapshotContentName != nil {
		var err error

		// Patch the DeletionPolicy of the VolumeSnapshotContent to set it to Retain.
		// This ensures that the volume snapshot in the storage provider is kept.
		if vsc, err = SetVolumeSnapshotContentDeletionPolicy(
			*vs.Status.BoundVolumeSnapshotContentName,
			client,
			snapshotv1api.VolumeSnapshotContentRetain,
		); err != nil {
			logger.Warnf("Failed to patch DeletionPolicy of VolumeSnapshot %s/%s",
				vs.Namespace, vs.Name)
			return
		}

		if err := client.Delete(context.TODO(), vsc); err != nil {
			logger.WithError(err).Warnf("Failed to delete the VolumeSnapshotContent %s", vsc.Name)
		}
	}
	if err := client.Delete(context.TODO(), &vs); err != nil {
		logger.WithError(err).Warnf("Failed to delete VolumeSnapshot %s", vs.Namespace+"/"+vs.Name)
	} else {
		logger.Infof("Deleted VolumeSnapshot %s and VolumeSnapshotContent %s",
			vs.Namespace+"/"+vs.Name, vsc.Name)
	}
}

// WaitUntilVSCHandleIsReady returns the VolumeSnapshotContent
// object associated with the volumesnapshot
func WaitUntilVSCHandleIsReady(
	volSnap *snapshotv1api.VolumeSnapshot,
	crClient crclient.Client,
	log logrus.FieldLogger,
	csiSnapshotTimeout time.Duration,
) (*snapshotv1api.VolumeSnapshotContent, error) {
	// We'll wait for the VSC to be reconciled, trying a fast poll interval first
	// before falling back to a slower poll interval for the full csiSnapshotTimeout.
	vsc := new(snapshotv1api.VolumeSnapshotContent)
	var interval time.Duration

	pollFunc := func(ctx context.Context) (bool, error) {
		vs := new(snapshotv1api.VolumeSnapshot)
		if err := crClient.Get(
			ctx,
			crclient.ObjectKeyFromObject(volSnap),
			vs,
		); err != nil {
			return false,
				errors.Wrapf(
					err,
					"failed to get volumesnapshot %s/%s",
					volSnap.Namespace, volSnap.Name,
				)
		}

		if vs.Status == nil || vs.Status.BoundVolumeSnapshotContentName == nil {
			log.Infof("Waiting for CSI driver to reconcile volumesnapshot %s/%s. Retrying in %ds",
				volSnap.Namespace, volSnap.Name, interval/time.Second)
			return false, nil
		}

		if err := crClient.Get(
			ctx,
			crclient.ObjectKey{
				Name: *vs.Status.BoundVolumeSnapshotContentName,
			},
			vsc,
		); err != nil {
			return false,
				errors.Wrapf(
					err,
					"failed to get VolumeSnapshotContent %s for VolumeSnapshot %s/%s",
					*vs.Status.BoundVolumeSnapshotContentName, vs.Namespace, vs.Name,
				)
		}

		// we need to wait for the VolumeSnapshotContent
		// to have a snapshot handle because during restore,
		// we'll use that snapshot handle as the source for
		// the VolumeSnapshotContent so it's statically
		// bound to the existing snapshot.
		if vsc.Status == nil ||
			vsc.Status.SnapshotHandle == nil {
			log.Infof(
				"Waiting for VolumeSnapshotContents %s to have snapshot handle. Retrying in %ds",
				vsc.Name, interval/time.Second)
			if vsc.Status != nil &&
				vsc.Status.Error != nil {
				log.Warnf("VolumeSnapshotContent %s has error: %v",
					vsc.Name, *vsc.Status.Error.Message)
			}
			return false, nil
		}

		return true, nil
	}

	var err error
	frequentPolling, err := strconv.ParseBool(os.Getenv("CSI_SNAPSHOT_EARLY_FREQUENT_POLLING"))

	if err == nil && frequentPolling {
		// The short interval for the first ten seconds is due to the fact that
		// Microsoft VSS backups have a hard-coded unfreeze call after 10 seconds,
		// so we need to minimize waiting time during the first 10 seconds.
		// First poll with a short interval and timeout.
		interval = 1 * time.Second
		timeout := 10 * time.Second
		err = wait.PollUntilContextTimeout(
			context.Background(),
			interval,
			timeout,
			true,
			pollFunc,
		)

		if err == nil {
			return vsc, nil
		}
		if !wait.Interrupted(err) {
			return nil, err
		}
	}

	// If the first poll timed out, poll with a longer interval and the full timeout.
	interval = 5 * time.Second
	err = wait.PollUntilContextTimeout(
		context.Background(),
		interval,
		csiSnapshotTimeout,
		true,
		pollFunc,
	)

	if err != nil {
		if wait.Interrupted(err) {
			if vsc != nil &&
				vsc.Status != nil &&
				vsc.Status.Error != nil {
				log.Errorf(
					"Timed out awaiting reconciliation of VolumeSnapshot, VolumeSnapshotContent %s has error: %v",
					vsc.Name, *vsc.Status.Error.Message)
				return nil,
					errors.Errorf("CSI got timed out with error: %v",
						*vsc.Status.Error.Message)
			} else {
				log.Errorf(
					"Timed out awaiting reconciliation of volumesnapshot %s/%s",
					volSnap.Namespace, volSnap.Name)
			}
		}
		return nil, err
	}

	return vsc, nil
}

func DiagnoseVS(vs *snapshotv1api.VolumeSnapshot, events *corev1api.EventList) string {
	vscName := ""
	readyToUse := false
	errMessage := ""

	if vs.Status != nil {
		if vs.Status.BoundVolumeSnapshotContentName != nil {
			vscName = *vs.Status.BoundVolumeSnapshotContentName
		}

		if vs.Status.ReadyToUse != nil {
			readyToUse = *vs.Status.ReadyToUse
		}

		if vs.Status.Error != nil && vs.Status.Error.Message != nil {
			errMessage = *vs.Status.Error.Message
		}
	}

	var diag strings.Builder
	_, _ = fmt.Fprintf(&diag, "VS %s/%s, bind to %s, readyToUse %v, errMessage %s\n", vs.Namespace, vs.Name, vscName, readyToUse, errMessage)

	if events != nil {
		for _, e := range events.Items {
			if e.InvolvedObject.UID == vs.UID && e.Type == corev1api.EventTypeWarning {
				_, _ = fmt.Fprintf(&diag, "VS event reason %s, message %s\n", e.Reason, e.Message)
			}
		}
	}

	return diag.String()
}

func DiagnoseVSC(vsc *snapshotv1api.VolumeSnapshotContent) string {
	handle := ""
	readyToUse := false
	errMessage := ""

	if vsc.Status != nil {
		if vsc.Status.SnapshotHandle != nil {
			handle = *vsc.Status.SnapshotHandle
		}

		if vsc.Status.ReadyToUse != nil {
			readyToUse = *vsc.Status.ReadyToUse
		}

		if vsc.Status.Error != nil && vsc.Status.Error.Message != nil {
			errMessage = *vsc.Status.Error.Message
		}
	}

	diag := fmt.Sprintf("VSC %s, readyToUse %v, errMessage %s, handle %s\n", vsc.Name, readyToUse, errMessage, handle)

	return diag
}

// GetVSCForVS returns the VolumeSnapshotContent object associated with the VolumeSnapshot.
func GetVSCForVS(
	ctx context.Context,
	vs *snapshotv1api.VolumeSnapshot,
	client crclient.Client,
) (*snapshotv1api.VolumeSnapshotContent, error) {
	if vs.Status == nil || vs.Status.BoundVolumeSnapshotContentName == nil {
		return nil, errors.Errorf("invalid snapshot info in volume snapshot %s", vs.Name)
	}

	vsc := new(snapshotv1api.VolumeSnapshotContent)

	if err := client.Get(
		ctx,
		crclient.ObjectKey{
			Name: *vs.Status.BoundVolumeSnapshotContentName,
		},
		vsc,
	); err != nil {
		return nil, errors.Wrap(err, "error getting volume snapshot content from API")
	}

	return vsc, nil
}

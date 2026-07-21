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
	"strings"
	"sync"
	"time"

	jsonpatch "github.com/evanphx/json-patch/v5"
	snapshotv1api "github.com/kubernetes-csi/external-snapshotter/client/v7/apis/volumesnapshot/v1"
	snapshotter "github.com/kubernetes-csi/external-snapshotter/client/v7/clientset/versioned/typed/volumesnapshot/v1"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	corev1api "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/retry"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/vmware-tanzu/velero/internal/catalogic"
	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/util/boolptr"
	"github.com/vmware-tanzu/velero/pkg/util/stringptr"
	"github.com/vmware-tanzu/velero/pkg/util/stringslice"

	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	k8swatch "k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
	watchtools "k8s.io/client-go/tools/watch"
)

const (
	waitInternal                          = 2 * time.Second
	volumeSnapshotContentProtectFinalizer = "velero.io/volume-snapshot-content-protect-finalizer"
	// snapshotProgressHeartbeatInterval is the fixed cadence at which
	// WaitUntilVSCHandleIsReady relays snapshot progress to the kubeagent while
	// waiting for a CSI snapshot. A heartbeat is emitted every interval even when
	// the snapshot state has not changed, so the kubeagent refreshes the job's
	// TTL and does not evict a healthy long-running snapshot at the 60-minute
	// timeout. The interval is deliberately coarse to avoid burdening the K8s API
	// server, the kubeagent, and KAS with excessive updates.
	snapshotProgressHeartbeatInterval = 5 * time.Minute
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
					fmt.Sprintf("error to get VolumeSnapshot %s/%s",
						volumeSnapshotNS, volumeSnapshot),
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

	err = wait.PollUntilContextTimeout(ctx, waitInternal, timeout, true, func(ctx context.Context) (bool, error) {
		_, err := snapshotClient.VolumeSnapshots(vsNamespace).Get(ctx, vsName, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				return true, nil
			}

			return false, errors.Wrapf(err, fmt.Sprintf("error to get VolumeSnapshot %s", vsName))
		}

		return false, nil
	})

	if err != nil {
		return errors.Wrapf(err, "error to assure VolumeSnapshot is deleted, %s", vsName)
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
	err = wait.PollUntilContextTimeout(ctx, waitInternal, timeout, true, func(ctx context.Context) (bool, error) {
		_, err := snapshotClient.VolumeSnapshotContents().Get(ctx, vscName, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				return true, nil
			}

			return false, errors.Wrapf(err, fmt.Sprintf("error to get VolumeSnapshotContent %s", vscName))
		}

		return false, nil
	})

	if err != nil {
		return errors.Wrapf(err, "error to assure VolumeSnapshotContent is deleted, %s", vscName)
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
		"no VolumeSnapshotClass configured for CSI driver %q (PVC %s, StorageClass %q). "+
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
) error {
	vsc := new(snapshotv1api.VolumeSnapshotContent)
	if err := crClient.Get(context.TODO(), crclient.ObjectKey{Name: vscName}, vsc); err != nil {
		return err
	}

	originVSC := vsc.DeepCopy()
	vsc.Spec.DeletionPolicy = snapshotv1api.VolumeSnapshotContentDelete

	return crClient.Patch(context.TODO(), vsc, crclient.MergeFrom(originVSC))
}

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
		err := SetVolumeSnapshotContentDeletionPolicy(
			*vs.Status.BoundVolumeSnapshotContentName,
			crClient,
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

// DeleteVolumeSnapshot handles the VolumeSnapshot instance deletion.
func DeleteVolumeSnapshot(
	vs snapshotv1api.VolumeSnapshot,
	vsc snapshotv1api.VolumeSnapshotContent,
	backup *velerov1api.Backup,
	client crclient.Client,
	logger logrus.FieldLogger,
) {
	vsReady := vs.Status != nil &&
		vs.Status.BoundVolumeSnapshotContentName != nil &&
		len(*vs.Status.BoundVolumeSnapshotContentName) > 0

	// Guard: if VS is not ready and DeletionPolicy is Delete, we must not
	// proceed — deleting the VS would cascade-delete the storage snapshot.
	if !vsReady {
		if vsc.Spec.DeletionPolicy == snapshotv1api.VolumeSnapshotContentDelete {
			vscInfo := ""
			if vsc.Name != "" {
				vscInfo = fmt.Sprintf(" and VolumeSnapshotContent %s (with DeletionPolicy=%s)", vsc.Name, vsc.Spec.DeletionPolicy)
			}
			logger.Warnf("VolumeSnapshot %s/%s is not ready; skipping deletion to prevent cascade-deleting the storage snapshot. "+
				"The VolumeSnapshot%s will remain in the cluster "+
				"until the retention period expires or the recovery point is explicitly deleted. "+
				"Manual deletion of either resource before that will destroy the storage snapshot. "+
				"To safely clean up manually, first patch the VolumeSnapshotContent DeletionPolicy to Retain, then delete the VolumeSnapshot.",
				vs.Namespace, vs.Name, vscInfo)
			return
		}
		logger.Infof("VolumeSnapshot %s/%s is not ready, but DeletionPolicy is %s. Proceeding with deletion.",
			vs.Namespace, vs.Name, vsc.Spec.DeletionPolicy)
	}

	// Patch DeletionPolicy to Retain before deleting, so the storage snapshot survives.
	modifyVSCFlag := vsReady && vsc.Spec.DeletionPolicy == snapshotv1api.VolumeSnapshotContentDelete

	if modifyVSCFlag {
		logger.Infof("VolumeSnapshotContent %s requires DeletionPolicy patch from Delete to Retain before deleting VolumeSnapshot %s/%s",
			vsc.Name, vs.Namespace, vs.Name)
	} else {
		logger.Infof("No DeletionPolicy patch needed for VolumeSnapshot %s/%s (VSCReady=%v, VSC=%q, DeletionPolicy=%s)",
			vs.Namespace, vs.Name, vsReady, vsc.Name, vsc.Spec.DeletionPolicy)
	}

	// Change VolumeSnapshotContent's DeletionPolicy to Retain before deleting VolumeSnapshot,
	// because VolumeSnapshotContent will be deleted by deleting VolumeSnapshot, when
	// DeletionPolicy is set to Delete, but Velero needs VSC for cleaning snapshot on cloud
	// in backup deletion.
	if modifyVSCFlag {
		logger.Debugf("Patching VolumeSnapshotContent %s", vsc.Name)
		originVSC := vsc.DeepCopy()
		vsc.Spec.DeletionPolicy = snapshotv1api.VolumeSnapshotContentRetain
		err := client.Patch(
			context.Background(),
			&vsc,
			crclient.MergeFrom(originVSC),
		)
		if err != nil {
			logger.Errorf(
				"fail to modify VolumeSnapshotContent %s DeletionPolicy to Retain: %s",
				vsc.Name, err.Error(),
			)
			return
		}

		defer func() {
			logger.Debugf("Start to recreate VolumeSnapshotContent %s", vsc.Name)
			err := recreateVolumeSnapshotContent(vsc, backup, client, logger)
			if err != nil {
				logger.Errorf(
					"fail to recreate VolumeSnapshotContent %s: %s",
					vsc.Name,
					err.Error(),
				)
			}
		}()
	}

	// Delete VolumeSnapshot from cluster
	logger.Debugf("Deleting VolumeSnapshot %s/%s", vs.Namespace, vs.Name)
	err := client.Delete(context.TODO(), &vs)
	if err != nil {
		logger.Errorf("fail to delete VolumeSnapshot %s/%s: %s",
			vs.Namespace, vs.Name, err.Error())
	}
}

// recreateVolumeSnapshotContent will delete then re-create VolumeSnapshotContent,
// because some parameter in VolumeSnapshotContent Spec is immutable,
// e.g. VolumeSnapshotRef and Source.
// Source is updated to let csi-controller thinks the VSC is statically
// provisioned with VS.
// Set VolumeSnapshotRef's UID to nil will let the csi-controller finds out
// the related VS is gone, then VSC can be deleted.
func recreateVolumeSnapshotContent(
	vsc snapshotv1api.VolumeSnapshotContent,
	backup *velerov1api.Backup,
	client crclient.Client,
	log logrus.FieldLogger,
) error {
	// Read resource timeout from backup annotation, if not set, use default value.
	timeout, err := time.ParseDuration(
		backup.Annotations[velerov1api.ResourceTimeoutAnnotation])
	if err != nil {
		log.Warnf("fail to parse resource timeout annotation %s: %s",
			backup.Annotations[velerov1api.ResourceTimeoutAnnotation], err.Error())
		timeout = 10 * time.Minute
	}
	log.Debugf("resource timeout is set to %s", timeout.String())
	interval := 1 * time.Second

	if err := client.Delete(context.TODO(), &vsc); err != nil {
		return errors.Wrapf(err, "fail to delete VolumeSnapshotContent: %s", vsc.Name)
	}

	// Check VolumeSnapshotContents is already deleted, before re-creating it.
	err = wait.PollUntilContextTimeout(
		context.Background(),
		interval,
		timeout,
		true,
		func(ctx context.Context) (bool, error) {
			tmpVSC := new(snapshotv1api.VolumeSnapshotContent)
			if err := client.Get(ctx, crclient.ObjectKeyFromObject(&vsc), tmpVSC); err != nil {
				if apierrors.IsNotFound(err) {
					return true, nil
				}
				return false, errors.Wrapf(
					err,
					fmt.Sprintf("failed to get VolumeSnapshotContent %s", vsc.Name),
				)
			}
			return false, nil
		},
	)
	if err != nil {
		return errors.Wrapf(err, "fail to retrieve VolumeSnapshotContent %s info", vsc.Name)
	}

	// Make the VolumeSnapshotContent static
	vsc.Spec.Source = snapshotv1api.VolumeSnapshotContentSource{
		SnapshotHandle: vsc.Status.SnapshotHandle,
	}
	// Set VolumeSnapshotRef to none exist one, because VolumeSnapshotContent
	// validation webhook will check whether name and namespace are nil.
	// external-snapshotter needs Source pointing to snapshot and VolumeSnapshot
	// reference's UID to nil to determine the VolumeSnapshotContent is deletable.
	vsc.Spec.VolumeSnapshotRef = corev1api.ObjectReference{
		APIVersion: snapshotv1api.SchemeGroupVersion.String(),
		Kind:       "VolumeSnapshot",
		Namespace:  "ns-" + string(vsc.UID),
		Name:       "name-" + string(vsc.UID),
	}
	// ResourceVersion shouldn't exist for new creation.
	vsc.ResourceVersion = ""
	if err := client.Create(context.TODO(), &vsc); err != nil {
		return errors.Wrapf(err, "fail to create VolumeSnapshotContent %s", vsc.Name)
	}

	return nil
}

// snapshotProgressHeartbeat periodically relays the current snapshot state to
// the kubeagent (via report) until ctx is cancelled. It fires on a fixed
// cadence even when the state has not changed, so the kubeagent keeps
// refreshing the job's TTL and a healthy long-running snapshot is not evicted
// at the job timeout. State that has not yet been observed (empty) is skipped,
// since report would have nothing meaningful to send.
func snapshotProgressHeartbeat(
	ctx context.Context,
	interval time.Duration,
	getState func() (state, message string),
	report func(state, message string) error,
	log logrus.FieldLogger,
) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			state, message := getState()
			if state == "" {
				// Nothing observed yet; report would no-op.
				continue
			}
			if err := report(state, message); err != nil {
				log.WithError(err).Warn("Failed to send snapshot progress heartbeat")
			} else {
				log.Infof("Sent snapshot progress heartbeat: state=%s", state)
			}
		}
	}
}

// WaitUntilVSCHandleIsReady returns the VolumeSnapshotContent object associated
// with the VolumeSnapshot. Instead of polling on a fixed interval, it uses
// watchtools.UntilWithSync for both the VS-binding phase and the VSC
// SnapshotHandle-ready phase, so it reacts immediately to reconciliation events.
// The full csiSnapshotTimeout budget is shared across both phases via a single
// context deadline.
func WaitUntilVSCHandleIsReady(
	volSnap *snapshotv1api.VolumeSnapshot,
	snapshotClient snapshotter.SnapshotV1Interface, // NEW parameter — see call-site instructions
	crClient crclient.Client,
	log logrus.FieldLogger,
	shouldWait bool,
	csiSnapshotTimeout time.Duration,
) (*snapshotv1api.VolumeSnapshotContent, error) {
	log.Infof("Start WaitUntilVSCHandleIsReady: VolSnap=%s/%s, ShouldWait=%v, Timeout=%v",
		volSnap.Namespace, volSnap.Name, shouldWait, csiSnapshotTimeout)

	// Fast-path: caller does not want to wait; return whatever is already bound.
	if !shouldWait {
		log.Info("ShouldWait=false; attempting direct fetch of VSC if bound")
		if volSnap.Status == nil || volSnap.Status.BoundVolumeSnapshotContentName == nil {
			return nil, nil
		}
		vsc := new(snapshotv1api.VolumeSnapshotContent)
		if err := crClient.Get(context.TODO(),
			crclient.ObjectKey{Name: *volSnap.Status.BoundVolumeSnapshotContentName}, vsc); err != nil {
			log.WithError(err).Errorf("Direct fetch failed for VSC %s",
				*volSnap.Status.BoundVolumeSnapshotContentName)
			return nil, errors.Wrap(err, "error getting VSC")
		}
		log.Infof("Fetched VSC %s successfully", vsc.Name)
		return vsc, nil
	}

	// Validate the backup label that is required by the progress-reporting defer below.
	jobID, ok := volSnap.Labels["velero.io/backup-name"]
	if !ok {
		err := errors.New("missing required label 'velero.io/backup-name' on VolumeSnapshot")
		log.Error(err)
		return nil, err
	}

	// snapshotState / snapshotStateMessage are written throughout the two watch
	// phases (from the watch callbacks) and read both by the periodic heartbeat
	// goroutine below and by the deferred progress-reporter. The mutex guards
	// against the resulting concurrent access.
	var (
		stateMu              sync.Mutex
		snapshotState        string
		snapshotStateMessage string
	)
	setSnapshotState := func(state, message string) {
		stateMu.Lock()
		snapshotState, snapshotStateMessage = state, message
		stateMu.Unlock()
	}
	getSnapshotState := func() (string, string) {
		stateMu.Lock()
		defer stateMu.Unlock()
		return snapshotState, snapshotStateMessage
	}

	// Heartbeat: while the two watch phases block waiting for CSI reconciliation,
	// the watch callbacks fire only on VS/VSC events, which may be minutes apart
	// on a large-volume snapshot. Relay the current snapshot state to the
	// kubeagent on a fixed cadence so that it refreshes the job's TTL. The update
	// is sent every interval even when the state is unchanged — the point is to
	// prove liveness, not to report a transition — so a snapshot that legitimately
	// runs past the 60-minute job timeout is not evicted.
	heartbeatCtx, stopHeartbeat := context.WithCancel(context.Background())
	var heartbeatWG sync.WaitGroup
	heartbeatWG.Add(1)
	go func() {
		defer heartbeatWG.Done()
		snapshotProgressHeartbeat(
			heartbeatCtx,
			snapshotProgressHeartbeatInterval,
			getSnapshotState,
			func(state, message string) error {
				return catalogic.UpdateSnapshotProgress(nil, volSnap, nil, state, message, jobID, log)
			},
			log,
		)
	}()

	defer func() {
		// Stop the heartbeat before writing the final status so the two cannot race
		// on the same ConfigMap.
		stopHeartbeat()
		heartbeatWG.Wait()

		state, message := getSnapshotState()
		uErr := catalogic.UpdateSnapshotProgress(nil, volSnap, nil, state, message, jobID, log)
		if uErr != nil {
			log.WithError(uErr).Error("Failed to update snapshot progress")
		}
		time.Sleep(500 * time.Millisecond)
		catalogic.DeleteSnapshotProgressConfigMap(jobID, log)
	}()

	// The plugin ConfigMap only carries an OPTIONAL CSI-snapshot-timeout override.
	// It is routinely absent by the time this runs (e.g. the kubeagent removes it
	// once the backup job finishes, around the finalize phase). Treat a missing or
	// unreadable ConfigMap as non-fatal and fall back to the timeout the caller
	// passed in (backup.Spec.CSISnapshotTimeout). Returning an error here would send
	// the caller (VolumeSnapshotBackupItemAction) down the destructive
	// CleanupVolumeSnapshot path, which flips the VSC DeletionPolicy to Delete and
	// cascade-deletes the underlying storage snapshot a restore may still need.
	config, err := catalogic.GetPluginConfig(jobID, log)
	if err != nil {
		log.WithError(err).Warnf(
			"Failed to get plugin config; falling back to caller-provided CSI snapshot timeout %v",
			csiSnapshotTimeout)
	} else {
		log.Infof("Plugin config: %+v", config)
		if config.CsiSnapshotTimeout > 0 {
			csiSnapshotTimeout = time.Duration(config.CsiSnapshotTimeout) * time.Minute
			log.Infof("Using configured CSI snapshot Timeout=%v", csiSnapshotTimeout)
		}
	}

	// A single deadline context spans both phases. If Phase 1 consumes 3 minutes,
	// Phase 2 has only the remaining 7 minutes — which is the correct behaviour.
	ctx, cancel := context.WithTimeout(context.Background(), csiSnapshotTimeout)
	defer cancel()

	// -------------------------------------------------------------------------
	// Phase 1 — Watch the VolumeSnapshot until BoundVolumeSnapshotContentName
	// is populated.
	//
	// watchtools.UntilWithSync does an initial List before starting the Watch,
	// so if the VS is already bound the condition fires with zero wait time.
	// Auto-reconnects on dropped watch connections are handled internally by
	// the Reflector that backs UntilWithSync.
	// -------------------------------------------------------------------------
	log.Infof("Phase1: Watching VolumeSnapshot %s/%s for VSC binding, Timeout=%v",
		volSnap.Namespace, volSnap.Name, csiSnapshotTimeout)

	vsFieldSelector := fields.OneTermEqualSelector("metadata.name", volSnap.Name).String()
	vsListWatch := &cache.ListWatch{
		ListFunc: func(opts metav1.ListOptions) (runtime.Object, error) {
			opts.FieldSelector = vsFieldSelector
			return snapshotClient.VolumeSnapshots(volSnap.Namespace).List(ctx, opts)
		},
		WatchFunc: func(opts metav1.ListOptions) (k8swatch.Interface, error) {
			opts.FieldSelector = vsFieldSelector
			return snapshotClient.VolumeSnapshots(volSnap.Namespace).Watch(ctx, opts)
		},
	}

	var boundVSCName string
	var latestVS *snapshotv1api.VolumeSnapshot

	if _, watchErr := watchtools.UntilWithSync(
		ctx,
		vsListWatch,
		&snapshotv1api.VolumeSnapshot{},
		nil, // no precondition; the condition func below is sufficient
		func(event k8swatch.Event) (bool, error) {
			switch event.Type {
			case k8swatch.Deleted:
				return false, fmt.Errorf(
					"VolumeSnapshot %s/%s was deleted while waiting for VSC binding",
					volSnap.Namespace, volSnap.Name)
			case k8swatch.Added, k8swatch.Modified:
				vs, ok := event.Object.(*snapshotv1api.VolumeSnapshot)
				if !ok {
					return false, nil
				}
				if vs.Status == nil || vs.Status.BoundVolumeSnapshotContentName == nil {
					msg := fmt.Sprintf(
						"Awaiting VolumeSnapshot reconciliation: %s/%s",
						volSnap.Namespace, volSnap.Name)
					setSnapshotState("pending", msg)
					log.Info(msg)
					return false, nil
				}
				boundVSCName = *vs.Status.BoundVolumeSnapshotContentName
				latestVS = vs
				log.Infof("VolumeSnapshot %s/%s bound to VSC %s",
					volSnap.Namespace, volSnap.Name, boundVSCName)
				return true, nil
			}
			return false, nil
		},
	); watchErr != nil {
		log.WithError(watchErr).Errorf(
			"Phase1 watch failed for VolumeSnapshot %s/%s", volSnap.Namespace, volSnap.Name)
		return nil, errors.Wrapf(watchErr,
			"timed out or failed watching VolumeSnapshot %s/%s for VSC binding",
			volSnap.Namespace, volSnap.Name)
	}
	// Update the caller's VolumeSnapshot with the live version from the cluster
	// so that Status (including BoundVolumeSnapshotContentName) is available to
	// downstream callers such as DeleteVolumeSnapshot.
	if latestVS != nil {
		volSnap.Status = latestVS.Status
	}
	// -------------------------------------------------------------------------
	// Interlude — Apply cc-pvc-name / cc-pvc-namespace annotations to the VSC.
	//
	// In the old implementation this happened inside the poll loop and was
	// therefore attempted on every tick.  Here we do it exactly once, after
	// Phase 1 has confirmed the binding, and before Phase 2 starts watching.
	// -------------------------------------------------------------------------
	vsc := new(snapshotv1api.VolumeSnapshotContent)
	if annotationErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if err := crClient.Get(ctx, crclient.ObjectKey{Name: boundVSCName}, vsc); err != nil {
			return err
		}
		if vsc.Annotations == nil {
			vsc.Annotations = make(map[string]string)
		}
		updated := false
		if _, exists := vsc.Annotations["cc-pvc-name"]; !exists &&
			latestVS.Spec.Source.PersistentVolumeClaimName != nil {
			vsc.Annotations["cc-pvc-name"] = *latestVS.Spec.Source.PersistentVolumeClaimName
			updated = true
		}
		if _, exists := vsc.Annotations["cc-pvc-namespace"]; !exists {
			vsc.Annotations["cc-pvc-namespace"] = latestVS.Namespace
			updated = true
		}
		if updated {
			if err := crClient.Update(ctx, vsc); err != nil {
				log.WithError(err).Warnf(
					"Failed VSC annotation update for %s; retrying", boundVSCName)
				return err
			}
			log.Infof("Updated PVC annotations on VSC %s", boundVSCName)
		}
		return nil
	}); annotationErr != nil {
		log.WithError(annotationErr).Errorf(
			"Failed to update annotations on VSC %s", boundVSCName)
		return nil, errors.Wrapf(annotationErr, "failed to annotate VSC %s", boundVSCName)
	}

	// -------------------------------------------------------------------------
	// Phase 2 — Watch the VolumeSnapshotContent until Status.SnapshotHandle is
	// non-nil.  The same context (and therefore the same deadline) is reused,
	// so the two phases share the total timeout budget.
	// -------------------------------------------------------------------------
	log.Infof("Phase2: Watching VSC %s for SnapshotHandle readiness", boundVSCName)

	vscFieldSelector := fields.OneTermEqualSelector("metadata.name", boundVSCName).String()
	vscListWatch := &cache.ListWatch{
		ListFunc: func(opts metav1.ListOptions) (runtime.Object, error) {
			opts.FieldSelector = vscFieldSelector
			return snapshotClient.VolumeSnapshotContents().List(ctx, opts)
		},
		WatchFunc: func(opts metav1.ListOptions) (k8swatch.Interface, error) {
			opts.FieldSelector = vscFieldSelector
			return snapshotClient.VolumeSnapshotContents().Watch(ctx, opts)
		},
	}

	if _, watchErr := watchtools.UntilWithSync(
		ctx,
		vscListWatch,
		&snapshotv1api.VolumeSnapshotContent{},
		nil,
		func(event k8swatch.Event) (bool, error) {
			switch event.Type {
			case k8swatch.Deleted:
				return false, fmt.Errorf(
					"VSC %s was deleted while waiting for SnapshotHandle", boundVSCName)
			case k8swatch.Added, k8swatch.Modified:
				updatedVSC, ok := event.Object.(*snapshotv1api.VolumeSnapshotContent)
				if !ok {
					return false, nil
				}
				if updatedVSC.Status == nil || updatedVSC.Status.SnapshotHandle == nil {
					msg := fmt.Sprintf(
						"VSC %s lacks SnapshotHandle", boundVSCName)
					setSnapshotState("pending", msg)
					log.Info(msg)
					if updatedVSC.Status != nil && updatedVSC.Status.Error != nil {
						log.Infof("VSC %s has error: %v",
							boundVSCName, *updatedVSC.Status.Error.Message)
					}
					return false, nil
				}
				// Capture the final, fully-populated VSC for the caller.
				vsc = updatedVSC
				log.Infof("VSC %s is ready with SnapshotHandle", boundVSCName)
				return true, nil
			}
			return false, nil
		},
	); watchErr != nil {
		log.WithError(watchErr).Errorf("Phase2 watch failed for VSC %s", boundVSCName)
		if vsc.Status != nil && vsc.Status.Error != nil {
			return nil, fmt.Errorf("CSI reconciliation timeout for VSC %s: %v",
				boundVSCName, *vsc.Status.Error.Message)
		}
		return nil, errors.Wrapf(watchErr,
			"timed out or failed watching VSC %s for SnapshotHandle", boundVSCName)
	}

	if vsc.Status != nil && vsc.Status.ReadyToUse != nil && *vsc.Status.ReadyToUse {
		setSnapshotState("completed", "CSI snapshot complete")
	} else {
		setSnapshotState("pending", fmt.Sprintf("VSC %s not ReadyToUse", vsc.Name))
	}

	log.Infof("Completed WaitUntilVSCHandleIsReady for %s/%s, VSC=%s",
		volSnap.Namespace, volSnap.Name, vsc.Name)
	return vsc, nil
}

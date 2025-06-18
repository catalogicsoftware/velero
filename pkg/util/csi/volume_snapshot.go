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
	// If a snapshot class is set for provider in PVC annotations, use that
	snapshotClass, err := GetVolumeSnapshotClassFromPVCAnnotationsForDriver(
		pvc, provisioner, snapshotClasses,
	)
	if err != nil {
		log.Debugf("Didn't find VolumeSnapshotClass from PVC annotations: %v", err)
	}
	if snapshotClass != nil {
		return snapshotClass, nil
	}

	// If there is no annotation in PVC, attempt to fetch it from backup annotations
	snapshotClass, err = GetVolumeSnapshotClassFromBackupAnnotationsForDriver(
		backup, provisioner, snapshotClasses)
	if err != nil {
		log.Debugf("Didn't find VolumeSnapshotClass from Backup annotations: %v", err)
	}
	if snapshotClass != nil {
		return snapshotClass, nil
	}

	// fallback to default behavior of fetching snapshot class based on label
	snapshotClass, err = GetVolumeSnapshotClassForStorageClass(
		provisioner, snapshotClasses)
	if err != nil || snapshotClass == nil {
		return nil, errors.Wrap(err, "error getting VolumeSnapshotClass")
	}

	return snapshotClass, nil
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
func GetVolumeSnapshotClassForStorageClass(
	provisioner string,
	snapshotClasses *snapshotv1api.VolumeSnapshotClassList,
) (*snapshotv1api.VolumeSnapshotClass, error) {
	n := 0
	var vsClass snapshotv1api.VolumeSnapshotClass
	// We pick the VolumeSnapshotClass that matches the CSI driver name
	// and has a 'velero.io/csi-volumesnapshot-class' label. This allows
	// multiple VolumeSnapshotClasses for the same driver with different
	// values for the other fields in the spec.
	for _, sc := range snapshotClasses.Items {
		_, hasCloudcasaLabelSelector := sc.Labels[velerov1api.CloudcasaVolumeSnapshotClassSelectorLabel]
		if sc.Driver == provisioner && hasCloudcasaLabelSelector {
			return &sc, nil
		}
	}
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
	// If there's only one volumesnapshotclass for the driver, return it.
	if n == 1 {
		return &vsClass, nil
	}
	return nil, fmt.Errorf(
		`failed to get VolumeSnapshotClass for provisioner %s, 
		ensure that the desired VolumeSnapshot class has the %s label`,
		provisioner, velerov1api.VolumeSnapshotClassSelectorLabel)
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
	modifyVSCFlag := false
	if vs.Status != nil &&
		vs.Status.BoundVolumeSnapshotContentName != nil &&
		len(*vs.Status.BoundVolumeSnapshotContentName) > 0 &&
		vsc.Spec.DeletionPolicy == snapshotv1api.VolumeSnapshotContentDelete {
		modifyVSCFlag = true
	} else {
		logger.Errorf("VolumeSnapshot %s/%s is not ready. This is not expected.",
			vs.Namespace, vs.Name)
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

// WaitUntilVSCHandleIsReady returns the VolumeSnapshotContent
// object associated with the volumesnapshot
func WaitUntilVSCHandleIsReady(
	volSnap *snapshotv1api.VolumeSnapshot,
	crClient crclient.Client,
	log logrus.FieldLogger,
	shouldWait bool,
	csiSnapshotTimeout time.Duration,
) (*snapshotv1api.VolumeSnapshotContent, error) {
	log.Infof("Start WaitUntilVSCHandleIsReady: VolSnap=%s/%s, shouldWait=%v, timeout=%v",
		volSnap.Namespace, volSnap.Name, shouldWait, csiSnapshotTimeout)

	if !shouldWait {
		log.Info("shouldWait=false; attempting direct fetch of VSC if bound")
		if volSnap.Status == nil || volSnap.Status.BoundVolumeSnapshotContentName == nil {
			// volumesnapshot hasn't been reconciled and we're not waiting for it.
			return nil, nil
		}
		vsc := new(snapshotv1api.VolumeSnapshotContent)
		err := crClient.Get(context.TODO(),
			crclient.ObjectKey{Name: *volSnap.Status.BoundVolumeSnapshotContentName}, vsc)
		if err != nil {
			log.Error(err, fmt.Sprintf("Direct fetch failed for VSC %s", *volSnap.Status.BoundVolumeSnapshotContentName))
			return nil, errors.Wrap(err, "error getting VSC")
		}
		log.Infof("Fetched VSC %s successfully", vsc.Name)
		return vsc, nil
	}

	// We'll wait 10m for the VSC to be reconciled polling
	// every 5s unless backup's csiSnapshotTimeout is set
	interval := 5 * time.Second
	vsc := new(snapshotv1api.VolumeSnapshotContent)
	var snapshotState, snapshotStateMessage string
	jobID, ok := volSnap.Labels["velero.io/backup-name"]
	if !ok {
		err := errors.New("missing required label 'velero.io/backup-name' on VolumeSnapshot")
		log.Error(err)
		return nil, err
	}

	defer func() {
		uErr := catalogic.UpdateSnapshotProgress(nil, volSnap, nil, snapshotState, snapshotStateMessage, jobID, log)
		if uErr != nil {
			log.WithError(uErr).Error("Failed to update snapshot progress")
		}
		catalogic.DeleteSnapshotProgressConfigMap(jobID, log)
	}()

	config, pollErr := catalogic.GetPluginConfig(jobID, log)
	if pollErr != nil {
		log.WithError(pollErr).Error("Failed to get plugin config")
		return nil, errors.Wrap(pollErr, "error getting plugin config")
	}
	log.Infof("Plugin config: %+v", config)
	if config.CsiSnapshotTimeout > 0 {
		csiSnapshotTimeout = time.Duration(config.CsiSnapshotTimeout) * time.Minute
		log.Infof("Using configured timeout %v", csiSnapshotTimeout)
	}

	log.Infof("Polling up to %v for CSI reconciliation of %s/%s", csiSnapshotTimeout, volSnap.Namespace, volSnap.Name)
	pollErr = wait.PollUntilContextTimeout(context.Background(), interval, csiSnapshotTimeout, true,
		func(ctx context.Context) (bool, error) {
			var vs snapshotv1api.VolumeSnapshot
			var getVsErr error
			for attempt := 1; attempt <= 3; attempt++ {
				getVsErr = crClient.Get(ctx, crclient.ObjectKeyFromObject(volSnap), &vs)
				if getVsErr == nil || !apierrors.IsNotFound(getVsErr) {
					break
				}
				time.Sleep(time.Duration(attempt) * 500 * time.Millisecond)
			}
			if getVsErr != nil {
				log.WithError(getVsErr).Errorf("Failed to fetch VolumeSnapshot %s/%s", volSnap.Namespace, volSnap.Name)
				return false, getVsErr
			}
			log.Infof("VolSnap %+v", vs)

			if vs.Status == nil || vs.Status.BoundVolumeSnapshotContentName == nil {
				snapshotState, snapshotStateMessage = "pending", fmt.Sprintf("Awaiting VolumeSnapshot reconciliation: %s/%s", volSnap.Namespace, volSnap.Name)
				log.Info(snapshotStateMessage)
				return false, nil
			}

			vscName := *vs.Status.BoundVolumeSnapshotContentName

			retryErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
				var getErr error
				for attempt := 1; attempt <= 3; attempt++ {
					getErr = crClient.Get(ctx, crclient.ObjectKey{Name: vscName}, vsc)
					if getErr == nil || !apierrors.IsNotFound(getErr) {
						break
					}
					time.Sleep(time.Duration(attempt) * 500 * time.Millisecond)
				}
				if getErr != nil {
					return getErr
				}

				if vsc.Annotations == nil {
					vsc.Annotations = make(map[string]string)
				}
				updated := false
				if _, ok := vsc.Annotations["cc-pvc-name"]; !ok && vs.Spec.Source.PersistentVolumeClaimName != nil {
					vsc.Annotations["cc-pvc-name"] = *vs.Spec.Source.PersistentVolumeClaimName
					updated = true
				}
				if _, ok := vsc.Annotations["cc-pvc-namespace"]; !ok {
					vsc.Annotations["cc-pvc-namespace"] = vs.Namespace
					updated = true
				}
				if updated {
					if err := crClient.Update(ctx, vsc); err != nil {
						log.WithError(err).Warnf("Failed VSC annotation update for %s; retrying", vscName)
						return err // retry.RetryOnConflict will handle the retry
					}
					log.Infof("Updated annotations on VSC %s", vscName)
				}
				return nil
			})

			if retryErr != nil {
				log.WithError(retryErr).Errorf("Failed to retry update on VSC %s", vscName)
				return false, retryErr
			}

			// If its not present, then, update the volumesnapshotcontent object with this information

			// we need to wait for the VolumeSnapshotContent
			// to have a snapshot handle because during restore,
			// we'll use that snapshot handle as the source for
			// the VolumeSnapshotContent so it's statically
			// bound to the existing snapshot.
			if vsc.Status == nil || vsc.Status.SnapshotHandle == nil {
				snapshotState, snapshotStateMessage = "pending", fmt.Sprintf("VSC %s lacks snapshot handle", vscName)
				log.Info(snapshotStateMessage)
				if vsc.Status != nil && vsc.Status.Error != nil {
					log.Infof("VSC %s has error: %v", vscName, *vsc.Status.Error.Message)
				}
				return false, nil
			}

			log.Infof("VSC %s is ready with snapshot handle", vscName)
			return true, nil
		})

	if pollErr != nil {
		log.WithError(pollErr).Error("Reconciliation polling failed")
		if wait.Interrupted(pollErr) && vsc.Status != nil && vsc.Status.Error != nil {
			return nil, fmt.Errorf("CSI reconciliation timeout: %v", *vsc.Status.Error.Message)
		}
		return nil, pollErr
	}

	if vsc.Status != nil && vsc.Status.ReadyToUse != nil && *vsc.Status.ReadyToUse {
		snapshotState, snapshotStateMessage = "completed", "CSI snapshot complete"
	} else {
		snapshotState, snapshotStateMessage = "pending", fmt.Sprintf("VSC %s not ReadyToUse", vsc.Name)
	}
	log.Infof("Completed WaitUntilVSCHandleIsReady for %s/%s", volSnap.Namespace, volSnap.Name)
	return vsc, nil
}

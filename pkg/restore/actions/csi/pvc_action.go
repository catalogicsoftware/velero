/*
Copyright the Velero contributors.

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
	"strconv"
	"strings"
	"time"

	snapshotv1api "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"
	corev1api "k8s.io/api/core/v1"
	storagev1api "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	utilrand "k8s.io/apimachinery/pkg/util/rand"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/vmware-tanzu/velero/internal/catalogic"
	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	velerov2alpha1 "github.com/vmware-tanzu/velero/pkg/apis/velero/v2alpha1"
	"github.com/vmware-tanzu/velero/pkg/client"
	kuberesource "github.com/vmware-tanzu/velero/pkg/kuberesource"
	"github.com/vmware-tanzu/velero/pkg/label"
	plugincommon "github.com/vmware-tanzu/velero/pkg/plugin/framework/common"
	"github.com/vmware-tanzu/velero/pkg/plugin/velero"
	riav2 "github.com/vmware-tanzu/velero/pkg/plugin/velero/restoreitemaction/v2"
	uploaderUtil "github.com/vmware-tanzu/velero/pkg/uploader/util"
	"github.com/vmware-tanzu/velero/pkg/util"
	"github.com/vmware-tanzu/velero/pkg/util/boolptr"
	kubeutil "github.com/vmware-tanzu/velero/pkg/util/kube"
)

const (
	AnnSelectedNode          = "volume.kubernetes.io/selected-node"
	GenerateNameRandomLength = 5
	HarvesterhCiIoOwnedBy    = "harvesterhci.io/owned-by"
	pvcDeleteTimeoutSeconds  = 2 * 60

	DefaultAzureFilesSleepDuration           = 10 * time.Second
	CCAzureFilesPvcRestoreWaitTimeAnnotation = "cloudcasa-azure-files-pvc-restore-wait-sec"
	azureFilesRestoreAnnotation              = "cloudcasa-restore-from-azure-files-snapshot"
	azureFilesDriver                         = "file.csi.azure.com"
)

// azureFilesSleep spaces out Azure Files PVC restores; tests replace it.
var azureFilesSleep = time.Sleep

// restoredByAzureFilesMover reports whether the CloudCasa Azure Files mover,
// rather than a CSI snapshot restore, restores a volume of this CSI driver.
func restoredByAzureFilesMover(restore *velerov1api.Restore, driver string) bool {
	_, mover := restore.Annotations[azureFilesRestoreAnnotation]
	return mover && driver == azureFilesDriver
}

// updateSnapshotProgress and deleteSnapshotProgressConfigMap relay restore
// progress to kubeagent; tests replace them.
var (
	updateSnapshotProgress          = catalogic.UpdateSnapshotProgress
	deleteSnapshotProgressConfigMap = catalogic.DeleteSnapshotProgressConfigMap
)

// pvcRestoreItemAction is a restore item action plugin for Velero
type pvcRestoreItemAction struct {
	log      logrus.FieldLogger
	crClient crclient.Client
}

// AppliesTo returns information indicating that the
// PVCRestoreItemAction should be run while restoring PVCs.
func (p *pvcRestoreItemAction) AppliesTo() (velero.ResourceSelector, error) {
	return velero.ResourceSelector{
		IncludedResources: []string{"persistentvolumeclaims"},
		//TODO: add label selector volumeSnapshotLabel
	}, nil
}

// Execute modifies the PVC's spec to use the VolumeSnapshot object as the
// data source ensuring that the newly provisioned volume can be pre-populated
// with data from the VolumeSnapshot.
func (p *pvcRestoreItemAction) Execute(
	input *velero.RestoreItemActionExecuteInput,
) (*velero.RestoreItemActionExecuteOutput, error) {
	var pvc, pvcFromBackup corev1api.PersistentVolumeClaim
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(
		input.Item.UnstructuredContent(), &pvc); err != nil {
		return nil, errors.WithStack(err)
	}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(
		input.ItemFromBackup.UnstructuredContent(), &pvcFromBackup); err != nil {
		return nil, errors.WithStack(err)
	}

	logger := p.log.WithFields(logrus.Fields{
		"Action":  "PVCRestoreItemAction",
		"PVC":     pvc.Namespace + "/" + pvc.Name,
		"Restore": input.Restore.Namespace + "/" + input.Restore.Name,
	})
	logger.Info("Starting PVCRestoreItemAction for PVC")

	if err := p.handleExistingPvc(input, &pvc); err != nil {
		logger.Warnf("Failed to handle existing PVC: %v", err)
	}

	// If PVC already exists, returns early.
	if exists, _ := p.isResourceExist(pvc, *input.Restore); exists {
		logger.Warnf("PVC already exists. Skip restore this PVC.")
		return &velero.RestoreItemActionExecuteOutput{
			UpdatedItem: input.Item,
		}, nil
	}

	delete(pvc.Annotations, HarvesterhCiIoOwnedBy)

	// If cross-namespace restore is configured, change the namespace
	// for PVC object to be restored
	newNamespace, ok := input.Restore.Spec.NamespaceMapping[pvc.GetNamespace()]
	if !ok {
		// Use original namespace
		newNamespace = pvc.Namespace
	}

	operationID := ""

	additionalItems := []velero.ResourceIdentifier{}
	if boolptr.IsSetToFalse(input.Restore.Spec.RestorePVs) {
		logger.Info("Restore did not request for PVs to be restored from snapshot")

		// If DR volumes are used, then do not reset the volume name.
		useDrVolumes := false
		if useDrVolumesStr, ok := input.Restore.Annotations["cloudcasa-use-dr-volumes"]; ok {
			var err error
			useDrVolumes, err = strconv.ParseBool(useDrVolumesStr)
			if err != nil {
				p.log.Errorf("Failed to convert \"cloudcasa-use-dr-volumes\" annotation to boolean: %v", err)
				return nil, errors.WithStack(err)
			}
		}

		if !useDrVolumes {
			pvc.Spec.VolumeName = ""
		}
		pvc.Spec.DataSource = nil
		pvc.Spec.DataSourceRef = nil
	} else {
		backup := new(velerov1api.Backup)
		err := p.crClient.Get(
			context.TODO(),
			crclient.ObjectKey{
				Namespace: input.Restore.Namespace,
				Name:      input.Restore.Spec.BackupName,
			},
			backup,
		)

		if err != nil {
			logger.Error("Fail to get backup for restore.")
			return nil, fmt.Errorf("fail to get backup for restore: %s", err.Error())
		}

		if boolptr.IsSetToTrue(backup.Spec.SnapshotMoveData) {
			logger.Info("Start DataMover restore.")

			// If PVC doesn't have a DataUploadNameLabel, which should be created
			// during backup, then CSI cannot handle the volume during to restore,
			// so return early to let Velero tries to fall back to Velero native snapshot.
			if _, ok := pvcFromBackup.Annotations[velerov1api.DataUploadNameAnnotation]; !ok {
				logger.Warnf("PVC doesn't have a DataUpload for data mover. Return.")
				return &velero.RestoreItemActionExecuteOutput{
					UpdatedItem: input.Item,
				}, nil
			}

			operationID = label.GetValidName(
				string(velerov1api.AsyncOperationIDPrefixDataDownload) +
					string(input.Restore.UID) + "." + string(pvcFromBackup.UID))
			dataDownload, err := restoreFromDataUploadResult(
				context.Background(), input.Restore, backup, &pvc, newNamespace,
				operationID, p.crClient)
			if err != nil {
				logger.Errorf("Fail to restore from DataUploadResult: %s", err.Error())
				return nil, errors.WithStack(err)
			}
			logger.Infof("DataDownload %s/%s is created successfully.",
				dataDownload.Namespace, dataDownload.Name)
		} else {
			//CSI restore
			vsName, nameOK := pvcFromBackup.Annotations[velerov1api.VolumeSnapshotLabel]
			if !nameOK {
				logger.Info("Skipping PVCRestoreItemAction for PVC, PVC does not have a CSI VolumeSnapshot.")
				return &velero.RestoreItemActionExecuteOutput{
					UpdatedItem: input.Item,
				}, nil
			}

			defer func() {
				time.Sleep(200 * time.Millisecond) // give some time for the update to be processed
				deleteSnapshotProgressConfigMap(input.Restore.Name, logger)
			}()

			azureFiles, err := p.restoreFromVolumeSnapshot(&pvc, newNamespace, vsName, input.Restore, logger)
			if err != nil {
				logger.Errorf("Failed to restore PVC from VolumeSnapshot.")
				relayRestoreProgress(&pvc, "error",
					fmt.Sprintf("failed to restore PVC %s/%s from the CSI snapshot", pvc.Name, pvc.Namespace),
					input.Restore.Name, logger)
				return nil, errors.WithStack(err)
			}
			relayRestoreProgress(&pvc, "completed",
				fmt.Sprintf("Successfully created PVC %s/%s from the CSI snapshot", pvc.Name, pvc.Namespace),
				input.Restore.Name, logger)

			// The Azure Files mover restores without the VolumeSnapshot.
			if !azureFiles {
				additionalItems = append(additionalItems, velero.ResourceIdentifier{
					GroupResource: kuberesource.VolumeSnapshots,
					Name:          vsName,
					Namespace:     pvc.Namespace,
				})

				// Force-restore the VolumeSnapshot even when restore resource filters
				// would otherwise exclude it (mirrors backup-side must-include).
				annotations := pvc.GetAnnotations()
				if annotations == nil {
					annotations = map[string]string{}
				}
				annotations[velerov1api.MustIncludeAdditionalItemRestoreAnnotation] = "true"
				pvc.SetAnnotations(annotations)
			}
		}
	}

	pvcMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&pvc)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	logger.Info("Returning from PVCRestoreItemAction for PVC")

	return &velero.RestoreItemActionExecuteOutput{
		UpdatedItem:     &unstructured.Unstructured{Object: pvcMap},
		OperationID:     operationID,
		AdditionalItems: additionalItems,
	}, nil
}

// restoreFromVolumeSnapshot points the PVC at its VolumeSnapshot as restored,
// or prepares it for the Azure Files mover, which it then reports. Backups from
// older engines carry the snapshot size only on the VolumeSnapshot, so the PVC
// request is raised to it when that is restored.
func (p *pvcRestoreItemAction) restoreFromVolumeSnapshot(
	pvc *corev1api.PersistentVolumeClaim,
	newNamespace, vsName string,
	restore *velerov1api.Restore,
	logger logrus.FieldLogger,
) (bool, error) {
	// Get CSI Driver name
	var csiDriverName string
	if storageClassName := pvc.Spec.StorageClassName; storageClassName != nil {
		storageClass := new(storagev1api.StorageClass)
		if err := p.crClient.Get(context.TODO(), crclient.ObjectKey{Name: *storageClassName}, storageClass); err != nil {
			return false, errors.Wrapf(err, "Failed to get StorageClass %s to check PVC %s/%s provisioner",
				*storageClassName, pvc.Namespace, pvc.Name)
		}
		csiDriverName = storageClass.Provisioner
		logger.Infof("Found StorageClass %s for PVC %s/%s", csiDriverName, pvc.Namespace, pvc.Name)
	} else {
		logger.Infof("StorageClass is not set for PVC %s/%s", pvc.Namespace, pvc.Name)
	}

	if restoredByAzureFilesMover(restore, csiDriverName) {
		logger.Infof("Processing Azure Files Restore using CloudCasa method. PVC %s/%s. CSI Driver: %s",
			pvc.Namespace, pvc.Name, csiDriverName)
		return true, p.processAzureRestore(pvc, vsName, restore, logger)
	}

	//To avoid confilcs, vs and vsc get a new uniq name based in restore UID
	// and vs name old name
	newVSName := util.GenerateSha256FromRestoreUIDAndVsName(string(restore.UID), vsName)

	vs := new(snapshotv1api.VolumeSnapshot)
	err := p.crClient.Get(context.TODO(), crclient.ObjectKey{Namespace: newNamespace, Name: newVSName}, vs)
	switch {
	case apierrors.IsNotFound(err):
		logger.Debugf("VolumeSnapshot %s/%s is restored later as an additional item", newNamespace, newVSName)
	case err != nil:
		return false, errors.Wrapf(err, "Failed to get Volumesnapshot %s/%s to restore PVC %s/%s",
			newNamespace, newVSName, newNamespace, pvc.Name)
	default:
		if size, exists := vs.Annotations[velerov1api.VolumeSnapshotRestoreSize]; exists {
			restoreSize, err := resource.ParseQuantity(size)
			if err != nil {
				return false, errors.Wrapf(err, "Failed to parse %s from annotation on Volumesnapshot %s/%s into restore size",
					size, vs.Namespace, vs.Name)
			}
			setPVCStorageResourceRequest(pvc, restoreSize, logger)
		}
	}

	logger.Debugf("Setting PVC source to VolumeSnapshot new name: %s", newVSName)
	resetPVCSourceToVolumeSnapshot(pvc, newVSName)
	return false, nil
}

// processAzureRestore leaves the PVC's data source alone and annotates it with
// what the CloudCasa Azure Files mover needs: the driver and the snapshot's
// VolumeSnapshotContent in the cluster.
func (p *pvcRestoreItemAction) processAzureRestore(pvc *corev1api.PersistentVolumeClaim, volumeSnapshotName string,
	restore *velerov1api.Restore, log logrus.FieldLogger) error {
	// Set annotation that the PVC users Azure File CSI driver
	kubeutil.AddAnnotations(&pvc.ObjectMeta, map[string]string{
		"cloudcasa-csi-driver-name": azureFilesDriver,
	})
	log.Infof("Found Azure Files CSI driver. PVC data source will not be changed.")

	// Add annotation to the PVC with VolumeSnapshotContent name
	vscList := new(snapshotv1api.VolumeSnapshotContentList)
	if err := p.crClient.List(context.TODO(), vscList); err != nil {
		return errors.Wrapf(err, "Failed to list VolumeSnapshotContents to restore PVC %s/%s", pvc.Namespace, pvc.Name)
	}
	var volumeSnapshotContentName string
	for _, vsc := range vscList.Items {
		if vsc.Spec.VolumeSnapshotRef.Name == volumeSnapshotName && strings.HasPrefix(vsc.Name, "snapcontent-") {
			volumeSnapshotContentName = vsc.Name
			break
		}
	}
	if volumeSnapshotContentName == "" {
		return errors.Errorf("Failed to get VolumeSnapshotContent for VolumeSnapshot %s/%s", pvc.Namespace, volumeSnapshotName)
	}
	kubeutil.AddAnnotations(&pvc.ObjectMeta, map[string]string{
		"cloudcasa-volume-snapshot-content-name": volumeSnapshotContentName,
	})

	// If too many Azure Files PVCs are restored at the same time, then it is highly possible
	// that we will hit Azure throttling errors. Because of that, we sleep for a specified amount of time,
	// and then we restore the PVC.
	pvcRestoreWaitTime := DefaultAzureFilesSleepDuration
	pvcRestoreWaitTimeStr, ok := restore.GetAnnotations()[CCAzureFilesPvcRestoreWaitTimeAnnotation]
	if !ok {
		log.Infof(
			"Restore object %q does not have %q annotation. Using default value: %v",
			restore.Name, CCAzureFilesPvcRestoreWaitTimeAnnotation, DefaultAzureFilesSleepDuration,
		)
	} else {
		pvcRestoreWaitTimeSec, err := strconv.ParseInt(pvcRestoreWaitTimeStr, 10, 64)
		if err != nil {
			log.Errorf(
				"Failed to convert value %v of the annotation %q to int64. Error: %v. Using default value: %v",
				pvcRestoreWaitTimeStr, CCAzureFilesPvcRestoreWaitTimeAnnotation, err, DefaultAzureFilesSleepDuration,
			)
		} else if pvcRestoreWaitTimeSec == 0 {
			log.Infof(
				"Value of %q annotation is set to 0. Using default value: %v",
				CCAzureFilesPvcRestoreWaitTimeAnnotation, DefaultAzureFilesSleepDuration,
			)
		} else {
			log.Infof(
				"Value of %q annotation is set to %v. Using this value as the sleep time before returning from restore PVC action plugin",
				CCAzureFilesPvcRestoreWaitTimeAnnotation, pvcRestoreWaitTimeSec,
			)
			pvcRestoreWaitTime = time.Duration(pvcRestoreWaitTimeSec) * time.Second
		}
	}

	log.Infof("Azure Files PVC Found %s/%s. Sleeping %v before returning from restore PVC action plugin", pvc.Namespace, pvc.Name, pvcRestoreWaitTime)
	azureFilesSleep(pvcRestoreWaitTime)
	return nil
}

// setPVCStorageResourceRequest raises the PVC's storage request to restoreSize.
func setPVCStorageResourceRequest(
	pvc *corev1api.PersistentVolumeClaim,
	restoreSize resource.Quantity,
	log logrus.FieldLogger,
) {
	if pvc.Spec.Resources.Requests == nil {
		pvc.Spec.Resources.Requests = corev1api.ResourceList{}
	}

	storageReq, exists := pvc.Spec.Resources.Requests[corev1api.ResourceStorage]
	if !exists || storageReq.Cmp(restoreSize) < 0 {
		pvc.Spec.Resources.Requests[corev1api.ResourceStorage] = restoreSize
		rs := pvc.Spec.Resources.Requests[corev1api.ResourceStorage]
		log.Infof("Resetting storage requests for PVC %s/%s to %s",
			pvc.Namespace, pvc.Name, rs.String())
	}
}

// relayRestoreProgress reports the PVC's restore state to kubeagent.
func relayRestoreProgress(pvc *corev1api.PersistentVolumeClaim, state, message, restoreName string, logger logrus.FieldLogger) {
	if uErr := updateSnapshotProgress(pvc, nil, nil, state, message, restoreName, logger); uErr != nil {
		logger.WithError(uErr).Error("<SNAPSHOT PROGRESS UPDATE> Failed to update progress for restoring PVC from CSI snapshot. Continuing...")
		return
	}
	if state == "completed" {
		logger.Info("< CSI SNAPSHOT PROGRESS UPDATE> Successfully updated progress for restoring PVC from CSI snapshot.")
	}
}

func resetPVCSourceToVolumeSnapshot(pvc *corev1api.PersistentVolumeClaim, vsName string) {
	// Restore operation for the PVC will use the VolumeSnapshot as the data source.
	// So clear out the volume name, which is a ref to the PV
	pvc.Spec.VolumeName = ""
	dataSource := &corev1api.TypedLocalObjectReference{
		APIGroup: &snapshotv1api.SchemeGroupVersion.Group,
		Kind:     "VolumeSnapshot",
		Name:     vsName,
	}
	pvc.Spec.DataSource = dataSource
	pvc.Spec.DataSourceRef = nil
}

func (p *pvcRestoreItemAction) Name() string {
	return "PVCRestoreItemAction"
}

func (p *pvcRestoreItemAction) Progress(
	operationID string,
	restore *velerov1api.Restore,
) (velero.OperationProgress, error) {
	progress := velero.OperationProgress{}

	if operationID == "" {
		return progress, riav2.InvalidOperationIDError(operationID)
	}
	logger := p.log.WithFields(logrus.Fields{
		"Action":      "PVCRestoreItemAction",
		"OperationID": operationID,
		"Namespace":   restore.Namespace,
	})

	dataDownload, err := getDataDownload(
		context.Background(),
		restore.Namespace,
		operationID,
		p.crClient,
	)
	if err != nil {
		logger.Errorf("fail to get DataDownload: %s", err.Error())
		return progress, err
	}
	if dataDownload.Status.Phase == velerov2alpha1.DataDownloadPhaseNew ||
		dataDownload.Status.Phase == "" {
		logger.Debugf("DataDownload is still not processed yet. Skip progress update.")
		return progress, nil
	}

	progress.Description = string(dataDownload.Status.Phase)
	progress.OperationUnits = "Bytes"
	progress.NCompleted = dataDownload.Status.Progress.BytesDone
	progress.NTotal = dataDownload.Status.Progress.TotalBytes

	if dataDownload.Status.StartTimestamp != nil {
		progress.Started = dataDownload.Status.StartTimestamp.Time
	}

	if dataDownload.Status.CompletionTimestamp != nil {
		progress.Updated = dataDownload.Status.CompletionTimestamp.Time
	}

	if dataDownload.Status.Phase == velerov2alpha1.DataDownloadPhaseCompleted {
		progress.Completed = true
	} else if dataDownload.Status.Phase == velerov2alpha1.DataDownloadPhaseCanceled {
		progress.Completed = true
		progress.Err = "DataDownload is canceled"
	} else if dataDownload.Status.Phase == velerov2alpha1.DataDownloadPhaseFailed {
		progress.Completed = true
		progress.Err = dataDownload.Status.Message
	}

	return progress, nil
}

func (p *pvcRestoreItemAction) Cancel(
	operationID string, restore *velerov1api.Restore) error {
	if operationID == "" {
		return riav2.InvalidOperationIDError(operationID)
	}
	logger := p.log.WithFields(logrus.Fields{
		"Action":      "PVCRestoreItemAction",
		"OperationID": operationID,
		"Namespace":   restore.Namespace,
	})

	dataDownload, err := getDataDownload(
		context.Background(),
		restore.Namespace,
		operationID,
		p.crClient,
	)
	if err != nil {
		logger.Errorf("fail to get DataDownload: %s", err.Error())
		return err
	}

	err = cancelDataDownload(context.Background(), p.crClient, dataDownload)
	if err != nil {
		logger.Errorf("fail to cancel DataDownload %s: %s", dataDownload.Name, err.Error())
	}
	return err
}

func (p *pvcRestoreItemAction) AreAdditionalItemsReady(
	additionalItems []velero.ResourceIdentifier,
	restore *velerov1api.Restore,
) (bool, error) {
	return true, nil
}

func getDataUploadResult(
	ctx context.Context,
	restore *velerov1api.Restore,
	pvc *corev1api.PersistentVolumeClaim,
	crClient crclient.Client,
) (*velerov2alpha1.DataUploadResult, error) {
	selectorStr := fmt.Sprintf("%s=%s,%s=%s,%s=%s",
		velerov1api.PVCNamespaceNameLabel,
		label.GetValidName(pvc.Namespace+"."+pvc.Name),
		velerov1api.RestoreUIDLabel,
		label.GetValidName(string(restore.UID)),
		velerov1api.ResourceUsageLabel,
		label.GetValidName(string(velerov1api.VeleroResourceUsageDataUploadResult)),
	)
	selector, _ := labels.Parse(selectorStr)

	cmList := new(corev1api.ConfigMapList)
	if err := crClient.List(
		ctx,
		cmList,
		&crclient.ListOptions{
			LabelSelector: selector,
			Namespace:     restore.Namespace,
		}); err != nil {
		return nil, errors.Wrapf(err,
			"error to get DataUpload result cm with labels %s", selectorStr)
	}

	if len(cmList.Items) == 0 {
		return nil, errors.Errorf(
			"no DataUpload result cm found with labels %s", selectorStr)
	}

	if len(cmList.Items) > 1 {
		return nil, errors.Errorf(
			"multiple DataUpload result cms found with labels %s", selectorStr)
	}

	jsonBytes, exist := cmList.Items[0].Data[string(restore.UID)]
	if !exist {
		return nil, errors.Errorf(
			"no DataUpload result found with restore key %s, restore %s",
			string(restore.UID), restore.Name)
	}

	result := velerov2alpha1.DataUploadResult{}
	if err := json.Unmarshal([]byte(jsonBytes), &result); err != nil {
		return nil, errors.Errorf(
			"error to unmarshal DataUploadResult, restore UID %s, restore name %s",
			string(restore.UID), restore.Name)
	}

	return &result, nil
}

func getDataDownload(
	ctx context.Context,
	namespace string,
	operationID string,
	crClient crclient.Client,
) (*velerov2alpha1.DataDownload, error) {
	dataDownloadList := new(velerov2alpha1.DataDownloadList)
	err := crClient.List(ctx, dataDownloadList, &crclient.ListOptions{
		LabelSelector: labels.SelectorFromSet(map[string]string{
			velerov1api.AsyncOperationIDLabel: operationID,
		}),
		Namespace: namespace,
	})
	if err != nil {
		return nil, errors.Wrap(err, "fail to list DataDownload")
	}

	if len(dataDownloadList.Items) == 0 {
		return nil, errors.Errorf("didn't find DataDownload")
	}

	if len(dataDownloadList.Items) > 1 {
		return nil, errors.Errorf("find multiple DataDownloads")
	}

	return &dataDownloadList.Items[0], nil
}

func cancelDataDownload(ctx context.Context, crClient crclient.Client,
	dataDownload *velerov2alpha1.DataDownload) error {
	updatedDataDownload := dataDownload.DeepCopy()
	updatedDataDownload.Spec.Cancel = true

	return crClient.Patch(ctx, updatedDataDownload, crclient.MergeFrom(dataDownload))
}

func newDataDownload(
	restore *velerov1api.Restore,
	backup *velerov1api.Backup,
	dataUploadResult *velerov2alpha1.DataUploadResult,
	pvc *corev1api.PersistentVolumeClaim,
	newNamespace, operationID string,
) *velerov2alpha1.DataDownload {
	dataDownload := &velerov2alpha1.DataDownload{
		TypeMeta: metav1.TypeMeta{
			APIVersion: velerov2alpha1.SchemeGroupVersion.String(),
			Kind:       "DataDownload",
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace:    restore.Namespace,
			GenerateName: restore.Name + "-",
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: velerov1api.SchemeGroupVersion.String(),
					Kind:       "Restore",
					Name:       restore.Name,
					UID:        restore.UID,
					Controller: boolptr.True(),
				},
			},
			Labels: map[string]string{
				velerov1api.RestoreNameLabel:      label.GetValidName(restore.Name),
				velerov1api.RestoreUIDLabel:       string(restore.UID),
				velerov1api.AsyncOperationIDLabel: operationID,
			},
		},
		Spec: velerov2alpha1.DataDownloadSpec{
			TargetVolume: velerov2alpha1.TargetVolumeSpec{
				PVC:       pvc.Name,
				Namespace: newNamespace,
			},
			BackupStorageLocation: dataUploadResult.BackupStorageLocation,
			DataMover:             dataUploadResult.DataMover,
			SnapshotID:            dataUploadResult.SnapshotID,
			SnapshotSize:          dataUploadResult.SnapshotSize,
			SourceNamespace:       dataUploadResult.SourceNamespace,
			OperationTimeout:      backup.Spec.CSISnapshotTimeout,
			NodeOS:                dataUploadResult.NodeOS,
		},
	}
	if restore.Spec.UploaderConfig != nil {
		dataDownload.Spec.DataMoverConfig = uploaderUtil.StoreRestoreConfig(restore.Spec.UploaderConfig)
	}
	return dataDownload
}

func restoreFromDataUploadResult(
	ctx context.Context,
	restore *velerov1api.Restore,
	backup *velerov1api.Backup,
	pvc *corev1api.PersistentVolumeClaim,
	newNamespace, operationID string,
	crClient crclient.Client,
) (*velerov2alpha1.DataDownload, error) {
	dataUploadResult, err := getDataUploadResult(ctx, restore, pvc, crClient)
	if err != nil {
		return nil, errors.Wrapf(err, "fail get DataUploadResult for restore: %s",
			restore.Name)
	}
	pvc.Spec.VolumeName = ""
	if pvc.Spec.Selector == nil {
		pvc.Spec.Selector = &metav1.LabelSelector{}
	}
	if pvc.Spec.Selector.MatchLabels == nil {
		pvc.Spec.Selector.MatchLabels = make(map[string]string)
	}
	pvc.Spec.Selector.MatchLabels[velerov1api.DynamicPVRestoreLabel] = label.
		GetValidName(fmt.Sprintf("%s.%s.%s", newNamespace,
			pvc.Name, utilrand.String(GenerateNameRandomLength)))

	dataDownload := newDataDownload(
		restore,
		backup,
		dataUploadResult,
		pvc,
		newNamespace,
		operationID,
	)
	err = crClient.Create(ctx, dataDownload)
	if err != nil {
		return nil, errors.Wrapf(err, "fail to create DataDownload")
	}

	return dataDownload, nil
}

func (p *pvcRestoreItemAction) isResourceExist(
	pvc corev1api.PersistentVolumeClaim,
	restore velerov1api.Restore,
) (bool, *corev1api.PersistentVolumeClaim) {
	// get target namespace to restore into, if different from source namespace
	targetNamespace := pvc.Namespace
	if target, ok := restore.Spec.NamespaceMapping[pvc.Namespace]; ok {
		targetNamespace = target
	}

	tmpPVC := new(corev1api.PersistentVolumeClaim)
	if err := p.crClient.Get(
		context.Background(),
		crclient.ObjectKey{
			Name:      pvc.Name,
			Namespace: targetNamespace,
		},
		tmpPVC,
	); err == nil {
		return true, tmpPVC
	}
	return false, nil
}

// handleExistingPvc checks if target PVC exists and deletes it if the existing resource policy is "update"
// and the PV has a "Retain" reclaim policy because PVC can't be updated and it will be in "Lost" state after restore.
// If PVC is attached to a pod we can't delete it.
func (p *pvcRestoreItemAction) handleExistingPvc(input *velero.RestoreItemActionExecuteInput, pvc *corev1api.PersistentVolumeClaim) error {
	if input.Restore.Spec.ExistingResourcePolicy != velerov1api.PolicyTypeUpdate {
		p.log.Info("Skipping check for existing PVC because existing resource policy is not \"update\"")
		return nil
	}

	targetNamespace := pvc.Namespace
	if newTargetNamespace, found := input.Restore.Spec.NamespaceMapping[pvc.Namespace]; found {
		targetNamespace = newTargetNamespace
	}

	p.log.Infof("Checking if PVC %s/%s exists", targetNamespace, pvc.Name)

	exists, existingPvc := p.isResourceExist(*pvc, *input.Restore)
	if !exists {
		p.log.Infof("PVC %s/%s not found", targetNamespace, pvc.Name)
		return nil
	}

	if existingPvc.Spec.VolumeName == "" {
		p.log.Infof("Existing PVC %s/%s has no associated PV", targetNamespace, pvc.Name)
		return nil
	}

	existingPv := new(corev1api.PersistentVolume)
	if err := p.crClient.Get(context.TODO(), crclient.ObjectKey{Name: existingPvc.Spec.VolumeName}, existingPv); err != nil {
		p.log.Errorf("Failed to get existing PV %s: %v", existingPvc.Spec.VolumeName, err)
		return err
	}

	if existingPv.Spec.PersistentVolumeReclaimPolicy != corev1api.PersistentVolumeReclaimRetain {
		p.log.Infof("Existing PV %s does not have \"Retain\" reclaim policy", existingPv.Name)
		return nil
	}

	podList := new(corev1api.PodList)
	if err := p.crClient.List(context.TODO(), podList, crclient.InNamespace(targetNamespace)); err != nil {
		p.log.Errorf("Failed to list pods in namespace %s: %s", targetNamespace, err)
		return err
	}

	for _, pod := range podList.Items {
		for _, vol := range pod.Spec.Volumes {
			if vol.PersistentVolumeClaim != nil && vol.PersistentVolumeClaim.ClaimName == pvc.Name {
				p.log.Infof("PVC %s/%s is attached to pod %s, can't delete it", targetNamespace, pvc.Name, pod.Name)
				return nil
			}
		}
	}

	p.log.Infof("Deleting existing PVC %s/%s because existing resource policy is \"update\" and PV %s has \"Retain\" reclaim policy",
		targetNamespace, pvc.Name, existingPv.Name)

	if err := p.crClient.Delete(context.TODO(), existingPvc); err != nil {
		p.log.Errorf("Failed to delete existing PVC %s/%s: %v", targetNamespace, pvc.Name, err)
		return err
	}

	for try := range pvcDeleteTimeoutSeconds {
		if exists, _ := p.isResourceExist(*pvc, *input.Restore); !exists {
			p.log.Infof("PVC %s/%s deleted successfully", targetNamespace, pvc.Name)
			return nil
		}

		if try%10 == 0 {
			p.log.Infof("Waiting for PVC %s/%s to be deleted", targetNamespace, pvc.Name)
		}

		time.Sleep(time.Second)
	}

	errorMessage := fmt.Sprintf("Failed to delete PVC %s/%s after %d seconds", targetNamespace, pvc.Name, pvcDeleteTimeoutSeconds)
	p.log.Error(errorMessage)

	return errors.New(errorMessage)
}

func NewPvcRestoreItemAction(f client.Factory) plugincommon.HandlerInitializer {
	return func(logger logrus.FieldLogger) (any, error) {
		crClient, err := f.KubebuilderClient()
		if err != nil {
			return nil, err
		}

		return &pvcRestoreItemAction{
			log:      logger,
			crClient: crClient,
		}, nil
	}
}

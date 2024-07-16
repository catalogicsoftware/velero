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
	"fmt"

	snapshotv1api "github.com/kubernetes-csi/external-snapshotter/client/v7/apis/volumesnapshot/v1"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/util/retry"

	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/kuberesource"
	"github.com/vmware-tanzu/velero/pkg/plugin/velero"
	csiutil "github.com/vmware-tanzu/velero/pkg/util/csi"
	kubeutil "github.com/vmware-tanzu/velero/pkg/util/kube"
	util "github.com/vmware-tanzu/velero/test/util/csi"
)

// volumeSnapshotContentBackupItemAction is a backup item action plugin to backup
// CSI VolumeSnapshotContent objects using Velero
type volumeSnapshotContentBackupItemAction struct {
	log logrus.FieldLogger
}

// AppliesTo returns information indicating that the
// VolumeSnapshotContentBackupItemAction action should be invoked to
// backup VolumeSnapshotContents.
func (p *volumeSnapshotContentBackupItemAction) AppliesTo() (velero.ResourceSelector, error) {
	return velero.ResourceSelector{
		IncludedResources: []string{"volumesnapshotcontents.snapshot.storage.k8s.io"},
	}, nil
}

// Execute returns the unmodified VolumeSnapshotContent object along
// with the snapshot deletion secret, if any, from its annotation
// as additional items to backup.
func (p *volumeSnapshotContentBackupItemAction) Execute(
	item runtime.Unstructured,
	backup *velerov1api.Backup,
) (
	runtime.Unstructured,
	[]velero.ResourceIdentifier,
	string,
	[]velero.ResourceIdentifier,
	error,
) {
	p.log.Infof("Executing VolumeSnapshotContentBackupItemAction")

	if backup.Status.Phase == velerov1api.BackupPhaseFinalizing ||
		backup.Status.Phase == velerov1api.BackupPhaseFinalizingPartiallyFailed {
		p.log.WithField("Backup", fmt.Sprintf("%s/%s", backup.Namespace, backup.Name)).
			WithField("BackupPhase", backup.Status.Phase).
			Debug("Skipping VolumeSnapshotContentBackupItemAction",
				"as backup is in finalizing phase.")
		return item, nil, "", nil, nil
	}

	var snapCont snapshotv1api.VolumeSnapshotContent
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(
		item.UnstructuredContent(),
		&snapCont,
	); err != nil {
		return nil, nil, "", nil, errors.WithStack(err)
	}

	_, snapshotClient, err := util.GetClients()
	if err != nil {
		return nil, nil, "", nil, errors.WithStack(err)
	}
	vs, err := snapshotClient.SnapshotV1().VolumeSnapshots(snapCont.Spec.VolumeSnapshotRef.Namespace).Get(context.TODO(), snapCont.Spec.VolumeSnapshotRef.Name, metav1.GetOptions{})
	if err != nil {
		return nil, nil, "", nil, errors.Wrapf(err, fmt.Sprintf("failed to get volumesnapshot from volumesnapshotcontent %s", snapCont.GetName()))
	}
	if vs == nil {
		return nil, nil, "", nil, fmt.Errorf("nil value of VolumeSnapshot received")
	}
	vals := map[string]string{}

	// RetryOnConflict uses exponential backoff to avoid exhausting the apiserver
	retryErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		vsc, err := snapshotClient.SnapshotV1().VolumeSnapshotContents().Get(context.TODO(), snapCont.Name, metav1.GetOptions{})
		if err != nil {
			return errors.Wrapf(err, fmt.Sprintf("failed to get volumesnapshotcontent %s", snapCont.Name))
		}
		if vsc.Annotations == nil {
			vsc.Annotations = make(map[string]string)
		}
		vals["cc-pvc-name"] = *vs.Spec.Source.PersistentVolumeClaimName
		vals["cc-pvc-namespace"] = vs.GetNamespace()
		kubeutil.AddAnnotations(&snapCont.ObjectMeta, vals)
		kubeutil.AddAnnotations(&vsc.ObjectMeta, vals)
		err = nil
		_, err = snapshotClient.SnapshotV1().VolumeSnapshotContents().Update(context.TODO(), vsc, metav1.UpdateOptions{})
		if err != nil {
			p.log.Errorf("Failed to update VolumeSnapshotContent %s, Error is %v ", vsc.GetName(), err)
		}
		return err
	})
	if retryErr != nil {
		p.log.Errorf("Failed to update VolumeSnapshotContent %s with pvc details in annotations. Error is %v", snapCont.Name, retryErr)
		return nil, nil, "", nil, errors.WithStack(retryErr)
	}

	p.log.Infof("VolumeSnapshotContent %s successfully updated with PVC details", snapCont.Name)

	additionalItems := []velero.ResourceIdentifier{}

	// we should backup the snapshot deletion secrets that may be referenced
	// in the VolumeSnapshotContent's annotation
	if csiutil.IsVolumeSnapshotContentHasDeleteSecret(&snapCont) {
		additionalItems = append(
			additionalItems,
			velero.ResourceIdentifier{
				GroupResource: kuberesource.Secrets,
				Name:          snapCont.Annotations[velerov1api.PrefixedSecretNameAnnotation],
				Namespace:     snapCont.Annotations[velerov1api.PrefixedSecretNamespaceAnnotation],
			})

		kubeutil.AddAnnotations(&snapCont.ObjectMeta, map[string]string{
			velerov1api.MustIncludeAdditionalItemAnnotation: "true",
		})
	}

	snapContMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&snapCont)
	if err != nil {
		return nil, nil, "", nil, errors.WithStack(err)
	}

	p.log.Infof(
		"Returning from VolumeSnapshotContentBackupItemAction",
		"with %d additionalItems to backup",
		len(additionalItems),
	)
	return &unstructured.Unstructured{Object: snapContMap}, additionalItems, "", nil, nil
}

// Name returns the plugin's name.
func (p *volumeSnapshotContentBackupItemAction) Name() string {
	return "VolumeSnapshotContentBackupItemAction"
}

// Progress is not implemented for VolumeSnapshotContentBackupItemAction.
func (p *volumeSnapshotContentBackupItemAction) Progress(
	operationID string,
	backup *velerov1api.Backup,
) (velero.OperationProgress, error) {
	return velero.OperationProgress{}, nil
}

// Cancel is not implemented for VolumeSnapshotContentBackupItemAction.
func (p *volumeSnapshotContentBackupItemAction) Cancel(
	operationID string,
	backup *velerov1api.Backup,
) error {
	// CSI Specification doesn't support canceling a snapshot creation.
	return nil
}

// NewVolumeSnapshotContentBackupItemAction returns a
// VolumeSnapshotContentBackupItemAction instance.
func NewVolumeSnapshotContentBackupItemAction(
	logger logrus.FieldLogger,
) (interface{}, error) {
	return &volumeSnapshotContentBackupItemAction{log: logger}, nil
}

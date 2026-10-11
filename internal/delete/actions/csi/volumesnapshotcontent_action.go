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

	"github.com/cockroachdb/errors"
	snapshotv1api "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	"github.com/sirupsen/logrus"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/vmware-tanzu/velero/pkg/client"
	plugincommon "github.com/vmware-tanzu/velero/pkg/plugin/framework/common"
	"github.com/vmware-tanzu/velero/pkg/plugin/velero"
	"github.com/vmware-tanzu/velero/pkg/util/boolptr"
	"github.com/vmware-tanzu/velero/pkg/util/csi"
	kubeutil "github.com/vmware-tanzu/velero/pkg/util/kube"
)

// volumeSnapshotContentDeleteItemAction is a restore item action plugin for Velero
type volumeSnapshotContentDeleteItemAction struct {
	log      logrus.FieldLogger
	crClient crclient.Client
}

// AppliesTo returns information indicating
// VolumeSnapshotContentRestoreItemAction action should be invoked
// while restoring VolumeSnapshotContent.snapshot.storage.k8s.io resources
func (p *volumeSnapshotContentDeleteItemAction) AppliesTo() (velero.ResourceSelector, error) {
	return velero.ResourceSelector{
		IncludedResources: []string{"volumesnapshotcontents.snapshot.storage.k8s.io"},
	}, nil
}

func (p *volumeSnapshotContentDeleteItemAction) Execute(
	input *velero.DeleteItemActionExecuteInput,
) error {
	p.log.Info("Starting VolumeSnapshotContentDeleteItemAction")

	var snapCont snapshotv1api.VolumeSnapshotContent
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(
		input.Item.UnstructuredContent(),
		&snapCont,
	); err != nil {
		return errors.Wrapf(err, "failed to convert VolumeSnapshotContent from unstructured")
	}

	// We don't want this DeleteItemAction plugin to delete
	// VolumeSnapshotContent taken outside of Velero.
	// So skip deleting VolumeSnapshotContent not have the backup name
	// in its labels.
	if !kubeutil.HasBackupLabel(&snapCont.ObjectMeta, input.Backup.Name) {
		p.log.Infof(
			"VolumeSnapshotContent %s was not taken by backup %s, skipping deletion",
			snapCont.Name,
			input.Backup.Name,
		)
		return nil
	}

	p.log.Infof("Deleting VolumeSnapshotContent %s", snapCont.Name)

	if _, err := csi.SetVolumeSnapshotContentDeletionPolicy(
		snapCont.Name,
		p.crClient,
		snapshotv1api.VolumeSnapshotContentDelete,
	); err != nil {
		// #4764: Leave a warning when VolumeSnapshotContent cannot be found for deletion.
		// Manual deleting VolumeSnapshotContent can cause this.
		// It's tricky for Velero to handle this inconsistency.
		// Even if Velero restores the VolumeSnapshotContent, CSI snapshot controller
		// may not delete it correctly due to the snapshot represented by VolumeSnapshotContent
		// already deleted on cloud provider.
		if apierrors.IsNotFound(err) {
			snapshotHandle := "<unknown>"
			if snapCont.Status != nil && snapCont.Status.SnapshotHandle != nil {
				snapshotHandle = *snapCont.Status.SnapshotHandle
			}
			p.log.Warnf(
				"VolumeSnapshotContent %s of backup %s cannot be found. May leave orphan snapshot %s on cloud provider.",
				snapCont.Name, input.Backup.Name, snapshotHandle)
			return nil
		}
		return errors.Wrapf(err,
			"failed to set DeletionPolicy on volumesnapshotcontent %s. Skipping deletion",
			snapCont.Name)
	}

	if err := p.crClient.Delete(
		context.TODO(),
		&snapCont,
	); err != nil && !apierrors.IsNotFound(err) {
		p.log.Infof("VolumeSnapshotContent %s not found", snapCont.Name)
		return err
	}

	return nil
}

var checkVSCReadiness = func(
	ctx context.Context,
	vsc *snapshotv1api.VolumeSnapshotContent,
	client crclient.Client,
) (bool, error) {
	tmpVSC := new(snapshotv1api.VolumeSnapshotContent)
	if err := client.Get(ctx, crclient.ObjectKeyFromObject(vsc), tmpVSC); err != nil {
		return false, errors.Wrapf(
			err, "failed to get VolumeSnapshotContent %s", vsc.Name,
		)
	}

	if tmpVSC.Status != nil && boolptr.IsSetToTrue(tmpVSC.Status.ReadyToUse) {
		return true, nil
	}

	// Fail fast on permanent CSI driver errors (e.g., InvalidSnapshot.NotFound)
	if tmpVSC.Status != nil && tmpVSC.Status.Error != nil && tmpVSC.Status.Error.Message != nil {
		return false, errors.Errorf(
			"VolumeSnapshotContent %s has error: %s", vsc.Name, *tmpVSC.Status.Error.Message,
		)
	}

	return false, nil
}

func NewVolumeSnapshotContentDeleteItemAction(
	f client.Factory,
) plugincommon.HandlerInitializer {
	return func(logger logrus.FieldLogger) (any, error) {
		crClient, err := f.KubebuilderClient()
		if err != nil {
			return nil, err
		}

		return &volumeSnapshotContentDeleteItemAction{
			log:      logger,
			crClient: crClient,
		}, nil
	}
}

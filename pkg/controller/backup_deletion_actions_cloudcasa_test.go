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

package controller

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/vmware-tanzu/velero/internal/delete"
	"github.com/vmware-tanzu/velero/internal/volume"
	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/builder"
	"github.com/vmware-tanzu/velero/pkg/plugin/clientmgmt"
	pluginmocks "github.com/vmware-tanzu/velero/pkg/plugin/mocks"
	"github.com/vmware-tanzu/velero/pkg/plugin/velero"
	deleteitemactionmocks "github.com/vmware-tanzu/velero/pkg/plugin/velero/mocks"
)

// A failed DeleteItemAction is reported on the delete request, and the rest of
// the deletion still runs: kubeagent does not retry a processed request, so
// stopping there would leave the backup's other snapshots behind for good.
func TestBackupDeletionReportsFailedDeleteActionsAndGoesOn(t *testing.T) {
	backup := builder.ForBackup(velerov1api.DefaultNamespace, "foo").StorageLocation("primary").Result()
	backup.UID = "uid"
	location := builder.ForBackupStorageLocation(velerov1api.DefaultNamespace, "primary").
		Provider("objStoreProvider").Bucket("bucket").Phase(velerov1api.BackupStorageLocationPhaseAvailable).Result()
	snapshotLocation := builder.ForVolumeSnapshotLocation(velerov1api.DefaultNamespace, "vsl-1").Provider("provider-1").Result()
	td := setupBackupDeletionControllerTest(t, defaultTestDbr(), backup, location, snapshotLocation)
	td.volumeSnapshotter.SnapshotsTaken.Insert("snap-1")
	pluginManager := &pluginmocks.Manager{}
	pluginManager.On("GetVolumeSnapshotter", "provider-1").Return(td.volumeSnapshotter, nil)
	pluginManager.On("GetDeleteItemActions").Return([]velero.DeleteItemAction{new(deleteitemactionmocks.DeleteItemAction)}, nil)
	pluginManager.On("CleanupClients")
	td.controller.newPluginManager = func(logrus.FieldLogger) clientmgmt.Manager { return pluginManager }
	td.backupStore.On("GetBackupVolumeSnapshots", "foo").Return([]*volume.Snapshot{{
		Spec: volume.SnapshotSpec{Location: "vsl-1"}, Status: volume.SnapshotStatus{ProviderSnapshotID: "snap-1"},
	}}, nil)
	td.backupStore.On("GetBackupContents", "foo").Return(io.NopCloser(bytes.NewReader([]byte("tarball"))), nil)
	td.backupStore.On("DeleteBackup", "foo").Return(nil)
	previous := invokeDeleteActions
	invokeDeleteActions = func(*delete.Context) error {
		return delete.ItemActionErrors{errors.New("error executing DeleteItemAction for volumesnapshotcontents vsc-1: refused")}
	}
	t.Cleanup(func() { invokeDeleteActions = previous })

	_, err := td.controller.Reconcile(t.Context(), td.req)
	require.NoError(t, err)

	res := &velerov1api.DeleteBackupRequest{}
	require.NoError(t, td.fakeClient.Get(t.Context(), td.req.NamespacedName, res))
	assert.Equal(t, velerov1api.DeleteBackupRequestPhaseProcessed, res.Status.Phase)
	assert.Equal(t, []string{"error executing DeleteItemAction for volumesnapshotcontents vsc-1: refused"}, res.Status.Errors)
	assert.Equal(t, 0, td.volumeSnapshotter.SnapshotsTaken.Len(), "the backup's other snapshots are still deleted")
	td.backupStore.AssertNotCalled(t, "DeleteBackup", mock.Anything)
}

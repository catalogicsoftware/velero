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
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/vmware-tanzu/velero/internal/volume"
	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/builder"
	"github.com/vmware-tanzu/velero/pkg/persistence/bundlecrypt"
	persistencemocks "github.com/vmware-tanzu/velero/pkg/persistence/mocks"
	"github.com/vmware-tanzu/velero/pkg/plugin/clientmgmt"
	pluginmocks "github.com/vmware-tanzu/velero/pkg/plugin/mocks"
	"github.com/vmware-tanzu/velero/pkg/plugin/velero"
	deleteitemactionmocks "github.com/vmware-tanzu/velero/pkg/plugin/velero/mocks"
	velerotest "github.com/vmware-tanzu/velero/pkg/test"
)

// The reconciler's retries of missing objects would add seconds to every
// not-found test case.
func init() {
	backupGetRetryInterval = time.Millisecond
	bslGetRetryInterval = time.Millisecond
}

// lateClient returns NotFound for the first gets of each kind of object, as
// for objects kubeagent has just created that the cache does not show yet.
type lateClient struct {
	client.Client
	misses map[string]int
	gets   map[string]int
}

func (c *lateClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	kind := fmt.Sprintf("%T", obj)
	c.gets[kind]++
	if c.gets[kind] <= c.misses[kind] {
		return apierrors.NewNotFound(schema.GroupResource{}, key.Name)
	}
	return c.Client.Get(ctx, key, obj, opts...)
}

// A Backup and a storage location that appear only after a few tries are
// still found, instead of failing the delete request as not found.
func TestBackupDeletionRetriesObjectsThatAppearLate(t *testing.T) {
	backup := builder.ForBackup(velerov1api.DefaultNamespace, "foo").StorageLocation("default").Result()
	location := builder.ForBackupStorageLocation(velerov1api.DefaultNamespace, "default").
		AccessMode(velerov1api.BackupStorageLocationAccessModeReadOnly).Result()
	td := setupBackupDeletionControllerTest(t, defaultTestDbr(), backup, location)
	late := &lateClient{
		Client: td.fakeClient,
		misses: map[string]int{"*v1.Backup": 3, "*v1.BackupStorageLocation": 3},
		gets:   map[string]int{},
	}
	td.controller.Client = late

	_, err := td.controller.Reconcile(t.Context(), td.req)
	require.NoError(t, err)

	res := &velerov1api.DeleteBackupRequest{}
	require.NoError(t, td.fakeClient.Get(t.Context(), td.req.NamespacedName, res))
	require.Len(t, res.Status.Errors, 1)
	assert.Contains(t, res.Status.Errors[0], "is currently in read-only mode", "both objects were found")
	assert.Equal(t, 4, late.gets["*v1.Backup"])
	assert.Equal(t, 4, late.gets["*v1.BackupStorageLocation"])
}

func TestIsObjectLockError(t *testing.T) {
	assert.False(t, isObjectLockError(nil))
	assert.True(t, isObjectLockError(errors.New("PUT failed: BlobImmutableDueToPolicy: This operation is not permitted")))
	assert.True(t, isObjectLockError(errors.New("AccessDenied: Access Denied because object protected by Object Lock")))
	assert.False(t, isObjectLockError(errors.New("AccessDenied: missing permission")))
	assert.False(t, isObjectLockError(errors.New("connection reset")))
}

// On a storage location marked immutable the backup data is left in place, and
// an object lock error from the store does not fail the delete request.
func TestBackupDeletionOnAnImmutableStore(t *testing.T) {
	tests := []struct {
		name       string
		objectLock bool
		deleteErr  error
		wantDelete bool
		wantErrors bool
	}{
		{name: "store marked immutable", objectLock: true},
		{name: "object lock error from the store", deleteErr: errors.New("BlobImmutableDueToPolicy"), wantDelete: true},
		{name: "other error from the store", deleteErr: errors.New("connection reset"), wantDelete: true, wantErrors: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			backup := builder.ForBackup(velerov1api.DefaultNamespace, "foo").StorageLocation("primary").Result()
			backup.UID = "uid"
			location := builder.ForBackupStorageLocation(velerov1api.DefaultNamespace, "primary").
				Provider("objStoreProvider").Bucket("bucket").Phase(velerov1api.BackupStorageLocationPhaseAvailable).Result()
			if tc.objectLock {
				location.Annotations = map[string]string{velerov1api.ObjectLockAnnotation: "true"}
			}
			td := setupBackupDeletionControllerTest(t, defaultTestDbr(), backup, location)
			pluginManager := &pluginmocks.Manager{}
			pluginManager.On("GetDeleteItemActions").Return([]velero.DeleteItemAction{}, nil)
			pluginManager.On("CleanupClients")
			td.controller.newPluginManager = func(logrus.FieldLogger) clientmgmt.Manager { return pluginManager }
			td.backupStore.On("GetBackupVolumeSnapshots", "foo").Return(nil, nil)
			td.backupStore.On("DeleteBackup", "foo").Return(tc.deleteErr)

			_, err := td.controller.Reconcile(t.Context(), td.req)
			require.NoError(t, err)

			if tc.wantDelete {
				td.backupStore.AssertCalled(t, "DeleteBackup", "foo")
			} else {
				td.backupStore.AssertNotCalled(t, "DeleteBackup", mock.Anything)
			}
			res := &velerov1api.DeleteBackupRequest{}
			getErr := td.fakeClient.Get(t.Context(), td.req.NamespacedName, res)
			if tc.wantErrors {
				require.NoError(t, getErr)
				assert.NotEmpty(t, res.Status.Errors)
			} else {
				assert.True(t, apierrors.IsNotFound(getErr), "the request completed and was removed: %v", res.Status.Errors)
			}
		})
	}
}

// Restore files on an immutable storage location are not deleted, even when
// the location is unavailable.
func TestRestoreFilesOnAnImmutableStoreAreKept(t *testing.T) {
	backup := builder.ForBackup(velerov1api.DefaultNamespace, "foo").StorageLocation("primary").Result()
	for _, objectLock := range []bool{true, false} {
		location := builder.ForBackupStorageLocation(velerov1api.DefaultNamespace, "primary").
			Provider("objStoreProvider").Bucket("bucket").Phase(velerov1api.BackupStorageLocationPhaseUnavailable).Result()
		if objectLock {
			location.Annotations = map[string]string{velerov1api.ObjectLockAnnotation: "true"}
		}
		backupStore := &persistencemocks.BackupStore{}
		backupStore.On("DeleteRestore", "restore-1").Return(nil).Maybe()
		pluginManager := &pluginmocks.Manager{}
		pluginManager.On("CleanupClients").Maybe()
		r := &restoreReconciler{
			namespace:         velerov1api.DefaultNamespace,
			kbClient:          velerotest.NewFakeControllerRuntimeClient(t, backup, location),
			logger:            velerotest.NewLogger(),
			newPluginManager:  func(logrus.FieldLogger) clientmgmt.Manager { return pluginManager },
			backupStoreGetter: NewFakeSingleObjectBackupStoreGetter(backupStore),
		}

		err := r.deleteExternalResources(builder.ForRestore(velerov1api.DefaultNamespace, "restore-1").Backup("foo").Result())

		require.NoError(t, err)
		if objectLock {
			backupStore.AssertNotCalled(t, "DeleteRestore", "restore-1")
		} else {
			backupStore.AssertCalled(t, "DeleteRestore", "restore-1")
		}
	}
}

// A tarball that cannot be decrypted stops the deletion before anything is
// removed: the backup's snapshots, its CR and its data all stay.
func TestBackupDeletionStopsOnAnUndecryptableTarball(t *testing.T) {
	backup := builder.ForBackup(velerov1api.DefaultNamespace, "foo").StorageLocation("primary").Result()
	backup.UID = "uid"
	location := builder.ForBackupStorageLocation(velerov1api.DefaultNamespace, "primary").
		Provider("objStoreProvider").Bucket("bucket").Phase(velerov1api.BackupStorageLocationPhaseAvailable).Result()
	snapshotLocation := builder.ForVolumeSnapshotLocation(velerov1api.DefaultNamespace, "vsl-1").Provider("provider-1").Result()
	csiSnapshot := builder.ForVolumeSnapshot("user-ns", "vs-1").
		ObjectMeta(builder.WithLabels(velerov1api.BackupNameLabel, "foo")).SourcePVC("some-pvc").Result()
	td := setupBackupDeletionControllerTest(t, defaultTestDbr(), backup, location, snapshotLocation, csiSnapshot)
	td.volumeSnapshotter.SnapshotsTaken.Insert("snap-1")
	pluginManager := &pluginmocks.Manager{}
	pluginManager.On("GetVolumeSnapshotter", "provider-1").Return(td.volumeSnapshotter, nil)
	pluginManager.On("GetDeleteItemActions").Return([]velero.DeleteItemAction{new(deleteitemactionmocks.DeleteItemAction)}, nil)
	pluginManager.On("CleanupClients")
	td.controller.newPluginManager = func(logrus.FieldLogger) clientmgmt.Manager { return pluginManager }
	td.backupStore.On("GetBackupVolumeSnapshots", "foo").Return([]*volume.Snapshot{{
		Spec: volume.SnapshotSpec{Location: "vsl-1"}, Status: volume.SnapshotStatus{ProviderSnapshotID: "snap-1"},
	}}, nil)
	td.backupStore.On("GetBackupContents", "foo").Return(nil, bundlecrypt.ErrWrongKey)

	_, err := td.controller.Reconcile(t.Context(), td.req)
	require.NoError(t, err)

	res := &velerov1api.DeleteBackupRequest{}
	require.NoError(t, td.fakeClient.Get(t.Context(), td.req.NamespacedName, res))
	assert.Equal(t, velerov1api.DeleteBackupRequestPhaseProcessed, res.Status.Phase)
	require.Len(t, res.Status.Errors, 1)
	assert.Contains(t, res.Status.Errors[0], "unable to read the encrypted tarball of backup foo")
	td.backupStore.AssertNotCalled(t, "DeleteBackup", mock.Anything)
	assert.Equal(t, 1, td.volumeSnapshotter.SnapshotsTaken.Len(), "the native snapshot is kept")
	require.NoError(t, td.fakeClient.Get(t.Context(), client.ObjectKeyFromObject(csiSnapshot), csiSnapshot), "the CSI snapshot is kept")
	require.NoError(t, td.fakeClient.Get(t.Context(), client.ObjectKeyFromObject(backup), &velerov1api.Backup{}), "the backup CR is kept")
}

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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/vmware-tanzu/velero/internal/hook"
	"github.com/vmware-tanzu/velero/internal/volume"
	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/builder"
	"github.com/vmware-tanzu/velero/pkg/itemoperation"
	"github.com/vmware-tanzu/velero/pkg/metrics"
	persistencemocks "github.com/vmware-tanzu/velero/pkg/persistence/mocks"
	"github.com/vmware-tanzu/velero/pkg/plugin/clientmgmt"
	pluginmocks "github.com/vmware-tanzu/velero/pkg/plugin/mocks"
	velerotest "github.com/vmware-tanzu/velero/pkg/test"
	"github.com/vmware-tanzu/velero/pkg/util/backup_store"
	"github.com/vmware-tanzu/velero/pkg/util/results"
)

// A ReadOnly storage location gets the restore's artifacts written to the local
// directory kubeagent copies them from; any other goes to the backup store.
func TestRestoreArtifactsGoLocalForAReadOnlyLocation(t *testing.T) {
	previous := restoreArtifactsDir
	restoreArtifactsDir = t.TempDir()
	t.Cleanup(func() { restoreArtifactsDir = previous })
	restore := builder.ForRestore(velerov1api.DefaultNamespace, "restore-1").Backup("backup-1").Result()
	store := &persistencemocks.BackupStore{}
	store.On("PutRestoreResults", "backup-1", "restore-1", mock.Anything).Return(nil)

	readOnly := builder.ForBackupStorageLocation(velerov1api.DefaultNamespace, "default").
		AccessMode(velerov1api.BackupStorageLocationAccessModeReadOnly).Result()
	w := newRestoreArtifactWriter(readOnly, store, restore, logrus.New())
	require.IsType(t, &backup_store.LocalArtifactWriter{}, w)
	require.NoError(t, putResultsWithWriter(restore, map[string]results.Result{}, w))
	_, err := os.Stat(filepath.Join(restoreArtifactsDir, "restore-1", "restore-restore-1-results.gz"))
	require.NoError(t, err)
	store.AssertNotCalled(t, "PutRestoreResults", mock.Anything, mock.Anything, mock.Anything)

	readWrite := builder.ForBackupStorageLocation(velerov1api.DefaultNamespace, "default").Result()
	w = newRestoreArtifactWriter(readWrite, store, restore, logrus.New())
	require.IsType(t, &backup_store.BackupStoreArtifactWriter{}, w)
	require.NoError(t, putResultsWithWriter(restore, map[string]results.Result{}, w))
	store.AssertCalled(t, "PutRestoreResults", "backup-1", "restore-1", mock.Anything)
}

// The finalizer does not upload its results to a ReadOnly storage location.
func TestRestoreFinalizerKeepsResultsLocalForAReadOnlyLocation(t *testing.T) {
	for _, readOnly := range []bool{true, false} {
		location := builder.ForBackupStorageLocation(velerov1api.DefaultNamespace, "default").Provider("myCloud").Bucket("bucket").Result()
		if readOnly {
			location.Spec.AccessMode = velerov1api.BackupStorageLocationAccessModeReadOnly
		}
		restore := builder.ForRestore(velerov1api.DefaultNamespace, "restore-1").
			Phase(velerov1api.RestorePhaseFinalizing).Backup("backup-1").Result()
		backup := defaultBackup().StorageLocation("default").Result()
		fakeClient := velerotest.NewFakeControllerRuntimeClientBuilder(t).Build()
		pluginManager := &pluginmocks.Manager{}
		pluginManager.On("CleanupClients")
		backupStore := &persistencemocks.BackupStore{}
		backupStore.On("GetBackupVolumeInfos", backup.Name).Return(nil, nil)
		backupStore.On("GetRestoredResourceList", restore.Name).Return(map[string][]string{}, nil)
		backupStore.On("GetRestoreItemOperations", restore.Name).Return([]*itemoperation.RestoreOperation{}, nil)
		backupStore.On("GetRestoreResults", restore.Name).Return(map[string]results.Result{}, nil)
		backupStore.On("PutRestoreResults", backup.Name, restore.Name, mock.Anything).Return(nil)
		tracker := hook.NewMultiHookTracker()
		tracker.Add(restore.Name, "ns", "pod", "con1", "s1", "h1", hook.PhasePre, 0) // never runs: one error
		r := NewRestoreFinalizerReconciler(velerotest.NewLogger(), velerov1api.DefaultNamespace, fakeClient,
			func(logrus.FieldLogger) clientmgmt.Manager { return pluginManager },
			NewFakeSingleObjectBackupStoreGetter(backupStore), metrics.NewServerMetrics(), fakeClient, tracker, time.Second)
		require.NoError(t, r.Client.Create(t.Context(), restore))
		require.NoError(t, r.Client.Create(t.Context(), backup))
		require.NoError(t, r.Client.Create(t.Context(), location))

		_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: types.NamespacedName{
			Namespace: restore.Namespace, Name: restore.Name}})
		require.NoError(t, err)

		if readOnly {
			backupStore.AssertNotCalled(t, "PutRestoreResults", mock.Anything, mock.Anything, mock.Anything)
		} else {
			backupStore.AssertCalled(t, "PutRestoreResults", backup.Name, restore.Name, mock.Anything)
		}
		after := &velerov1api.Restore{}
		require.NoError(t, fakeClient.Get(t.Context(), types.NamespacedName{Namespace: restore.Namespace, Name: restore.Name}, after))
		assert.Equal(t, velerov1api.RestorePhasePartiallyFailed, after.Status.Phase, "readOnly=%v", readOnly)
	}
}

// When a restored PVC cannot be found for its PV patch, the finalizer logs the
// PVCs it can see and where it expected this one.
func TestPVPatchLogsThePVCsItCanSee(t *testing.T) {
	logger, hook := logrustest.NewNullLogger()
	ctx := &finalizerContext{
		logger:          logger,
		crClient:        velerotest.NewFakeControllerRuntimeClient(t, builder.ForPersistentVolumeClaim("other-ns", "other").Result()),
		restore:         builder.ForRestore(velerov1api.DefaultNamespace, "restore-1").Result(),
		restoredPVCList: map[string]struct{}{"ns1/pvc1": {}},
		volumeInfo: []*volume.BackupVolumeInfo{{
			BackupMethod: "PodVolumeBackup", PVCName: "pvc1", PVName: "pv1", PVCNamespace: "ns1",
			PVInfo: &volume.PVInfo{ReclaimPolicy: "Delete"},
		}},
		resourceTimeout: time.Second,
	}

	errs := ctx.patchDynamicPVWithVolumeInfo()

	assert.NotEmpty(t, errs.Namespaces["ns1"])
	var messages []string
	for _, e := range hook.AllEntries() {
		messages = append(messages, e.Message)
	}
	assert.Contains(t, messages, "Found PVC: other in Namespace: other-ns")
	assert.Contains(t, messages, "Expected PVC pvc1 to be in namespace ns1, but it was not found.")
}

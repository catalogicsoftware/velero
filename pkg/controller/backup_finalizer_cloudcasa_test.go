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
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	testclocks "k8s.io/utils/clock/testing"
	ctrl "sigs.k8s.io/controller-runtime"

	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/builder"
	"github.com/vmware-tanzu/velero/pkg/itemoperation"
	"github.com/vmware-tanzu/velero/pkg/kuberesource"
	"github.com/vmware-tanzu/velero/pkg/metrics"
	persistencemocks "github.com/vmware-tanzu/velero/pkg/persistence/mocks"
	"github.com/vmware-tanzu/velero/pkg/plugin/clientmgmt"
	pluginmocks "github.com/vmware-tanzu/velero/pkg/plugin/mocks"
	"github.com/vmware-tanzu/velero/pkg/plugin/velero"
	velerotest "github.com/vmware-tanzu/velero/pkg/test"
)

// An immutable store refuses to overwrite the backup's files, as it did before
// the finalizer retried: the backup still completes, without retrying the
// refused upload, and keeps the files written earlier in the backup.
func TestBackupFinalizerCompletesWhenAnImmutableStoreRefusesTheUpload(t *testing.T) {
	origBackoff := objectStorageBackoff
	objectStorageBackoff = wait.Backoff{Duration: time.Millisecond, Steps: 3}
	t.Cleanup(func() { objectStorageBackoff = origBackoff })

	tests := []struct {
		name           string
		withOperations bool
		metadataErr    error
		contentsErr    error
	}{
		{name: "metadata refused by an Azure immutability policy",
			metadataErr: errors.New("PUT velero-backup.json: 409 BlobImmutableDueToPolicy")},
		{name: "contents refused by S3 object lock", withOperations: true,
			contentsErr: errors.New("AccessDenied: Access Denied because object protected by Object Lock")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fakeClock := testclocks.NewFakeClock(time.Now())
			now := metav1.NewTime(fakeClock.Now())
			location := builder.ForBackupStorageLocation(velerov1api.DefaultNamespace, "default").Result()
			backup := builder.ForBackup(velerov1api.DefaultNamespace, "backup-1").
				StorageLocation("default").ObjectMeta(builder.WithUID("foo")).
				StartTimestamp(fakeClock.Now()).Phase(velerov1api.BackupPhaseFinalizing).Result()
			client := velerotest.NewFakeControllerRuntimeClient(t, backup, location)
			pluginManager := &pluginmocks.Manager{}
			backupStore := &persistencemocks.BackupStore{}
			backupper := new(fakeBackupper)
			reconciler := NewBackupFinalizerReconciler(client, velerotest.NewFakeControllerRuntimeClient(t, backup, location),
				fakeClock, backupper, func(logrus.FieldLogger) clientmgmt.Manager { return pluginManager }, NewBackupTracker(),
				NewFakeSingleObjectBackupStoreGetter(backupStore), logrus.StandardLogger(), metrics.NewServerMetrics(), 10*time.Minute)

			var operations []*itemoperation.BackupOperation
			if test.withOperations {
				operations = []*itemoperation.BackupOperation{{
					Spec: itemoperation.BackupOperationSpec{
						BackupName: "backup-1", BackupUID: "foo", BackupItemAction: "foo", OperationID: "operation-1",
						ResourceIdentifier: velero.ResourceIdentifier{GroupResource: kuberesource.Pods, Namespace: "ns-1", Name: "pod-1"},
					},
					Status: itemoperation.OperationStatus{Phase: itemoperation.OperationPhaseCompleted, Created: &now},
				}}
				pluginManager.On("GetBackupItemActionsV2").Return(nil, nil)
				backupStore.On("GetBackupContents", mock.Anything).Return(io.NopCloser(bytes.NewReader([]byte("hello world"))), nil)
				backupStore.On("PutBackupContents", mock.Anything, mock.Anything).Return(test.contentsErr)
				backupper.On("FinalizeBackup", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
					mock.Anything, mock.Anything).Return(nil)
			}
			pluginManager.On("CleanupClients").Return(nil)
			backupStore.On("GetBackupItemOperations", backup.Name).Return(operations, nil)
			backupStore.On("PutBackupMetadata", mock.Anything, mock.Anything).Return(test.metadataErr)
			backupStore.On("GetBackupVolumeInfos", mock.Anything).Return(nil, nil)
			backupStore.On("PutBackupVolumeInfos", mock.Anything, mock.Anything).Return(nil)

			_, err := reconciler.Reconcile(t.Context(), ctrl.Request{NamespacedName: types.NamespacedName{
				Namespace: backup.Namespace, Name: backup.Name}})
			require.NoError(t, err)

			after := velerov1api.Backup{}
			require.NoError(t, client.Get(t.Context(), types.NamespacedName{Namespace: backup.Namespace, Name: backup.Name}, &after))
			assert.Equal(t, velerov1api.BackupPhaseCompleted, after.Status.Phase)
			assert.NotNil(t, after.Status.CompletionTimestamp)
			backupStore.AssertNumberOfCalls(t, "PutBackupMetadata", 1)
			if test.withOperations {
				backupStore.AssertNumberOfCalls(t, "PutBackupContents", 1)
			}
		})
	}
}

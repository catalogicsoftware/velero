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
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"
	testclocks "k8s.io/utils/clock/testing"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/event"

	"github.com/vmware-tanzu/velero/internal/hook"
	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/builder"
	"github.com/vmware-tanzu/velero/pkg/instance"
	"github.com/vmware-tanzu/velero/pkg/metrics"
	persistencemocks "github.com/vmware-tanzu/velero/pkg/persistence/mocks"
	"github.com/vmware-tanzu/velero/pkg/plugin/clientmgmt"
	pluginmocks "github.com/vmware-tanzu/velero/pkg/plugin/mocks"
	velerotest "github.com/vmware-tanzu/velero/pkg/test"
)

const (
	ownInstance     = "job-a"
	foreignInstance = "job-b"
)

// withInstanceScope installs a scope for the test and restores the previous one.
func withInstanceScope(t *testing.T, scope instance.Scope) {
	t.Helper()
	previous := InstanceScope()
	SetInstanceScope(scope)
	t.Cleanup(func() { SetInstanceScope(previous) })
}

func TestInstancePredicateFollowsScope(t *testing.T) {
	withInstanceScope(t, instance.New(ownInstance))
	p := instancePredicate()

	own := builder.ForBackup("ns", "own").ObjectMeta(builder.WithLabels(instance.LabelKey, ownInstance)).Result()
	foreign := builder.ForBackup("ns", "foreign").ObjectMeta(builder.WithLabels(instance.LabelKey, foreignInstance)).Result()
	unlabeled := builder.ForBackup("ns", "unlabeled").Result()

	assert.True(t, p.Create(event.CreateEvent{Object: own}))
	assert.False(t, p.Create(event.CreateEvent{Object: foreign}))
	assert.False(t, p.Create(event.CreateEvent{Object: unlabeled}))
	assert.False(t, p.Generic(event.GenericEvent{Object: foreign}))
	assert.False(t, p.Update(event.UpdateEvent{ObjectOld: own, ObjectNew: foreign}))

	// The predicate reads the scope per event: the shared engine owns only unlabeled CRs.
	SetInstanceScope(instance.New(""))
	assert.True(t, p.Create(event.CreateEvent{Object: unlabeled}))
	assert.False(t, p.Create(event.CreateEvent{Object: own}))
}

func TestGCReconcilerSkipsForeignBackup(t *testing.T) {
	withInstanceScope(t, instance.New(ownInstance))
	fakeClock := testclocks.NewFakeClock(time.Now())
	location := builder.ForBackupStorageLocation(velerov1api.DefaultNamespace, "default").Result()

	for _, tc := range []struct {
		name      string
		labelVal  string
		expectDBR bool
	}{
		{"foreign expired backup is ignored", foreignInstance, false},
		{"own expired backup is garbage collected", ownInstance, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backup := builder.ForBackup(velerov1api.DefaultNamespace, "backup-1").
				StorageLocation("default").
				Expiration(fakeClock.Now().Add(-time.Minute)).
				ObjectMeta(builder.WithLabels(instance.LabelKey, tc.labelVal)).
				Result()
			fakeClient := velerotest.NewFakeControllerRuntimeClient(t, backup, location)
			gcr := mockGCReconciler(fakeClient, fakeClock, time.Hour)

			_, err := gcr.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{
				Namespace: backup.Namespace, Name: backup.Name}})
			require.NoError(t, err)

			requests := &velerov1api.DeleteBackupRequestList{}
			require.NoError(t, fakeClient.List(ctx, requests))
			assert.Equal(t, tc.expectDBR, len(requests.Items) > 0)
		})
	}
}

func TestScheduleReconcilerSkipsForeignSchedule(t *testing.T) {
	withInstanceScope(t, instance.New(ownInstance))

	for _, tc := range []struct {
		name          string
		labelVal      string
		expectEnabled bool
	}{
		{"foreign schedule is ignored", foreignInstance, false},
		{"own schedule is reconciled", ownInstance, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := velerotest.NewFakeControllerRuntimeClientBuilder(t).Build()
			reconciler := NewScheduleReconciler("ns", velerotest.NewLogger(), client, metrics.NewServerMetrics(), false)
			reconciler.clock = testclocks.NewFakeClock(time.Now())

			schedule := builder.ForSchedule("ns", "sched").
				CronSchedule("0 * * * *").
				ObjectMeta(builder.WithLabels(instance.LabelKey, tc.labelVal)).
				Result()
			require.NoError(t, client.Create(ctx, schedule))

			_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "ns", Name: "sched"}})
			require.NoError(t, err)

			result := &velerov1api.Schedule{}
			require.NoError(t, client.Get(ctx, types.NamespacedName{Namespace: "ns", Name: "sched"}, result))
			assert.Equal(t, tc.expectEnabled, result.Status.Phase == velerov1api.SchedulePhaseEnabled)
		})
	}
}

func TestBackupDeletionReconcilerSkipsForeignRequest(t *testing.T) {
	withInstanceScope(t, instance.New(ownInstance))

	for _, tc := range []struct {
		name            string
		labelVal        string
		expectProcessed bool
	}{
		{"foreign request is ignored", foreignInstance, false},
		{"own request is processed", ownInstance, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dbr := defaultTestDbr()
			dbr.Spec.BackupName = "" // fails validation fast, without touching any backup
			dbr.Labels[instance.LabelKey] = tc.labelVal
			td := setupBackupDeletionControllerTest(t, dbr)

			_, err := td.controller.Reconcile(ctx, td.req)
			require.NoError(t, err)

			result := &velerov1api.DeleteBackupRequest{}
			require.NoError(t, td.fakeClient.Get(ctx, td.req.NamespacedName, result))
			assert.Equal(t, tc.expectProcessed, result.Status.Phase == velerov1api.DeleteBackupRequestPhaseProcessed)
		})
	}
}

func TestRestoreFinalizerReconcilerSkipsForeignRestore(t *testing.T) {
	withInstanceScope(t, instance.New(ownInstance))

	fakeClient := velerotest.NewFakeControllerRuntimeClientBuilder(t).Build()
	pluginManager := &pluginmocks.Manager{}
	backupStore := &persistencemocks.BackupStore{}
	r := NewRestoreFinalizerReconciler(
		velerotest.NewLogger(),
		velerov1api.DefaultNamespace,
		fakeClient,
		func(logrus.FieldLogger) clientmgmt.Manager { return pluginManager },
		NewFakeSingleObjectBackupStoreGetter(backupStore),
		metrics.NewServerMetrics(),
		fakeClient,
		hook.NewMultiHookTracker(),
		10*time.Minute,
	)

	restore := builder.ForRestore(velerov1api.DefaultNamespace, "restore-1").
		Phase(velerov1api.RestorePhaseFinalizing).
		Backup("backup-1").
		ObjectMeta(builder.WithLabels(instance.LabelKey, foreignInstance)).
		Result()
	require.NoError(t, fakeClient.Create(ctx, restore))

	_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{
		Namespace: restore.Namespace, Name: restore.Name}})
	require.NoError(t, err)

	result := &velerov1api.Restore{}
	require.NoError(t, fakeClient.Get(ctx, types.NamespacedName{Namespace: restore.Namespace, Name: restore.Name}, result))
	assert.Equal(t, velerov1api.RestorePhaseFinalizing, result.Status.Phase, "foreign restore must be left untouched")
	backupStore.AssertNotCalled(t, "GetBackupMetadata")
}

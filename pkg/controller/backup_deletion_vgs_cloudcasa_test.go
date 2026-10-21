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
	"slices"
	"strings"
	"testing"
	"time"

	volumegroupsnapshotv1beta2 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumegroupsnapshot/v1beta2"
	snapshotv1api "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/builder"
	"github.com/vmware-tanzu/velero/pkg/plugin/clientmgmt"
	pluginmocks "github.com/vmware-tanzu/velero/pkg/plugin/mocks"
	"github.com/vmware-tanzu/velero/pkg/plugin/velero"
)

const testGroupContentFinalizer = "groupsnapshot.storage.kubernetes.io/volumegroupsnapshotcontent-bound-protection"

func groupStub(name string, labels map[string]string, found bool, statusErr string) *volumegroupsnapshotv1beta2.VolumeGroupSnapshotContent {
	stub := &volumegroupsnapshotv1beta2.VolumeGroupSnapshotContent{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels, Finalizers: []string{testGroupContentFinalizer}},
		Spec: volumegroupsnapshotv1beta2.VolumeGroupSnapshotContentSpec{
			DeletionPolicy: snapshotv1api.VolumeSnapshotContentDelete,
			Driver:         "hostpath.csi.k8s.io",
			Source: volumegroupsnapshotv1beta2.VolumeGroupSnapshotContentSource{
				GroupSnapshotHandles: &volumegroupsnapshotv1beta2.GroupSnapshotHandles{
					VolumeGroupSnapshotHandle: "group-" + name,
					VolumeSnapshotHandles:     []string{"snap-a", "snap-b"},
				},
			},
		},
		Status: &volumegroupsnapshotv1beta2.VolumeGroupSnapshotContentStatus{},
	}
	if found {
		handle := "group-" + name
		stub.Status.VolumeGroupSnapshotHandle = &handle
	}
	if statusErr != "" {
		stub.Status.Error = &snapshotv1api.VolumeSnapshotError{Message: &statusErr}
	}
	return stub
}

// fakeGroupSnapshotter removes the finalizer of a deleted group content whose
// group it found, as the snapshotter does after DeleteVolumeGroupSnapshot.
func fakeGroupSnapshotter(ctx context.Context, c client.Client, deleted *[]string) {
	for ctx.Err() == nil {
		list := &volumegroupsnapshotv1beta2.VolumeGroupSnapshotContentList{}
		if c.List(ctx, list) == nil {
			for i := range list.Items {
				stub := &list.Items[i]
				if stub.DeletionTimestamp == nil || stub.Status == nil || stub.Status.VolumeGroupSnapshotHandle == nil {
					continue
				}
				*deleted = append(*deleted, *stub.Status.VolumeGroupSnapshotHandle)
				stub.Finalizers = nil
				_ = c.Update(ctx, stub)
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// The deletion deletes, through the stub group contents its delete actions left,
// the backup's group snapshots on the storage, and only those.
func TestBackupDeletionDeletesGroupSnapshotsThroughTheirStubs(t *testing.T) {
	tests := []struct {
		name        string
		stub        *volumegroupsnapshotv1beta2.VolumeGroupSnapshotContent
		wantDeleted []string
		wantError   string
	}{
		{name: "group found", stub: groupStub("g1", map[string]string{velerov1api.BackupNameLabel: "foo"}, true, ""), wantDeleted: []string{"group-g1"}},
		{name: "group already gone", stub: groupStub("g2", map[string]string{velerov1api.BackupNameLabel: "foo"}, false,
			`failed to list group snapshot: "rpc error: code = NotFound desc = no such group"`)},
		{name: "group refused", stub: groupStub("g3", map[string]string{velerov1api.BackupNameLabel: "foo"}, false,
			`rpc error: code = InvalidArgument desc = Snapshot IDs do not match the GroupSnapshot IDs`),
			wantError: "group snapshot group-g3 was not deleted: rpc error: code = InvalidArgument"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			backup := builder.ForBackup(velerov1api.DefaultNamespace, "foo").StorageLocation("primary").Result()
			backup.UID = "uid"
			location := builder.ForBackupStorageLocation(velerov1api.DefaultNamespace, "primary").
				Provider("objStoreProvider").Bucket("bucket").Phase(velerov1api.BackupStorageLocationPhaseAvailable).Result()
			restoreStub := groupStub("restore-stub", map[string]string{velerov1api.RestoreNameLabel: "r1"}, true, "")
			restoreStub.Spec.DeletionPolicy = snapshotv1api.VolumeSnapshotContentRetain
			otherBackupStub := groupStub("other", map[string]string{velerov1api.BackupNameLabel: "bar"}, true, "")
			td := setupBackupDeletionControllerTest(t, defaultTestDbr(), []runtime.Object{backup, location, tc.stub, restoreStub, otherBackupStub}...)
			pluginManager := &pluginmocks.Manager{}
			pluginManager.On("GetDeleteItemActions").Return([]velero.DeleteItemAction{}, nil)
			pluginManager.On("CleanupClients")
			td.controller.newPluginManager = func(logrus.FieldLogger) clientmgmt.Manager { return pluginManager }
			td.backupStore.On("GetBackupVolumeSnapshots", "foo").Return(nil, nil)
			td.backupStore.On("DeleteBackup", "foo").Return(nil)
			ctx, cancel := context.WithCancel(t.Context())
			var deleted []string
			done := make(chan struct{})
			go func() { fakeGroupSnapshotter(ctx, td.fakeClient, &deleted); close(done) }()

			_, err := td.controller.Reconcile(t.Context(), td.req)
			cancel()
			<-done
			require.NoError(t, err)

			assert.Equal(t, tc.wantDeleted, deleted)
			assert.True(t, apierrors.IsNotFound(td.fakeClient.Get(t.Context(), client.ObjectKeyFromObject(tc.stub), tc.stub)), "the stub is gone")
			for _, kept := range []*volumegroupsnapshotv1beta2.VolumeGroupSnapshotContent{restoreStub, otherBackupStub} {
				require.NoError(t, td.fakeClient.Get(t.Context(), client.ObjectKeyFromObject(kept), kept))
				assert.Nil(t, kept.DeletionTimestamp)
			}
			res := &velerov1api.DeleteBackupRequest{}
			getErr := td.fakeClient.Get(t.Context(), td.req.NamespacedName, res)
			if tc.wantError == "" {
				assert.True(t, apierrors.IsNotFound(getErr), "the request completed and was removed: %v", res.Status.Errors)
				return
			}
			require.NoError(t, getErr)
			assert.True(t, slices.ContainsFunc(res.Status.Errors, func(e string) bool { return strings.Contains(e, tc.wantError) }),
				"errors: %v", res.Status.Errors)
		})
	}
}

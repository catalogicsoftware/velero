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
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	snapshotv1api "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"

	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/builder"
	factorymocks "github.com/vmware-tanzu/velero/pkg/client/mocks"
	"github.com/vmware-tanzu/velero/pkg/plugin/velero"
	velerotest "github.com/vmware-tanzu/velero/pkg/test"
)

// fakeClientWithErrors wraps a real client and injects errors for specific operations.
type fakeClientWithErrors struct {
	crclient.Client
	getError    error
	patchError  error
	deleteError error
}

type fakeClientWithCallTracking struct {
	crclient.Client
	events *[]string
}

func (c *fakeClientWithCallTracking) Create(ctx context.Context, obj crclient.Object, opts ...crclient.CreateOption) error {
	*c.events = append(*c.events, "create")
	return c.Client.Create(ctx, obj, opts...)
}

func (c *fakeClientWithCallTracking) Delete(ctx context.Context, obj crclient.Object, opts ...crclient.DeleteOption) error {
	*c.events = append(*c.events, "delete")
	return c.Client.Delete(ctx, obj, opts...)
}

func (c *fakeClientWithErrors) Get(ctx context.Context, key crclient.ObjectKey, obj crclient.Object, opts ...crclient.GetOption) error {
	if c.getError != nil {
		return c.getError
	}
	return c.Client.Get(ctx, key, obj, opts...)
}

func (c *fakeClientWithErrors) Patch(ctx context.Context, obj crclient.Object, patch crclient.Patch, opts ...crclient.PatchOption) error {
	if c.patchError != nil {
		return c.patchError
	}
	return c.Client.Patch(ctx, obj, patch, opts...)
}

func (c *fakeClientWithErrors) Delete(ctx context.Context, obj crclient.Object, opts ...crclient.DeleteOption) error {
	if c.deleteError != nil {
		return c.deleteError
	}
	return c.Client.Delete(ctx, obj, opts...)
}

func TestVSCExecute(t *testing.T) {
	handle := "snap-handle-1"
	backupVSC := func(status *snapshotv1api.VolumeSnapshotContentStatus) *snapshotv1api.VolumeSnapshotContent {
		vsc := builder.ForVolumeSnapshotContent("snapcontent-1").
			ObjectMeta(builder.WithLabelsMap(map[string]string{velerov1api.BackupNameLabel: "backup"})).
			Status(status).Result()
		vsc.Spec.DeletionPolicy = snapshotv1api.VolumeSnapshotContentRetain
		return vsc
	}
	tests := []struct {
		name        string
		item        *snapshotv1api.VolumeSnapshotContent
		inCluster   bool
		errClient   *fakeClientWithErrors
		wantDeleted bool
		wantErr     bool
		wantWarning string
	}{
		{
			name:      "not taken by the backup",
			item:      builder.ForVolumeSnapshotContent("snapcontent-1").Result(),
			inCluster: true,
		},
		{
			name:        "retained in the cluster: deleted with its snapshot",
			item:        backupVSC(&snapshotv1api.VolumeSnapshotContentStatus{SnapshotHandle: &handle}),
			inCluster:   true,
			wantDeleted: true,
		},
		{
			name: "gone from the cluster: deleted through a temporary content",
			item: backupVSC(&snapshotv1api.VolumeSnapshotContentStatus{SnapshotHandle: &handle}),
		},
		{
			name:        "gone from the cluster, no status in the backup",
			item:        backupVSC(nil),
			wantWarning: "May leave orphan snapshot <unknown> on cloud provider.",
		},
		{
			name:      "policy change fails",
			item:      backupVSC(&snapshotv1api.VolumeSnapshotContentStatus{SnapshotHandle: &handle}),
			inCluster: true,
			errClient: &fakeClientWithErrors{patchError: errors.New("patch refused")},
			wantErr:   true,
		},
		{
			name:      "delete fails",
			item:      backupVSC(&snapshotv1api.VolumeSnapshotContentStatus{SnapshotHandle: &handle}),
			inCluster: true,
			errClient: &fakeClientWithErrors{deleteError: errors.New("delete refused")},
			wantErr:   true,
		},
	}
	previousSleep := sleepBetweenTempVSCCreateAndDelete
	sleepBetweenTempVSCCreateAndDelete = func(time.Duration) {}
	t.Cleanup(func() { sleepBetweenTempVSCCreateAndDelete = previousSleep })
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var objects []runtime.Object
			if test.inCluster {
				objects = append(objects, test.item.DeepCopy())
			}
			fakeClient := velerotest.NewFakeControllerRuntimeClient(t, objects...)
			var client crclient.Client = fakeClient
			if test.errClient != nil {
				test.errClient.Client = fakeClient
				client = test.errClient
			}
			logger, hook := logrustest.NewNullLogger()
			p := volumeSnapshotContentDeleteItemAction{log: logger, crClient: client}
			item, err := runtime.DefaultUnstructuredConverter.ToUnstructured(test.item)
			require.NoError(t, err)

			err = p.Execute(&velero.DeleteItemActionExecuteInput{
				Item:   &unstructured.Unstructured{Object: item},
				Backup: builder.ForBackup("velero", "backup").Result(),
			})

			if test.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			inCluster := new(snapshotv1api.VolumeSnapshotContent)
			getErr := fakeClient.Get(t.Context(), crclient.ObjectKey{Name: "snapcontent-1"}, inCluster)
			if test.wantDeleted {
				require.True(t, apierrors.IsNotFound(getErr), "the VolumeSnapshotContent is deleted")
			} else if test.inCluster {
				require.NoError(t, getErr, "the VolumeSnapshotContent is kept")
			}
			if test.wantWarning != "" {
				require.Contains(t, hook.LastEntry().Message, test.wantWarning)
				require.Equal(t, logrus.WarnLevel, hook.LastEntry().Level)
			}
		})
	}
}

// The policy is Delete when the VolumeSnapshotContent is deleted, so the CSI
// snapshot controller also deletes the snapshot on the storage provider.
func TestVSCExecuteDeletesTheSnapshotWithTheContent(t *testing.T) {
	handle := "snap-handle-1"
	vsc := builder.ForVolumeSnapshotContent("snapcontent-1").
		ObjectMeta(builder.WithLabelsMap(map[string]string{velerov1api.BackupNameLabel: "backup"})).
		Status(&snapshotv1api.VolumeSnapshotContentStatus{SnapshotHandle: &handle}).Result()
	vsc.Spec.DeletionPolicy = snapshotv1api.VolumeSnapshotContentRetain
	fakeClient := velerotest.NewFakeControllerRuntimeClient(t, vsc.DeepCopy())
	client := &policyAtDelete{Client: fakeClient}
	p := volumeSnapshotContentDeleteItemAction{log: logrus.StandardLogger(), crClient: client}
	item, err := runtime.DefaultUnstructuredConverter.ToUnstructured(vsc)
	require.NoError(t, err)

	require.NoError(t, p.Execute(&velero.DeleteItemActionExecuteInput{
		Item:   &unstructured.Unstructured{Object: item},
		Backup: builder.ForBackup("velero", "backup").Result(),
	}))

	require.Equal(t, []snapshotv1api.DeletionPolicy{snapshotv1api.VolumeSnapshotContentDelete}, client.policies)
}

// policyAtDelete records the in-cluster policy of each VolumeSnapshotContent it deletes.
type policyAtDelete struct {
	crclient.Client
	policies []snapshotv1api.DeletionPolicy
}

func (c *policyAtDelete) Delete(ctx context.Context, obj crclient.Object, opts ...crclient.DeleteOption) error {
	current := new(snapshotv1api.VolumeSnapshotContent)
	if err := c.Client.Get(ctx, crclient.ObjectKeyFromObject(obj), current); err == nil {
		c.policies = append(c.policies, current.Spec.DeletionPolicy)
	}
	return c.Client.Delete(ctx, obj, opts...)
}

func TestVSCAppliesTo(t *testing.T) {
	p := volumeSnapshotContentDeleteItemAction{
		log: logrus.StandardLogger(),
	}
	selector, err := p.AppliesTo()

	require.NoError(t, err)

	require.Equal(
		t,
		velero.ResourceSelector{
			IncludedResources: []string{"volumesnapshotcontents.snapshot.storage.k8s.io"},
		},
		selector,
	)
}

func TestNewVolumeSnapshotContentDeleteItemAction(t *testing.T) {
	logger := logrus.StandardLogger()
	crClient := velerotest.NewFakeControllerRuntimeClient(t)

	f := &factorymocks.Factory{}
	f.On("KubebuilderClient").Return(nil, fmt.Errorf(""))
	plugin := NewVolumeSnapshotContentDeleteItemAction(f)
	_, err := plugin(logger)
	require.Error(t, err)

	f1 := &factorymocks.Factory{}
	f1.On("KubebuilderClient").Return(crClient, nil)
	plugin1 := NewVolumeSnapshotContentDeleteItemAction(f1)
	_, err1 := plugin1(logger)
	require.NoError(t, err1)
}

func TestCheckVSCReadiness(t *testing.T) {
	tests := []struct {
		name      string
		vsc       *snapshotv1api.VolumeSnapshotContent
		createVSC bool
		expectErr bool
		ready     bool
	}{
		{
			name: "VSC not exist",
			vsc: &snapshotv1api.VolumeSnapshotContent{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "vsc-1",
					Namespace: "velero",
				},
			},
			createVSC: false,
			expectErr: true,
			ready:     false,
		},
		{
			name: "VSC not ready",
			vsc: &snapshotv1api.VolumeSnapshotContent{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "vsc-1",
					Namespace: "velero",
				},
			},
			createVSC: true,
			expectErr: false,
			ready:     false,
		},
		{
			name: "VSC has error from CSI driver",
			vsc: &snapshotv1api.VolumeSnapshotContent{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "vsc-1",
					Namespace: "velero",
				},
				Status: &snapshotv1api.VolumeSnapshotContentStatus{
					ReadyToUse: boolPtr(false),
					Error: &snapshotv1api.VolumeSnapshotError{
						Message: stringPtr("InvalidSnapshot.NotFound: The snapshot 'snap-0abc123' does not exist."),
					},
				},
			},
			createVSC: true,
			expectErr: true,
			ready:     false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			crClient := velerotest.NewFakeControllerRuntimeClient(t)
			if test.createVSC {
				require.NoError(t, crClient.Create(t.Context(), test.vsc))
			}

			ready, err := checkVSCReadiness(t.Context(), test.vsc, crClient)
			require.Equal(t, test.ready, ready)
			if test.expectErr {
				require.Error(t, err)
			}
		})
	}
}

func TestVSCExecute_CreateSleepDeleteOrder(t *testing.T) {
	snapshotHandleStr := "test"
	vsc := builder.ForVolumeSnapshotContent("bar").
		ObjectMeta(builder.WithLabelsMap(map[string]string{velerov1api.BackupNameLabel: "backup"})).
		Status(&snapshotv1api.VolumeSnapshotContentStatus{SnapshotHandle: &snapshotHandleStr}).
		Result()

	vscMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(vsc)
	require.NoError(t, err)

	events := make([]string, 0, 3)
	realClient := velerotest.NewFakeControllerRuntimeClient(t)
	trackingClient := &fakeClientWithCallTracking{Client: realClient, events: &events}

	originalSleep := sleepBetweenTempVSCCreateAndDelete
	t.Cleanup(func() {
		sleepBetweenTempVSCCreateAndDelete = originalSleep
	})

	sleepBetweenTempVSCCreateAndDelete = func(d time.Duration) {
		require.Equal(t, tempVSCCreateDeleteGap, d)
		events = append(events, "sleep")
	}

	p := volumeSnapshotContentDeleteItemAction{log: logrus.StandardLogger(), crClient: trackingClient}
	err = p.Execute(&velero.DeleteItemActionExecuteInput{
		Item:   &unstructured.Unstructured{Object: vscMap},
		Backup: builder.ForBackup("velero", "backup").Result(),
	})
	require.NoError(t, err)
	require.Equal(t, []string{"create", "sleep", "delete"}, events)
}

func boolPtr(b bool) *bool {
	return &b
}

func stringPtr(s string) *string {
	return &s
}

// A content that is gone from the cluster is re-created from the backup under
// a temporary name with policy Delete and the backed-up snapshot handle, and
// deleted, so the CSI driver deletes the storage snapshot.
func TestVSCExecuteDeletesTheSnapshotOfAGoneContent(t *testing.T) {
	handle, class := "snap-handle-1", "class-1"
	vsc := builder.ForVolumeSnapshotContent("snapcontent-1").
		ObjectMeta(builder.WithLabelsMap(map[string]string{velerov1api.BackupNameLabel: "backup"})).
		VolumeSnapshotClassName(class).
		Status(&snapshotv1api.VolumeSnapshotContentStatus{SnapshotHandle: &handle}).Result()
	vsc.Spec.DeletionPolicy = snapshotv1api.VolumeSnapshotContentRetain
	vsc.UID = "uid-1"
	client := &recordingClient{Client: velerotest.NewFakeControllerRuntimeClient(t)}
	previousSleep := sleepBetweenTempVSCCreateAndDelete
	sleepBetweenTempVSCCreateAndDelete = func(time.Duration) {}
	t.Cleanup(func() { sleepBetweenTempVSCCreateAndDelete = previousSleep })
	p := volumeSnapshotContentDeleteItemAction{log: logrus.StandardLogger(), crClient: client}
	item, err := runtime.DefaultUnstructuredConverter.ToUnstructured(vsc)
	require.NoError(t, err)

	require.NoError(t, p.Execute(&velero.DeleteItemActionExecuteInput{
		Item:   &unstructured.Unstructured{Object: item},
		Backup: builder.ForBackup("velero", "backup").Result(),
	}))

	require.Len(t, client.created, 1)
	temp := client.created[0]
	require.Regexp(t, `^vsc-[0-9a-f-]{36}$`, temp.Name)
	require.Equal(t, snapshotv1api.VolumeSnapshotContentDelete, temp.Spec.DeletionPolicy)
	require.Equal(t, handle, *temp.Spec.Source.SnapshotHandle)
	require.Nil(t, temp.Spec.Source.VolumeHandle)
	require.Nil(t, temp.Spec.VolumeSnapshotClassName)
	require.Equal(t, "name-uid-1", temp.Spec.VolumeSnapshotRef.Name)
	require.Equal(t, "ns-uid-1", temp.Spec.VolumeSnapshotRef.Namespace)
	require.Equal(t, []string{temp.Name}, client.deleted)
}

// recordingClient records the VolumeSnapshotContents created and deleted.
type recordingClient struct {
	crclient.Client
	created []snapshotv1api.VolumeSnapshotContent
	deleted []string
}

func (c *recordingClient) Create(ctx context.Context, obj crclient.Object, opts ...crclient.CreateOption) error {
	if vsc, ok := obj.(*snapshotv1api.VolumeSnapshotContent); ok {
		c.created = append(c.created, *vsc.DeepCopy())
	}
	return c.Client.Create(ctx, obj, opts...)
}

func (c *recordingClient) Delete(ctx context.Context, obj crclient.Object, opts ...crclient.DeleteOption) error {
	c.deleted = append(c.deleted, obj.GetName())
	return c.Client.Delete(ctx, obj, opts...)
}

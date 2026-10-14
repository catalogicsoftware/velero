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
	"testing"
	"time"

	volumegroupsnapshotv1beta2 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumegroupsnapshot/v1beta2"
	snapshotv1api "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"

	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/builder"
	"github.com/vmware-tanzu/velero/pkg/plugin/velero"
	velerotest "github.com/vmware-tanzu/velero/pkg/test"
)

func groupMemberContent(name, handle, members string) *snapshotv1api.VolumeSnapshotContent {
	group := "group-1"
	vsc := builder.ForVolumeSnapshotContent(name).
		ObjectMeta(builder.WithLabelsMap(map[string]string{velerov1api.BackupNameLabel: "backup"})).
		Driver("hostpath.csi.k8s.io").
		VolumeSnapshotRef("ns", "vs-"+name, "").
		Status(&snapshotv1api.VolumeSnapshotContentStatus{SnapshotHandle: &handle, VolumeGroupSnapshotHandle: &group}).Result()
	if members != "" {
		vsc.Annotations = map[string]string{velerov1api.VolumeGroupSnapshotMemberHandlesAnnotation: members}
	}
	return vsc
}

func executeVSCDelete(t *testing.T, p *volumeSnapshotContentDeleteItemAction, vsc *snapshotv1api.VolumeSnapshotContent) {
	t.Helper()
	item, err := runtime.DefaultUnstructuredConverter.ToUnstructured(vsc)
	require.NoError(t, err)
	require.NoError(t, p.Execute(&velero.DeleteItemActionExecuteInput{
		Item:   &unstructured.Unstructured{Object: item},
		Backup: builder.ForBackup("velero", "backup").Result(),
	}))
}

func groupStubs(t *testing.T, p *volumeSnapshotContentDeleteItemAction) []volumegroupsnapshotv1beta2.VolumeGroupSnapshotContent {
	t.Helper()
	stubs := &volumegroupsnapshotv1beta2.VolumeGroupSnapshotContentList{}
	require.NoError(t, p.crClient.List(t.Context(), stubs))
	return stubs.Items
}

// A group member's snapshot cannot be deleted on its own: its group is left to
// one stub group content (policy Delete, all members), and its content object
// goes as Retain, so the snapshotter never tries the member alone.
func TestVSCExecuteLeavesAGroupMemberToItsGroup(t *testing.T) {
	inCluster := groupMemberContent("snapcontent-a", "snap-a", "")
	inCluster.Spec.DeletionPolicy = snapshotv1api.VolumeSnapshotContentRetain
	client := &recordingClient{Client: velerotest.NewFakeControllerRuntimeClient(t, inCluster)}
	policies := &policyAtDelete{Client: client}
	p := &volumeSnapshotContentDeleteItemAction{log: logrus.StandardLogger(), crClient: policies}
	previousSleep := sleepBetweenTempVSCCreateAndDelete
	sleepBetweenTempVSCCreateAndDelete = func(time.Duration) {}
	t.Cleanup(func() { sleepBetweenTempVSCCreateAndDelete = previousSleep })

	executeVSCDelete(t, p, groupMemberContent("snapcontent-a", "snap-a", `["snap-a","snap-b"]`))
	executeVSCDelete(t, p, groupMemberContent("snapcontent-b", "snap-b", `["snap-a","snap-b"]`))

	stubs := groupStubs(t, p)
	require.Len(t, stubs, 1)
	stub := stubs[0]
	assert.Equal(t, snapshotv1api.VolumeSnapshotContentDelete, stub.Spec.DeletionPolicy)
	assert.Equal(t, "hostpath.csi.k8s.io", stub.Spec.Driver)
	assert.Equal(t, "group-1", stub.Spec.Source.GroupSnapshotHandles.VolumeGroupSnapshotHandle)
	assert.Equal(t, []string{"snap-a", "snap-b"}, stub.Spec.Source.GroupSnapshotHandles.VolumeSnapshotHandles)
	assert.Equal(t, "backup", stub.Labels[velerov1api.BackupNameLabel])
	assert.Contains(t, stub.Finalizers, "groupsnapshot.storage.kubernetes.io/volumegroupsnapshotcontent-bound-protection")
	assert.Equal(t, "ns", stub.Spec.VolumeGroupSnapshotRef.Namespace)
	assert.Empty(t, stub.Spec.VolumeGroupSnapshotRef.UID)

	assert.Empty(t, client.created, "no temporary content for a group member")
	assert.Equal(t, []snapshotv1api.DeletionPolicy{snapshotv1api.VolumeSnapshotContentRetain}, policies.policies)
	assert.Empty(t, groupMemberContents(t, p))
}

// A backup made before the members were recorded on the content gets them
// from the contents in the cluster that report the same group.
func TestVSCExecuteFindsTheGroupMembersInTheCluster(t *testing.T) {
	restoredCopy := groupMemberContent("restored-b", "snap-b", "")
	restoredCopy.Labels = nil
	p := &volumeSnapshotContentDeleteItemAction{
		log:      logrus.StandardLogger(),
		crClient: velerotest.NewFakeControllerRuntimeClient(t, groupMemberContent("snapcontent-a", "snap-a", ""), restoredCopy),
	}

	executeVSCDelete(t, p, groupMemberContent("snapcontent-a", "snap-a", ""))

	stubs := groupStubs(t, p)
	require.Len(t, stubs, 1)
	assert.Equal(t, []string{"snap-a", "snap-b"}, stubs[0].Spec.Source.GroupSnapshotHandles.VolumeSnapshotHandles)
}

func groupMemberContents(t *testing.T, p *volumeSnapshotContentDeleteItemAction) []string {
	t.Helper()
	list := &snapshotv1api.VolumeSnapshotContentList{}
	require.NoError(t, p.crClient.List(t.Context(), list))
	var names []string
	for _, vsc := range list.Items {
		names = append(names, vsc.Name)
	}
	return names
}

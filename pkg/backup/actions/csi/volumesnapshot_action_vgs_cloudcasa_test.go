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

	snapshotv1api "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"

	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/builder"
	velerotest "github.com/vmware-tanzu/velero/pkg/test"
)

func groupMemberVSC(name, snapshotHandle, groupHandle string) *snapshotv1api.VolumeSnapshotContent {
	return builder.ForVolumeSnapshotContent(name).Status(&snapshotv1api.VolumeSnapshotContentStatus{
		SnapshotHandle:            &snapshotHandle,
		VolumeGroupSnapshotHandle: &groupHandle,
	}).Result()
}

// A restore re-imports the group snapshot with all its members, so the backup
// records every member's snapshot handle on each member VolumeSnapshot.
func TestVSExecuteRecordsGroupMemberHandles(t *testing.T) {
	vs := builder.ForVolumeSnapshot("ns", "vs-a").
		ObjectMeta(builder.WithLabels(velerov1api.BackupNameLabel, "backup")).
		Status().BoundVolumeSnapshotContentName("vsc-a").Result()
	staticHandle := "snap-b"
	group1 := "group-1"
	restoredCopy := builder.ForVolumeSnapshotContent("vsc-b-restored").Status(&snapshotv1api.VolumeSnapshotContentStatus{
		VolumeGroupSnapshotHandle: &group1,
	}).Result()
	restoredCopy.Spec.Source.SnapshotHandle = &staticHandle
	client := velerotest.NewFakeControllerRuntimeClient(t, vs,
		groupMemberVSC("vsc-a", "snap-a", "group-1"),
		groupMemberVSC("vsc-b", "snap-b", "group-1"),
		restoredCopy,
		groupMemberVSC("vsc-c", "snap-c", "group-2"),
		builder.ForVolumeSnapshotContent("vsc-d").Result(),
	)
	vsBIA := volumeSnapshotBackupItemAction{log: logrus.New(), crClient: client}
	item, err := runtime.DefaultUnstructuredConverter.ToUnstructured(vs)
	require.NoError(t, err)

	out, _, _, _, err := vsBIA.Execute(&unstructured.Unstructured{Object: item},
		builder.ForBackup("velero", "backup").Phase(velerov1api.BackupPhaseInProgress).Result())
	require.NoError(t, err)

	want := `["snap-a","snap-b"]`
	backedUp := new(snapshotv1api.VolumeSnapshot)
	require.NoError(t, runtime.DefaultUnstructuredConverter.FromUnstructured(out.UnstructuredContent(), backedUp))
	assert.Equal(t, want, backedUp.Annotations[velerov1api.VolumeGroupSnapshotMemberHandlesAnnotation])
	inCluster := new(snapshotv1api.VolumeSnapshot)
	require.NoError(t, client.Get(t.Context(), crclient.ObjectKeyFromObject(vs), inCluster))
	assert.Equal(t, want, inCluster.Annotations[velerov1api.VolumeGroupSnapshotMemberHandlesAnnotation])
	// The content carries them too: a backup deletion sees only the content.
	content := new(snapshotv1api.VolumeSnapshotContent)
	require.NoError(t, client.Get(t.Context(), crclient.ObjectKey{Name: "vsc-a"}, content))
	assert.Equal(t, want, content.Annotations[velerov1api.VolumeGroupSnapshotMemberHandlesAnnotation])
	assert.Equal(t, "backup", content.Labels[velerov1api.BackupNameLabel])
}

func TestVSExecuteNoGroupMemberHandlesOutsideAGroup(t *testing.T) {
	handle := "snap-a"
	vs := builder.ForVolumeSnapshot("ns", "vs-a").
		ObjectMeta(builder.WithLabels(velerov1api.BackupNameLabel, "backup")).
		Status().BoundVolumeSnapshotContentName("vsc-a").Result()
	vsc := builder.ForVolumeSnapshotContent("vsc-a").
		Status(&snapshotv1api.VolumeSnapshotContentStatus{SnapshotHandle: &handle}).Result()
	vsBIA := volumeSnapshotBackupItemAction{log: logrus.New(), crClient: velerotest.NewFakeControllerRuntimeClient(t, vs, vsc)}
	item, err := runtime.DefaultUnstructuredConverter.ToUnstructured(vs)
	require.NoError(t, err)

	out, _, _, _, err := vsBIA.Execute(&unstructured.Unstructured{Object: item},
		builder.ForBackup("velero", "backup").Phase(velerov1api.BackupPhaseInProgress).Result())
	require.NoError(t, err)

	backedUp := new(snapshotv1api.VolumeSnapshot)
	require.NoError(t, runtime.DefaultUnstructuredConverter.FromUnstructured(out.UnstructuredContent(), backedUp))
	assert.NotContains(t, backedUp.Annotations, velerov1api.VolumeGroupSnapshotMemberHandlesAnnotation)
}

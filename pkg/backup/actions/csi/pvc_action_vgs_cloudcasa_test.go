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

	volumegroupsnapshotv1beta2 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumegroupsnapshot/v1beta2"
	snapshotv1api "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1api "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/vmware-tanzu/velero/internal/catalogic"
	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/builder"
	"github.com/vmware-tanzu/velero/pkg/kuberesource"
	velerotest "github.com/vmware-tanzu/velero/pkg/test"
)

// The snapshot controller writes the owner reference of a member VolumeSnapshot
// in the API version it serves: v1 since external-snapshotter v8.6.0, v1beta2
// or v1beta1 before. The member belongs to the group whatever that version is.
func TestHasOwnerReferenceAcceptsEveryGroupSnapshotAPIVersion(t *testing.T) {
	vgs := &volumegroupsnapshotv1beta2.VolumeGroupSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "velero-demo-6tcs4", Namespace: "ns", UID: "de0f9a41"},
	}
	for _, version := range []string{"v1", "v1beta2", "v1beta1"} {
		t.Run(version, func(t *testing.T) {
			obj := &metav1.ObjectMeta{OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "groupsnapshot.storage.k8s.io/" + version,
				Kind:       kuberesource.VGSKind,
				Name:       vgs.Name,
				UID:        vgs.UID,
			}}}
			assert.True(t, hasOwnerReference(obj, vgs))
		})
	}
}

// The PVC that created the group goes on with the VolumeSnapshot from the map;
// CloudCasa's snapshot wait needs the backup label the update just added.
func TestUpdateVGSCreatedVSKeepsTheUpdatedSnapshotInTheMap(t *testing.T) {
	backup := &velerov1api.Backup{ObjectMeta: metav1.ObjectMeta{Name: "6acdc8986f5eb06abf282035", UID: "backup-uid"}}
	vgs := &volumegroupsnapshotv1beta2.VolumeGroupSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "velero-demo-kmgrr", Namespace: "ns", UID: "vgs-uid"},
	}
	vs := &snapshotv1api.VolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "snapshot-c6eb",
			Namespace: "ns",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "groupsnapshot.storage.k8s.io/v1", Kind: kuberesource.VGSKind, Name: vgs.Name, UID: vgs.UID,
			}},
		},
		Spec:   snapshotv1api.VolumeSnapshotSpec{Source: snapshotv1api.VolumeSnapshotSource{PersistentVolumeClaimName: ptr.To("group-pvc-a")}},
		Status: &snapshotv1api.VolumeSnapshotStatus{VolumeGroupSnapshotName: ptr.To(vgs.Name), ReadyToUse: ptr.To(true)},
	}
	action := &pvcBackupItemAction{log: velerotest.NewLogger(), crClient: velerotest.NewFakeControllerRuntimeClient(t, vgs, vs)}
	vsMap := map[string]*snapshotv1api.VolumeSnapshot{"group-pvc-a": vs.DeepCopy()}

	require.NoError(t, action.updateVGSCreatedVS(t.Context(), vsMap, vgs, backup))

	assert.Equal(t, backup.Name, vsMap["group-pvc-a"].Labels[velerov1api.BackupNameLabel])
	assert.Empty(t, vsMap["group-pvc-a"].OwnerReferences)
}

// A grouped PVC that CloudCasa backs up without a snapshot (LIVE, SKIP) is
// snapshotted anyway: the group selects its PVCs by label. That is a
// misconfiguration the backup warns of, naming the PVC.
func TestGroupWarnsOfAGroupedPVCBackedUpWithoutASnapshot(t *testing.T) {
	withPluginConfig(t, &catalogic.PluginConfig{SnapshotWherePossible: true})
	logger, hook := logrustest.NewNullLogger()
	p := &pvcBackupItemAction{log: logger, crClient: velerotest.NewFakeControllerRuntimeClient(t)}
	pvcs := []corev1api.PersistentVolumeClaim{
		*builder.ForPersistentVolumeClaim("ns", "pvc-a").StorageClass("sc").Result(),
		*builder.ForPersistentVolumeClaim("ns", "pvc-b").StorageClass("sc").
			ObjectMeta(builder.WithAnnotations("cloudcasa.io/backup-method", "LIVE")).Result(),
	}

	skipped := p.groupedPVCsNotSnapshotted(pvcs, "hostpath.csi.k8s.io", "cloudcasa.io/volume-group", "demo", "backup-1")

	assert.Equal(t, map[string]bool{"pvc-b": true}, skipped)

	var warnings []string
	for _, entry := range hook.AllEntries() {
		if entry.Level == logrus.WarnLevel {
			warnings = append(warnings, entry.Message)
		}
	}
	require.Len(t, warnings, 1)
	assert.Contains(t, warnings[0], "ns/pvc-b")
	assert.Contains(t, warnings[0], "cloudcasa.io/volume-group=demo")
}

// CloudCasa marks the default group snapshot class of a driver with its own
// label; a class with only Velero's label is not picked.
func TestDetermineVGSClassUsesTheCloudCasaLabel(t *testing.T) {
	class := func(name string, labels map[string]string) *volumegroupsnapshotv1beta2.VolumeGroupSnapshotClass {
		return &volumegroupsnapshotv1beta2.VolumeGroupSnapshotClass{
			ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels},
			Driver:     "hostpath.csi.k8s.io",
		}
	}
	backup := builder.ForBackup("velero", "backup-1").Result()
	pvc := builder.ForPersistentVolumeClaim("ns", "pvc-a").Result()

	p := &pvcBackupItemAction{log: logrus.New(), crClient: velerotest.NewFakeControllerRuntimeClient(t,
		class("velero-labelled", map[string]string{"velero.io/csi-volumegroupsnapshot-class": "true"}),
		class("cloudcasa-labelled", map[string]string{"cloudcasa.io/csi-volumegroupsnapshot-class": "true"}))}
	name, err := p.determineVGSClass(t.Context(), "hostpath.csi.k8s.io", backup, pvc)
	require.NoError(t, err)
	assert.Equal(t, "cloudcasa-labelled", name)

	p = &pvcBackupItemAction{log: logrus.New(), crClient: velerotest.NewFakeControllerRuntimeClient(t,
		class("velero-labelled", map[string]string{"velero.io/csi-volumegroupsnapshot-class": "true"}))}
	_, err = p.determineVGSClass(t.Context(), "hostpath.csi.k8s.io", backup, pvc)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cloudcasa.io/csi-volumegroupsnapshot-class=true")
}

func groupMember(name, pvcName, content string) *snapshotv1api.VolumeSnapshot {
	vs := builder.ForVolumeSnapshot("ns", name).SourcePVC(pvcName).Status().BoundVolumeSnapshotContentName(content).Result()
	group := "velero-demo-x"
	vs.Status.VolumeGroupSnapshotName = &group
	return vs
}

func groupMemberContentObject(name, handle string) *snapshotv1api.VolumeSnapshotContent {
	groupHandle := "group-1"
	vsc := builder.ForVolumeSnapshotContent(name).DeletionPolicy(snapshotv1api.VolumeSnapshotContentDelete).
		Status(&snapshotv1api.VolumeSnapshotContentStatus{SnapshotHandle: &handle, VolumeGroupSnapshotHandle: &groupHandle}).Result()
	vsc.Finalizers = []string{"snapshot.storage.kubernetes.io/volumesnapshotcontent-bound-protection"}
	return vsc
}

func readyGroup(infos ...volumegroupsnapshotv1beta2.VolumeSnapshotInfo) (*volumegroupsnapshotv1beta2.VolumeGroupSnapshot, *volumegroupsnapshotv1beta2.VolumeGroupSnapshotContent) {
	contentName := "groupcontent-1"
	vgs := &volumegroupsnapshotv1beta2.VolumeGroupSnapshot{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "velero-demo-x"},
		Status:     &volumegroupsnapshotv1beta2.VolumeGroupSnapshotStatus{BoundVolumeGroupSnapshotContentName: &contentName},
	}
	vgsc := &volumegroupsnapshotv1beta2.VolumeGroupSnapshotContent{
		ObjectMeta: metav1.ObjectMeta{Name: contentName},
		Status:     &volumegroupsnapshotv1beta2.VolumeGroupSnapshotContentStatus{VolumeSnapshotInfoList: infos},
	}
	return vgs, vgsc
}

// The group content lists every member the storage snapshotted, so that list is
// recorded on each member. A member CloudCasa backs up without a snapshot keeps
// no snapshot objects; its storage snapshot stays with the group, which the list
// still names.
func TestSettleGroupMembersRecordsTheMembersAndDropsASkippedOne(t *testing.T) {
	vgs, vgsc := readyGroup(
		volumegroupsnapshotv1beta2.VolumeSnapshotInfo{VolumeHandle: "vol-b", SnapshotHandle: "snap-b"},
		volumegroupsnapshotv1beta2.VolumeSnapshotInfo{VolumeHandle: "vol-a", SnapshotHandle: "snap-a"})
	vsA, vsB := groupMember("vs-a", "pvc-a", "content-a"), groupMember("vs-b", "pvc-b", "content-b")
	client := velerotest.NewFakeControllerRuntimeClient(t, vgs, vgsc, vsA, vsB,
		groupMemberContentObject("content-a", "snap-a"), groupMemberContentObject("content-b", "snap-b"))
	p := &pvcBackupItemAction{log: logrus.New(), crClient: client}
	vsMap := map[string]*snapshotv1api.VolumeSnapshot{"pvc-a": vsA, "pvc-b": vsB}

	require.NoError(t, p.settleGroupMembers(t.Context(), vgs, vsMap, map[string]bool{"pvc-b": true}))

	want := `["snap-a","snap-b"]`
	kept := &snapshotv1api.VolumeSnapshot{}
	require.NoError(t, client.Get(t.Context(), crclient.ObjectKeyFromObject(vsA), kept))
	assert.Equal(t, want, kept.Annotations[velerov1api.VolumeGroupSnapshotMemberHandlesAnnotation])
	assert.Equal(t, want, vsMap["pvc-a"].Annotations[velerov1api.VolumeGroupSnapshotMemberHandlesAnnotation])
	assert.NotContains(t, vsMap, "pvc-b")
	assert.True(t, apierrors.IsNotFound(client.Get(t.Context(), crclient.ObjectKeyFromObject(vsB), &snapshotv1api.VolumeSnapshot{})))
	assert.True(t, apierrors.IsNotFound(client.Get(t.Context(), crclient.ObjectKey{Name: "content-b"}, &snapshotv1api.VolumeSnapshotContent{})))
	contentA := &snapshotv1api.VolumeSnapshotContent{}
	require.NoError(t, client.Get(t.Context(), crclient.ObjectKey{Name: "content-a"}, contentA))
	assert.Equal(t, snapshotv1api.VolumeSnapshotContentDelete, contentA.Spec.DeletionPolicy)
}

// Without a member list from the group content, nothing is recorded and a
// skipped member keeps its objects: they are then the only record of its
// snapshot for the group's later deletion.
func TestSettleGroupMembersKeepsEverythingWithoutAMemberList(t *testing.T) {
	vgs, vgsc := readyGroup()
	vsA, vsB := groupMember("vs-a", "pvc-a", "content-a"), groupMember("vs-b", "pvc-b", "content-b")
	client := velerotest.NewFakeControllerRuntimeClient(t, vgs, vgsc, vsA, vsB,
		groupMemberContentObject("content-a", "snap-a"), groupMemberContentObject("content-b", "snap-b"))
	p := &pvcBackupItemAction{log: logrus.New(), crClient: client}
	vsMap := map[string]*snapshotv1api.VolumeSnapshot{"pvc-a": vsA, "pvc-b": vsB}

	require.NoError(t, p.settleGroupMembers(t.Context(), vgs, vsMap, map[string]bool{"pvc-b": true}))

	kept := &snapshotv1api.VolumeSnapshot{}
	require.NoError(t, client.Get(t.Context(), crclient.ObjectKeyFromObject(vsA), kept))
	assert.NotContains(t, kept.Annotations, velerov1api.VolumeGroupSnapshotMemberHandlesAnnotation)
	require.NoError(t, client.Get(t.Context(), crclient.ObjectKeyFromObject(vsB), &snapshotv1api.VolumeSnapshot{}))
	require.NoError(t, client.Get(t.Context(), crclient.ObjectKey{Name: "content-b"}, &snapshotv1api.VolumeSnapshotContent{}))
}

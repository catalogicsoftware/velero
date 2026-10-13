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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

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

	p.warnOfGroupedPVCsNotSnapshotted(pvcs, "hostpath.csi.k8s.io", "cloudcasa.io/volume-group", "demo", "backup-1")

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

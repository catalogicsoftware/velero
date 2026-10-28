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

	snapshotv1api "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"

	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/builder"
	velerotest "github.com/vmware-tanzu/velero/pkg/test"
)

// A member of a group snapshot keeps the snapshotter's bound-protection
// finalizer after its VolumeSnapshot is deleted: nothing in the cluster removes
// it. Keeping the content (D1) must still finish, and well before the timeout.
func TestDeleteVolumeSnapshotKeepsTheContentOfAGroupMember(t *testing.T) {
	handle, groupHandle := "3f2e1f45", "3f2e1ede"
	vs := builder.ForVolumeSnapshot("ns", "snapshot-6912").
		ObjectMeta(builder.WithLabels(velerov1api.BackupNameLabel, "backup-1")).
		Status().BoundVolumeSnapshotContentName("snapcontent-6912").Result()
	vsc := builder.ForVolumeSnapshotContent("snapcontent-6912").
		ObjectMeta(builder.WithLabels(velerov1api.BackupNameLabel, "backup-1"),
			builder.WithAnnotations("cc-pvc-name", "group-pvc-b", "cc-pvc-namespace", "ns")).
		DeletionPolicy(snapshotv1api.VolumeSnapshotContentDelete).
		VolumeSnapshotRef("ns", "snapshot-6912", "uid-1").
		Status(&snapshotv1api.VolumeSnapshotContentStatus{SnapshotHandle: &handle, VolumeGroupSnapshotHandle: &groupHandle}).Result()
	vsc.Finalizers = []string{"snapshot.storage.kubernetes.io/volumesnapshotcontent-bound-protection"}
	backup := builder.ForBackup("velero", "backup-1").ObjectMeta(
		builder.WithAnnotations(velerov1api.ResourceTimeoutAnnotation, "3s")).Result()
	client := velerotest.NewFakeControllerRuntimeClient(t, vs, vsc)

	start := time.Now()
	DeleteVolumeSnapshot(*vs, *vsc, backup, client, logrus.New())
	assert.Less(t, time.Since(start), 2*time.Second, "keeping the content waited for a finalizer nobody removes")

	kept := &snapshotv1api.VolumeSnapshotContent{}
	require.NoError(t, client.Get(t.Context(), crclient.ObjectKey{Name: "snapcontent-6912"}, kept))
	assert.Nil(t, kept.DeletionTimestamp)
	assert.Equal(t, snapshotv1api.VolumeSnapshotContentRetain, kept.Spec.DeletionPolicy)
	require.NotNil(t, kept.Spec.Source.SnapshotHandle)
	assert.Equal(t, handle, *kept.Spec.Source.SnapshotHandle)
	assert.Equal(t, "group-pvc-b", kept.Annotations["cc-pvc-name"])
}

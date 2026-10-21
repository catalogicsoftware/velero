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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"

	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/builder"
	velerotest "github.com/vmware-tanzu/velero/pkg/test"
	"github.com/vmware-tanzu/velero/pkg/util"
)

func groupMemberVS(name, snapshotHandle, members string) *snapshotv1api.VolumeSnapshot {
	annotations := map[string]string{
		velerov1api.VolumeGroupSnapshotHandleAnnotation: "group-1",
		velerov1api.VolumeSnapshotHandleAnnotation:      snapshotHandle,
		velerov1api.DriverNameAnnotation:                "hostpath.csi.k8s.io",
	}
	if members != "" {
		annotations[velerov1api.VolumeGroupSnapshotMemberHandlesAnnotation] = members
	}
	return &snapshotv1api.VolumeSnapshot{ObjectMeta: metav1.ObjectMeta{
		Name: name, Namespace: "ns", Annotations: annotations,
	}}
}

func stubVGSCHandles(t *testing.T, client crclient.Client) []string {
	t.Helper()
	stub := &volumegroupsnapshotv1beta2.VolumeGroupSnapshotContent{}
	require.NoError(t, client.Get(t.Context(),
		crclient.ObjectKey{Name: util.GenerateSha256FromRestoreUIDAndVsName("restore-uid", "group-1")}, stub))
	return stub.Spec.Source.GroupSnapshotHandles.VolumeSnapshotHandles
}

// The stub's handles cannot change once it exists, and a driver may refuse a
// group whose handles are not all its members, so the first member lists them all.
func TestEnsureStubVGSCExistsListsAllGroupMembers(t *testing.T) {
	client := velerotest.NewFakeControllerRuntimeClient(t)
	p := &volumeSnapshotRestoreItemAction{log: logrus.StandardLogger(), crClient: client}
	restore := builder.ForRestore("velero", "restore").ObjectMeta(builder.WithUID("restore-uid")).Result()

	require.NoError(t, p.ensureStubVGSCExists(t.Context(), groupMemberVS("vs-a", "snap-a", `["snap-a","snap-b"]`), restore))
	assert.Equal(t, []string{"snap-a", "snap-b"}, stubVGSCHandles(t, client))

	require.NoError(t, p.ensureStubVGSCExists(t.Context(), groupMemberVS("vs-b", "snap-b", `["snap-a","snap-b"]`), restore))
	assert.Equal(t, []string{"snap-a", "snap-b"}, stubVGSCHandles(t, client))
}

func TestEnsureStubVGSCExistsWithoutGroupMemberList(t *testing.T) {
	for name, members := range map[string]string{
		"no list":             "",
		"unreadable list":     "snap-x,snap-y",
		"list without the VS": `["snap-x"]`,
	} {
		t.Run(name, func(t *testing.T) {
			client := velerotest.NewFakeControllerRuntimeClient(t)
			p := &volumeSnapshotRestoreItemAction{log: logrus.StandardLogger(), crClient: client}
			restore := builder.ForRestore("velero", "restore").ObjectMeta(builder.WithUID("restore-uid")).Result()

			require.NoError(t, p.ensureStubVGSCExists(t.Context(), groupMemberVS("vs-a", "snap-a", members), restore))
			assert.Contains(t, stubVGSCHandles(t, client), "snap-a")
		})
	}
}

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
	"errors"
	"testing"

	snapshotv1api "github.com/kubernetes-csi/external-snapshotter/client/v7/apis/volumesnapshot/v1"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"

	velerotest "github.com/vmware-tanzu/velero/pkg/test"
)

// conflictOnceClient fails the first Update with a conflict, the way the
// external snapshotter racing us on the same VolumeSnapshotContent does.
type conflictOnceClient struct {
	crclient.Client
	conflicts int
}

func (c *conflictOnceClient) Update(ctx context.Context, obj crclient.Object,
	opts ...crclient.UpdateOption) error {
	if c.conflicts > 0 {
		c.conflicts--
		return apierrors.NewConflict(
			schema.GroupResource{Group: "snapshot.storage.k8s.io", Resource: "volumesnapshotcontents"},
			obj.GetName(), errors.New("the object has been modified"))
	}
	return c.Client.Update(ctx, obj, opts...)
}

func annotationFixtures() (*snapshotv1api.VolumeSnapshot, *snapshotv1api.VolumeSnapshotContent) {
	pvcName := "csi-pvc"
	vs := &snapshotv1api.VolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "vs-1", Namespace: "test-csi-snapshot"},
		Spec: snapshotv1api.VolumeSnapshotSpec{
			Source: snapshotv1api.VolumeSnapshotSource{PersistentVolumeClaimName: &pvcName},
		},
	}
	vsc := &snapshotv1api.VolumeSnapshotContent{
		ObjectMeta: metav1.ObjectMeta{Name: "snapcontent-1"},
	}
	return vs, vsc
}

// A conflict the retry recovers from must not reach warn level: Velero counts
// warn entries into Backup.Status.Warnings, so a backup that fully succeeded
// would be reported to the user as having a warning.
func TestAnnotateVSCRecoveredConflictIsNotAWarning(t *testing.T) {
	vs, vsc := annotationFixtures()
	client := &conflictOnceClient{
		Client:    velerotest.NewFakeControllerRuntimeClient(t, vsc),
		conflicts: 1,
	}
	log, hook := test.NewNullLogger()
	log.SetLevel(logrus.DebugLevel)

	err := annotateVSCWithPVC(context.Background(), client,
		new(snapshotv1api.VolumeSnapshotContent), vsc.Name, vs, log)
	require.NoError(t, err, "the retry must absorb a single conflict")

	// logrus orders levels by severity descending, so anything at or below
	// WarnLevel numerically is warn or worse.
	for _, entry := range hook.AllEntries() {
		require.Greater(t, int(entry.Level), int(logrus.WarnLevel),
			"a recovered conflict was logged at %s: %q", entry.Level, entry.Message)
	}

	stored := new(snapshotv1api.VolumeSnapshotContent)
	require.NoError(t, client.Get(context.Background(), crclient.ObjectKey{Name: vsc.Name}, stored))
	require.Equal(t, "csi-pvc", stored.Annotations["cc-pvc-name"])
	require.Equal(t, "test-csi-snapshot", stored.Annotations["cc-pvc-namespace"])
}

// A conflict that outlasts the retry must still fail, so the caller reports it.
func TestAnnotateVSCUnrecoveredConflictFails(t *testing.T) {
	vs, vsc := annotationFixtures()
	client := &conflictOnceClient{
		Client:    velerotest.NewFakeControllerRuntimeClient(t, vsc),
		conflicts: 100,
	}
	log, _ := test.NewNullLogger()

	err := annotateVSCWithPVC(context.Background(), client,
		new(snapshotv1api.VolumeSnapshotContent), vsc.Name, vs, log)
	require.Error(t, err, "a conflict that never clears must surface")
	require.True(t, apierrors.IsConflict(err))
}

// Annotations already present are left alone and nothing is written.
func TestAnnotateVSCAlreadyAnnotatedWritesNothing(t *testing.T) {
	vs, vsc := annotationFixtures()
	vsc.Annotations = map[string]string{
		"cc-pvc-name":      "csi-pvc",
		"cc-pvc-namespace": "test-csi-snapshot",
	}
	client := &conflictOnceClient{
		Client:    velerotest.NewFakeControllerRuntimeClient(t, vsc),
		conflicts: 100, // any Update at all would fail the test
	}
	log, _ := test.NewNullLogger()

	require.NoError(t, annotateVSCWithPVC(context.Background(), client,
		new(snapshotv1api.VolumeSnapshotContent), vsc.Name, vs, log))
}

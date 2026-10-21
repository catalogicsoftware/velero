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
	"compress/gzip"
	"encoding/json"
	"io"
	"os"
	"testing"

	snapshotv1api "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	pkgbackup "github.com/vmware-tanzu/velero/pkg/backup"
	"github.com/vmware-tanzu/velero/pkg/builder"
	"github.com/vmware-tanzu/velero/pkg/persistence"
	persistencemocks "github.com/vmware-tanzu/velero/pkg/persistence/mocks"
	velerotest "github.com/vmware-tanzu/velero/pkg/test"
	"github.com/vmware-tanzu/velero/pkg/util/results"
)

// names decodes a gzipped JSON list of named objects.
func names[T any](t *testing.T, r io.Reader, name func(T) string) []string {
	t.Helper()
	require.NotNil(t, r, "the list is uploaded")
	gz, err := gzip.NewReader(r)
	require.NoError(t, err)
	var items []T
	require.NoError(t, json.NewDecoder(gz).Decode(&items))
	var out []string
	for _, item := range items {
		out = append(out, name(item))
	}
	return out
}

// CloudCasa's catalog lists a backup's CSI-snapshotted PVCs from the snapshot
// and content lists, so the backup uploads them as v1.14.0.x did.
func TestPersistBackupUploadsTheCSISnapshotLists(t *testing.T) {
	vs := builder.ForVolumeSnapshot("ns", "vs-1").ObjectMeta(builder.WithLabels(velerov1api.BackupNameLabel, "backup-1")).Result()
	vsc := builder.ForVolumeSnapshotContent("vsc-1").Result()
	class := builder.ForVolumeSnapshotClass("class-1").Driver("driver-1").Result()
	req := &pkgbackup.Request{
		Backup:           defaultBackup().Result(),
		BackedUpItems:    pkgbackup.NewBackedUpItemsMap(),
		SkippedPVTracker: pkgbackup.NewSkipPVTracker(),
	}
	contents, err := os.CreateTemp(t.TempDir(), "contents")
	require.NoError(t, err)
	log, err := os.CreateTemp(t.TempDir(), "log")
	require.NoError(t, err)
	var info persistence.BackupInfo
	backupStore := &persistencemocks.BackupStore{}
	backupStore.On("PutBackup", mock.Anything).Run(func(args mock.Arguments) {
		info = args.Get(0).(persistence.BackupInfo)
	}).Return(nil)

	errs := persistBackup(req, contents, log, backupStore,
		[]snapshotv1api.VolumeSnapshot{*vs}, []snapshotv1api.VolumeSnapshotContent{*vsc},
		[]snapshotv1api.VolumeSnapshotClass{*class}, map[string]results.Result{},
		velerotest.NewFakeControllerRuntimeClient(t), velerotest.NewLogger())

	require.Empty(t, errs)
	assert.Equal(t, []string{"vs-1"}, names(t, info.CSIVolumeSnapshots, func(v snapshotv1api.VolumeSnapshot) string { return v.Name }))
	assert.Equal(t, []string{"vsc-1"}, names(t, info.CSIVolumeSnapshotContents, func(v snapshotv1api.VolumeSnapshotContent) string { return v.Name }))
	assert.Equal(t, []string{"class-1"}, names(t, info.CSIVolumeSnapshotClasses, func(v snapshotv1api.VolumeSnapshotClass) string { return v.Name }))
}

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

package persistence

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// CloudCasa's catalog reads a backup's CSI snapshot, content and class lists
// from the store, as v1.14.0.x uploaded them.
func TestPutBackupUploadsTheCSISnapshotLists(t *testing.T) {
	harness := newObjectBackupStoreTestHarness("foo", "")

	err := harness.PutBackup(BackupInfo{
		Name:                      "backup-1",
		Metadata:                  newStringReadSeeker("metadata"),
		Contents:                  newStringReadSeeker("contents"),
		CSIVolumeSnapshots:        newStringReadSeeker("snapshots"),
		CSIVolumeSnapshotContents: newStringReadSeeker("contents"),
		CSIVolumeSnapshotClasses:  newStringReadSeeker("classes"),
	})

	require.NoError(t, err)
	data := harness.objectStore.Data[harness.bucket]
	assert.Equal(t, []byte("snapshots"), data["backups/backup-1/backup-1-csi-volumesnapshots.json.gz"])
	assert.Equal(t, []byte("contents"), data["backups/backup-1/backup-1-csi-volumesnapshotcontents.json.gz"])
	assert.Equal(t, []byte("classes"), data["backups/backup-1/backup-1-csi-volumesnapshotclasses.json.gz"])
}

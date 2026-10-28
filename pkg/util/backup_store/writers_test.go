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

package backup_store

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	persistencemocks "github.com/vmware-tanzu/velero/pkg/persistence/mocks"
)

// The local writer puts each artifact in <base>/<restore> under the names
// kubeagent copies out of the engine pod.
func TestLocalArtifactWriter(t *testing.T) {
	base := t.TempDir()
	w := NewLocalArtifactWriterWithPath(base, logrus.New())

	require.NoError(t, w.WriteLog("r1", strings.NewReader("log")))
	require.NoError(t, w.WriteResults("r1", strings.NewReader("results")))
	require.NoError(t, w.WriteRestoredResourceList("r1", strings.NewReader("resources")))
	require.NoError(t, w.WriteItemOperations("r1", strings.NewReader("operations")))
	require.NoError(t, w.WriteVolumeInfo("r1", strings.NewReader("volumes")))

	dir := filepath.Join(base, "r1")
	assert.Equal(t, dir, w.GetRestoreArtifactDir("r1"))
	want := map[string]string{
		"restore-r1-logs.gz":                "log",
		"restore-r1-results.gz":             "results",
		"restore-r1-resource-list.json.gz":  "resources",
		"restore-r1-itemoperations.json.gz": "operations",
		"r1-volumeinfo.json.gz":             "volumes",
	}
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	assert.Len(t, entries, len(want))
	for name, content := range want {
		data, err := os.ReadFile(filepath.Join(dir, name))
		require.NoError(t, err, name)
		assert.Equal(t, content, string(data), name)
		info, err := os.Stat(filepath.Join(dir, name))
		require.NoError(t, err)
		assert.Equal(t, os.FileMode(0666), info.Mode().Perm(), name)
	}
	info, err := os.Stat(dir)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0777), info.Mode().Perm())
}

// The backup store writer stores each artifact with the matching BackupStore call.
func TestBackupStoreArtifactWriter(t *testing.T) {
	store := &persistencemocks.BackupStore{}
	store.On("PutRestoreLog", "b1", "r1", mock.Anything).Return(nil)
	store.On("PutRestoreResults", "b1", "r1", mock.Anything).Return(nil)
	store.On("PutRestoredResourceList", "r1", mock.Anything).Return(nil)
	store.On("PutRestoreItemOperations", "r1", mock.Anything).Return(nil)
	store.On("PutRestoreVolumeInfo", "r1", mock.Anything).Return(nil)
	w := NewBackupStoreArtifactWriter(store, "b1", logrus.New())

	require.NoError(t, w.WriteLog("r1", strings.NewReader("log")))
	require.NoError(t, w.WriteResults("r1", strings.NewReader("results")))
	require.NoError(t, w.WriteRestoredResourceList("r1", strings.NewReader("resources")))
	require.NoError(t, w.WriteItemOperations("r1", strings.NewReader("operations")))
	require.NoError(t, w.WriteVolumeInfo("r1", strings.NewReader("volumes")))

	store.AssertExpectations(t)
}

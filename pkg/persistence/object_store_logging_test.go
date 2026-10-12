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
	"io"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The logging wrapper passes every call through unchanged and logs its outcome,
// so a failed object store call always leaves an error line naming the key.
func TestLoggingObjectStore(t *testing.T) {
	logger, hook := logrustest.NewNullLogger()
	store := newLoggingObjectStore(newInMemoryObjectStore("bucket"), logger)

	require.NoError(t, store.PutObject("bucket", "backups/b1/b1.tar.gz", strings.NewReader("data")))
	exists, err := store.ObjectExists("bucket", "backups/b1/b1.tar.gz")
	require.NoError(t, err)
	assert.True(t, exists)
	rc, err := store.GetObject("bucket", "backups/b1/b1.tar.gz")
	require.NoError(t, err)
	data, err := io.ReadAll(rc)
	require.NoError(t, err)
	assert.Equal(t, "data", string(data))
	keys, err := store.ListObjects("bucket", "backups/")
	require.NoError(t, err)
	assert.Equal(t, []string{"backups/b1/b1.tar.gz"}, keys)
	require.NoError(t, store.DeleteObject("bucket", "backups/b1/b1.tar.gz"))
	for _, entry := range hook.AllEntries() {
		assert.Equal(t, logrus.InfoLevel, entry.Level, entry.Message)
	}

	hook.Reset()
	_, err = store.GetObject("no-such-bucket", "backups/b1/b1.tar.gz")
	require.Error(t, err)
	last := hook.LastEntry()
	require.NotNil(t, last)
	assert.Equal(t, logrus.ErrorLevel, last.Level)
	assert.Equal(t, "ObjectStore: GetObject failed", last.Message)
	assert.Equal(t, "backups/b1/b1.tar.gz", last.Data["Key"])
	assert.Equal(t, err, last.Data[logrus.ErrorKey])
}

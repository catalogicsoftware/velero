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
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/builder"
	"github.com/vmware-tanzu/velero/pkg/persistence/bundlecrypt"
	velerotest "github.com/vmware-tanzu/velero/pkg/test"
)

func writeBundleKey(t *testing.T, key []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "cloud.job-1")
	require.NoError(t, os.WriteFile(path, key, 0o600))
	return path
}

func testBundleKey() []byte {
	return bytes.Repeat([]byte{0x42}, bundlecrypt.KeySize)
}

func encryptedLocation(keyFile string, required bool) *velerov1api.BackupStorageLocation {
	annotations := []string{velerov1api.BundleKeyFileAnnotation, keyFile}
	if required {
		annotations = append(annotations, velerov1api.BundleEncryptionRequiredAnnotation, "true")
	}
	return builder.ForBackupStorageLocation("cloudcasa-io", "aws-us-east-1-readwrite-job-1").
		ObjectMeta(builder.WithAnnotations(annotations...)).
		Provider("provider-1").Bucket("bucket").Prefix("prefix").Result()
}

func readObject(t *testing.T, store interface {
	GetObject(bucket, key string) (io.ReadCloser, error)
}, key string) ([]byte, error) {
	t.Helper()
	body, err := store.GetObject("bucket", key)
	if err != nil {
		return nil, err
	}
	defer body.Close()
	return io.ReadAll(body)
}

func TestBundleEncryptionIsOffWithoutTheAnnotation(t *testing.T) {
	plain := newInMemoryObjectStore("bucket")
	location := builder.ForBackupStorageLocation("cloudcasa-io", "bsl").Provider("provider-1").Bucket("bucket").Result()

	store, err := withBundleEncryption(plain, location)
	require.NoError(t, err)
	assert.Same(t, plain, store)
	assert.False(t, bundleEncrypted(location))
}

func TestEncryptingObjectStoreRoundTrip(t *testing.T) {
	memory := newInMemoryObjectStore("bucket")
	location := encryptedLocation(writeBundleKey(t, testBundleKey()), false)
	store, err := withBundleEncryption(memory, location)
	require.NoError(t, err)
	assert.True(t, bundleEncrypted(location))

	contents := []byte(strings.Repeat("resource data ", 10000))
	require.NoError(t, store.PutObject("bucket", "backups/job-1/job-1.tar.gz", bytes.NewReader(contents)))

	stored := memory.Data["bucket"]["backups/job-1/job-1.tar.gz"]
	assert.True(t, bundlecrypt.IsEncrypted(stored), "the stored object must be encrypted")
	assert.False(t, bytes.Contains(stored, []byte("resource data")))

	got, err := readObject(t, store, "backups/job-1/job-1.tar.gz")
	require.NoError(t, err)
	assert.Equal(t, contents, got)

	exists, err := store.ObjectExists("bucket", "backups/job-1/job-1.tar.gz")
	require.NoError(t, err)
	assert.True(t, exists)
	keys, err := store.ListObjects("bucket", "backups/job-1/")
	require.NoError(t, err)
	assert.Equal(t, []string{"backups/job-1/job-1.tar.gz"}, keys)
	require.NoError(t, store.DeleteObject("bucket", "backups/job-1/job-1.tar.gz"))
	assert.Empty(t, memory.Data["bucket"])
}

func TestEncryptingObjectStoreReadsPlainObjects(t *testing.T) {
	memory := newInMemoryObjectStore("bucket")
	memory.Data["bucket"]["backups/old/velero-backup.json"] = []byte(`{"kind":"Backup"}`)
	keyFile := writeBundleKey(t, testBundleKey())

	store, err := withBundleEncryption(memory, encryptedLocation(keyFile, false))
	require.NoError(t, err)
	got, err := readObject(t, store, "backups/old/velero-backup.json")
	require.NoError(t, err)
	assert.Equal(t, `{"kind":"Backup"}`, string(got))

	strict, err := withBundleEncryption(memory, encryptedLocation(keyFile, true))
	require.NoError(t, err)
	_, err = readObject(t, strict, "backups/old/velero-backup.json")
	require.ErrorIs(t, err, bundlecrypt.ErrNotEncrypted)
}

func TestEncryptingObjectStoreRejectsChangedObjects(t *testing.T) {
	memory := newInMemoryObjectStore("bucket")
	store, err := withBundleEncryption(memory, encryptedLocation(writeBundleKey(t, testBundleKey()), true))
	require.NoError(t, err)
	require.NoError(t, store.PutObject("bucket", "backups/job-1/job-1-logs.gz", strings.NewReader("log lines")))

	memory.Data["bucket"]["backups/job-1/job-1-logs.gz"][60] ^= 0x01
	_, err = readObject(t, store, "backups/job-1/job-1-logs.gz")
	require.Error(t, err)
	assert.True(t, bundlecrypt.IsDecryptError(err), "got %v", err)

	other, err := withBundleEncryption(memory, encryptedLocation(writeBundleKey(t, bytes.Repeat([]byte{1}, bundlecrypt.KeySize)), false))
	require.NoError(t, err)
	require.NoError(t, store.PutObject("bucket", "backups/job-1/job-1-logs.gz", strings.NewReader("log lines")))
	_, err = readObject(t, other, "backups/job-1/job-1-logs.gz")
	require.ErrorIs(t, err, bundlecrypt.ErrWrongKey)
}

func TestEncryptingObjectStoreNeedsAGoodKeyFile(t *testing.T) {
	memory := newInMemoryObjectStore("bucket")

	_, err := withBundleEncryption(memory, encryptedLocation(filepath.Join(t.TempDir(), "missing"), false))
	require.ErrorContains(t, err, "unable to read the bundle key")

	_, err = withBundleEncryption(memory, encryptedLocation(writeBundleKey(t, []byte("too short")), false))
	require.ErrorContains(t, err, "is 9 bytes, not 32")
}

func TestEncryptingObjectStoreRefusesSignedURLs(t *testing.T) {
	store, err := withBundleEncryption(newInMemoryObjectStore("bucket"), encryptedLocation(writeBundleKey(t, testBundleKey()), false))
	require.NoError(t, err)
	_, err = store.CreateSignedURL("bucket", "backups/job-1/job-1-logs.gz", time.Minute)
	require.ErrorContains(t, err, "encrypted")
}

func TestBackupStoreGetterEncryptsBundleFiles(t *testing.T) {
	memory := newInMemoryObjectStore("bucket")
	getter := NewObjectBackupStoreGetter(velerotest.NewFakeCredentialsFileStore("", nil))
	location := encryptedLocation(writeBundleKey(t, testBundleKey()), false)

	backupStore, err := getter.Get(location, objectStoreGetter{"provider-1": memory}, velerotest.NewLogger())
	require.NoError(t, err)

	require.NoError(t, backupStore.PutBackupContents("job-1", strings.NewReader("tarball")))
	stored := memory.Data["bucket"]["prefix/backups/job-1/job-1.tar.gz"]
	assert.True(t, bundlecrypt.IsEncrypted(stored))

	contents, err := backupStore.GetBackupContents("job-1")
	require.NoError(t, err)
	defer contents.Close()
	got, err := io.ReadAll(contents)
	require.NoError(t, err)
	assert.Equal(t, "tarball", string(got))

	_, err = getter.Get(encryptedLocation(filepath.Join(t.TempDir(), "missing"), false),
		objectStoreGetter{"provider-1": memory}, velerotest.NewLogger())
	require.ErrorContains(t, err, "unable to read the bundle key")
}

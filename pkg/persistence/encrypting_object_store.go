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
	"os"
	"time"

	"github.com/pkg/errors"

	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/persistence/bundlecrypt"
	"github.com/vmware-tanzu/velero/pkg/plugin/velero"
)

// encryptingObjectStore encrypts every object written through it and
// decrypts encrypted objects read through it. Plain objects are read as they
// are, unless the location requires encryption.
type encryptingObjectStore struct {
	delegate velero.ObjectStore
	key      []byte
	required bool
}

// bundleEncrypted reports whether a location encrypts its bundle files.
func bundleEncrypted(location *velerov1api.BackupStorageLocation) bool {
	return location.GetAnnotations()[velerov1api.BundleKeyFileAnnotation] != ""
}

// withBundleEncryption wraps store when the location carries a bundle key.
func withBundleEncryption(store velero.ObjectStore, location *velerov1api.BackupStorageLocation) (velero.ObjectStore, error) {
	keyFile := location.GetAnnotations()[velerov1api.BundleKeyFileAnnotation]
	if keyFile == "" {
		return store, nil
	}
	key, err := os.ReadFile(keyFile)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to read the bundle key of backup storage location %s", location.Name)
	}
	if len(key) != bundlecrypt.KeySize {
		return nil, errors.Errorf("the bundle key of backup storage location %s is %d bytes, not %d",
			location.Name, len(key), bundlecrypt.KeySize)
	}
	required := location.GetAnnotations()[velerov1api.BundleEncryptionRequiredAnnotation] == "true"
	return &encryptingObjectStore{delegate: store, key: key, required: required}, nil
}

func (e *encryptingObjectStore) Init(config map[string]string) error {
	return e.delegate.Init(config)
}

func (e *encryptingObjectStore) PutObject(bucket, key string, body io.Reader) error {
	sealed, err := bundlecrypt.Encrypt(body, e.key)
	if err != nil {
		return errors.Wrapf(err, "unable to encrypt %s", key)
	}
	return e.delegate.PutObject(bucket, key, sealed)
}

func (e *encryptingObjectStore) ObjectExists(bucket, key string) (bool, error) {
	return e.delegate.ObjectExists(bucket, key)
}

func (e *encryptingObjectStore) GetObject(bucket, key string) (io.ReadCloser, error) {
	body, err := e.delegate.GetObject(bucket, key)
	if err != nil {
		return nil, err
	}
	plain, err := bundlecrypt.Open(body, e.key, e.required)
	if err != nil {
		body.Close()
		return nil, errors.Wrapf(err, "unable to read %s", key)
	}
	return &decryptedObject{Reader: plain, Closer: body}, nil
}

func (e *encryptingObjectStore) ListCommonPrefixes(bucket, prefix, delimiter string) ([]string, error) {
	return e.delegate.ListCommonPrefixes(bucket, prefix, delimiter)
}

func (e *encryptingObjectStore) ListObjects(bucket, prefix string) ([]string, error) {
	return e.delegate.ListObjects(bucket, prefix)
}

func (e *encryptingObjectStore) DeleteObject(bucket, key string) error {
	return e.delegate.DeleteObject(bucket, key)
}

// CreateSignedURL is refused: the URL would serve the encrypted bytes.
func (e *encryptingObjectStore) CreateSignedURL(bucket, key string, ttl time.Duration) (string, error) {
	return "", errors.Errorf("no signed URL for %s: the bundle files of this location are encrypted", key)
}

type decryptedObject struct {
	io.Reader
	io.Closer
}

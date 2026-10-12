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
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"

	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/builder"
)

// The reconciler's retries of missing objects would add seconds to every
// not-found test case.
func init() {
	backupGetRetryInterval = time.Millisecond
	bslGetRetryInterval = time.Millisecond
}

// lateClient returns NotFound for the first gets of each kind of object, as
// for objects kubeagent has just created that the cache does not show yet.
type lateClient struct {
	client.Client
	misses map[string]int
	gets   map[string]int
}

func (c *lateClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	kind := fmt.Sprintf("%T", obj)
	c.gets[kind]++
	if c.gets[kind] <= c.misses[kind] {
		return apierrors.NewNotFound(schema.GroupResource{}, key.Name)
	}
	return c.Client.Get(ctx, key, obj, opts...)
}

// A Backup and a storage location that appear only after a few tries are
// still found, instead of failing the delete request as not found.
func TestBackupDeletionRetriesObjectsThatAppearLate(t *testing.T) {
	backup := builder.ForBackup(velerov1api.DefaultNamespace, "foo").StorageLocation("default").Result()
	location := builder.ForBackupStorageLocation(velerov1api.DefaultNamespace, "default").Result()
	td := setupBackupDeletionControllerTest(t, defaultTestDbr(), backup, location)
	late := &lateClient{
		Client: td.fakeClient,
		misses: map[string]int{"*v1.Backup": 3, "*v1.BackupStorageLocation": 3},
		gets:   map[string]int{},
	}
	td.controller.Client = late

	_, err := td.controller.Reconcile(t.Context(), td.req)
	require.NoError(t, err)

	res := &velerov1api.DeleteBackupRequest{}
	require.NoError(t, td.fakeClient.Get(t.Context(), td.req.NamespacedName, res))
	require.Len(t, res.Status.Errors, 1)
	assert.Contains(t, res.Status.Errors[0], "is currently in Unavailable state", "both objects were found")
	assert.Equal(t, 4, late.gets["*v1.Backup"])
	assert.Equal(t, 4, late.gets["*v1.BackupStorageLocation"])
}

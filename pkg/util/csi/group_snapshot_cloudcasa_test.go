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
	"testing"

	volumegroupsnapshotv1beta2 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumegroupsnapshot/v1beta2"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/vmware-tanzu/velero/pkg/builder"
	velerotest "github.com/vmware-tanzu/velero/pkg/test"
)

type noGroupSnapshotAPI struct{ crclient.Client }

func (c noGroupSnapshotAPI) List(ctx context.Context, list crclient.ObjectList, opts ...crclient.ListOption) error {
	if _, ok := list.(*volumegroupsnapshotv1beta2.VolumeGroupSnapshotContentList); ok {
		return &meta.NoKindMatchError{GroupKind: schema.GroupKind{Group: "groupsnapshot.storage.k8s.io", Kind: "VolumeGroupSnapshotContent"}}
	}
	return c.Client.List(ctx, list, opts...)
}

// Most clusters have no group snapshot API; deleting a backup there is not an error.
func TestDeleteGroupSnapshotStubsWithoutTheGroupSnapshotAPI(t *testing.T) {
	client := noGroupSnapshotAPI{velerotest.NewFakeControllerRuntimeClient(t)}
	backup := builder.ForBackup("velero", "backup").Result()

	assert.Empty(t, DeleteGroupSnapshotStubs(t.Context(), client, backup, logrus.StandardLogger()))
}

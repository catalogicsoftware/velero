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

package restore

import (
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/vmware-tanzu/velero/internal/volume"
	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/archive"
	"github.com/vmware-tanzu/velero/pkg/builder"
	velerotest "github.com/vmware-tanzu/velero/pkg/test"
)

const (
	vsResource      = "volumesnapshots.snapshot.storage.k8s.io"
	vsClassResource = "volumesnapshotclasses.snapshot.storage.k8s.io"
)

func namedTestBackupResources() map[string]*archive.ResourceItems {
	items := func(byNS map[string][]string) *archive.ResourceItems {
		return &archive.ResourceItems{ItemsByNamespace: byNS}
	}
	return map[string]*archive.ResourceItems{
		"pods":                   items(map[string][]string{"ns-1": {"a", "b"}, "ns-2": {"a"}}),
		"persistentvolumes":      items(map[string][]string{"": {"pv-1", "pv-2"}}),
		"persistentvolumeclaims": items(map[string][]string{"ns-1": {"pvc-csi", "pvc-fs"}}),
		vsResource:               items(map[string][]string{"ns-1": {"vs-1"}}),
		vsClassResource:          items(map[string][]string{"": {"class-1"}}),
	}
}

// namedTestContext returns a restore context whose cluster either has the
// VolumeSnapshotClass "class-1" or not.
func namedTestContext(t *testing.T, classExists bool, excluded ...string) *restoreContext {
	t.Helper()
	dynamicClient := &velerotest.FakeDynamicClient{}
	if classExists {
		dynamicClient.On("Get", "class-1", metav1.GetOptions{}).Return(&unstructured.Unstructured{}, nil)
	} else {
		dynamicClient.On("Get", "class-1", metav1.GetOptions{}).Return((*unstructured.Unstructured)(nil),
			apierrors.NewNotFound(schema.GroupResource{Group: "snapshot.storage.k8s.io", Resource: "volumesnapshotclasses"}, "class-1"))
	}
	dynamicFactory := &velerotest.FakeDynamicFactory{}
	dynamicFactory.On("ClientForGroupVersionResource",
		schema.GroupVersion{Group: "snapshot.storage.k8s.io", Version: "v1"}, mock.Anything, "").Return(dynamicClient, nil)

	return &restoreContext{
		log:            logrus.New(),
		restore:        builder.ForRestore(velerov1api.DefaultNamespace, "restore-1").ExcludedResources(excluded...).Result(),
		dynamicFactory: dynamicFactory,
		backupVolumeInfoMap: map[string]volume.BackupVolumeInfo{
			"pv-csi": {
				PVCName:         "pvc-csi",
				PVCNamespace:    "ns-1",
				BackupMethod:    volume.CSISnapshot,
				CSISnapshotInfo: &volume.CSISnapshotInfo{VSName: "vs-1", VSCLSName: "class-1"},
			},
		},
	}
}

func itemsOf(filtered map[string]*archive.ResourceItems) map[string]map[string][]string {
	out := map[string]map[string][]string{}
	for resource, items := range filtered {
		out[resource] = items.ItemsByNamespace
	}
	return out
}

func TestFilterBackupResourcesByNamedResources(t *testing.T) {
	named := map[string]string{
		"pods":                   "ns-1/a",
		"persistentvolumes":      "pv-1",
		"persistentvolumeclaims": "ns-1/pvc-csi, ns-1/pvc-fs",
		"configmaps":             "ns-1/not-in-backup",
	}

	t.Run("named items plus the snapshot and missing class of a CSI PVC", func(t *testing.T) {
		filtered := filterBackupResourcesByNamedResources(namedTestContext(t, false), namedTestBackupResources(), named)
		assert.Equal(t, map[string]map[string][]string{
			"pods":                   {"ns-1": {"a"}},
			"persistentvolumes":      {"": {"pv-1"}},
			"persistentvolumeclaims": {"ns-1": {"pvc-csi", "pvc-fs"}},
			vsResource:               {"ns-1": {"vs-1"}},
			vsClassResource:          {"": {"class-1"}},
		}, itemsOf(filtered))
	})

	t.Run("a class that exists in the cluster is not restored", func(t *testing.T) {
		filtered := filterBackupResourcesByNamedResources(namedTestContext(t, true), namedTestBackupResources(), named)
		assert.NotContains(t, filtered, vsClassResource)
		assert.Equal(t, map[string][]string{"ns-1": {"vs-1"}}, filtered[vsResource].ItemsByNamespace)
	})

	t.Run("explicitly excluded snapshot resources are not added", func(t *testing.T) {
		filtered := filterBackupResourcesByNamedResources(namedTestContext(t, false, vsResource, vsClassResource),
			namedTestBackupResources(), named)
		assert.NotContains(t, filtered, vsResource)
		assert.NotContains(t, filtered, vsClassResource)
		assert.Equal(t, map[string][]string{"ns-1": {"pvc-csi", "pvc-fs"}}, filtered["persistentvolumeclaims"].ItemsByNamespace)
	})

	t.Run("a name in another namespace does not match", func(t *testing.T) {
		filtered := filterBackupResourcesByNamedResources(namedTestContext(t, false), namedTestBackupResources(),
			map[string]string{"pods": "ns-3/a"})
		assert.Empty(t, filtered)
	})
}

// Backups made before the volume info carried the snapshot names fall back to
// the PVC and VolumeSnapshot extracted from the backup tarball.
func TestGetVolumeSnapshotDetailsFromExtractedBackup(t *testing.T) {
	restoreDir := "/tmp/velero-restore-1"
	pvcJSON := `{"apiVersion":"v1","kind":"PersistentVolumeClaim","metadata":{"name":"pvc-csi","namespace":"ns-1",` +
		`"annotations":{"velero.io/volume-snapshot-name":"vs-1"}}}`
	vsJSON := `{"apiVersion":"snapshot.storage.k8s.io/v1","kind":"VolumeSnapshot","metadata":{"name":"vs-1","namespace":"ns-1"},` +
		`"spec":{"volumeSnapshotClassName":"class-1"}}`
	fs := velerotest.NewFakeFileSystem().
		WithFile(archive.GetItemFilePath(restoreDir, "persistentvolumeclaims", "ns-1", "pvc-csi"), []byte(pvcJSON)).
		WithFile(archive.GetItemFilePath(restoreDir, vsResource, "ns-1", "vs-1"), []byte(vsJSON))
	ctx := &restoreContext{log: logrus.New(), restoreDir: restoreDir, fileSystem: fs}
	volumeInfo := &volume.BackupVolumeInfo{BackupMethod: volume.CSISnapshot, CSISnapshotInfo: &volume.CSISnapshotInfo{}}

	vsName, vsClassName := getVolumeSnapshotDetails(ctx, "persistentvolumeclaims", "ns-1", "pvc-csi", volumeInfo)

	assert.Equal(t, "vs-1", vsName)
	assert.Equal(t, "class-1", vsClassName)
}

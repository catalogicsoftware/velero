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

package backup

import (
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1api "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/vmware-tanzu/velero/pkg/builder"
	"github.com/vmware-tanzu/velero/pkg/kuberesource"
	"github.com/vmware-tanzu/velero/pkg/util/collections"
)

// Unit tests run outside a cluster, where the CloudCasa Velero ConfigMap
// cannot be read; by default they get no configuration.
func init() {
	getVeleroConfig = func(logrus.FieldLogger, string) (*veleroConfig, error) { return &veleroConfig{}, nil }
}

func TestGetVeleroConfigFrom(t *testing.T) {
	log := logrus.New()

	cfg, err := getVeleroConfigFrom(fake.NewSimpleClientset(), log, "job-1")
	require.NoError(t, err)
	assert.Nil(t, cfg.ExcludedPvcs, "a missing ConfigMap means no exclusions")

	cm := &corev1api.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: CloudCasaNamespace, Name: VeleroConfigMapPrefix + "job-1"},
		BinaryData: map[string][]byte{"excludedPvcs": []byte(`{"ns/pvc-a":true}`)},
	}
	cfg, err = getVeleroConfigFrom(fake.NewSimpleClientset(cm), log, "job-1")
	require.NoError(t, err)
	assert.Equal(t, map[string]bool{"ns/pvc-a": true}, cfg.ExcludedPvcs)

	cm.BinaryData["excludedPvcs"] = []byte(`not json`)
	_, err = getVeleroConfigFrom(fake.NewSimpleClientset(cm), log, "job-1")
	require.Error(t, err)
}

// Unattached PVCs listed by kubeagent, and their PVs, are not backed up.
func TestBackupItemInternalSkipsExcludedPvcs(t *testing.T) {
	req := baseRequest()
	req.BackedUpItems = NewBackedUpItemsMap()
	ib := newTestItemBackupper(req)
	ib.excludedPvcs = map[string]bool{"ns/excluded": true}

	toItem := func(obj runtime.Object) *unstructured.Unstructured {
		m, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
		require.NoError(t, err)
		return &unstructured.Unstructured{Object: m}
	}
	tests := []struct {
		name     string
		obj      runtime.Object
		gr       string
		selected bool
	}{
		{"excluded PVC", builder.ForPersistentVolumeClaim("ns", "excluded").Result(), "pvc", false},
		{"PV of an excluded PVC", builder.ForPersistentVolume("pv-x").ClaimRef("ns", "excluded").Result(), "pv", false},
		{"unbound PV", builder.ForPersistentVolume("pv-free").Result(), "pv", false},
		{"PV of a kept PVC", builder.ForPersistentVolume("pv-k").ClaimRef("ns", "kept").Result(), "pv", true},
		{"kept PVC", builder.ForPersistentVolumeClaim("ns", "kept").Result(), "pvc", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gr := kuberesource.PersistentVolumeClaims
			if tc.gr == "pv" {
				gr = kuberesource.PersistentVolumes
			}
			// An already backed-up item stops before any I/O, which is enough to
			// tell a skipped item (false) from a selected one (true).
			item := toItem(tc.obj)
			req.BackedUpItems.AddItem(itemKey{resource: resourceKey(item), namespace: item.GetNamespace(), name: item.GetName()})

			selected, _, err := ib.backupItemInternal(logrus.New(), item, gr, gr.WithVersion("v1"), false, false, nil)

			require.NoError(t, err)
			assert.Equal(t, tc.selected, selected)
		})
	}
}

// A Namespace object is not backed up when its name is excluded from the backup.
func TestBackupItemInternalSkipsAnExcludedNamespaceObject(t *testing.T) {
	req := baseRequest()
	req.BackedUpItems = NewBackedUpItemsMap()
	req.NamespaceIncludesExcludes = collections.NewNamespaceIncludesExcludes().Includes("*").Excludes("ns-excluded")
	ib := newTestItemBackupper(req)
	for name, selected := range map[string]bool{"ns-excluded": false, "ns-kept": true} {
		t.Run(name, func(t *testing.T) {
			m, err := runtime.DefaultUnstructuredConverter.ToUnstructured(builder.ForNamespace(name).Result())
			require.NoError(t, err)
			item := &unstructured.Unstructured{Object: m}
			// An already backed-up item stops before any I/O, which is enough to
			// tell a skipped item (false) from a selected one (true).
			req.BackedUpItems.AddItem(itemKey{resource: resourceKey(item), name: name})

			got, _, err := ib.backupItemInternal(logrus.New(), item, kuberesource.Namespaces, kuberesource.Namespaces.WithVersion("v1"), false, false, nil)

			require.NoError(t, err)
			assert.Equal(t, selected, got)
		})
	}
}

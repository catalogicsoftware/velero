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
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1api "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"

	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/builder"
	"github.com/vmware-tanzu/velero/pkg/plugin/velero"
	velerotest "github.com/vmware-tanzu/velero/pkg/test"
)

// executeRestoreOnPVC runs the action on pvc and returns the PVC it would create.
func executeRestoreOnPVC(t *testing.T, p *pvcRestoreItemAction, restore *velerov1api.Restore,
	pvc *corev1api.PersistentVolumeClaim) (*velero.RestoreItemActionExecuteOutput, *corev1api.PersistentVolumeClaim, error) {
	t.Helper()
	item, err := runtime.DefaultUnstructuredConverter.ToUnstructured(pvc)
	require.NoError(t, err)
	output, err := p.Execute(&velero.RestoreItemActionExecuteInput{
		Item:           &unstructured.Unstructured{Object: item},
		ItemFromBackup: &unstructured.Unstructured{Object: item},
		Restore:        restore,
	})
	if err != nil {
		return nil, nil, err
	}
	result := new(corev1api.PersistentVolumeClaim)
	require.NoError(t, runtime.DefaultUnstructuredConverter.FromUnstructured(output.UpdatedItem.UnstructuredContent(), result))
	return output, result, nil
}

// A DR volume restore keeps the PVC on its existing volume.
func TestExecuteKeepsTheVolumeOfADrVolume(t *testing.T) {
	tests := []struct {
		name       string
		annotation map[string]string
		wantVolume string
		wantErr    bool
	}{
		{name: "no annotation", wantVolume: ""},
		{name: "DR volumes", annotation: map[string]string{"cloudcasa-use-dr-volumes": "true"}, wantVolume: "pv-1"},
		{name: "DR volumes off", annotation: map[string]string{"cloudcasa-use-dr-volumes": "false"}, wantVolume: ""},
		{name: "invalid value", annotation: map[string]string{"cloudcasa-use-dr-volumes": "sometimes"}, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := &pvcRestoreItemAction{log: logrus.New(), crClient: velerotest.NewFakeControllerRuntimeClient(t)}
			restore := builder.ForRestore("velero", "restore-1").RestorePVs(false).Result()
			restore.Annotations = tc.annotation
			pvc := builder.ForPersistentVolumeClaim("ns", "pvc-1").VolumeName("pv-1").Result()

			_, restored, err := executeRestoreOnPVC(t, p, restore, pvc)

			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantVolume, restored.Spec.VolumeName)
			assert.Nil(t, restored.Spec.DataSource)
		})
	}
}

func TestExecuteRemovesTheHarvesterOwner(t *testing.T) {
	p := &pvcRestoreItemAction{log: logrus.New(), crClient: velerotest.NewFakeControllerRuntimeClient(t)}
	pvc := builder.ForPersistentVolumeClaim("ns", "pvc-1").
		ObjectMeta(builder.WithAnnotations(HarvesterhCiIoOwnedBy, "vm-1", "keep", "me")).Result()

	_, restored, err := executeRestoreOnPVC(t, p, builder.ForRestore("velero", "restore-1").RestorePVs(false).Result(), pvc)

	require.NoError(t, err)
	assert.NotContains(t, restored.Annotations, HarvesterhCiIoOwnedBy)
	assert.Equal(t, "me", restored.Annotations["keep"])
}

// With the "update" policy, an existing PVC on a Retain PV that no pod uses is
// deleted, so the restored PVC can take its place.
func TestExecuteReplacesAnExistingPVCOnARetainedVolume(t *testing.T) {
	existingPVC := func(ns string) *corev1api.PersistentVolumeClaim {
		return builder.ForPersistentVolumeClaim(ns, "pvc-1").VolumeName("pv-1").Result()
	}
	retainPV := builder.ForPersistentVolume("pv-1").ReclaimPolicy(corev1api.PersistentVolumeReclaimRetain).Result()
	tests := []struct {
		name        string
		restore     *velerov1api.Restore
		objects     []runtime.Object
		wantDeleted bool
	}{
		{
			name:        "update policy, Retain PV, unused",
			restore:     builder.ForRestore("velero", "restore-1").ExistingResourcePolicy("update").RestorePVs(false).Result(),
			objects:     []runtime.Object{existingPVC("ns"), retainPV},
			wantDeleted: true,
		},
		{
			name:    "no update policy",
			restore: builder.ForRestore("velero", "restore-1").RestorePVs(false).Result(),
			objects: []runtime.Object{existingPVC("ns"), retainPV},
		},
		{
			name:    "Delete PV",
			restore: builder.ForRestore("velero", "restore-1").ExistingResourcePolicy("update").RestorePVs(false).Result(),
			objects: []runtime.Object{existingPVC("ns"),
				builder.ForPersistentVolume("pv-1").ReclaimPolicy(corev1api.PersistentVolumeReclaimDelete).Result()},
		},
		{
			name:    "a pod uses the PVC",
			restore: builder.ForRestore("velero", "restore-1").ExistingResourcePolicy("update").RestorePVs(false).Result(),
			objects: []runtime.Object{existingPVC("ns"), retainPV,
				builder.ForPod("ns", "pod-1").Volumes(builder.ForVolume("data").PersistentVolumeClaimSource("pvc-1").Result()).Result()},
		},
		{
			name: "mapped namespace",
			restore: builder.ForRestore("velero", "restore-1").ExistingResourcePolicy("update").RestorePVs(false).
				NamespaceMappings("ns", "ns-2").Result(),
			objects:     []runtime.Object{existingPVC("ns-2"), retainPV},
			wantDeleted: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := velerotest.NewFakeControllerRuntimeClient(t, tc.objects...)
			p := &pvcRestoreItemAction{log: logrus.New(), crClient: client}
			pvc := builder.ForPersistentVolumeClaim("ns", "pvc-1").VolumeName("pv-1").Result()

			_, restored, err := executeRestoreOnPVC(t, p, tc.restore, pvc)
			require.NoError(t, err)

			targetNamespace := "ns"
			if mapped, ok := tc.restore.Spec.NamespaceMapping["ns"]; ok {
				targetNamespace = mapped
			}
			getErr := client.Get(t.Context(), crclient.ObjectKey{Namespace: targetNamespace, Name: "pvc-1"}, new(corev1api.PersistentVolumeClaim))
			if tc.wantDeleted {
				assert.True(t, apierrors.IsNotFound(getErr), "existing PVC deleted")
				assert.Empty(t, restored.Spec.VolumeName, "the PVC is restored, not skipped")
			} else {
				require.NoError(t, getErr, "existing PVC kept")
				assert.Equal(t, "pv-1", restored.Spec.VolumeName, "the PVC is skipped")
			}
		})
	}
}

/*
Copyright 2019 the Velero contributors.

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

package actions

import (
	"context"
	"testing"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1api "k8s.io/api/core/v1"
	storagev1api "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/vmware-tanzu/velero/pkg/builder"
	"github.com/vmware-tanzu/velero/pkg/plugin/velero"
)

// TestChangeStorageClassActionExecute runs the ChangeStorageClassAction's Execute
// method and validates that the item's storage class is modified (or not) as expected.
// Validation is done by comparing the result of the Execute method to the test case's
// desired result.
func TestChangeStorageClassActionExecute(t *testing.T) {
	tests := []struct {
		name              string
		pvOrPvcOrSTS      interface{}
		configMap         *corev1api.ConfigMap
		storageClass      *storagev1api.StorageClass
		storageClassSlice []*storagev1api.StorageClass
		want              interface{}
		wantErr           error
	}{
		{
			name:         "a valid mapping for a persistent volume is applied correctly",
			pvOrPvcOrSTS: builder.ForPersistentVolume("pv-1").StorageClass("storageclass-1").Result(),
			configMap: builder.ForConfigMap("velero", "change-storage-classs").
				ObjectMeta(builder.WithLabels("velero.io/plugin-config", "", "velero.io/change-storage-class", "RestoreItemAction")).
				Data("storageclass-1", "storageclass-2").
				Result(),
			storageClass: builder.ForStorageClass("storageclass-2").Result(),
			want:         builder.ForPersistentVolume("pv-1").StorageClass("storageclass-2").Result(),
		},
		{
			name:         "a valid mapping for a persistent volume claim is applied correctly",
			pvOrPvcOrSTS: builder.ForPersistentVolumeClaim("velero", "pvc-1").StorageClass("storageclass-1").Result(),
			configMap: builder.ForConfigMap("velero", "change-storage-classs").
				ObjectMeta(builder.WithLabels("velero.io/plugin-config", "", "velero.io/change-storage-class", "RestoreItemAction")).
				Data("storageclass-1", "storageclass-2").
				Result(),
			storageClass: builder.ForStorageClass("storageclass-2").Result(),
			want:         builder.ForPersistentVolumeClaim("velero", "pvc-1").StorageClass("storageclass-2").Result(),
		},
		{
			name:         "when no config map exists for the plugin, the item is returned as-is",
			pvOrPvcOrSTS: builder.ForPersistentVolume("pv-1").StorageClass("storageclass-1").Result(),
			configMap: builder.ForConfigMap("velero", "change-storage-classs").
				ObjectMeta(builder.WithLabels("velero.io/plugin-config", "", "velero.io/some-other-plugin", "RestoreItemAction")).
				Data("storageclass-1", "storageclass-2").
				Result(),
			want: builder.ForPersistentVolume("pv-1").StorageClass("storageclass-1").Result(),
		},
		{
			name:         "when no storage class mappings exist in the plugin config map, the item is returned as-is",
			pvOrPvcOrSTS: builder.ForPersistentVolume("pv-1").StorageClass("storageclass-1").Result(),
			configMap: builder.ForConfigMap("velero", "change-storage-classs").
				ObjectMeta(builder.WithLabels("velero.io/plugin-config", "", "velero.io/change-storage-class", "RestoreItemAction")).
				Result(),
			want: builder.ForPersistentVolume("pv-1").StorageClass("storageclass-1").Result(),
		},
		{
			name:         "when persistent volume has no storage class, the item is returned as-is",
			pvOrPvcOrSTS: builder.ForPersistentVolume("pv-1").Result(),
			configMap: builder.ForConfigMap("velero", "change-storage-classs").
				ObjectMeta(builder.WithLabels("velero.io/plugin-config", "", "velero.io/change-storage-class", "RestoreItemAction")).
				Data("storageclass-1", "storageclass-2").
				Result(),
			want: builder.ForPersistentVolume("pv-1").Result(),
		},
		{
			name:         "when persistent volume claim has no storage class, the item is returned as-is",
			pvOrPvcOrSTS: builder.ForPersistentVolumeClaim("velero", "pvc-1").Result(),
			configMap: builder.ForConfigMap("velero", "change-storage-classs").
				ObjectMeta(builder.WithLabels("velero.io/plugin-config", "", "velero.io/change-storage-class", "RestoreItemAction")).
				Data("storageclass-1", "storageclass-2").
				Result(),
			want: builder.ForPersistentVolumeClaim("velero", "pvc-1").Result(),
		},
		{
			name:         "when persistent volume's storage class has no mapping in the config map, the item is returned as-is",
			pvOrPvcOrSTS: builder.ForPersistentVolume("pv-1").StorageClass("storageclass-1").Result(),
			configMap: builder.ForConfigMap("velero", "change-storage-classs").
				ObjectMeta(builder.WithLabels("velero.io/plugin-config", "", "velero.io/change-storage-class", "RestoreItemAction")).
				Data("storageclass-3", "storageclass-4").
				Result(),
			want: builder.ForPersistentVolume("pv-1").StorageClass("storageclass-1").Result(),
		},
		{
			name:         "when persistent volume claim's storage class has no mapping in the config map, the item is returned as-is",
			pvOrPvcOrSTS: builder.ForPersistentVolumeClaim("velero", "pvc-1").StorageClass("storageclass-1").Result(),
			configMap: builder.ForConfigMap("velero", "change-storage-classs").
				ObjectMeta(builder.WithLabels("velero.io/plugin-config", "", "velero.io/change-storage-class", "RestoreItemAction")).
				Data("storageclass-3", "storageclass-4").
				Result(),
			want: builder.ForPersistentVolumeClaim("velero", "pvc-1").StorageClass("storageclass-1").Result(),
		},
		{
			name:         "when persistent volume's storage class is mapped to a nonexistent storage class, an error is returned",
			pvOrPvcOrSTS: builder.ForPersistentVolume("pv-1").StorageClass("storageclass-1").Result(),
			configMap: builder.ForConfigMap("velero", "change-storage-classs").
				ObjectMeta(builder.WithLabels("velero.io/plugin-config", "", "velero.io/change-storage-class", "RestoreItemAction")).
				Data("storageclass-1", "nonexistent-storage-class").
				Result(),
			wantErr: errors.New("error getting storage class nonexistent-storage-class from API: storageclasses.storage.k8s.io \"nonexistent-storage-class\" not found"),
		},
		{
			name:         "when persistent volume claim's storage class is mapped to a nonexistent storage class, an error is returned",
			pvOrPvcOrSTS: builder.ForPersistentVolumeClaim("velero", "pvc-1").StorageClass("storageclass-1").Result(),
			configMap: builder.ForConfigMap("velero", "change-storage-classs").
				ObjectMeta(builder.WithLabels("velero.io/plugin-config", "", "velero.io/change-storage-class", "RestoreItemAction")).
				Data("storageclass-1", "nonexistent-storage-class").
				Result(),
			wantErr: errors.New("error getting storage class nonexistent-storage-class from API: storageclasses.storage.k8s.io \"nonexistent-storage-class\" not found"),
		},
		{
			name:         "when statefulset's VolumeClaimTemplates has only one pvc, a valid mapping for a statefulset is applied correctly",
			pvOrPvcOrSTS: builder.ForStatefulSet("velero", "sts-1").StorageClass("storageclass-1").Result(),
			configMap: builder.ForConfigMap("velero", "change-storage-classs").
				ObjectMeta(builder.WithLabels("velero.io/plugin-config", "", "velero.io/change-storage-class", "RestoreItemAction")).
				Data("storageclass-1", "storageclass-2").
				Result(),
			storageClass: builder.ForStorageClass("storageclass-2").Result(),
			want:         builder.ForStatefulSet("velero", "sts-1").StorageClass("storageclass-2").Result(),
		},
		{
			name:         "when statefulset's VolumeClaimTemplates has more than one same pvc's storageClassName, a valid mapping for a statefulset is applied correctly",
			pvOrPvcOrSTS: builder.ForStatefulSet("velero", "sts-1").StorageClass("storageclass-1", "storageclass-1").Result(),
			configMap: builder.ForConfigMap("velero", "change-storage-classs").
				ObjectMeta(builder.WithLabels("velero.io/plugin-config", "", "velero.io/change-storage-class", "RestoreItemAction")).
				Data("storageclass-1", "storageclass-2", "storageclass-3", "storageclass-4").
				Result(),
			storageClass: builder.ForStorageClass("storageclass-2").Result(),
			want:         builder.ForStatefulSet("velero", "sts-1").StorageClass("storageclass-2", "storageclass-2").Result(),
		},
		{
			name:         "when statefulset's VolumeClaimTemplates has more than one different pvc's storageClassName, a valid mapping for a statefulset is applied correctly",
			pvOrPvcOrSTS: builder.ForStatefulSet("velero", "sts-1").StorageClass("storageclass-1", "storageclass-2", "storageclass-3").Result(),
			configMap: builder.ForConfigMap("velero", "change-storage-classs").
				ObjectMeta(builder.WithLabels("velero.io/plugin-config", "", "velero.io/change-storage-class", "RestoreItemAction")).
				Data("storageclass-1", "storageclass-a", "storageclass-2", "storageclass-b", "storageclass-3", "storageclass-c").
				Result(),
			storageClassSlice: builder.ForStorageClassSlice("storageclass-a", "storageclass-b", "storageclass-c").SliceResult(),
			want:              builder.ForStatefulSet("velero", "sts-1").StorageClass("storageclass-a", "storageclass-b", "storageclass-c").Result(),
		},
		{
			name:         "when no config map exists for the plugin, the statefulset item is returned as-is",
			pvOrPvcOrSTS: builder.ForStatefulSet("velero", "sts-1").StorageClass("storageclass-1").Result(),
			configMap: builder.ForConfigMap("velero", "change-storage-classs").
				ObjectMeta(builder.WithLabels("velero.io/plugin-config", "", "velero.io/some-other-plugin", "RestoreItemAction")).
				Data("storageclass-1", "storageclass-2").
				Result(),
			want: builder.ForStatefulSet("velero", "sts-1").StorageClass("storageclass-1").Result(),
		},
		{
			name:         "when no storage class mappings exist in the plugin config map, the statefulset item is returned as-is",
			pvOrPvcOrSTS: builder.ForStatefulSet("velero", "sts-1").StorageClass("storageclass-1").Result(),
			configMap: builder.ForConfigMap("velero", "change-storage-classs").
				ObjectMeta(builder.WithLabels("velero.io/plugin-config", "", "velero.io/change-storage-class", "RestoreItemAction")).
				Result(),
			want: builder.ForStatefulSet("velero", "sts-1").StorageClass("storageclass-1").Result(),
		},
		{
			name:         "when persistent volume claim has no storage class, the statefulset item is returned as-is",
			pvOrPvcOrSTS: builder.ForStatefulSet("velero", "sts-1").Result(),
			configMap: builder.ForConfigMap("velero", "change-storage-classs").
				ObjectMeta(builder.WithLabels("velero.io/plugin-config", "", "velero.io/change-storage-class", "RestoreItemAction")).
				Result(),
			want: builder.ForStatefulSet("velero", "sts-1").Result(),
		},
		{
			name:         "when statefulset's storage class has no mapping in the config map, the item is returned as-is",
			pvOrPvcOrSTS: builder.ForStatefulSet("velero", "sts-1").StorageClass("storageclass-1").Result(),
			configMap: builder.ForConfigMap("velero", "change-storage-classs").
				ObjectMeta(builder.WithLabels("velero.io/plugin-config", "", "velero.io/change-storage-class", "RestoreItemAction")).
				Data("storageclass-3", "storageclass-4").
				Result(),
			want: builder.ForStatefulSet("velero", "sts-1").StorageClass("storageclass-1").Result(),
		},
		{
			name:         "when statefulset's storage class is mapped to a nonexistent storage class, an error is returned",
			pvOrPvcOrSTS: builder.ForStatefulSet("velero", "sts-1").StorageClass("storageclass-1").Result(),
			configMap: builder.ForConfigMap("velero", "change-storage-classs").
				ObjectMeta(builder.WithLabels("velero.io/plugin-config", "", "velero.io/change-storage-class", "RestoreItemAction")).
				Data("storageclass-1", "nonexistent-storage-class").
				Result(),
			wantErr: errors.New("error getting storage class nonexistent-storage-class from API: storageclasses.storage.k8s.io \"nonexistent-storage-class\" not found"),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			clientset := fake.NewSimpleClientset()
			a := NewChangeStorageClassAction(
				logrus.StandardLogger(),
				clientset.CoreV1().ConfigMaps("velero"),
				clientset.StorageV1().StorageClasses(),
			)

			// set up test data
			const restoreName = "restore-1"
			if tc.configMap != nil {
				// The kubeagent creates one change-storage-class ConfigMap per restore,
				// named cloudcasa-io-change-sc-<restoreName>. Rename the test's change-sc
				// ConfigMap to that per-restore name so the by-name lookup finds it; leave
				// other-plugin ConfigMaps under their own name so the lookup misses them.
				if tc.configMap.Labels["velero.io/change-storage-class"] == "RestoreItemAction" {
					tc.configMap.Name = changeStorageClassConfigMapPrefix + restoreName
				}
				_, err := clientset.CoreV1().ConfigMaps(tc.configMap.Namespace).Create(context.TODO(), tc.configMap, metav1.CreateOptions{})
				require.NoError(t, err)
			}

			if tc.storageClass != nil {
				_, err := clientset.StorageV1().StorageClasses().Create(context.TODO(), tc.storageClass, metav1.CreateOptions{})
				require.NoError(t, err)
			}

			if tc.storageClassSlice != nil {
				for _, storageClass := range tc.storageClassSlice {
					_, err := clientset.StorageV1().StorageClasses().Create(context.TODO(), storageClass, metav1.CreateOptions{})
					require.NoError(t, err)
				}
			}

			unstructuredMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(tc.pvOrPvcOrSTS)
			require.NoError(t, err)

			input := &velero.RestoreItemActionExecuteInput{
				Item: &unstructured.Unstructured{
					Object: unstructuredMap,
				},
				Restore: builder.ForRestore("velero", restoreName).Result(),
			}

			// execute method under test
			res, err := a.Execute(input)

			// validate for both error and non-error cases
			switch {
			case tc.wantErr != nil:
				assert.EqualError(t, err, tc.wantErr.Error())
			default:
				assert.NoError(t, err)

				wantUnstructured, err := runtime.DefaultUnstructuredConverter.ToUnstructured(tc.want)
				require.NoError(t, err)

				assert.Equal(t, &unstructured.Unstructured{Object: wantUnstructured}, res.UpdatedItem)
			}
		})
	}
}

// TestChangeStorageClassActionExecutePerRestore verifies that the action selects the
// ConfigMap belonging to the restore being processed (by name), so that restores
// running in parallel — and ConfigMaps orphaned by other/crashed restores — never
// cross-contaminate and never trigger a "more than one ConfigMap" error.
func TestChangeStorageClassActionExecutePerRestore(t *testing.T) {
	newCM := func(restoreName, oldSC, newSC string) *corev1api.ConfigMap {
		return builder.ForConfigMap("velero", changeStorageClassConfigMapPrefix+restoreName).
			ObjectMeta(builder.WithLabels(
				"velero.io/plugin-config", "",
				"velero.io/change-storage-class", "RestoreItemAction",
				"cloudcasa.io/change-storage-class", "true",
				"cloudcasa.io/job-id-for-change-storage-class", restoreName,
			)).
			Data(oldSC, newSC).
			Result()
	}

	tests := []struct {
		name           string
		restoreName    string
		configMaps     []*corev1api.ConfigMap
		storageClasses []string
		wantSC         string // expected storageClassName on the restored PVC
	}{
		{
			name:        "selects this restore's mapping and ignores other restores'",
			restoreName: "restore-1",
			configMaps: []*corev1api.ConfigMap{
				newCM("restore-1", "storageclass-1", "storageclass-a"),
				newCM("restore-2", "storageclass-1", "storageclass-b"), // other restore, must be ignored
			},
			storageClasses: []string{"storageclass-a", "storageclass-b"},
			wantSC:         "storageclass-a",
		},
		{
			name:        "no ConfigMap for this restore is a no-op even when others exist",
			restoreName: "restore-1",
			configMaps: []*corev1api.ConfigMap{
				newCM("restore-2", "storageclass-1", "storageclass-b"),
				newCM("restore-3", "storageclass-1", "storageclass-c"),
			},
			wantSC: "storageclass-1", // unchanged
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			clientset := fake.NewSimpleClientset()
			a := NewChangeStorageClassAction(
				logrus.StandardLogger(),
				clientset.CoreV1().ConfigMaps("velero"),
				clientset.StorageV1().StorageClasses(),
			)

			for _, cm := range tc.configMaps {
				_, err := clientset.CoreV1().ConfigMaps(cm.Namespace).Create(context.TODO(), cm, metav1.CreateOptions{})
				require.NoError(t, err)
			}
			for _, sc := range tc.storageClasses {
				_, err := clientset.StorageV1().StorageClasses().Create(context.TODO(), builder.ForStorageClass(sc).Result(), metav1.CreateOptions{})
				require.NoError(t, err)
			}

			pvc := builder.ForPersistentVolumeClaim("app", "pvc-1").StorageClass("storageclass-1").Result()
			unstructuredMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(pvc)
			require.NoError(t, err)

			input := &velero.RestoreItemActionExecuteInput{
				Item:    &unstructured.Unstructured{Object: unstructuredMap},
				Restore: builder.ForRestore("velero", tc.restoreName).Result(),
			}

			res, err := a.Execute(input)
			require.NoError(t, err)

			gotPVC := new(corev1api.PersistentVolumeClaim)
			require.NoError(t, runtime.DefaultUnstructuredConverter.FromUnstructured(res.UpdatedItem.UnstructuredContent(), gotPVC))
			gotSC := ""
			if gotPVC.Spec.StorageClassName != nil {
				gotSC = *gotPVC.Spec.StorageClassName
			}
			assert.Equal(t, tc.wantSC, gotSC)
		})
	}
}

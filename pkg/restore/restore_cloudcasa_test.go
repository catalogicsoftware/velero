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
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1api "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8stypes "k8s.io/apimachinery/pkg/types"

	"github.com/vmware-tanzu/velero/internal/volume"
	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/builder"
	"github.com/vmware-tanzu/velero/pkg/test"
	"github.com/vmware-tanzu/velero/pkg/types"
	kubeutil "github.com/vmware-tanzu/velero/pkg/util/kube"
	"github.com/vmware-tanzu/velero/pkg/util/results"
)

// runRestore restores the tarball with the harness and returns the results.
func runRestore(t *testing.T, h *harness, restore *velerov1api.Restore, tarball io.Reader,
	apiResources ...*test.APIResource) (results.Result, results.Result) {
	t.Helper()
	for _, r := range apiResources {
		h.DiscoveryClient.WithAPIResource(r)
	}
	require.NoError(t, h.restorer.discoveryHelper.Refresh())
	return h.restorer.Restore(&Request{
		Log:          h.log,
		Restore:      restore,
		Backup:       defaultBackup().Result(),
		BackupReader: tarball,
	}, nil, nil)
}

// namespacesInCluster lists the namespaces the restore created or found.
func namespacesInCluster(t *testing.T, h *harness) []string {
	t.Helper()
	list, err := h.KubeClient.CoreV1().Namespaces().List(t.Context(), metav1.ListOptions{})
	require.NoError(t, err)
	var names []string
	for _, ns := range list.Items {
		names = append(names, ns.Name)
	}
	return names
}

// Namespace objects are restored even when only some namespaces are included,
// but never those the restore's namespace filter leaves out.
func TestRestoreNamespacesFollowTheNamespaceFilter(t *testing.T) {
	h := newHarness(t)
	tarball := test.NewTarWriter(t).
		AddItems("namespaces",
			builder.ForNamespace("ns-1").Result(),
			builder.ForNamespace("ns-empty").Result(),
			builder.ForNamespace("ns-2").Result(),
		).
		AddItems("pods",
			builder.ForPod("ns-1", "pod-1").Result(),
			builder.ForPod("ns-2", "pod-2").Result(),
		).
		Done()

	warnings, errs := runRestore(t, h, defaultRestore().IncludedNamespaces("ns-1", "ns-empty").Result(), tarball,
		test.Pods(), test.Namespaces())

	assertEmptyResults(t, warnings, errs)
	assert.ElementsMatch(t, []string{"ns-1", "ns-empty"}, namespacesInCluster(t, h))
}

// With the update policy, an existing namespace takes the backed-up labels.
func TestRestoreUpdatesAnExistingNamespaceUnderTheUpdatePolicy(t *testing.T) {
	tests := []struct {
		name       string
		policy     string
		wantLabels map[string]string
	}{
		{
			name:   "update policy",
			policy: string(velerov1api.PolicyTypeUpdate),
			wantLabels: map[string]string{
				"team": "backup", "tier": "gold",
				velerov1api.BackupNameLabel: "backup-1", velerov1api.RestoreNameLabel: "restore-1",
			},
		},
		{
			name:       "no policy",
			wantLabels: map[string]string{"team": "cluster"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			existing := builder.ForNamespace("ns-1").ObjectMeta(builder.WithLabels("team", "cluster")).Result()
			_, err := h.KubeClient.CoreV1().Namespaces().Create(t.Context(), existing, metav1.CreateOptions{})
			require.NoError(t, err)
			h.AddItems(t, test.Namespaces(existing))
			tarball := test.NewTarWriter(t).
				AddItems("namespaces",
					builder.ForNamespace("ns-1").ObjectMeta(builder.WithLabels("team", "backup", "tier", "gold")).Result()).
				AddItems("pods", builder.ForPod("ns-1", "pod-1").Result()).
				Done()
			restore := defaultRestore().IncludedNamespaces("ns-1").ExistingResourcePolicy(tc.policy).Result()

			_, errs := runRestore(t, h, restore, tarball, test.Pods(), test.Namespaces())

			assertEmptyResults(t, errs)
			got, err := h.DynamicClient.Resource(test.Namespaces().GVR()).Get(t.Context(), "ns-1", metav1.GetOptions{})
			require.NoError(t, err)
			assert.Equal(t, tc.wantLabels, got.GetLabels())
		})
	}
}

// With the update policy, a PV still claimed by an earlier PVC of the same
// name loses that stale binding, so the restored PVC can bind to it.
func TestRestoreClearsAStaleVolumeBindingUnderTheUpdatePolicy(t *testing.T) {
	tests := []struct {
		name        string
		policy      string
		wantCleared bool
	}{
		{name: "update policy", policy: string(velerov1api.PolicyTypeUpdate), wantCleared: true},
		{name: "no policy"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			pv := builder.ForPersistentVolume("pv-1").ClaimRef("ns-1", "pvc-1").
				ReclaimPolicy(corev1api.PersistentVolumeReclaimRetain).
				ObjectMeta(builder.WithAnnotations(kubeutil.KubeAnnBindCompleted, "yes")).Result()
			pv.Spec.ClaimRef.UID = k8stypes.UID("uid-of-the-deleted-pvc")
			pv.Spec.ClaimRef.ResourceVersion = "5"
			require.NoError(t, h.restorer.kbClient.Create(t.Context(), pv.DeepCopy()))
			h.AddItems(t, test.PVs(pv))
			tarball := test.NewTarWriter(t).
				AddItems("persistentvolumeclaims", builder.ForPersistentVolumeClaim("ns-1", "pvc-1").Result()).
				Done()
			restore := defaultRestore().ExistingResourcePolicy(tc.policy).Result()

			_, errs := runRestore(t, h, restore, tarball, test.PVCs(), test.PVs())

			assertEmptyResults(t, errs)
			got, err := h.DynamicClient.Resource(test.PVs().GVR()).Get(t.Context(), "pv-1", metav1.GetOptions{})
			require.NoError(t, err)
			uid, _, _ := unstructured.NestedString(got.Object, "spec", "claimRef", "uid")
			name, _, _ := unstructured.NestedString(got.Object, "spec", "claimRef", "name")
			assert.Equal(t, "pvc-1", name, "the claim stays reserved for the PVC name")
			if tc.wantCleared {
				assert.Empty(t, uid)
				assert.NotContains(t, got.GetAnnotations(), kubeutil.KubeAnnBindCompleted)
			} else {
				assert.Equal(t, "uid-of-the-deleted-pvc", uid)
			}
		})
	}
}

// A cross-cluster restore with a storage class mapping re-provisions a
// Retain-policy PV through its PVC instead of restoring the PV as it was.
func TestRestoreReprovisionsRetainVolumesAcrossClusters(t *testing.T) {
	skipped := map[string]volume.BackupVolumeInfo{"pv-1": {PVName: "pv-1", Skipped: true}}
	tests := []struct {
		name          string
		crossCluster  bool
		mapping       bool
		volumeInfoMap map[string]volume.BackupVolumeInfo
		wantPVs       []string
		wantVolume    string
	}{
		{name: "cross-cluster with a mapping", crossCluster: true, mapping: true, volumeInfoMap: skipped, wantPVs: []string{}},
		{name: "cross-cluster with a mapping, no volume info", crossCluster: true, mapping: true, wantPVs: []string{}},
		{name: "cross-cluster without a mapping", crossCluster: true, volumeInfoMap: skipped, wantPVs: []string{"/pv-1"}, wantVolume: "pv-1"},
		{name: "a mapping on the same cluster", mapping: true, volumeInfoMap: skipped, wantPVs: []string{"/pv-1"}, wantVolume: "pv-1"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			h.restorer.resourcePriorities = types.Priorities{HighPriorities: []string{"persistentvolumes", "persistentvolumeclaims"}}
			restore := defaultRestore().Result()
			if tc.crossCluster {
				restore.Annotations = map[string]string{"cloudcasa-cross-cluster-restore": "true"}
			}
			if tc.mapping {
				cm := builder.ForConfigMap(restore.Namespace, "cloudcasa-io-change-sc-restore-1").
					ObjectMeta(builder.WithLabels("velero.io/plugin-config", "", "velero.io/change-storage-class", "RestoreItemAction",
						"cloudcasa.io/job-id-for-change-storage-class", restore.Name)).
					Data("sc-old", "sc-new").Result()
				require.NoError(t, h.restorer.kbClient.Create(t.Context(), cm))
			}
			tarball := test.NewTarWriter(t).
				AddItems("persistentvolumes",
					builder.ForPersistentVolume("pv-1").ReclaimPolicy(corev1api.PersistentVolumeReclaimRetain).
						StorageClass("sc-old").ClaimRef("ns-1", "pvc-1").Result()).
				AddItems("persistentvolumeclaims",
					builder.ForPersistentVolumeClaim("ns-1", "pvc-1").VolumeName("pv-1").StorageClass("sc-old").Result()).
				Done()
			for _, r := range []*test.APIResource{test.PVs(), test.PVCs()} {
				h.DiscoveryClient.WithAPIResource(r)
			}
			require.NoError(t, h.restorer.discoveryHelper.Refresh())

			_, errs := h.restorer.Restore(&Request{
				Log:                 h.log,
				Restore:             restore,
				Backup:              defaultBackup().Result(),
				BackupReader:        tarball,
				BackupVolumeInfoMap: tc.volumeInfoMap,
			}, nil, nil)

			assertEmptyResults(t, errs)
			assertAPIContents(t, h, map[*test.APIResource][]string{test.PVs(): tc.wantPVs})
			pvc, err := h.DynamicClient.Resource(test.PVCs().GVR()).Namespace("ns-1").Get(t.Context(), "pvc-1", metav1.GetOptions{})
			require.NoError(t, err)
			volumeName, _, _ := unstructured.NestedString(pvc.Object, "spec", "volumeName")
			assert.Equal(t, tc.wantVolume, volumeName)
		})
	}
}

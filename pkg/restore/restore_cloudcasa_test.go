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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/builder"
	"github.com/vmware-tanzu/velero/pkg/test"
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

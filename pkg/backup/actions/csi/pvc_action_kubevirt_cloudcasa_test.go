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
	"errors"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/vmware-tanzu/velero/internal/catalogic"
	"github.com/vmware-tanzu/velero/pkg/builder"
)

// runningCbtVM is a ready KubeVirt VM with CBT enabled whose disks are the given PVCs.
func runningCbtVM(namespace, name string, claimNames ...string) *unstructured.Unstructured {
	var volumes []any
	for _, claimName := range claimNames {
		volumes = append(volumes, map[string]any{"name": claimName, "persistentVolumeClaim": map[string]any{"claimName": claimName}})
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "kubevirt.io/v1",
		"kind":       "VirtualMachine",
		"metadata":   map[string]any{"namespace": namespace, "name": name},
		"spec":       map[string]any{"template": map[string]any{"spec": map[string]any{"volumes": volumes}}},
		"status":     map[string]any{"ready": true, "changedBlockTracking": map[string]any{"state": "Enabled"}},
	}}
}

// countingClient counts the List calls made through it.
func countingClient(lists *int, listErr error, objects ...crclient.Object) crclient.Client {
	return fake.NewClientBuilder().WithObjects(objects...).WithInterceptorFuncs(interceptor.Funcs{
		List: func(ctx context.Context, client crclient.WithWatch, list crclient.ObjectList, opts ...crclient.ListOption) error {
			*lists++
			if listErr != nil {
				return listErr
			}
			return client.List(ctx, list, opts...)
		},
	}).Build()
}

// With KubeVirt CBT enabled, a disk of a running CBT VM is never snapshotted:
// this rule comes before every other one.
func TestShouldSkipSnapshotOfARunningCbtVmDisk(t *testing.T) {
	snapshotClass := map[string]string{"sc": "SNAPSHOT"}
	tests := []struct {
		name    string
		config  catalogic.PluginConfig
		pvcName string
		want    bool
	}{
		{name: "VM disk with CBT enabled", config: catalogic.PluginConfig{SnapshotWherePossible: true, KubevirtCbtEnabled: true}, pvcName: "disk", want: true},
		{name: "VM disk beats storage class method SNAPSHOT", config: catalogic.PluginConfig{SnapshotWherePossible: true, KubevirtCbtEnabled: true, StorageClassBackupMethodMap: snapshotClass}, pvcName: "disk", want: true},
		{name: "VM disk with CBT disabled", config: catalogic.PluginConfig{SnapshotWherePossible: true}, pvcName: "disk", want: false},
		{name: "other PVC with CBT enabled", config: catalogic.PluginConfig{SnapshotWherePossible: true, KubevirtCbtEnabled: true}, pvcName: "data", want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			config := tc.config
			withPluginConfig(t, &config)
			lists := 0
			p := &pvcBackupItemAction{log: logrus.New(), crClient: countingClient(&lists, nil, runningCbtVM("ns", "vm", "disk"))}
			pvc := builder.ForPersistentVolumeClaim("ns", tc.pvcName).StorageClass("sc").Result()

			skip, err := p.shouldSkipSnapshot(pvc, "pv", "ebs.csi.aws.com", "backup-1")

			require.NoError(t, err)
			assert.Equal(t, tc.want, skip)
			if !tc.config.KubevirtCbtEnabled {
				assert.Zero(t, lists, "VMs are not listed when CBT is disabled")
			}
		})
	}
}

func TestShouldSkipSnapshotFailsWhenVMsCannotBeListed(t *testing.T) {
	withPluginConfig(t, &catalogic.PluginConfig{SnapshotWherePossible: true, KubevirtCbtEnabled: true})
	lists := 0
	p := &pvcBackupItemAction{log: logrus.New(), crClient: countingClient(&lists, errors.New("connection refused"))}

	_, err := p.shouldSkipSnapshot(builder.ForPersistentVolumeClaim("ns", "disk").Result(), "pv", "ebs.csi.aws.com", "backup-1")

	require.EqualError(t, err, "error listing running CBT-enabled KubeVirt VM disks in namespace ns: "+
		"listing KubeVirt VirtualMachines in namespace ns: connection refused")
}

// The VM disks of a namespace are listed once per job and kept for an hour.
func TestRunningCbtVmPvcsCache(t *testing.T) {
	lists := 0
	p := &pvcBackupItemAction{log: logrus.New(), crClient: countingClient(&lists, nil,
		runningCbtVM("ns-1", "vm-1", "disk-1"),
		runningCbtVM("ns-2", "vm-2", "disk-2"),
	)}
	ctx := context.Background()

	pvcs, err := p.runningCbtVmPvcs(ctx, "backup-1", "ns-1")
	require.NoError(t, err)
	assert.Equal(t, map[string]bool{"disk-1": true}, pvcs)
	_, err = p.runningCbtVmPvcs(ctx, "backup-1", "ns-1")
	require.NoError(t, err)
	assert.Equal(t, 1, lists, "the second call is served from the cache")

	pvcs, err = p.runningCbtVmPvcs(ctx, "backup-1", "ns-2")
	require.NoError(t, err)
	assert.Equal(t, map[string]bool{"disk-2": true}, pvcs)
	_, err = p.runningCbtVmPvcs(ctx, "backup-2", "ns-1")
	require.NoError(t, err)
	assert.Equal(t, 3, lists, "each job and namespace is listed on its own")

	// An entry older than an hour is listed again; other expired entries are dropped.
	p.cbtVmPvcsCache["backup-1/ns-1"] = cbtVmPvcsCacheEntry{pvcSet: map[string]bool{"stale": true}, created: time.Now().Add(-61 * time.Minute)}
	p.cbtVmPvcsCache["backup-1/ns-2"] = cbtVmPvcsCacheEntry{pvcSet: map[string]bool{"stale": true}, created: time.Now().Add(-2 * time.Hour)}
	pvcs, err = p.runningCbtVmPvcs(ctx, "backup-1", "ns-1")
	require.NoError(t, err)
	assert.Equal(t, map[string]bool{"disk-1": true}, pvcs)
	assert.Equal(t, 4, lists)
	assert.NotContains(t, p.cbtVmPvcsCache, "backup-1/ns-2")
	assert.Contains(t, p.cbtVmPvcsCache, "backup-2/ns-1")
}

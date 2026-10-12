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
	"bytes"
	"encoding/json"
	"testing"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1api "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	kbclient "sigs.k8s.io/controller-runtime/pkg/client"

	velerov1 "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/builder"
	"github.com/vmware-tanzu/velero/pkg/kuberesource"
	"github.com/vmware-tanzu/velero/pkg/plugin/velero"
	biav2 "github.com/vmware-tanzu/velero/pkg/plugin/velero/backupitemaction/v2"
	"github.com/vmware-tanzu/velero/pkg/test"
	kubevirtutil "github.com/vmware-tanzu/velero/pkg/util/kubevirt"
)

func virtualMachines(items ...metav1.Object) *test.APIResource {
	return &test.APIResource{
		Group:      "kubevirt.io",
		Version:    "v1",
		Name:       "virtualmachines",
		ShortName:  "vm",
		Kind:       "VirtualMachine",
		Namespaced: true,
		Items:      items,
	}
}

func newVirtualMachine(namespace, name string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "kubevirt.io/v1",
		"kind":       "VirtualMachine",
		"metadata":   map[string]any{"namespace": namespace, "name": name},
	}}
}

// vmProgress reads the job's KubeVirt VM progress ConfigMap.
func vmProgress(t *testing.T, client kbclient.Client, namespace, jobID string) map[string]kubevirtutil.VMStatus {
	t.Helper()
	configMap := &corev1api.ConfigMap{}
	require.NoError(t, client.Get(t.Context(), kbclient.ObjectKey{Namespace: namespace, Name: jobID + "-cc-vm-progress"}, configMap))
	var data struct {
		JobID   string                           `json:"jobID"`
		JobType string                           `json:"jobType"`
		VMs     map[string]kubevirtutil.VMStatus `json:"vms"`
	}
	require.NoError(t, json.Unmarshal([]byte(configMap.Data["vmStatusData"]), &data))
	assert.Equal(t, jobID, data.JobID)
	return data.VMs
}

// The backup of a KubeVirt VM and of the items its action adds is reported in
// the job's VM progress ConfigMap; other resources are not.
func TestBackupReportsKubeVirtVMProgress(t *testing.T) {
	itemBlockPool := StartItemBlockWorkerPool(t.Context(), 1, logrus.StandardLogger())
	defer itemBlockPool.Stop()

	h := newHarness(t, itemBlockPool)
	backup := defaultBackup().Result()
	req := &Request{
		Backup:           backup,
		SkippedPVTracker: NewSkipPVTracker(),
		BackedUpItems:    NewBackedUpItemsMap(),
		WorkerPool:       itemBlockPool,
	}
	h.addItems(t, test.Pods(builder.ForPod("ns-1", "pod-1").Result()))
	h.addItems(t, test.PVCs(
		builder.ForPersistentVolumeClaim("ns-1", "disk-1").Result(),
		builder.ForPersistentVolumeClaim("ns-1", "disk-2").Result(),
		builder.ForPersistentVolumeClaim("ns-1", "pod-disk").Result(),
	))
	h.addItems(t, virtualMachines(newVirtualMachine("ns-1", "vm-1"), newVirtualMachine("ns-1", "vm-2")))

	disks := map[string][]velero.ResourceIdentifier{
		"vm-1":  {{GroupResource: kuberesource.PersistentVolumeClaims, Namespace: "ns-1", Name: "disk-1"}},
		"vm-2":  {{GroupResource: kuberesource.PersistentVolumeClaims, Namespace: "ns-1", Name: "disk-2"}},
		"pod-1": {{GroupResource: kuberesource.PersistentVolumeClaims, Namespace: "ns-1", Name: "pod-disk"}},
	}
	actions := []biav2.BackupItemAction{
		&pluggableAction{
			selector: velero.ResourceSelector{IncludedResources: []string{"virtualmachines.kubevirt.io", "pods"}},
			executeFunc: func(item runtime.Unstructured, _ *velerov1.Backup) (runtime.Unstructured, []velero.ResourceIdentifier, string, []velero.ResourceIdentifier, error) {
				name := item.UnstructuredContent()["metadata"].(map[string]any)["name"].(string)
				return item, disks[name], "", nil, nil
			},
		},
		&pluggableAction{
			selector: velero.ResourceSelector{IncludedResources: []string{"persistentvolumeclaims"}},
			executeFunc: func(item runtime.Unstructured, _ *velerov1.Backup) (runtime.Unstructured, []velero.ResourceIdentifier, string, []velero.ResourceIdentifier, error) {
				if item.UnstructuredContent()["metadata"].(map[string]any)["name"] == "disk-2" {
					return nil, nil, "", nil, errors.New("snapshot failed")
				}
				return item, nil, "", nil, nil
			},
		},
	}

	require.NoError(t, h.backupper.Backup(h.log, req, bytes.NewBuffer([]byte{}), actions, nil, nil))

	vms := vmProgress(t, h.backupper.kbClient, backup.Namespace, backup.Name)
	require.Len(t, vms, 2, "only VMs are reported")

	vm1 := vms["ns-1/vm-1"]
	assert.Equal(t, "COMPLETED", vm1.Status)
	assert.Equal(t, "Backup completed successfully", vm1.Message)
	assert.Equal(t, []kubevirtutil.AdditionalResourceStatus{{
		ResourceType: "persistentvolumeclaims",
		ResourceName: "ns-1/disk-1",
		Status:       "COMPLETED",
		Message:      "Backup of additional resource completed successfully",
	}}, vm1.AdditionalResources)

	// disk-2 failed before the VM was reached; the VM still reports that failure.
	vm2 := vms["ns-1/vm-2"]
	assert.Equal(t, "FAILED", vm2.Status)
	assert.Contains(t, vm2.Message, "snapshot failed")
	require.Len(t, vm2.AdditionalResources, 1)
	assert.Equal(t, "ns-1/disk-2", vm2.AdditionalResources[0].ResourceName)
	assert.Equal(t, "FAILED", vm2.AdditionalResources[0].Status)
	assert.Contains(t, vm2.AdditionalResources[0].Message, "snapshot failed")
}

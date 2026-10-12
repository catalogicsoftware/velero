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
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1api "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"

	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/builder"
	"github.com/vmware-tanzu/velero/pkg/kuberesource"
	"github.com/vmware-tanzu/velero/pkg/plugin/velero"
	riav2 "github.com/vmware-tanzu/velero/pkg/plugin/velero/restoreitemaction/v2"
	"github.com/vmware-tanzu/velero/pkg/test"
	kubevirtutil "github.com/vmware-tanzu/velero/pkg/util/kubevirt"
)

func newVirtualMachine(namespace, name string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "kubevirt.io/v1",
		"kind":       "VirtualMachine",
		"metadata":   map[string]any{"namespace": namespace, "name": name},
	}}
}

// vmRestoreAction returns each VM's disks as additional items and waits for them.
type vmRestoreAction struct {
	disks    map[string][]velero.ResourceIdentifier
	notReady map[string]bool
}

func (a *vmRestoreAction) Name() string { return "test/kubevirt-vm" }

func (a *vmRestoreAction) AppliesTo() (velero.ResourceSelector, error) {
	return velero.ResourceSelector{IncludedResources: []string{"virtualmachines.kubevirt.io"}}, nil
}

func (a *vmRestoreAction) Execute(input *velero.RestoreItemActionExecuteInput) (*velero.RestoreItemActionExecuteOutput, error) {
	name := input.Item.UnstructuredContent()["metadata"].(map[string]any)["name"].(string)
	return &velero.RestoreItemActionExecuteOutput{
		UpdatedItem:            input.Item,
		AdditionalItems:        a.disks[name],
		WaitForAdditionalItems: true,
	}, nil
}

func (a *vmRestoreAction) Progress(string, *velerov1api.Restore) (velero.OperationProgress, error) {
	return velero.OperationProgress{}, nil
}

func (a *vmRestoreAction) Cancel(string, *velerov1api.Restore) error { return nil }

func (a *vmRestoreAction) AreAdditionalItemsReady(items []velero.ResourceIdentifier, _ *velerov1api.Restore) (bool, error) {
	for _, item := range items {
		if a.notReady[item.Name] {
			return false, errors.New("data volume import failed")
		}
	}
	return true, nil
}

// The restore of a KubeVirt VM and of the items its action adds is reported
// in the job's VM progress ConfigMap, keyed by the VM's target namespace.
func TestRestoreReportsKubeVirtVMProgress(t *testing.T) {
	h := newHarness(t)
	tarball := test.NewTarWriter(t).
		AddItems("persistentvolumeclaims",
			builder.ForPersistentVolumeClaim("ns-1", "disk-1").Result(),
			builder.ForPersistentVolumeClaim("ns-1", "disk-2").Result(),
		).
		AddItems("virtualmachines.kubevirt.io", newVirtualMachine("ns-1", "vm-1"), newVirtualMachine("ns-1", "vm-2")).
		Done()
	vms := &test.APIResource{Group: "kubevirt.io", Version: "v1", Name: "virtualmachines", Kind: "VirtualMachine", Namespaced: true}
	for _, r := range []*test.APIResource{test.PVCs(), vms} {
		h.DiscoveryClient.WithAPIResource(r)
	}
	require.NoError(t, h.restorer.discoveryHelper.Refresh())
	action := &vmRestoreAction{
		disks: map[string][]velero.ResourceIdentifier{
			"vm-1": {
				{GroupResource: kuberesource.PersistentVolumeClaims, Namespace: "ns-1", Name: "disk-1"},
				{GroupResource: kuberesource.PersistentVolumeClaims, Namespace: "ns-1", Name: "not-in-backup"},
			},
			"vm-2": {{GroupResource: kuberesource.PersistentVolumeClaims, Namespace: "ns-1", Name: "disk-2"}},
		},
		notReady: map[string]bool{"disk-2": true},
	}
	restore := defaultRestore().NamespaceMappings("ns-1", "ns-2").Result()

	_, errs := h.restorer.Restore(&Request{
		Log:          h.log,
		Restore:      restore,
		Backup:       defaultBackup().Result(),
		BackupReader: tarball,
	}, []riav2.RestoreItemAction{action}, nil)
	require.Len(t, errs.Namespaces["ns-2"], 1)

	configMap := &corev1api.ConfigMap{}
	require.NoError(t, h.restorer.kbClient.Get(t.Context(),
		crclient.ObjectKey{Namespace: restore.Namespace, Name: "restore-1-cc-vm-progress"}, configMap))
	assert.Equal(t, map[string]string{"cc-job-id": "restore-1"}, configMap.Labels)
	var data struct {
		JobType string                           `json:"jobType"`
		VMs     map[string]kubevirtutil.VMStatus `json:"vms"`
	}
	require.NoError(t, json.Unmarshal([]byte(configMap.Data["vmStatusData"]), &data))
	assert.Equal(t, "Restore", data.JobType)

	// The VM is keyed by its target namespace; its disks keep their backup namespace.
	assert.Equal(t, map[string]kubevirtutil.VMStatus{
		"ns-2/vm-1": {
			VMName:  "ns-2/vm-1",
			Status:  "COMPLETED",
			Message: "Restore completed successfully",
			AdditionalResources: []kubevirtutil.AdditionalResourceStatus{{
				ResourceType: "persistentvolumeclaims",
				ResourceName: "ns-1/disk-1",
				Status:       "COMPLETED",
				Message:      "Restore of additional resource completed successfully",
			}},
		},
		"ns-2/vm-2": {
			VMName:  "ns-2/vm-2",
			Status:  "FAILED",
			Message: "Restore failed: namespace ns-2: error verifying additional items are ready to use: data volume import failed",
			AdditionalResources: []kubevirtutil.AdditionalResourceStatus{{
				ResourceType: "persistentvolumeclaims",
				ResourceName: "ns-1/disk-2",
				Status:       "COMPLETED",
				Message:      "Restore of additional resource completed successfully",
			}},
		},
	}, data.VMs)
}

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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

var vmsGVR = (&test.APIResource{Group: "kubevirt.io", Version: "v1", Name: "virtualmachines"}).GVR()

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

// namesSeenAction records the names of each item it is run for, as restored
// and as backed up.
type namesSeenAction struct {
	vmRestoreAction
	seen []string
}

func (a *namesSeenAction) AppliesTo() (velero.ResourceSelector, error) {
	return velero.ResourceSelector{}, nil
}

func (a *namesSeenAction) Execute(input *velero.RestoreItemActionExecuteInput) (*velero.RestoreItemActionExecuteOutput, error) {
	item := input.Item.(*unstructured.Unstructured)
	fromBackup := input.ItemFromBackup.(*unstructured.Unstructured)
	a.seen = append(a.seen, item.GetKind()+" "+fromBackup.GetName()+" as "+item.GetName())
	if item.GetKind() != "VirtualMachine" {
		return velero.NewRestoreItemActionExecuteOutput(input.Item), nil
	}
	return a.vmRestoreAction.Execute(input)
}

// A VM restored under a new name next to the running original gets copies of
// its disk and secrets under new names. The original VM and its objects are
// not touched, and a secret that another VM of the restore keeps is restored
// under its own name too.
func TestRestoreCreatesARenamedVMWithCopiesOfWhatItUses(t *testing.T) {
	h := newHarness(t)
	vms := &test.APIResource{Group: "kubevirt.io", Version: "v1", Name: "virtualmachines", Kind: "VirtualMachine", Namespaced: true}
	for _, r := range []*test.APIResource{test.PVCs(), test.Secrets(), vms} {
		h.DiscoveryClient.WithAPIResource(r)
	}
	require.NoError(t, h.restorer.discoveryHelper.Refresh())

	running := newVirtualMachine("ns-1", "vm-1")
	running.Object["spec"] = map[string]any{"running": true}
	_, err := h.DynamicClient.Resource(vmsGVR).Namespace("ns-1").Create(t.Context(), running, metav1.CreateOptions{})
	require.NoError(t, err)

	tarball := test.NewTarWriter(t).
		AddItems("persistentvolumeclaims", builder.ForPersistentVolumeClaim("ns-1", "vm-1-disk").Result()).
		AddItems("secrets",
			builder.ForSecret("ns-1", "vm-1-cloudinit").Result(),
			builder.ForSecret("ns-1", "shared").Result(),
		).
		AddItems("virtualmachines.kubevirt.io", newVirtualMachine("ns-1", "vm-1"), newVirtualMachine("ns-1", "vm-2")).
		Done()
	renames, err := json.Marshal([]kubevirtutil.VMRename{{
		Namespace: "ns-1", Name: "vm-1", NewName: "vm-1-clone",
		Items: []kubevirtutil.RenamedItem{
			{Resource: "persistentvolumeclaims", Name: "vm-1-disk", NewName: "vm-1-clone-disk"},
			{Resource: "secrets", Name: "vm-1-cloudinit", NewName: "vm-1-clone-cloudinit"},
			{Resource: "secrets", Name: "shared", NewName: "vm-1-clone-shared", KeepOriginal: true},
		},
	}})
	require.NoError(t, err)
	restore := defaultRestore().
		ObjectMeta(builder.WithAnnotations(kubevirtutil.VMRenamesAnnotation, string(renames))).
		IncludedNamespaces("ns-1").
		Result()
	action := &namesSeenAction{vmRestoreAction: vmRestoreAction{disks: map[string][]velero.ResourceIdentifier{
		"vm-1-clone": {
			{GroupResource: kuberesource.PersistentVolumeClaims, Namespace: "ns-1", Name: "vm-1-disk"},
			{GroupResource: kuberesource.Secrets, Namespace: "ns-1", Name: "vm-1-cloudinit"},
			{GroupResource: kuberesource.Secrets, Namespace: "ns-1", Name: "shared"},
		},
	}}}

	warnings, errs := h.restorer.Restore(&Request{
		Log:                  h.log,
		Restore:              restore,
		Backup:               defaultBackup().Result(),
		BackupReader:         tarball,
		DisableInformerCache: true,
	}, []riav2.RestoreItemAction{action}, nil)
	assert.Empty(t, warnings.Namespaces)
	assert.Empty(t, errs.Namespaces)
	assert.Empty(t, errs.Velero)

	assertAPIContents(t, h, map[*test.APIResource][]string{
		test.PVCs():    {"ns-1/vm-1-clone-disk"},
		test.Secrets(): {"ns-1/shared", "ns-1/vm-1-clone-cloudinit", "ns-1/vm-1-clone-shared"},
		vms:            {"ns-1/vm-1", "ns-1/vm-1-clone", "ns-1/vm-2"},
	})
	original, err := h.DynamicClient.Resource(vmsGVR).Namespace("ns-1").Get(t.Context(), "vm-1", metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, map[string]any{"running": true}, original.Object["spec"], "the original VM is left alone")

	assert.ElementsMatch(t, []string{
		"PersistentVolumeClaim vm-1-disk as vm-1-clone-disk",
		"Secret vm-1-cloudinit as vm-1-clone-cloudinit",
		"Secret shared as shared",
		"Secret shared as vm-1-clone-shared",
		"VirtualMachine vm-1 as vm-1-clone",
		"VirtualMachine vm-2 as vm-2",
	}, action.seen, "actions see each copy once, with the backed up item under its own name")

	configMap := &corev1api.ConfigMap{}
	require.NoError(t, h.restorer.kbClient.Get(t.Context(),
		crclient.ObjectKey{Namespace: restore.Namespace, Name: "restore-1-cc-vm-progress"}, configMap))
	var data struct {
		VMs map[string]kubevirtutil.VMStatus `json:"vms"`
	}
	require.NoError(t, json.Unmarshal([]byte(configMap.Data["vmStatusData"]), &data))
	require.Contains(t, data.VMs, "ns-1/vm-1-clone")
	var reported []string
	for _, r := range data.VMs["ns-1/vm-1-clone"].AdditionalResources {
		reported = append(reported, r.ResourceType+" "+r.ResourceName)
	}
	assert.ElementsMatch(t, []string{
		"persistentvolumeclaims ns-1/vm-1-clone-disk",
		"secrets ns-1/vm-1-clone-cloudinit",
		"secrets ns-1/vm-1-clone-shared",
	}, reported, "the VM's objects are reported under the names they were restored as")
}

// A copy whose name is in use, say by the disk of an earlier clone, is not
// restored over that object, and the renamed VM is not created, so that it
// never uses an object that is not its own.
func TestARenamedVMIsNotRestoredWhenTheNameOfACopyIsInUse(t *testing.T) {
	for _, disableInformerCache := range []bool{true, false} {
		h := newHarness(t)
		vms := &test.APIResource{Group: "kubevirt.io", Version: "v1", Name: "virtualmachines", Kind: "VirtualMachine", Namespaced: true}
		h.DiscoveryClient.WithAPIResource(test.Secrets())
		h.DiscoveryClient.WithAPIResource(vms)
		h.AddItems(t, test.PVCs(builder.ForPersistentVolumeClaim("ns-1", "vm-1-clone-disk").
			ObjectMeta(builder.WithLabels("disk-of", "earlier-clone")).Result()))

		tarball := test.NewTarWriter(t).
			AddItems("persistentvolumeclaims", builder.ForPersistentVolumeClaim("ns-1", "vm-1-disk").Result()).
			AddItems("secrets", builder.ForSecret("ns-1", "vm-1-cloudinit").Result()).
			AddItems("virtualmachines.kubevirt.io", newVirtualMachine("ns-1", "vm-1")).
			Done()
		renames, err := json.Marshal([]kubevirtutil.VMRename{{
			Namespace: "ns-1", Name: "vm-1", NewName: "vm-1-clone",
			Items: []kubevirtutil.RenamedItem{
				{Resource: "persistentvolumeclaims", Name: "vm-1-disk", NewName: "vm-1-clone-disk"},
				{Resource: "secrets", Name: "vm-1-cloudinit", NewName: "vm-1-clone-cloudinit"},
			},
		}})
		require.NoError(t, err)
		restore := defaultRestore().
			ObjectMeta(builder.WithAnnotations(kubevirtutil.VMRenamesAnnotation, string(renames))).
			IncludedNamespaces("ns-1").
			Result()
		action := &namesSeenAction{vmRestoreAction: vmRestoreAction{disks: map[string][]velero.ResourceIdentifier{
			"vm-1-clone": {
				{GroupResource: kuberesource.PersistentVolumeClaims, Namespace: "ns-1", Name: "vm-1-disk"},
				{GroupResource: kuberesource.Secrets, Namespace: "ns-1", Name: "vm-1-cloudinit"},
			},
		}}}

		_, errs := h.restorer.Restore(&Request{
			Log:                  h.log,
			Restore:              restore,
			Backup:               defaultBackup().Result(),
			BackupReader:         tarball,
			DisableInformerCache: disableInformerCache,
		}, []riav2.RestoreItemAction{action}, nil)

		require.Len(t, errs.Namespaces["ns-1"], 2, "disableInformerCache=%v", disableInformerCache)
		assert.Contains(t, errs.Namespaces["ns-1"][0], `could not restore persistentvolumeclaims/ns-1/vm-1-disk as vm-1-clone-disk: namespace "ns-1" already has a PersistentVolumeClaim of that name`)
		assert.Contains(t, errs.Namespaces["ns-1"][1], "VirtualMachine ns-1/vm-1 was not restored as vm-1-clone")
		assert.Contains(t, errs.Namespaces["ns-1"][1], "(persistentvolumeclaims vm-1-clone-disk)")
		for _, e := range errs.Namespaces["ns-1"] {
			assert.NotContains(t, e, "already exists", "KAS hides messages with that phrase")
		}

		assertAPIContents(t, h, map[*test.APIResource][]string{
			test.PVCs():    {"ns-1/vm-1-clone-disk"},
			test.Secrets(): {"ns-1/vm-1-clone-cloudinit"},
			vms:            {},
		})
		disk, err := h.DynamicClient.Resource(test.PVCs().GVR()).Namespace("ns-1").Get(t.Context(), "vm-1-clone-disk", metav1.GetOptions{})
		require.NoError(t, err)
		assert.Equal(t, map[string]string{"disk-of": "earlier-clone"}, disk.GetLabels(), "the disk in use is left alone")

		configMap := &corev1api.ConfigMap{}
		require.NoError(t, h.restorer.kbClient.Get(t.Context(),
			crclient.ObjectKey{Namespace: restore.Namespace, Name: "restore-1-cc-vm-progress"}, configMap))
		var data struct {
			VMs map[string]kubevirtutil.VMStatus `json:"vms"`
		}
		require.NoError(t, json.Unmarshal([]byte(configMap.Data["vmStatusData"]), &data))
		assert.Equal(t, "FAILED", data.VMs["ns-1/vm-1-clone"].Status)
	}
}

// A restore whose VM renames cannot be read fails before it restores anything.
func TestRestoreWithUnreadableVMRenamesFails(t *testing.T) {
	h := newHarness(t)
	tarball := test.NewTarWriter(t).
		AddItems("secrets", builder.ForSecret("ns-1", "s").Result()).
		Done()
	h.DiscoveryClient.WithAPIResource(test.Secrets())
	require.NoError(t, h.restorer.discoveryHelper.Refresh())
	restore := defaultRestore().
		ObjectMeta(builder.WithAnnotations(kubevirtutil.VMRenamesAnnotation, "{")).
		Result()

	_, errs := h.restorer.Restore(&Request{
		Log:          h.log,
		Restore:      restore,
		Backup:       defaultBackup().Result(),
		BackupReader: tarball,
	}, nil, nil)

	require.Len(t, errs.Velero, 1)
	assert.Contains(t, errs.Velero[0], kubevirtutil.VMRenamesAnnotation)
	assertAPIContents(t, h, map[*test.APIResource][]string{test.Secrets(): {}})
}

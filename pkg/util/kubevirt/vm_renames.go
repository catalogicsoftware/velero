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

package kubevirt

import (
	"encoding/json"
	"fmt"
	"slices"

	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
)

// VMRenamesAnnotation on a Restore lists the VMs it restores under new names.
// The engine restores the VM and the objects it uses under the new names, and
// the KubeVirt plugin points each renamed VM at its copies.
const VMRenamesAnnotation = "cloudcasa.io/vm-renames"

// VMRename is a VM of the backup restored as NewName, in its mapped namespace.
type VMRename struct {
	Namespace         string        `json:"namespace"`
	Name              string        `json:"name"`
	NewName           string        `json:"newName"`
	ClearMacAddresses bool          `json:"clearMacAddresses,omitempty"`
	NewFirmwareUUID   bool          `json:"newFirmwareUUID,omitempty"`
	Items             []RenamedItem `json:"items,omitempty"`
}

// RenamedItem is an object in the VM's namespace that the VM uses, restored as
// NewName for this VM. Resource is a group resource, e.g. "secrets" or
// "datavolumes.cdi.kubevirt.io". KeepOriginal also restores it under its own
// name, for a VM of the restore that keeps it.
type RenamedItem struct {
	Resource     string `json:"resource"`
	Name         string `json:"name"`
	NewName      string `json:"newName"`
	KeepOriginal bool   `json:"keepOriginal,omitempty"`
}

// ItemRenames gives the names under which a restore creates each backed up
// item. A nil ItemRenames renames nothing.
type ItemRenames struct {
	newNames     map[ItemKey][]string
	keepOriginal map[ItemKey]bool
	vmItems      map[ItemKey]map[ItemKey]string
	// takenNames are copies not restored because the name was in use
	takenNames map[ItemKey]bool
}

// VMRenamesOf reads the VM renames of a restore. A restore without them gets nil.
func VMRenamesOf(restore *velerov1api.Restore) (*ItemRenames, error) {
	value, ok := restore.Annotations[VMRenamesAnnotation]
	if !ok {
		return nil, nil
	}

	var vms []VMRename
	if err := json.Unmarshal([]byte(value), &vms); err != nil {
		return nil, fmt.Errorf("invalid annotation %s: %w", VMRenamesAnnotation, err)
	}

	renames := &ItemRenames{
		newNames:     map[ItemKey][]string{},
		keepOriginal: map[ItemKey]bool{},
		vmItems:      map[ItemKey]map[ItemKey]string{},
		takenNames:   map[ItemKey]bool{},
	}
	for _, vm := range vms {
		if vm.Namespace == "" || vm.Name == "" || vm.NewName == "" || vm.NewName == vm.Name {
			return nil, fmt.Errorf("invalid annotation %s: VM %s/%s cannot be restored as %q", VMRenamesAnnotation, vm.Namespace, vm.Name, vm.NewName)
		}
		vmKey := ItemKey{Resource: kubeVirtVMGroupResource.String(), Namespace: vm.Namespace, Name: vm.Name}
		if _, ok := renames.vmItems[vmKey]; ok {
			return nil, fmt.Errorf("invalid annotation %s: VM %s/%s is listed twice", VMRenamesAnnotation, vm.Namespace, vm.Name)
		}
		renames.newNames[vmKey] = []string{vm.NewName}
		renames.vmItems[vmKey] = map[ItemKey]string{}

		for _, item := range vm.Items {
			if item.Resource == "" || item.Name == "" || item.NewName == "" || item.NewName == item.Name {
				return nil, fmt.Errorf("invalid annotation %s: %s %s/%s of VM %s cannot be restored as %q",
					VMRenamesAnnotation, item.Resource, vm.Namespace, item.Name, vm.Name, item.NewName)
			}
			itemKey := ItemKey{Resource: item.Resource, Namespace: vm.Namespace, Name: item.Name}
			renames.vmItems[vmKey][itemKey] = item.NewName
			if !slices.Contains(renames.newNames[itemKey], item.NewName) {
				renames.newNames[itemKey] = append(renames.newNames[itemKey], item.NewName)
			}
			renames.keepOriginal[itemKey] = renames.keepOriginal[itemKey] || item.KeepOriginal
		}
	}
	return renames, nil
}

// NamesFor returns the names of the copies of a backed up item, and whether
// the item is also restored under its own name. No names means no rename.
func (r *ItemRenames) NamesFor(resource, namespace, name string) (newNames []string, keepOriginal bool) {
	if r == nil {
		return nil, false
	}
	key := ItemKey{Resource: resource, Namespace: namespace, Name: name}
	return r.newNames[key], r.keepOriginal[key]
}

// NameTaken records that the copy newName of an item in the backup namespace
// was not restored because an object of that name is already in the cluster.
func (r *ItemRenames) NameTaken(resource, namespace, newName string) {
	r.takenNames[ItemKey{Resource: resource, Namespace: namespace, Name: newName}] = true
}

// TakenNamesOf returns the copies of a VM's items that were not restored
// because their names were in use, as "resource name". The VM is named as in
// the backup.
func (r *ItemRenames) TakenNamesOf(vmNamespace, vmName string) []string {
	if r == nil {
		return nil
	}
	var taken []string
	vmKey := ItemKey{Resource: kubeVirtVMGroupResource.String(), Namespace: vmNamespace, Name: vmName}
	for item, newName := range r.vmItems[vmKey] {
		if r.takenNames[ItemKey{Resource: item.Resource, Namespace: vmNamespace, Name: newName}] {
			taken = append(taken, item.Resource+" "+newName)
		}
	}
	slices.Sort(taken)
	return taken
}

// NameInVM returns the name under which an item that a VM uses is restored
// for that VM. Both are named as in the backup.
func (r *ItemRenames) NameInVM(vmNamespace, vmName, resource, name string) string {
	if r == nil {
		return name
	}
	vmKey := ItemKey{Resource: kubeVirtVMGroupResource.String(), Namespace: vmNamespace, Name: vmName}
	if newName, ok := r.vmItems[vmKey][ItemKey{Resource: resource, Namespace: vmNamespace, Name: name}]; ok {
		return newName
	}
	return name
}

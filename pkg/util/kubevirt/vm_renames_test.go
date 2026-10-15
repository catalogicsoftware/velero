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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
)

func restoreWithVMRenames(value string) *velerov1api.Restore {
	return &velerov1api.Restore{ObjectMeta: metav1.ObjectMeta{
		Name:        "r1",
		Annotations: map[string]string{VMRenamesAnnotation: value},
	}}
}

func TestARestoreWithoutVMRenamesRenamesNothing(t *testing.T) {
	renames, err := VMRenamesOf(&velerov1api.Restore{})
	require.NoError(t, err)

	newNames, keepOriginal := renames.NamesFor("persistentvolumeclaims", "ns1", "disk")
	assert.Empty(t, newNames)
	assert.False(t, keepOriginal)
	assert.Equal(t, "disk", renames.NameInVM("ns1", "vm1", "persistentvolumeclaims", "disk"))
}

func TestVMRenamesGiveEachItemTheNamesOfItsCopies(t *testing.T) {
	renames, err := VMRenamesOf(restoreWithVMRenames(`[
		{"namespace": "ns1", "name": "vm1", "newName": "vm1-clone", "items": [
			{"resource": "persistentvolumeclaims", "name": "vm1-disk", "newName": "vm1-clone-disk"},
			{"resource": "secrets", "name": "shared", "newName": "vm1-clone-shared", "keepOriginal": true}
		]},
		{"namespace": "ns1", "name": "vm2", "newName": "vm2-clone", "items": [
			{"resource": "secrets", "name": "shared", "newName": "vm2-clone-shared"}
		]}
	]`))
	require.NoError(t, err)

	newNames, keepOriginal := renames.NamesFor("virtualmachines.kubevirt.io", "ns1", "vm1")
	assert.Equal(t, []string{"vm1-clone"}, newNames)
	assert.False(t, keepOriginal)

	newNames, keepOriginal = renames.NamesFor("persistentvolumeclaims", "ns1", "vm1-disk")
	assert.Equal(t, []string{"vm1-clone-disk"}, newNames)
	assert.False(t, keepOriginal)

	newNames, keepOriginal = renames.NamesFor("secrets", "ns1", "shared")
	assert.Equal(t, []string{"vm1-clone-shared", "vm2-clone-shared"}, newNames)
	assert.True(t, keepOriginal)

	newNames, _ = renames.NamesFor("secrets", "ns2", "shared")
	assert.Empty(t, newNames, "items are matched in the VM's own namespace only")

	assert.Equal(t, "vm2-clone-shared", renames.NameInVM("ns1", "vm2", "secrets", "shared"))
	assert.Equal(t, "other", renames.NameInVM("ns1", "vm2", "secrets", "other"))
}

func TestVMRenamesThatTheEngineCannotApplyAreRejected(t *testing.T) {
	for name, value := range map[string]string{
		"not JSON":          `{`,
		"no new name":       `[{"namespace": "ns1", "name": "vm1"}]`,
		"no namespace":      `[{"name": "vm1", "newName": "vm1-clone"}]`,
		"same name":         `[{"namespace": "ns1", "name": "vm1", "newName": "vm1"}]`,
		"VM listed twice":   `[{"namespace": "ns1", "name": "vm1", "newName": "a"}, {"namespace": "ns1", "name": "vm1", "newName": "b"}]`,
		"item without kind": `[{"namespace": "ns1", "name": "vm1", "newName": "a", "items": [{"name": "d", "newName": "e"}]}]`,
		"item same name":    `[{"namespace": "ns1", "name": "vm1", "newName": "a", "items": [{"resource": "secrets", "name": "d", "newName": "d"}]}]`,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := VMRenamesOf(restoreWithVMRenames(value))
			assert.Error(t, err)
		})
	}
}

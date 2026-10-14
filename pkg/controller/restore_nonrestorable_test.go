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

package controller

import (
	"testing"

	"github.com/stretchr/testify/assert"

	api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
)

// The engine's own CRs must never be restored, whatever the API group is.
func TestNonRestorableResourcesUseAPIGroup(t *testing.T) {
	for _, resource := range []string{"backups", "restores", "resticrepositories", "backuprepositories"} {
		name := resource + "." + api.SchemeGroupVersion.Group
		assert.Contains(t, nonRestorableResources, name)
	}
	for _, name := range nonRestorableResources {
		assert.NotContains(t, name, ".velero.io", "%s still names the old API group", name)
	}
}

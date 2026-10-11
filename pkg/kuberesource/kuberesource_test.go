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

package kuberesource

import (
	"testing"

	"github.com/stretchr/testify/assert"

	velerov2alpha1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v2alpha1"
)

func TestDataUploadsUsesAPIGroup(t *testing.T) {
	assert.Equal(t, velerov2alpha1api.SchemeGroupVersion.Group, DataUploads.Group)
	assert.Equal(t, "datauploads", DataUploads.Resource)
	assert.Equal(t, "datauploads."+velerov2alpha1api.SchemeGroupVersion.Group, DataUploads.String())
}

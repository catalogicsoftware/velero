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

package volumehelper

import (
	"testing"

	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1api "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"

	"github.com/vmware-tanzu/velero/pkg/builder"
	"github.com/vmware-tanzu/velero/pkg/kuberesource"
	velerotest "github.com/vmware-tanzu/velero/pkg/test"
)

// A PVC without a PV returns its error to the caller without logging it here
// too, so the backup counts it once.
func TestShouldPerformSnapshotDoesNotLogTheMissingPVTwice(t *testing.T) {
	logger, hook := logrustest.NewNullLogger()
	vh := NewVolumeHelperImpl(nil, ptr.To(true), logger, velerotest.NewFakeControllerRuntimeClient(t), false, false)
	pvc := builder.ForPersistentVolumeClaim("ns", "pvc-pending").StorageClass("sc").Phase(corev1api.ClaimPending).Result()
	obj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(pvc)
	require.NoError(t, err)

	_, err = vh.ShouldPerformSnapshot(&unstructured.Unstructured{Object: obj}, kuberesource.PersistentVolumeClaims)

	require.Error(t, err)
	for _, e := range hook.AllEntries() {
		assert.NotEqual(t, logrus.ErrorLevel, e.Level, e.Message)
	}
}

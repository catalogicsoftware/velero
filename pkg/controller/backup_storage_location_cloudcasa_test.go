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
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/vmware-tanzu/velero/internal/storage"
	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/builder"
	"github.com/vmware-tanzu/velero/pkg/metrics"
	persistencemocks "github.com/vmware-tanzu/velero/pkg/persistence/mocks"
	"github.com/vmware-tanzu/velero/pkg/plugin/clientmgmt"
	pluginmocks "github.com/vmware-tanzu/velero/pkg/plugin/mocks"
	velerotest "github.com/vmware-tanzu/velero/pkg/test"
)

// A non-default location that is not validated yet logs no error or warning.
func TestLogReconciledPhaseIsQuietForAnUnvalidatedLocation(t *testing.T) {
	logger, hook := logrustest.NewNullLogger()
	r := &backupStorageLocationReconciler{log: logger, metrics: metrics.NewServerMetrics()}
	list := velerov1api.BackupStorageLocationList{Items: []velerov1api.BackupStorageLocation{
		*builder.ForBackupStorageLocation(velerov1api.DefaultNamespace, "job-bsl").Result(),
	}}

	r.logReconciledPhase(false, list, nil)

	for _, e := range hook.AllEntries() {
		assert.NotEqual(t, logrus.WarnLevel, e.Level, e.Message)
		assert.NotEqual(t, logrus.ErrorLevel, e.Level, e.Message)
	}

	r.logReconciledPhase(false, list, []string{"bucket not reachable"})
	require.NotNil(t, hook.LastEntry())
	assert.Equal(t, logrus.ErrorLevel, hook.LastEntry().Level, "a real failure is still reported")
}

// A request for a location that is gone names it and lists the ones there are.
func TestBSLReconcileNamesAMissingLocation(t *testing.T) {
	logger, hook := logrustest.NewNullLogger()
	other := builder.ForBackupStorageLocation(velerov1api.DefaultNamespace, "other").Result()
	pluginManager := &pluginmocks.Manager{}
	pluginManager.On("CleanupClients")
	r := NewBackupStorageLocationReconciler(t.Context(), velerotest.NewFakeControllerRuntimeClient(t, other),
		storage.DefaultBackupLocationInfo{}, func(logrus.FieldLogger) clientmgmt.Manager { return pluginManager },
		NewFakeSingleObjectBackupStoreGetter(&persistencemocks.BackupStore{}), metrics.NewServerMetrics(), logger)

	_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: velerov1api.DefaultNamespace, Name: "gone"}})
	require.NoError(t, err)

	var messages []string
	for _, e := range hook.AllEntries() {
		messages = append(messages, e.Message)
	}
	assert.Contains(t, messages, "BackupStorageLocation velero/gone is not found")
	assert.Condition(t, func() bool {
		for _, m := range messages {
			if strings.HasPrefix(m, "Found BSLs: ") && strings.Contains(m, "other") {
				return true
			}
		}
		return false
	}, "the existing locations are listed")
}

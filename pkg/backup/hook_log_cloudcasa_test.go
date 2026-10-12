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
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"

	velerov1 "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/builder"
	"github.com/vmware-tanzu/velero/pkg/kuberesource"
	"github.com/vmware-tanzu/velero/pkg/plugin/velero"
	biav2 "github.com/vmware-tanzu/velero/pkg/plugin/velero/backupitemaction/v2"
	ibav1 "github.com/vmware-tanzu/velero/pkg/plugin/velero/itemblockaction/v1"
	"github.com/vmware-tanzu/velero/pkg/test"
)

// CloudCasa's catalog builds its hook activity and the VM "no guest agent"
// warning from the name, namespace and resource on the hook log lines.
func TestBackupHookLogLinesNameThePod(t *testing.T) {
	itemBlockPool := StartItemBlockWorkerPool(t.Context(), 1, logrus.StandardLogger())
	defer itemBlockPool.Stop()
	h := newHarness(t, itemBlockPool)
	backup := defaultBackup().Hooks(velerov1.BackupHooks{Resources: []velerov1.BackupResourceHookSpec{{
		Name:     "hook-1",
		PreHooks: []velerov1.BackupResourceHook{{Exec: &velerov1.ExecHook{Command: []string{"true"}}}},
	}}}).Result()
	req := &Request{
		Backup:           backup,
		SkippedPVTracker: NewSkipPVTracker(),
		BackedUpItems:    NewBackedUpItemsMap(),
		WorkerPool:       itemBlockPool,
	}
	podCommandExecutor := new(test.MockPodCommandExecutor)
	h.backupper.podCommandExecutor = podCommandExecutor
	var hookLog *logrus.Entry
	podCommandExecutor.On("ExecutePodCommand", mock.Anything, mock.Anything, "ns-1", "pod-1", "hook-1", mock.Anything).
		Run(func(args mock.Arguments) { hookLog = args.Get(0).(*logrus.Entry) }).Return(nil)
	h.addItems(t, test.Pods(builder.ForPod("ns-1", "pod-1").Result()))

	require.NoError(t, h.backupper.Backup(h.log, req, bytes.NewBuffer([]byte{}), nil, nil, nil))

	require.NotNil(t, hookLog)
	assert.Equal(t, "pod-1", hookLog.Data["name"])
	assert.Equal(t, "ns-1", hookLog.Data["namespace"])
	assert.Equal(t, "pods", hookLog.Data["resource"])
	assert.EqualValues(t, "pre", hookLog.Data["hookPhase"])
}

// A missing related object is reported once, by the backup item action's
// "Additional item was not found" warning, as v1.14.0.x did; KAS's catalog
// counts every warning line.
func TestMissingRelatedItemIsWarnedOnce(t *testing.T) {
	itemBlockPool := StartItemBlockWorkerPool(t.Context(), 1, logrus.StandardLogger())
	defer itemBlockPool.Stop()
	h := newHarness(t, itemBlockPool)
	req := &Request{
		Backup:           defaultBackup().Result(),
		SkippedPVTracker: NewSkipPVTracker(),
		BackedUpItems:    NewBackedUpItemsMap(),
		WorkerPool:       itemBlockPool,
	}
	h.addItems(t, test.Pods(builder.ForPod("ns-1", "pod-1").Result()))
	h.addItems(t, test.PVCs())
	missing := []velero.ResourceIdentifier{{GroupResource: kuberesource.PersistentVolumeClaims, Namespace: "ns-1", Name: "missing"}}
	ibActions := []ibav1.ItemBlockAction{&pluggableIBA{
		selector: velero.ResourceSelector{IncludedResources: []string{"pods"}},
		getRelatedItemsFunc: func(runtime.Unstructured, *velerov1.Backup) ([]velero.ResourceIdentifier, error) {
			return missing, nil
		},
	}}
	actions := []biav2.BackupItemAction{&pluggableAction{
		selector: velero.ResourceSelector{IncludedResources: []string{"pods"}},
		executeFunc: func(item runtime.Unstructured, _ *velerov1.Backup) (runtime.Unstructured, []velero.ResourceIdentifier, string, []velero.ResourceIdentifier, error) {
			return item, missing, "", nil, nil
		},
	}}
	logger, hook := logrustest.NewNullLogger()

	require.NoError(t, h.backupper.Backup(logger, req, bytes.NewBuffer([]byte{}), actions, ibActions, nil))

	var warnings []string
	for _, e := range hook.AllEntries() {
		if e.Level <= logrus.WarnLevel && strings.Contains(e.Message, "not found in Kubernetes API") {
			warnings = append(warnings, e.Message)
		}
	}
	assert.Equal(t, []string{"Additional item was not found in Kubernetes API, can't back it up"}, warnings)
}

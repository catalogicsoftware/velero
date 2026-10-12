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
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	kbClient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/vmware-tanzu/velero/pkg/builder"
	"github.com/vmware-tanzu/velero/pkg/kuberesource"
	velerotest "github.com/vmware-tanzu/velero/pkg/test"
	"github.com/vmware-tanzu/velero/pkg/util/results"
)

func TestIsKubeVirtVMResource(t *testing.T) {
	assert.True(t, IsKubeVirtVMResource(schema.GroupResource{Group: "kubevirt.io", Resource: "virtualmachines"}))
	assert.False(t, IsKubeVirtVMResource(schema.GroupResource{Group: "kubevirt.io", Resource: "virtualmachineinstances"}))
	assert.False(t, IsKubeVirtVMResource(schema.GroupResource{Group: "example.io", Resource: "virtualmachines"}))
	assert.False(t, IsKubeVirtVMResource(kuberesource.PersistentVolumeClaims))
}

func TestToError(t *testing.T) {
	assert.NoError(t, ToError(results.Result{}))

	err := ToError(results.Result{
		Velero:     []string{"velero error"},
		Cluster:    []string{"cluster error"},
		Namespaces: map[string][]string{"ns-1": {"pvc error"}},
	})
	require.Error(t, err)
	assert.Equal(t, "velero error; cluster error; namespace ns-1: pvc error", err.Error())

	// The messages are text, not a format string.
	err = ToError(results.Result{Velero: []string{"disk 100% full"}})
	require.Error(t, err)
	assert.Equal(t, "disk 100% full", err.Error())
}

// vmStatusData returns the progress ConfigMap's vmStatusData and checks its label.
func vmStatusData(t *testing.T, client kbClient.Client, namespace, name, jobID string) string {
	t.Helper()
	configMap := &corev1.ConfigMap{}
	require.NoError(t, client.Get(context.Background(), kbClient.ObjectKey{Namespace: namespace, Name: name}, configMap))
	assert.Equal(t, map[string]string{"cc-job-id": jobID}, configMap.Labels)
	return configMap.Data["vmStatusData"]
}

func TestKubeVirtVMBackupProgress(t *testing.T) {
	log := velerotest.NewLogger()
	client := fake.NewClientBuilder().Build()
	backup := builder.ForBackup("cloudcasa-io", "bk-1").Result()
	pvcs := schema.GroupResource{Resource: "persistentvolumeclaims"}

	KubeVirtVMStartOp(log, "vm-a", "ns-1", backup, client)
	assert.JSONEq(t, `{"jobID":"bk-1","jobType":"Backup","vms":{
		"ns-1/vm-a":{"vmName":"ns-1/vm-a","status":"ACTIVE","additionalResources":null,"message":"Backup In Progress"}}}`,
		vmStatusData(t, client, "cloudcasa-io", "bk-1-cc-vm-progress", "bk-1"))

	KubeVirtVMStartOp(log, "vm-b", "ns-1", backup, client)
	KubeVirtVMAdditionalResourceOpResult(log, pvcs, "vm-a", "ns-1", "disk-a", "ns-1", backup, true, nil, client)
	KubeVirtVMAdditionalResourceOpResult(log, pvcs, "vm-b", "ns-1", "disk-b", "ns-1", backup, true, nil, client)
	// A second result for the same resource replaces the first one.
	KubeVirtVMAdditionalResourceOpResult(log, pvcs, "vm-b", "ns-1", "disk-b", "ns-1", backup, false, errors.New("no snapshot"), client)
	KubeVirtVMOpResult(log, kubeVirtVMGroupResource, "vm-a", "ns-1", backup, true, nil, client)
	KubeVirtVMOpResult(log, kubeVirtVMGroupResource, "vm-b", "ns-1", backup, false, errors.New("boom"), client)

	assert.JSONEq(t, `{"jobID":"bk-1","jobType":"Backup","vms":{
		"ns-1/vm-a":{"vmName":"ns-1/vm-a","status":"COMPLETED","message":"Backup completed successfully",
			"additionalResources":[{"resourceType":"persistentvolumeclaims","resourceName":"ns-1/disk-a",
				"status":"COMPLETED","message":"Backup of additional resource completed successfully"}]},
		"ns-1/vm-b":{"vmName":"ns-1/vm-b","status":"FAILED","message":"Backup failed: boom",
			"additionalResources":[{"resourceType":"persistentvolumeclaims","resourceName":"ns-1/disk-b",
				"status":"FAILED","message":"Backup failed: no snapshot"}]}}}`,
		vmStatusData(t, client, "cloudcasa-io", "bk-1-cc-vm-progress", "bk-1"))
}

func TestKubeVirtVMRestoreProgress(t *testing.T) {
	log := velerotest.NewLogger()
	client := fake.NewClientBuilder().Build()
	restore := builder.ForRestore("cloudcasa-io", "rs-1").Result()

	KubeVirtVMStartOp(log, "vm-a", "target-ns", restore, client)
	KubeVirtVMOpResult(log, kubeVirtVMGroupResource, "vm-a", "target-ns", restore, true, nil, client)

	assert.JSONEq(t, `{"jobID":"rs-1","jobType":"Restore","vms":{
		"target-ns/vm-a":{"vmName":"target-ns/vm-a","status":"COMPLETED","additionalResources":null,
			"message":"Restore completed successfully"}}}`,
		vmStatusData(t, client, "cloudcasa-io", "rs-1-cc-vm-progress", "rs-1"))
}

func TestKubeVirtVMResultsNeedAStartedVM(t *testing.T) {
	log := velerotest.NewLogger()
	client := fake.NewClientBuilder().Build()
	backup := builder.ForBackup("cloudcasa-io", "bk-1").Result()
	pvcs := schema.GroupResource{Resource: "persistentvolumeclaims"}

	// No ConfigMap: nothing is created.
	KubeVirtVMOpResult(log, kubeVirtVMGroupResource, "vm-a", "ns-1", backup, true, nil, client)
	KubeVirtVMAdditionalResourceOpResult(log, pvcs, "vm-a", "ns-1", "disk-a", "ns-1", backup, true, nil, client)
	err := client.Get(context.Background(), kbClient.ObjectKey{Namespace: "cloudcasa-io", Name: "bk-1-cc-vm-progress"}, &corev1.ConfigMap{})
	assert.True(t, apierrors.IsNotFound(err), "unexpected error %v", err)

	// A VM that was never started is not added.
	KubeVirtVMStartOp(log, "vm-a", "ns-1", backup, client)
	KubeVirtVMOpResult(log, kubeVirtVMGroupResource, "vm-b", "ns-1", backup, true, nil, client)
	KubeVirtVMAdditionalResourceOpResult(log, pvcs, "vm-b", "ns-1", "disk-b", "ns-1", backup, true, nil, client)
	assert.JSONEq(t, `{"jobID":"bk-1","jobType":"Backup","vms":{
		"ns-1/vm-a":{"vmName":"ns-1/vm-a","status":"ACTIVE","additionalResources":null,"message":"Backup In Progress"}}}`,
		vmStatusData(t, client, "cloudcasa-io", "bk-1-cc-vm-progress", "bk-1"))

	// Only a Backup or a Restore has a progress ConfigMap.
	KubeVirtVMStartOp(log, "vm-a", "ns-1", "not-a-job", client)
}

func virtualMachine(namespace, name string, status map[string]any, volumes ...any) *unstructured.Unstructured {
	vm := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "kubevirt.io/v1",
		"kind":       "VirtualMachine",
		"metadata":   map[string]any{"namespace": namespace, "name": name},
		"spec": map[string]any{"template": map[string]any{"spec": map[string]any{
			"volumes": volumes,
		}}},
	}}
	if status != nil {
		vm.Object["status"] = status
	}
	return vm
}

func pvcVolume(claimName string) any {
	return map[string]any{"name": claimName, "persistentVolumeClaim": map[string]any{"claimName": claimName}}
}

func TestRunningCbtVmPvcs(t *testing.T) {
	cbtRunning := map[string]any{"ready": true, "changedBlockTracking": map[string]any{"state": "Enabled"}}
	client := fake.NewClientBuilder().WithObjects(
		virtualMachine("ns-1", "vm-cbt", cbtRunning,
			map[string]any{"name": "root", "dataVolume": map[string]any{"name": "root-dv"}},
			pvcVolume("data-pvc"),
			map[string]any{"name": "cloudinit", "cloudInitNoCloud": map[string]any{"userData": "#cloud-config"}},
		),
		virtualMachine("ns-1", "vm-stopped",
			map[string]any{"ready": false, "changedBlockTracking": map[string]any{"state": "Enabled"}},
			pvcVolume("stopped-pvc")),
		virtualMachine("ns-1", "vm-no-cbt",
			map[string]any{"ready": true, "changedBlockTracking": map[string]any{"state": "Disabled"}},
			pvcVolume("no-cbt-pvc")),
		virtualMachine("ns-1", "vm-no-status", nil, pvcVolume("no-status-pvc")),
		virtualMachine("ns-2", "vm-other-namespace", cbtRunning, pvcVolume("other-pvc")),
	).Build()

	pvcs, err := RunningCbtVmPvcs(context.Background(), client, "ns-1")
	require.NoError(t, err)
	assert.Equal(t, map[string]bool{"root-dv": true, "data-pvc": true}, pvcs)

	pvcs, err = RunningCbtVmPvcs(context.Background(), client, "ns-3")
	require.NoError(t, err)
	assert.Empty(t, pvcs)
}

func TestRunningCbtVmPvcsListErrors(t *testing.T) {
	failingList := func(listErr error) kbClient.Client {
		return fake.NewClientBuilder().WithInterceptorFuncs(interceptor.Funcs{
			List: func(context.Context, kbClient.WithWatch, kbClient.ObjectList, ...kbClient.ListOption) error {
				return listErr
			},
		}).Build()
	}

	// No KubeVirt in the cluster: no VM disks.
	noKind := &meta.NoKindMatchError{GroupKind: schema.GroupKind{Group: "kubevirt.io", Kind: "VirtualMachine"}}
	pvcs, err := RunningCbtVmPvcs(context.Background(), failingList(noKind), "ns-1")
	require.NoError(t, err)
	assert.Equal(t, map[string]bool{}, pvcs)

	notFound := apierrors.NewNotFound(schema.GroupResource{Group: "kubevirt.io", Resource: "virtualmachines"}, "")
	pvcs, err = RunningCbtVmPvcs(context.Background(), failingList(notFound), "ns-1")
	require.NoError(t, err)
	assert.Equal(t, map[string]bool{}, pvcs)

	pvcs, err = RunningCbtVmPvcs(context.Background(), failingList(errors.New("connection refused")), "ns-1")
	require.EqualError(t, err, "listing KubeVirt VirtualMachines in namespace ns-1: connection refused")
	assert.Nil(t, pvcs)
}

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

package csi

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	snapshotv1api "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1api "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/vmware-tanzu/velero/internal/catalogic"
	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/builder"
	velerotest "github.com/vmware-tanzu/velero/pkg/test"
)

func TestShouldSkipSnapshot(t *testing.T) {
	sc := func(name, provisioner string, params map[string]string) runtime.Object {
		storageClass := builder.ForStorageClass(name).Provisioner(provisioner).Result()
		storageClass.Parameters = params
		return storageClass
	}
	tests := []struct {
		name        string
		config      catalogic.PluginConfig
		annotation  string
		scName      string
		provisioner string
		objects     []runtime.Object
		want        bool
	}{
		{name: "plain snapshot", config: catalogic.PluginConfig{SnapshotWherePossible: true}, scName: "sc", provisioner: "ebs.csi.aws.com", want: false},
		{name: "snapshots disabled", config: catalogic.PluginConfig{SnapshotWherePossible: false}, scName: "sc", provisioner: "ebs.csi.aws.com", want: true},
		{name: "live-copy driver", config: catalogic.PluginConfig{SnapshotWherePossible: true}, scName: "sc", provisioner: "nfs.csi.k8s.io", want: true},
		{name: "azlocal is a live-copy driver", config: catalogic.PluginConfig{SnapshotWherePossible: true}, scName: "sc", provisioner: "azlocal.csi.azure.com", want: true},
		{name: "storage class method LIVE", config: catalogic.PluginConfig{SnapshotWherePossible: true, StorageClassBackupMethodMap: map[string]string{"sc": "LIVE_FROM_PVC"}}, scName: "sc", provisioner: "ebs.csi.aws.com", want: true},
		{name: "storage class method SNAPSHOT beats a live-copy driver", config: catalogic.PluginConfig{SnapshotWherePossible: true, StorageClassBackupMethodMap: map[string]string{"sc": "SNAPSHOT"}}, scName: "sc", provisioner: "nfs.csi.k8s.io", want: false},
		{name: "storage class method SNAPSHOT beats snapshots disabled", config: catalogic.PluginConfig{SnapshotWherePossible: false, StorageClassBackupMethodMap: map[string]string{"sc": "SNAPSHOT"}}, scName: "sc", provisioner: "ebs.csi.aws.com", want: false},
		{name: "PVC annotation SKIP", config: catalogic.PluginConfig{SnapshotWherePossible: true}, annotation: "skip", scName: "sc", provisioner: "ebs.csi.aws.com", want: true},
		{name: "PVC annotation overrides storage class method", config: catalogic.PluginConfig{SnapshotWherePossible: true, StorageClassBackupMethodMap: map[string]string{"sc": "LIVE"}}, annotation: " Snapshot ", scName: "sc", provisioner: "ebs.csi.aws.com", want: false},
		{name: "invalid PVC annotation is ignored", config: catalogic.PluginConfig{SnapshotWherePossible: true}, annotation: "sometimes", scName: "sc", provisioner: "ebs.csi.aws.com", want: false},
		{name: "Azure Files NFS", config: catalogic.PluginConfig{SnapshotWherePossible: true}, scName: "azfile-nfs", provisioner: "file.csi.azure.com",
			objects: []runtime.Object{sc("azfile-nfs", "file.csi.azure.com", map[string]string{"protocol": "nfs"})}, want: true},
		{name: "Azure Files SMB", config: catalogic.PluginConfig{SnapshotWherePossible: true}, scName: "azfile", provisioner: "file.csi.azure.com",
			objects: []runtime.Object{sc("azfile", "file.csi.azure.com", nil)}, want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			config := tc.config
			withPluginConfig(t, &config)
			pvcBuilder := builder.ForPersistentVolumeClaim("ns", "pvc").StorageClass(tc.scName)
			if tc.annotation != "" {
				pvcBuilder = pvcBuilder.ObjectMeta(builder.WithAnnotations("cloudcasa.io/backup-method", tc.annotation))
			}
			p := &pvcBackupItemAction{log: logrus.New(), crClient: velerotest.NewFakeControllerRuntimeClient(t, tc.objects...)}

			skip, err := p.shouldSkipSnapshot(pvcBuilder.Result(), "pv", tc.provisioner, "backup-1")

			require.NoError(t, err)
			assert.Equal(t, tc.want, skip)
		})
	}
}

// cloudcasaPVCFixture is a bound CSI PVC whose driver has a labelled class.
func cloudcasaPVCFixture(pvcOpts ...builder.ObjectMetaOpt) []runtime.Object {
	return []runtime.Object{
		builder.ForPersistentVolumeClaim("ns", "pvc-1").VolumeName("pv-1").StorageClass("sc-1").
			Phase(corev1api.ClaimBound).ObjectMeta(pvcOpts...).Result(),
		builder.ForPersistentVolume("pv-1").CSI("hostpath", "vol-1").Result(),
		builder.ForStorageClass("sc-1").Provisioner("hostpath").Result(),
		builder.ForVolumeSnapshotClass("class-1").Driver("hostpath").
			ObjectMeta(builder.WithLabels(velerov1api.VolumeSnapshotClassSelectorLabel, "")).Result(),
	}
}

func executeOnPVC(t *testing.T, p *pvcBackupItemAction, backup *velerov1api.Backup) (*corev1api.PersistentVolumeClaim, error) {
	t.Helper()
	pvc := &corev1api.PersistentVolumeClaim{}
	require.NoError(t, p.crClient.Get(t.Context(), crclient.ObjectKey{Namespace: "ns", Name: "pvc-1"}, pvc))
	item, err := runtime.DefaultUnstructuredConverter.ToUnstructured(pvc)
	require.NoError(t, err)
	out, _, _, _, err := p.Execute(&unstructured.Unstructured{Object: item}, backup)
	if err != nil || out == nil {
		return nil, err
	}
	result := &corev1api.PersistentVolumeClaim{}
	require.NoError(t, runtime.DefaultUnstructuredConverter.FromUnstructured(out.UnstructuredContent(), result))
	return result, nil
}

// bindFirstVolumeSnapshot plays the CSI snapshot controller for the first
// VolumeSnapshot created in the namespace. The fake client's watch does not
// replay what happened between the wait's list and watch, so the bound
// snapshot is written again once the wait is surely watching.
func bindFirstVolumeSnapshot(t *testing.T, client crclient.Client) <-chan error {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		var vsList snapshotv1api.VolumeSnapshotList
		err := wait.PollUntilContextTimeout(t.Context(), 50*time.Millisecond, 10*time.Second, true, func(ctx context.Context) (bool, error) {
			if err := client.List(ctx, &vsList, &crclient.ListOptions{Namespace: "ns"}); err != nil {
				return false, err
			}
			return len(vsList.Items) > 0, nil
		})
		if err != nil {
			done <- err
			return
		}
		vscName, handle, ready := "vsc-1", "handle-1", true
		if err := client.Create(t.Context(), builder.ForVolumeSnapshotContent(vscName).
			Status(&snapshotv1api.VolumeSnapshotContentStatus{SnapshotHandle: &handle}).Result()); err != nil {
			done <- err
			return
		}
		vsList.Items[0].Status = &snapshotv1api.VolumeSnapshotStatus{BoundVolumeSnapshotContentName: &vscName, ReadyToUse: &ready}
		if err := client.Update(t.Context(), &vsList.Items[0]); err != nil {
			done <- err
			return
		}
		time.Sleep(200 * time.Millisecond)
		vs := &snapshotv1api.VolumeSnapshot{}
		if err := client.Get(t.Context(), crclient.ObjectKeyFromObject(&vsList.Items[0]), vs); err != nil {
			done <- err
			return
		}
		done <- client.Update(t.Context(), vs)
	}()
	return done
}

// The backed-up PVC carries what CloudCasa's restore and movers read.
func TestExecuteNamesAndLabelsTheSnapshotForCloudCasa(t *testing.T) {
	client := velerotest.NewFakeControllerRuntimeClient(t, cloudcasaPVCFixture()...)
	p := &pvcBackupItemAction{log: logrus.New(), crClient: client}
	bound := bindFirstVolumeSnapshot(t, client)

	pvc, err := executeOnPVC(t, p, builder.ForBackup("velero", "backup-1").CSISnapshotTimeout(time.Minute).Result())
	require.NoError(t, err)
	require.NoError(t, <-bound)

	vsName := pvc.Annotations[velerov1api.VolumeSnapshotLabel]
	assert.True(t, strings.HasPrefix(vsName, "cc-pvc-1-"), "VolumeSnapshot %q is named cc-<pvc>-", vsName)
	assert.Equal(t, "pv-1", pvc.Annotations["cloudcasa-initial-volume-name"])
	assert.Equal(t, "hostpath", pvc.Annotations["cloudcasa-storage-class-provisioner"])
	assert.Equal(t, "true", pvc.Annotations[velerov1api.MustIncludeAdditionalItemAnnotation])
	assert.Equal(t, "pv-1", pvc.Labels["cloudcasa-initial-volume-name"])
	assert.Equal(t, vsName, pvc.Labels[velerov1api.VolumeSnapshotLabel])
}

// The backup method rules run before any snapshot, so a PVC in a
// VolumeGroupSnapshot group is not snapshotted when its method is LIVE.
func TestExecuteSkipsBeforeAGroupSnapshot(t *testing.T) {
	client := velerotest.NewFakeControllerRuntimeClient(t, cloudcasaPVCFixture(
		builder.WithAnnotations("cloudcasa.io/backup-method", "LIVE"),
		builder.WithLabels("app-group", "g1"))...)
	p := &pvcBackupItemAction{log: logrus.New(), crClient: client}
	backup := builder.ForBackup("velero", "backup-1").CSISnapshotTimeout(time.Minute).Result()
	backup.Spec.VolumeGroupSnapshotLabelKey = "app-group"

	pvc, err := executeOnPVC(t, p, backup)

	require.NoError(t, err)
	require.NotNil(t, pvc)
	assert.NotContains(t, pvc.Annotations, velerov1api.VolumeSnapshotLabel)
	vsList := &snapshotv1api.VolumeSnapshotList{}
	require.NoError(t, client.List(t.Context(), vsList))
	assert.Empty(t, vsList.Items, "no VolumeSnapshot for a LIVE PVC")
}

// A class that cannot be resolved fails the PVC with the resolver's own
// message, unwrapped, and kubeagent gets the same message as an error.
func TestExecuteRelaysClassResolutionFailure(t *testing.T) {
	var reportedState, reportedMessage string
	previous := updateSnapshotProgress
	updateSnapshotProgress = func(_ *corev1api.PersistentVolumeClaim, _ *snapshotv1api.VolumeSnapshot,
		_ *snapshotv1api.VolumeSnapshotContent, state, message, _ string, _ logrus.FieldLogger) error {
		reportedState, reportedMessage = state, message
		return nil
	}
	t.Cleanup(func() { updateSnapshotProgress = previous })

	objects := cloudcasaPVCFixture()[:3] // no VolumeSnapshotClass
	client := velerotest.NewFakeControllerRuntimeClient(t, objects...)
	p := &pvcBackupItemAction{log: logrus.New(), crClient: client}
	backup := builder.ForBackup("velero", "backup-1").
		ObjectMeta(builder.WithAnnotations(velerov1api.SnapshotClassRequiredDriversAnnotation, "hostpath")).Result()

	_, err := executeOnPVC(t, p, backup)

	require.Error(t, err)
	assert.True(t, strings.HasPrefix(err.Error(), "No VolumeSnapshotClass configured for CSI driver"), err.Error())
	assert.Equal(t, "error", reportedState)
	assert.Equal(t, err.Error(), reportedMessage)
}

// Labels and annotations are separate sets: the cloudcasa-* keys are in both,
// the must-include flag is only an annotation, and no stray keys appear.
func TestExecuteKeepsLabelsAndAnnotationsApart(t *testing.T) {
	client := velerotest.NewFakeControllerRuntimeClient(t, cloudcasaPVCFixture()...)
	p := &pvcBackupItemAction{log: logrus.New(), crClient: client}
	bound := bindFirstVolumeSnapshot(t, client)

	pvc, err := executeOnPVC(t, p, builder.ForBackup("velero", "backup-1").CSISnapshotTimeout(time.Minute).Result())
	require.NoError(t, err)
	require.NoError(t, <-bound)

	assert.NotContains(t, pvc.Labels, velerov1api.VolumeNamespaceLabel)
	assert.NotContains(t, pvc.Annotations, velerov1api.VolumeNamespaceLabel)
	assert.NotContains(t, pvc.Labels, velerov1api.MustIncludeAdditionalItemAnnotation)
	assert.Equal(t, "true", pvc.Annotations[velerov1api.MustIncludeAdditionalItemAnnotation])
	for _, key := range []string{"cloudcasa-initial-volume-name", "cloudcasa-storage-class-provisioner", velerov1api.VolumeSnapshotLabel} {
		assert.Contains(t, pvc.Labels, key)
		assert.Contains(t, pvc.Annotations, key)
	}
}

// When relaying progress fails, the log names the relay error.
func TestExecuteLogsTheProgressRelayError(t *testing.T) {
	previous := updateSnapshotProgress
	updateSnapshotProgress = func(*corev1api.PersistentVolumeClaim, *snapshotv1api.VolumeSnapshot,
		*snapshotv1api.VolumeSnapshotContent, string, string, string, logrus.FieldLogger) error {
		return errors.New("configmap write refused")
	}
	t.Cleanup(func() { updateSnapshotProgress = previous })

	logger, hook := logrustest.NewNullLogger()
	client := velerotest.NewFakeControllerRuntimeClient(t, cloudcasaPVCFixture()[:2]...) // no StorageClass
	p := &pvcBackupItemAction{log: logger, crClient: client}

	_, err := executeOnPVC(t, p, builder.ForBackup("velero", "backup-1").Result())
	require.Error(t, err)

	found := false
	for _, entry := range hook.AllEntries() {
		if strings.Contains(entry.Message, "Failed to update snapshot progress") {
			found = fmt.Sprint(entry.Data[logrus.ErrorKey]) == "configmap write refused"
		}
	}
	assert.True(t, found, "the relay error is logged")
}

// kubeagent shows the PVC as active while the CSI driver takes the snapshot,
// so "pending" is relayed once the snapshot exists and before the wait ends.
func TestExecuteReportsPendingWhileWaiting(t *testing.T) {
	pending := make(chan struct{})
	var states []string
	var pendingMessage string
	previous := updateSnapshotProgress
	updateSnapshotProgress = func(_ *corev1api.PersistentVolumeClaim, _ *snapshotv1api.VolumeSnapshot,
		_ *snapshotv1api.VolumeSnapshotContent, state, message, _ string, _ logrus.FieldLogger) error {
		if state == "" {
			return nil
		}
		states = append(states, state)
		if state == "pending" && len(states) == 1 {
			pendingMessage = message
			close(pending)
		}
		return nil
	}
	t.Cleanup(func() { updateSnapshotProgress = previous })

	client := velerotest.NewFakeControllerRuntimeClient(t, cloudcasaPVCFixture()...)
	p := &pvcBackupItemAction{log: logrus.New(), crClient: client}
	bound := make(chan error, 1)
	go func() {
		select {
		case <-pending:
			bound <- <-bindFirstVolumeSnapshot(t, client)
		case <-time.After(10 * time.Second):
			bound <- errors.New("no pending report")
		}
	}()

	_, err := executeOnPVC(t, p, builder.ForBackup("velero", "backup-1").CSISnapshotTimeout(3*time.Second).Result())
	require.NoError(t, err)
	require.NoError(t, <-bound)

	assert.Equal(t, []string{"pending"}, states)
	assert.Equal(t, "Waiting for CSI driver to reconcile volumesnapshot ns/pvc-1", pendingMessage)
}

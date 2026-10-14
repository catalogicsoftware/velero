package controller

import (
	"sync"
	"testing"

	snapshotv1api "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1api "k8s.io/api/core/v1"
	storagev1api "k8s.io/api/storage/v1"
	"time"

	"github.com/vmware-tanzu/velero/internal/volume"
	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/builder"
	velerotest "github.com/vmware-tanzu/velero/pkg/test"
)

type pvcProgress struct{ namespace, name, state, message, job string }

// recordPVCProgress replaces the relay to kubeagent and returns what was relayed.
func recordPVCProgress(t *testing.T) *[]pvcProgress {
	t.Helper()
	var (
		lock    sync.Mutex
		relayed []pvcProgress
	)
	previous, previousDelete := updatePVCProgress, deletePVCProgressConfigMap
	updatePVCProgress = func(pvc *corev1api.PersistentVolumeClaim, _ *snapshotv1api.VolumeSnapshot,
		_ *snapshotv1api.VolumeSnapshotContent, state, message, jobID string, _ logrus.FieldLogger) error {
		lock.Lock()
		defer lock.Unlock()
		relayed = append(relayed, pvcProgress{pvc.Namespace, pvc.Name, state, message, jobID})
		return nil
	}
	deletePVCProgressConfigMap = func(string, logrus.FieldLogger) {}
	t.Cleanup(func() { updatePVCProgress, deletePVCProgressConfigMap = previous, previousDelete })
	return &relayed
}

// The CSI PVC restore action reports a PVC as completed as soon as it has
// written it. A PVC that does not bind is therefore reported failed here, under
// the same namespace and name as the completed report (the backed-up namespace).
func TestFinalizerReportsAPVCThatDoesNotBindAsFailed(t *testing.T) {
	volumeInfo := []*volume.BackupVolumeInfo{{
		BackupMethod: volume.CSISnapshot,
		PVCName:      "pvc1",
		PVName:       "pv1",
		PVCNamespace: "ns1",
		PVInfo:       &volume.PVInfo{ReclaimPolicy: string(corev1api.PersistentVolumeReclaimDelete)},
	}}
	immediate := storagev1api.VolumeBindingImmediate
	storageClass := builder.ForStorageClass("sc1").Result()
	storageClass.VolumeBindingMode = &immediate

	tests := []struct {
		name      string
		pvc       *corev1api.PersistentVolumeClaim
		pv        *corev1api.PersistentVolume
		wantState string
	}{
		{
			name:      "the PVC stays pending",
			pvc:       builder.ForPersistentVolumeClaim("ns2", "pvc1").StorageClass("sc1").Phase(corev1api.ClaimPending).Result(),
			wantState: "error",
		},
		{
			name: "the PVC binds",
			pvc:  builder.ForPersistentVolumeClaim("ns2", "pvc1").StorageClass("sc1").VolumeName("new-pv1").Phase(corev1api.ClaimBound).Result(),
			pv: builder.ForPersistentVolume("new-pv1").ClaimRef("ns2", "pvc1").Phase(corev1api.VolumeBound).
				ReclaimPolicy(corev1api.PersistentVolumeReclaimDelete).Result(),
		},
		{
			name: "the PVC is bound but its PV belongs to another claim",
			pvc:  builder.ForPersistentVolumeClaim("ns2", "pvc1").StorageClass("sc1").VolumeName("new-pv1").Phase(corev1api.ClaimBound).Result(),
			pv: builder.ForPersistentVolume("new-pv1").ClaimRef("ns2", "other").Phase(corev1api.VolumeBound).
				ReclaimPolicy(corev1api.PersistentVolumeReclaimRetain).Result(),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			relayed := recordPVCProgress(t)
			restore := builder.ForRestore(velerov1api.DefaultNamespace, "restore-job").NamespaceMappings("ns1", "ns2").Result()
			ctx := &finalizerContext{
				logger:          velerotest.NewLogger(),
				crClient:        velerotest.NewFakeControllerRuntimeClientBuilder(t).Build(),
				restore:         restore,
				restoredPVCList: map[string]struct{}{"ns2/pvc1": {}},
				volumeInfo:      volumeInfo,
				resourceTimeout: time.Second,
			}
			require.NoError(t, ctx.crClient.Create(t.Context(), storageClass.DeepCopy()))
			require.NoError(t, ctx.crClient.Create(t.Context(), tc.pvc))
			if tc.pv != nil {
				require.NoError(t, ctx.crClient.Create(t.Context(), tc.pv))
			}

			ctx.patchDynamicPVWithVolumeInfo()

			if tc.wantState == "" {
				assert.Empty(t, *relayed)
				return
			}
			require.Len(t, *relayed, 1)
			assert.Equal(t, pvcProgress{"ns1", "pvc1", tc.wantState, (*relayed)[0].message, "restore-job"}, (*relayed)[0])
			assert.Contains(t, (*relayed)[0].message, "did not bind")
		})
	}
}

package controller

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1api "k8s.io/api/core/v1"
	storagev1api "k8s.io/api/storage/v1"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/vmware-tanzu/velero/internal/volume"
	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/builder"
	velerotest "github.com/vmware-tanzu/velero/pkg/test"
	kubevirtutil "github.com/vmware-tanzu/velero/pkg/util/kubevirt"
)

// A PVC of a VM restored under a new name is restored only as its copy, so
// the finalizer waits for the copy and patches the copy's PV.
func TestFinalizerHandlesTheCopyOfARenamedVMsPVC(t *testing.T) {
	volumeInfo := []*volume.BackupVolumeInfo{{
		BackupMethod: volume.CSISnapshot,
		PVCName:      "vm-1-disk",
		PVName:       "pv1",
		PVCNamespace: "ns1",
		PVInfo:       &volume.PVInfo{ReclaimPolicy: string(corev1api.PersistentVolumeReclaimRetain)},
	}}
	immediate := storagev1api.VolumeBindingImmediate
	storageClass := builder.ForStorageClass("sc1").Result()
	storageClass.VolumeBindingMode = &immediate
	vmOptions, err := json.Marshal([]kubevirtutil.VMOptions{{
		Namespace: "ns1", Name: "vm-1", NewName: "vm-1-clone",
		Items: []kubevirtutil.RenamedItem{{Resource: "persistentvolumeclaims", Name: "vm-1-disk", NewName: "vm-1-clone-disk"}},
	}})
	require.NoError(t, err)
	restore := builder.ForRestore(velerov1api.DefaultNamespace, "restore-job").
		ObjectMeta(builder.WithAnnotations(kubevirtutil.VMOptionsAnnotation, string(vmOptions))).
		Result()
	itemRenames, err := kubevirtutil.VMOptionsOf(restore)
	require.NoError(t, err)

	ctx := &finalizerContext{
		logger:          velerotest.NewLogger(),
		crClient:        velerotest.NewFakeControllerRuntimeClientBuilder(t).Build(),
		restore:         restore,
		restoredPVCList: map[string]struct{}{"ns1/vm-1-clone-disk": {}},
		volumeInfo:      volumeInfo,
		itemRenames:     itemRenames,
		resourceTimeout: time.Second,
	}
	require.NoError(t, ctx.crClient.Create(t.Context(), storageClass))
	require.NoError(t, ctx.crClient.Create(t.Context(), builder.ForPersistentVolumeClaim("ns1", "vm-1-clone-disk").
		StorageClass("sc1").VolumeName("new-pv1").Phase(corev1api.ClaimBound).Result()))
	require.NoError(t, ctx.crClient.Create(t.Context(), builder.ForPersistentVolume("new-pv1").
		ClaimRef("ns1", "vm-1-clone-disk").Phase(corev1api.VolumeBound).
		ReclaimPolicy(corev1api.PersistentVolumeReclaimDelete).Result()))

	errs := ctx.patchDynamicPVWithVolumeInfo()

	assert.True(t, errs.IsEmpty(), "%v", errs)
	pv := &corev1api.PersistentVolume{}
	require.NoError(t, ctx.crClient.Get(t.Context(), crclient.ObjectKey{Name: "new-pv1"}, pv))
	assert.Equal(t, corev1api.PersistentVolumeReclaimRetain, pv.Spec.PersistentVolumeReclaimPolicy)
}

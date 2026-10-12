package catalogic

import (
	"testing"

	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1api "k8s.io/api/core/v1"
	storagev1api "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Outside a cluster the in-cluster config cannot be built; the delete must
// log and return instead of dereferencing a nil config.
func TestDeleteSnapshotProgressConfigMapOutsideCluster(t *testing.T) {
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	t.Setenv("KUBERNETES_SERVICE_PORT", "")

	assert.NotPanics(t, func() {
		DeleteSnapshotProgressConfigMap("job-1", logrus.New())
	})
}

// The Azure Files helper logs its errors readably (logrus has no %w).
func TestSetStaticAzureAnotationLogsTheError(t *testing.T) {
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	t.Setenv("KUBERNETES_SERVICE_PORT", "")
	log, hook := logrustest.NewNullLogger()
	pvc := &corev1api.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "pvc"}}
	storageClass := &storagev1api.StorageClass{Provisioner: "file.csi.azure.com"}

	err := SetStaticAzureAnotation(pvc, map[string]string{}, storageClass, log)

	require.Error(t, err)
	entry := hook.LastEntry()
	require.NotNil(t, entry)
	assert.Equal(t, logrus.ErrorLevel, entry.Level)
	assert.Equal(t, "Failed to get in-cluster Kubernetes config: "+err.Error(), entry.Message)
}

package catalogic

import (
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
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

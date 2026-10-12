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
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/utils/clock"

	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/builder"
	"github.com/vmware-tanzu/velero/pkg/discovery"
	velerotest "github.com/vmware-tanzu/velero/pkg/test"
	"github.com/vmware-tanzu/velero/pkg/util/logging"
)

// A per-job engine never validates the shared, unlabeled storage location, so
// a backup must not depend on the location's phase.
func TestPrepareBackupRequestIgnoresTheLocationPhase(t *testing.T) {
	for _, phase := range []velerov1api.BackupStorageLocationPhase{"", velerov1api.BackupStorageLocationPhaseUnavailable} {
		t.Run("phase "+phaseName(phase), func(t *testing.T) {
			logger := logging.DefaultLogger(logrus.DebugLevel, logging.FormatText)
			apiServer := velerotest.NewAPIServer(t)
			discoveryHelper, err := discovery.NewHelper(apiServer.DiscoveryClient, logger)
			require.NoError(t, err)
			location := builder.ForBackupStorageLocation("velero", "job-bsl").Phase(phase).Result()
			c := &backupReconciler{
				discoveryHelper: discoveryHelper,
				kbClient:        velerotest.NewFakeControllerRuntimeClient(t, location),
				clock:           &clock.RealClock{},
				formatFlag:      logging.FormatText,
			}

			res := c.prepareBackupRequest(t.Context(), defaultBackup().StorageLocation("job-bsl").Result(), logger)
			defer res.WorkerPool.Stop()

			assert.Empty(t, res.Status.ValidationErrors)
			require.NotNil(t, res.StorageLocation)
			assert.Equal(t, "job-bsl", res.StorageLocation.Name)
		})
	}
}

// phaseName names a storage location phase in a subtest name.
func phaseName(phase velerov1api.BackupStorageLocationPhase) string {
	if phase == "" {
		return "none"
	}
	return string(phase)
}

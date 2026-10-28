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
	"time"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/builder"
	"github.com/vmware-tanzu/velero/pkg/features"
	"github.com/vmware-tanzu/velero/pkg/metrics"
	velerotest "github.com/vmware-tanzu/velero/pkg/test"
)

// A backup CR made without a start time, such as kubeagent's stub for a
// restore, is finalized without a duration metric instead of a panic.
func TestRecordBackupMetricsWithoutStartTime(t *testing.T) {
	backup := builder.ForBackup(velerov1api.DefaultNamespace, "backup-1").
		CompletionTimestamp(time.Now()).Result()

	assert.NotPanics(t, func() {
		recordBackupMetrics(velerotest.NewLogger(), backup, nil, metrics.NewServerMetrics(), true)
	})
}

// More completed than attempted snapshots must not drive a Prometheus
// counter negative, which panics.
func TestRecordBackupMetricsClampsSnapshotFailures(t *testing.T) {
	features.Enable(velerov1api.CSIFeatureFlag)
	t.Cleanup(func() { features.Disable(velerov1api.CSIFeatureFlag) })
	backup := builder.ForBackup(velerov1api.DefaultNamespace, "backup-1").Result()
	backup.Status.StartTimestamp = &metav1.Time{Time: time.Now()}
	backup.Status.VolumeSnapshotsAttempted, backup.Status.VolumeSnapshotsCompleted = 0, 1
	backup.Status.CSIVolumeSnapshotsAttempted, backup.Status.CSIVolumeSnapshotsCompleted = 0, 1

	for _, finalize := range []bool{false, true} {
		assert.NotPanics(t, func() {
			recordBackupMetrics(velerotest.NewLogger(), backup, nil, metrics.NewServerMetrics(), finalize)
		}, "finalize=%v", finalize)
	}
}

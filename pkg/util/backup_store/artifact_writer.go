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

package backup_store

import (
	"io"
)

// ArtifactWriter defines the interface for writing restore artifacts
// It abstracts away the details of where artifacts are written (BackupStore vs Local)
type ArtifactWriter interface {
	// WriteLog writes the restore log to the configured location
	// Parameters: restoreName - name of the restore, logData - gzipped log data
	WriteLog(restoreName string, logData io.Reader) error

	// WriteResults writes restore results (warnings/errors) to configured location
	// Parameters: restoreName - name of the restore, results - gzipped results JSON
	WriteResults(restoreName string, results io.Reader) error

	// WriteRestoredResourceList writes list of restored resources
	// Parameters: restoreName - name of the restore, resources - gzipped resource list JSON
	WriteRestoredResourceList(restoreName string, resources io.Reader) error

	// WriteItemOperations writes restore item operations
	// Parameters: restoreName - name of the restore, operations - gzipped operations JSON
	WriteItemOperations(restoreName string, operations io.Reader) error

	// WriteVolumeInfo writes restore volume info
	// Parameters: restoreName - name of the restore, volumeInfo - gzipped volume info JSON
	WriteVolumeInfo(restoreName string, volumeInfo io.Reader) error

	// GetRestoreArtifactDir returns the directory path where artifacts are stored
	// Returns: string - path to artifact directory (e.g., "/scratch/restore-result/my-restore")
	GetRestoreArtifactDir(restoreName string) string
}

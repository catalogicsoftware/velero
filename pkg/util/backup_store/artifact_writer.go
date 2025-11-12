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

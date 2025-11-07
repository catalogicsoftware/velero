package backup_store

import (
	"fmt"
	"io"

	"github.com/sirupsen/logrus"
	"github.com/vmware-tanzu/velero/pkg/persistence"
)

// BackupStoreArtifactWriter adapts BackupStore to implement ArtifactWriter interface
// Used when BackupStorageLocation has AccessMode set to ReadWrite (default behavior)
type BackupStoreArtifactWriter struct {
	backupStore persistence.BackupStore
	backupName  string
	logger      logrus.FieldLogger
}

// NewBackupStoreArtifactWriter creates a new BackupStoreArtifactWriter
func NewBackupStoreArtifactWriter(
	backupStore persistence.BackupStore,
	backupName string,
	logger logrus.FieldLogger,
) *BackupStoreArtifactWriter {
	return &BackupStoreArtifactWriter{
		backupStore: backupStore,
		backupName:  backupName,
		logger:      logger,
	}
}

// WriteLog delegates to BackupStore.PutRestoreLog
func (w *BackupStoreArtifactWriter) WriteLog(restoreName string, logData io.Reader) error {
	w.logger.WithFields(logrus.Fields{
		"RestoreName":  restoreName,
		"ArtifactType": "RestoreLog",
		"Location":     "BackupStore",
	}).Debug("Writing restore log to BackupStore")

	return w.backupStore.PutRestoreLog(w.backupName, restoreName, logData)
}

// WriteResults delegates to BackupStore.PutRestoreResults
func (w *BackupStoreArtifactWriter) WriteResults(restoreName string, results io.Reader) error {
	w.logger.WithFields(logrus.Fields{
		"RestoreName":  restoreName,
		"ArtifactType": "RestoreResults",
		"Location":     "BackupStore",
	}).Debug("Writing restore results to BackupStore")

	return w.backupStore.PutRestoreResults(w.backupName, restoreName, results)
}

// WriteRestoredResourceList delegates to BackupStore.PutRestoredResourceList
func (w *BackupStoreArtifactWriter) WriteRestoredResourceList(restoreName string, resources io.Reader) error {
	w.logger.WithFields(logrus.Fields{
		"RestoreName":  restoreName,
		"ArtifactType": "RestoredResourcesList",
		"Location":     "BackupStore",
	}).Debug("Writing restored resources list to BackupStore")

	return w.backupStore.PutRestoredResourceList(restoreName, resources)
}

// WriteItemOperations delegates to BackupStore.PutRestoreItemOperations
func (w *BackupStoreArtifactWriter) WriteItemOperations(restoreName string, operations io.Reader) error {
	w.logger.WithFields(logrus.Fields{
		"RestoreName":  restoreName,
		"ArtifactType": "RestoreItemOperations",
		"Location":     "BackupStore",
	}).Debug("Writing restore item operations to BackupStore")

	return w.backupStore.PutRestoreItemOperations(restoreName, operations)
}

// WriteVolumeInfo delegates to BackupStore.PutRestoreVolumeInfo
func (w *BackupStoreArtifactWriter) WriteVolumeInfo(restoreName string, volumeInfo io.Reader) error {
	w.logger.WithFields(logrus.Fields{
		"RestoreName":  restoreName,
		"ArtifactType": "RestoreVolumeInfo",
		"Location":     "BackupStore",
	}).Debug("Writing restore volume info to BackupStore")

	return w.backupStore.PutRestoreVolumeInfo(restoreName, volumeInfo)
}

// GetRestoreArtifactDir returns a string representation of where artifacts will be stored
func (w *BackupStoreArtifactWriter) GetRestoreArtifactDir(restoreName string) string {
	return fmt.Sprintf("BackupStore[backup=%s, restore=%s]", w.backupName, restoreName)
}

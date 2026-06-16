package backup_store

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/sys/unix"
)

// LocalArtifactWriter implements ArtifactWriter for local filesystem storage
// Used when BackupStorageLocation has AccessMode set to ReadOnly
type LocalArtifactWriter struct {
	basePath string
	logger   logrus.FieldLogger
	mu       sync.Mutex
}

const (
	DefaultBasePath = "/scratch/restore-result"
)

// NewLocalArtifactWriter creates a LocalArtifactWriter with default base path
func NewLocalArtifactWriter(logger logrus.FieldLogger) *LocalArtifactWriter {
	return &LocalArtifactWriter{
		basePath: DefaultBasePath,
		logger:   logger,
	}
}

// NewLocalArtifactWriterWithPath creates a LocalArtifactWriter with custom base path
// Useful for testing or alternate storage locations
func NewLocalArtifactWriterWithPath(basePath string, logger logrus.FieldLogger) *LocalArtifactWriter {
	return &LocalArtifactWriter{
		basePath: basePath,
		logger:   logger,
	}
}

// GetRestoreArtifactDir returns the directory path for a specific restore
func (w *LocalArtifactWriter) GetRestoreArtifactDir(restoreName string) string {
	return filepath.Join(w.basePath, restoreName)
}

// ensureRestoreDir creates the restore artifact directory with proper permissions
// Thread-safe using mutex to handle concurrent restore operations
func (w *LocalArtifactWriter) ensureRestoreDir(restoreName string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	restoreDir := w.GetRestoreArtifactDir(restoreName)

	// Temporarily set the umask to 0 to allow creating the directory with 0777
	oldMask := unix.Umask(0)
	// Use defer to guarantee we restore the original umask
	defer unix.Umask(oldMask)

	if err := os.MkdirAll(restoreDir, 0777); err != nil {
		w.logger.WithError(err).WithFields(logrus.Fields{
			"RestoreName": restoreName,
			"Directory":   restoreDir,
			"Permissions": "0777",
		}).Error("Failed to create restore artifact directory")
		return errors.Wrapf(err, "failed to create restore directory %s", restoreDir)
	}

	w.logger.WithFields(logrus.Fields{
		"RestoreName": restoreName,
		"Directory":   restoreDir,
	}).Debug("Restore artifact directory ensured")

	return nil
}

// writeFile writes data from reader to a file in the restore directory
// Internal helper used by all public Write* methods
func (w *LocalArtifactWriter) writeFile(restoreName, fileName string, data io.Reader) error {
	if err := w.ensureRestoreDir(restoreName); err != nil {
		return err
	}

	restoreDir := w.GetRestoreArtifactDir(restoreName)
	filePath := filepath.Join(restoreDir, fileName)

	file, err := os.Create(filePath)
	if err != nil {
		w.logger.WithError(err).WithFields(logrus.Fields{
			"RestoreName": restoreName,
			"FilePath":    filePath,
		}).Error("Failed to create artifact file")
		return errors.Wrapf(err, "failed to create file %s", filePath)
	}
	defer file.Close()

	if err := file.Chmod(0666); err != nil {
		w.logger.WithError(err).WithFields(logrus.Fields{
			"RestoreName": restoreName,
			"FilePath":    filePath,
			"Permissions": "0666",
		}).Error("Failed to set permissions on artifact file")
		return errors.Wrapf(err, "failed to set permissions on file %s", filePath)
	}

	bytesWritten, err := io.Copy(file, data)
	if err != nil {
		w.logger.WithError(err).WithFields(logrus.Fields{
			"RestoreName":  restoreName,
			"FilePath":     filePath,
			"BytesWritten": bytesWritten,
		}).Error("Failed to write artifact file")
		return errors.Wrapf(err, "failed to write file %s", filePath)
	}

	w.logger.WithFields(logrus.Fields{
		"RestoreName":  restoreName,
		"FileName":     fileName,
		"FilePath":     filePath,
		"BytesWritten": bytesWritten,
	}).Debug("Artifact file written successfully")

	return nil
}

// WriteLog writes the restore log to local filesystem
func (w *LocalArtifactWriter) WriteLog(restoreName string, logData io.Reader) error {
	w.logger.WithFields(logrus.Fields{
		"RestoreName":  restoreName,
		"ArtifactType": "RestoreLog",
	}).Info("Writing restore log to local artifact directory")

	fileName := fmt.Sprintf("restore-%s-logs.gz", restoreName)
	return w.writeFile(restoreName, fileName, logData)
}

// WriteResults writes restore results (warnings/errors) to local filesystem
func (w *LocalArtifactWriter) WriteResults(restoreName string, results io.Reader) error {
	w.logger.WithFields(logrus.Fields{
		"RestoreName":  restoreName,
		"ArtifactType": "RestoreResults",
	}).Info("Writing restore results to local artifact directory")

	fileName := fmt.Sprintf("restore-%s-results.gz", restoreName)
	return w.writeFile(restoreName, fileName, results)
}

// WriteRestoredResourceList writes the list of restored resources to local filesystem
func (w *LocalArtifactWriter) WriteRestoredResourceList(restoreName string, resources io.Reader) error {
	w.logger.WithFields(logrus.Fields{
		"RestoreName":  restoreName,
		"ArtifactType": "RestoredResourcesList",
	}).Info("Writing restored resources list to local artifact directory")

	fileName := fmt.Sprintf("restore-%s-resource-list.json.gz", restoreName)
	return w.writeFile(restoreName, fileName, resources)
}

// WriteItemOperations writes restore item operations to local filesystem
func (w *LocalArtifactWriter) WriteItemOperations(restoreName string, operations io.Reader) error {
	w.logger.WithFields(logrus.Fields{
		"RestoreName":  restoreName,
		"ArtifactType": "RestoreItemOperations",
	}).Info("Writing restore item operations to local artifact directory")

	fileName := fmt.Sprintf("restore-%s-itemoperations.json.gz", restoreName)
	return w.writeFile(restoreName, fileName, operations)
}

// WriteVolumeInfo writes restore volume info to local filesystem
func (w *LocalArtifactWriter) WriteVolumeInfo(restoreName string, volumeInfo io.Reader) error {
	w.logger.WithFields(logrus.Fields{
		"RestoreName":  restoreName,
		"ArtifactType": "RestoreVolumeInfo",
	}).Info("Writing restore volume info to local artifact directory")

	fileName := fmt.Sprintf("%s-volumeinfo.json.gz", restoreName)
	return w.writeFile(restoreName, fileName, volumeInfo)
}

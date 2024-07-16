package catalogic

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	snapshotv1api "github.com/kubernetes-csi/external-snapshotter/client/v7/apis/volumesnapshot/v1"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	corev1api "k8s.io/api/core/v1"
	storagev1api "k8s.io/api/storage/v1"
	kerror "k8s.io/apimachinery/pkg/api/errors"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// UpdateSnapshotProgress updates the configmap in order to relay the
// snapshot progress to KubeAgent
func UpdateSnapshotProgress(
	pvc *corev1api.PersistentVolumeClaim,
	vs *snapshotv1api.VolumeSnapshot,
	vsc *snapshotv1api.VolumeSnapshotContent,
	state string,
	stateMessage string,
	jobID string,
	log logrus.FieldLogger,
) error {
	log.Info("Update Snapshot Progress - Starting to relay snapshot progress to KubAgent")
	if state == "" {
		return nil
	}
	var err error
	// Fill in the PVC realted information
	var pvcProgressObject = PvcInfo{}
	if pvc != nil {
		pvcProgressObject.Name = pvc.GetName()
		pvcProgressObject.Namespace = pvc.GetNamespace()
		pvcProgressObject.PvName = pvc.Spec.VolumeName
	} else if vs != nil {
		if vs.Spec.Source.PersistentVolumeClaimName != nil {
			pvcProgressObject.Name = *vs.Spec.Source.PersistentVolumeClaimName
		}
		pvcProgressObject.Namespace = vs.GetNamespace()
	}
	if vsc != nil {
		if vsc.Spec.Source.VolumeHandle != nil {
			pvcProgressObject.VolumeId = *vsc.Spec.Source.VolumeHandle
		}
		if vsc.Spec.Source.SnapshotHandle != nil {
			pvcProgressObject.SnapshotId = *vsc.Spec.Source.SnapshotHandle
		}
	}
	pvcProgressObject.SnapshotType = "CSI"
	// Fill in the snapshot progress related information
	var progress = PvcSnapshotProgressData{}
	currentTimeString := time.Now().UTC().Format(TimeFormat)
	progress.State = state
	progress.Message = currentTimeString + " " + stateMessage
	progress.JobId = jobID
	progress.Pvc = &pvcProgressObject
	log.Info("Update Snapshot Progress -", "Progress Payload", progress)
	// Prepare the paylod to be embedded into the configmap
	requestData := make(map[string][]byte)
	if requestData["snapshot_progress_payload"], err = json.Marshal(progress); err != nil {
		newErr := errors.Wrap(err, "Failed to marshal progress while creating the snapshot progress configmap")
		log.Error(newErr, "JSON marshalling failed")
		return newErr
	}
	log.Info("Update Snapshot Progress -", "Marsahlled the JSON payload")
	// create the configmap object.
	moverConfigMap := corev1api.ConfigMap{
		TypeMeta: v1.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: "v1",
		},
		ObjectMeta: v1.ObjectMeta{
			Name:      SnapshotProgressUpdateConfigMapName,
			Namespace: CloudCasaNamespace,
		},
		BinaryData: requestData,
	}
	log.Info("Update Snapshot Progress -", "Created the configmap object")
	// creates the in-cluster config
	config, err := rest.InClusterConfig()
	if err != nil {
		newErr := errors.Wrap(err, "Failed to create in-cluster config")
		log.Error(newErr, "Failed to create in-cluster config")
		return newErr

	}
	log.Info("Update Snapshot Progress -", "Created in-cluster config")
	// Create clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		newErr := errors.Wrap(err, "Failed to create clientset")
		log.Error(newErr, "Failed to create clientset")
		return newErr
	}
	log.Info("Update Snapshot Progress -", "Created clientset")
	//Create or update the configmap
	var mcm *corev1api.ConfigMap
	if _, mErr := clientset.CoreV1().ConfigMaps(CloudCasaNamespace).Get(context.TODO(), SnapshotProgressUpdateConfigMapName, v1.GetOptions{}); kerror.IsNotFound(mErr) {
		mcm, err = clientset.CoreV1().ConfigMaps(CloudCasaNamespace).Create(context.TODO(), &moverConfigMap, v1.CreateOptions{})
		if err != nil {
			newErr := errors.Wrap(err, "Failed to create configmap to report snapshotprogress")
			log.Error(newErr, "Failed to create configmap")
			return newErr

		}
		log.Info("Created configmap to report snapshot progress", "Configmap Name", mcm.GetName())
	} else {
		mcm, err = clientset.CoreV1().ConfigMaps(CloudCasaNamespace).Update(context.TODO(), &moverConfigMap, v1.UpdateOptions{})
		if err != nil {
			newErr := errors.Wrap(err, "Failed to update configmap to report snapshotprogress")
			log.Error(newErr, "Failed to update configmap")
			return newErr
		}
		log.Info("Updated configmap to report snapshot progress", "Configmap Name", mcm.GetName())
	}
	log.Info("finished relaying snapshot progress to KubeAgent")
	return nil
}

// DeleteSnapshotProgressConfigMap deletes the configmap used to report snapshot progress
func DeleteSnapshotProgressConfigMap(log logrus.FieldLogger) {
	// creates the in-cluster config
	config, err := rest.InClusterConfig()
	if err != nil {
		log.Error(errors.Wrap(err, "Failed to create in-cluster config"))
	}
	// creates the clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Error(errors.Wrap(err, "Failed to create in-cluster clientset"))
	}
	err = clientset.CoreV1().ConfigMaps(CloudCasaNamespace).Delete(context.TODO(), SnapshotProgressUpdateConfigMapName, v1.DeleteOptions{})
	if err != nil {
		log.Error(errors.Wrap(err, "Failed to delete configmap used to report snapshot progress"))
	} else {
		log.Info("Deleted configmap used to report snapshot progress", "Configmap Name", SnapshotProgressUpdateConfigMapName)
	}
}

// GetPluginConfig reads the configmap that contains config parameters for this plugin
func GetPluginConfig(log logrus.FieldLogger) (*PluginConfig, error) {
	clientset, err := GetClientset(log)
	if err != nil {
		return nil, err
	}

	configMap, err := clientset.CoreV1().ConfigMaps(CloudCasaNamespace).Get(context.TODO(), VeleroCsiPluginConfigMapName, v1.GetOptions{})
	if err != nil {
		log.Error(errors.Wrapf(err, "Failed to get %q configmap in %q namespace", VeleroCsiPluginConfigMapName, CloudCasaNamespace))
		return nil, err
	}

	snapshotWherePossibleString := string(configMap.BinaryData["snapshotWherePossible"])
	snapshotWherePossible, err := strconv.ParseBool(snapshotWherePossibleString)
	if err != nil {
		log.Error(errors.Wrapf(err, "Failed to parse snapshotWherePossible value %q from %q", snapshotWherePossibleString,
			VeleroCsiPluginConfigMapName))
		return nil, err
	}
	if snapshotWherePossible {
		log.Info("Will snapshot PVCs where possible")
	} else {
		log.Info("Will backup all PVCs live")
	}

	snapshotLonghornString := string(configMap.BinaryData["snapshotLonghorn"])
	snapshotLonghorn, err := strconv.ParseBool(snapshotLonghornString)
	if err != nil {
		log.Error(errors.Wrapf(err, "Failed to parse snapshotLonghorn value %q from %q", snapshotLonghornString, VeleroCsiPluginConfigMapName))
		return nil, err
	}
	if snapshotLonghorn {
		log.Info("Will snapshot Longhorn PVCs instead of doing live backup")
	}

	csiSnapshotTimeoutString := string(configMap.BinaryData["csiSnapshotTimeout"])
	csiSnapshotTimeout, err := strconv.Atoi(csiSnapshotTimeoutString)
	if err != nil {
		log.Error(errors.Wrapf(err, "Failed to parse csiSnapshotTimeout value %q from %q", csiSnapshotTimeoutString, VeleroCsiPluginConfigMapName))
		return nil, err
	}
	if csiSnapshotTimeout != 0 {
		log.Info("CSI snapshot timeout is set", "timeout", csiSnapshotTimeout)
	}

	storageClassBackupMethodMapBytes := configMap.BinaryData["storageClassBackupMethodMap"]
	var storageClassBackupMethodMap map[string]string
	if err := json.Unmarshal(storageClassBackupMethodMapBytes, &storageClassBackupMethodMap); err != nil {
		log.Error(errors.Wrapf(err, "Failed to parse storageClassBackupMethodMap value %q from %q", string(storageClassBackupMethodMapBytes),
			VeleroCsiPluginConfigMapName))
		return nil, err
	}
	if len(storageClassBackupMethodMap) != 0 {
		log.Info("Storage class backup method is set", "method", storageClassBackupMethodMap)
	}

	return &PluginConfig{
		SnapshotWherePossible:       snapshotWherePossible,
		SnapshotLonghorn:            snapshotLonghorn,
		CsiSnapshotTimeout:          csiSnapshotTimeout,
		StorageClassBackupMethodMap: storageClassBackupMethodMap,
	}, nil
}

func GetClientset(log logrus.FieldLogger) (*kubernetes.Clientset, error) {
	// creates the in-cluster config
	config, err := rest.InClusterConfig()
	if err != nil {
		log.Error(errors.Wrap(err, "Failed to create in-cluster config"))
		return nil, err
	}
	// creates the clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Error(errors.Wrap(err, "Failed to create in-cluster clientset"))
		return nil, err
	}

	return clientset, nil
}

func SetStaticAzureAnotation(pvc *corev1api.PersistentVolumeClaim, vals map[string]string, storageClass *storagev1api.StorageClass, log logrus.FieldLogger) error {
	// For Azure File PVs, we need to check if the volume has been provisioned statically.
	// If so, we need to store file share name, secret name, and secret namespace as annotation.
	// Then, during restore, our Azure Files Mover will pick these values to restore from a
	// statically provisioned volume.
	if storageClass.Provisioner == "file.csi.azure.com" {
		log.Infof("Found %s PVC %s/%s. Getting %s PV details", storageClass.Provisioner, pvc.Namespace, pvc.Name, pvc.Spec.VolumeName)
		config, err := rest.InClusterConfig()
		if err != nil {
			log.Errorf("Failed to get in-cluster Kubernetes config: %w", err)
			return errors.WithStack(err)
		}

		clientset, err := kubernetes.NewForConfig(config)
		if err != nil {
			log.Errorf("Failed to get Kubernetes clientset for in-cluster config: %w", err)
			return errors.WithStack(err)
		}
		pv, err := clientset.CoreV1().PersistentVolumes().Get(context.TODO(), pvc.Spec.VolumeName, v1.GetOptions{})
		if err != nil {
			log.Errorf("Failed to get PV %s for PVC %s/%s: %w", pvc.Spec.VolumeName, pvc.Namespace, pvc.Name, err)
			return errors.WithStack(err)
		}
		if pv.Spec.CSI != nil {
			var shareName, secretName, secretNamespace string
			var isDynamicallyProvisioned bool
			// Check if the PV was provisioned dynamically or statically
			volumeAttributes := pv.Spec.CSI.VolumeAttributes
			if _, exists := volumeAttributes["csi.storage.k8s.io/pv/name"]; exists {
				log.Infof("Dynamically provisioned %s PV %s bounded with PVC %s/%s found", storageClass.Provisioner, pv.Name, pvc.Namespace, pvc.Name)
				isDynamicallyProvisioned = true

			} else {
				log.Infof("Statically provisioned %s PV %s bounded with PVC %s/%s found", storageClass.Provisioner, pv.Name, pvc.Namespace, pvc.Name)
				isDynamicallyProvisioned = false
			}

			// Get name of the File Share where files are stored
			shareName, err = GetAzureFileShareName(*pvc, pv, storageClass, log, isDynamicallyProvisioned)
			if err != nil {
				log.Errorf(
					"Could not find file share name for %s PV %s bounded with PVC %s/%s",
					storageClass.Provisioner, pv.Name, pvc.Namespace, pvc.Namespace,
				)
				return errors.WithStack(err)
			}
			log.Infof("Found file share name for %s PV %s bounded with PVC %s/%s: %s ", storageClass.Provisioner, pv.Name, pvc.Namespace, pvc.Name, secretNamespace)

			// Get secret that stores credentials to Azure Storage Account
			secretNamespace, err = GetAzureFileSecretNamespace(*pvc, pv, storageClass, log, isDynamicallyProvisioned)
			if err != nil {
				log.Errorf(
					"Could not find secret namesapce for %s PV %s bounded with PVC %s/%s",
					storageClass.Provisioner, pv.Name, pvc.Namespace, pvc.Namespace,
				)
				return errors.WithStack(err)
			}
			log.Infof("Found secret namespace for %s PV %s bounded with PVC %s/%s: %s ", storageClass.Provisioner, pv.Name, pvc.Namespace, pvc.Name, secretNamespace)

			// Get name of the secret that stores credentials to Azure Storage Account
			secretName, err = GetAzureFileSecretName(*pvc, pv, storageClass, log, isDynamicallyProvisioned)
			if err != nil {
				log.Errorf(
					"Could not find secret name for %s PV %s bounded with PVC %s/%s",
					storageClass.Provisioner, pv.Name, pvc.Namespace, pvc.Namespace,
				)
				return errors.WithStack(err)
			}
			log.Infof("Found secret name for %s PV %s bounded with PVC %s/%s: %s ", storageClass.Provisioner, pv.Name, pvc.Namespace, pvc.Name, secretNamespace)

			// Set annotation that will be later read by the Azure Files Mover
			ccAzureFilesAnnotations := map[string]string{
				StaticAzureFilePvcAnnotation:                shareName,
				StaticAzureFilePvcSecretNameAnnotation:      secretName,
				StaticAzureFilePvcSecretNamespaceAnnotation: secretNamespace,
			}
			for k, v := range ccAzureFilesAnnotations {
				vals[k] = v
			}
			log.Infof("Setting new annotations %v for PVC %s/%s", vals, pvc.Namespace, pvc.Name)
		}
	}
	return nil
}

// GetAzureFileSecretNamespace returns namespace where secret for Azure Storage Account
// is present.
func GetAzureFileSecretNamespace(pvc corev1api.PersistentVolumeClaim, pv *corev1api.PersistentVolume, sc *storagev1api.StorageClass, log logrus.FieldLogger, isDynamicallyProvisioned bool) (string, error) {
	var secretNamespace string
	var exists bool

	volumeAttributes := pv.Spec.CSI.VolumeAttributes
	nodeStageSecretRef := pv.Spec.CSI.NodeStageSecretRef

	// In case of statically provisioned PVs, first look at nodeStageSecretRef
	// and see if secret details are set there.
	// If not, follow the procedure for dynamically provisioned volumes.
	if !isDynamicallyProvisioned {
		if nodeStageSecretRef == nil {
			log.Infof(
				"Statically provisioned %s PV %s bounded with PVC %s/%s does not have \"nodeStageSecretRef\" set. Checking volume attributes for secret namespace",
				sc.Provisioner, pv.Name, pvc.Namespace, pvc.Name,
			)
		} else {
			secretNamespace = nodeStageSecretRef.Namespace
			if secretNamespace != "" {
				log.Infof(
					"Statically provisioned %s PV %s bounded with PVC %s/%s has \"nodeStageSecretRef.namespace\" set.",
					sc.Provisioner, pv.Name, pvc.Namespace, pvc.Name,
				)
				return secretNamespace, nil
			}
			log.Infof(
				"Statically provisioned %s PV %s bounded with PVC %s/%s does not have \"nodeStageSecretRef.namespace\" set. Checking volume attributes for secret namespace",
				sc.Provisioner, pv.Name, pvc.Namespace, pvc.Name,
			)
		}
	}

	secretNamespace, exists = volumeAttributes["secretnamespace"]
	if !exists {
		secretNamespace, exists = volumeAttributes["secretNamespace"]
		if !exists {
			if isDynamicallyProvisioned {
				log.Infof(
					"Dynamically provisioned %s PV %s bounded with PVC %s/%s does not have \"secretnamespace\" or \"secretNamespace\" attribute. Assuming PVC namespace as the secret namespace",
					sc.Provisioner, pv.Name, pvc.Namespace, pvc.Name,
				)
				secretNamespace = pv.GetName()
			} else {
				err := fmt.Errorf(
					"%s PV %s bounded with PVC %s/%s does not have \"secretnamespace\" or \"secretNamespace\" attribute",
					sc.Provisioner, pv.Name, pvc.Namespace, pvc.Name,
				)
				return "", errors.WithStack(err)
			}
		}
	}
	return secretNamespace, nil
}

// GetAzureFileSecretName check if secret name is stored in volume details.
// If not, it returns an error.
func GetAzureFileSecretName(pvc corev1api.PersistentVolumeClaim, pv *corev1api.PersistentVolume, sc *storagev1api.StorageClass, log logrus.FieldLogger, isDynamicallyProvisioned bool) (string, error) {
	var secretName string
	var exists bool

	volumeAttributes := pv.Spec.CSI.VolumeAttributes
	nodeStageSecretRef := pv.Spec.CSI.NodeStageSecretRef

	// In case of statically provisioned PVs, first look at nodeStageSecretRef
	// and see if secret details  are set there.
	// If not, follow the procedure for dynamically provisioned volumes.
	if !isDynamicallyProvisioned {
		if nodeStageSecretRef == nil {
			log.Infof(
				"Statically provisioned %s PV %s bounded with PVC %s/%s does not have \"nodeStageSecretRef\" set. Checking volume attributes for secret name",
				sc.Provisioner, pv.Name, pvc.Namespace, pvc.Name,
			)
		} else {
			secretName = nodeStageSecretRef.Name
			if secretName != "" {
				log.Infof(
					"Statically provisioned %s PV %s bounded with PVC %s/%s has \"nodeStageSecretRef.name\" set.",
					sc.Provisioner, pv.Name, pvc.Namespace, pvc.Name,
				)
				return secretName, nil
			}
			log.Infof(
				"Statically provisioned %s PV %s bounded with PVC %s/%s does not have \"nodeStageSecretRef.name\" set. Checking volume attributes for secret name",
				sc.Provisioner, pv.Name, pvc.Namespace, pvc.Name,
			)
		}
	}

	secretName, exists = volumeAttributes["secretname"]
	if !exists {
		secretName, exists = volumeAttributes["secretName"]
		if !exists {
			if isDynamicallyProvisioned {
				log.Infof(
					"Dynamically provisioned %s PV %s bounded with PVC %s/%s does not have \"secretName\" or \"secretname\" attribute",
					sc.Provisioner, pv.Name, pvc.Namespace, pvc.Name,
				)
				// If secret name is not present in the attributes, it means the storage Account
				// was created the CSI drvier. In such case, get the Storage account name from
				// volumeHandle.
				// Example:
				// volumeHandle: <resource-group>#<storage-account-name>#<share-name>###<namespace>
				volumeHandleSplit := strings.Split(pv.Spec.CSI.VolumeHandle, "#")
				if len(volumeHandleSplit) < 2 {
					err := fmt.Errorf(
						"%s PV %s bounded with PVC %s/%s does not have \"secretname\" or \"secretName\" attribute. Failed to generate one",
						sc.Provisioner, pv.Name, pvc.Namespace, pvc.Name,
					)
					return "", errors.WithStack(err)
				}
				storageAccountName := volumeHandleSplit[1]
				secretName = fmt.Sprintf("azure-storage-account-%s-secret", storageAccountName)
			} else {
				err := fmt.Errorf(
					"%s PV %s bounded with PVC %s/%s does not have \"secretname\" or \"secretName\" attribute",
					sc.Provisioner, pv.Name, pvc.Namespace, pvc.Name,
				)
				return "", errors.WithStack(err)
			}
		}
	}
	return secretName, nil
}

// GetAzureFileShareName returns name of the Azure File share where files for a PV
// are stored.
// It checks for presence of "shareName" and "sharename" attribute in the Volume.
// If none of them is set, it assumes PV name as the file share name.
func GetAzureFileShareName(pvc corev1api.PersistentVolumeClaim, pv *corev1api.PersistentVolume, sc *storagev1api.StorageClass, log logrus.FieldLogger, isDynamicallyProvisioned bool) (string, error) {
	var shareName string
	var exists bool

	// Get share name. First check the attribute, if the shareName is not set,
	// assume name of the PV.
	volumeAttributes := pv.Spec.CSI.VolumeAttributes
	shareName, exists = volumeAttributes["shareName"]
	if !exists {
		shareName, exists = volumeAttributes["sharename"]
		if !exists {
			if isDynamicallyProvisioned {
				log.Infof(
					"Dynamically provisioned %s PV %s bounded with PVC %s/%s does not have \"shareName\" or \"sharename\" attribute. Assuming PV name as the file share name",
					sc.Provisioner, pv.Name, pvc.Namespace, pvc.Name,
				)
				shareName = pv.GetName()
			} else {
				err := fmt.Errorf(
					"%s PV %s bounded with PVC %s/%s does not have \"sharename\" or \"shareName\" attribute",
					sc.Provisioner, pv.Name, pvc.Namespace, pvc.Name,
				)
				return "", errors.WithStack(err)
			}
		}
	}
	return shareName, nil
}

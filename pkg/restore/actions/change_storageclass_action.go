/*
Copyright 2019 the Velero contributors.

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

package actions

import (
	"context"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	storagev1client "k8s.io/client-go/kubernetes/typed/storage/v1"

	"github.com/vmware-tanzu/velero/pkg/plugin/velero"
)

// changeStorageClassConfigMapPrefix is the name prefix of the per-restore
// ConfigMap the kubeagent creates for storage-class remapping. The full name is
// changeStorageClassConfigMapPrefix + <restore name>. One ConfigMap is created
// per restore so that restores running in parallel each select their own mapping.
const changeStorageClassConfigMapPrefix = "cloudcasa-io-change-sc-"

// ChangeStorageClassAction updates a PV or PVC's storage class name
// if a mapping is found in the plugin's config map.
type ChangeStorageClassAction struct {
	logger             logrus.FieldLogger
	configMapClient    corev1client.ConfigMapInterface
	storageClassClient storagev1client.StorageClassInterface
}

// NewChangeStorageClassAction is the constructor for ChangeStorageClassAction.
func NewChangeStorageClassAction(
	logger logrus.FieldLogger,
	configMapClient corev1client.ConfigMapInterface,
	storageClassClient storagev1client.StorageClassInterface,
) *ChangeStorageClassAction {
	return &ChangeStorageClassAction{
		logger:             logger,
		configMapClient:    configMapClient,
		storageClassClient: storageClassClient,
	}
}

// AppliesTo returns the resources that ChangeStorageClassAction should
// be run for.
func (a *ChangeStorageClassAction) AppliesTo() (velero.ResourceSelector, error) {
	return velero.ResourceSelector{
		IncludedResources: []string{"persistentvolumeclaims", "persistentvolumes", "statefulsets"},
	}, nil
}

// Execute updates the item's spec.storageClassName if a mapping is found
// in the config map for the plugin.
func (a *ChangeStorageClassAction) Execute(input *velero.RestoreItemActionExecuteInput) (*velero.RestoreItemActionExecuteOutput, error) {
	a.logger.Info("Executing ChangeStorageClassAction")
	defer a.logger.Info("Done executing ChangeStorageClassAction")

	// The kubeagent runs restores in parallel and creates one change-storage-class
	// ConfigMap per restore, named "cloudcasa-io-change-sc-<restoreName>". Fetch this
	// restore's ConfigMap by name instead of listing on the shared plugin-config
	// label: with concurrent restores that list would either error ("found more than
	// one ConfigMap ...") or pick another restore's mapping. A by-name Get is also
	// immune to orphaned ConfigMaps left behind by crashed restores.
	cmName := changeStorageClassConfigMapPrefix + input.Restore.GetName()
	a.logger.Debugf("Getting change-storage-class ConfigMap %s", cmName)
	config, err := a.configMapClient.Get(context.TODO(), cmName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		a.logger.Debugf("No change-storage-class ConfigMap %s; no storage class mappings", cmName)
		return velero.NewRestoreItemActionExecuteOutput(input.Item), nil
	}
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if len(config.Data) == 0 {
		a.logger.Debug("No storage class mappings found")
		return velero.NewRestoreItemActionExecuteOutput(input.Item), nil
	}

	obj, ok := input.Item.(*unstructured.Unstructured)
	if !ok {
		return nil, errors.Errorf("object was of unexpected type %T", input.Item)
	}

	log := a.logger.WithFields(map[string]interface{}{
		"kind":      obj.GetKind(),
		"namespace": obj.GetNamespace(),
		"name":      obj.GetName(),
	})

	// change StatefulSet volumeClaimTemplates storageClassName
	if obj.GetKind() == "StatefulSet" {
		sts := new(appsv1.StatefulSet)
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.UnstructuredContent(), sts); err != nil {
			return nil, err
		}

		if len(sts.Spec.VolumeClaimTemplates) > 0 {
			for index, pvc := range sts.Spec.VolumeClaimTemplates {
				exists, newStorageClass, err := a.isStorageClassExist(log, pvc.Spec.StorageClassName, config)
				if err != nil {
					return nil, err
				} else if !exists {
					continue
				}

				log.Infof("Updating item's storage class name to %s", newStorageClass)
				sts.Spec.VolumeClaimTemplates[index].Spec.StorageClassName = &newStorageClass
			}

			newObj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(sts)
			if err != nil {
				return nil, errors.Wrap(err, "convert obj to StatefulSet failed")
			}
			obj.Object = newObj
		}
	} else {
		// use the unstructured helpers here since this code is for both PVs and PVCs, and the
		// field names are the same for both types.
		storageClass, _, err := unstructured.NestedString(obj.UnstructuredContent(), "spec", "storageClassName")
		if err != nil {
			return nil, errors.Wrap(err, "error getting item's spec.storageClassName")
		}

		exists, newStorageClass, err := a.isStorageClassExist(log, &storageClass, config)
		if err != nil {
			return nil, err
		} else if !exists {
			return velero.NewRestoreItemActionExecuteOutput(input.Item), nil
		}

		log.Infof("Updating item's storage class name to %s", newStorageClass)

		if err := unstructured.SetNestedField(obj.UnstructuredContent(), newStorageClass, "spec", "storageClassName"); err != nil {
			return nil, errors.Wrap(err, "unable to set item's spec.storageClassName")
		}
	}
	return velero.NewRestoreItemActionExecuteOutput(obj), nil
}

func (a *ChangeStorageClassAction) isStorageClassExist(log *logrus.Entry, storageClass *string, cm *corev1.ConfigMap) (exists bool, newStorageClass string, err error) {
	if storageClass == nil || *storageClass == "" {
		log.Debug("Item has no storage class specified")
		return false, "", nil
	}

	newStorageClass, ok := cm.Data[*storageClass]
	if !ok {
		log.Debugf("No mapping found for storage class %s", *storageClass)
		return false, "", nil
	}

	// validate that new storage class exists
	if _, err := a.storageClassClient.Get(context.TODO(), newStorageClass, metav1.GetOptions{}); err != nil {
		return false, "", errors.Wrapf(err, "error getting storage class %s from API", newStorageClass)
	}

	return true, newStorageClass, nil
}

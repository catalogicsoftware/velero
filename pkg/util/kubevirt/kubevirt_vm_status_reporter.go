package kubevirt

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/util/results"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"

	kbClient "sigs.k8s.io/controller-runtime/pkg/client"
)

// Define constants for KubeVirt VM group and resource names.
const (
	kubeVirtGroup    = "kubevirt.io"
	kubeVirtResource = "virtualmachines"

	configMapRetryInterval = 100 * time.Millisecond
	configMapRetryTimeout  = 2 * time.Second

	vmProgressSuffix = "cc-vm-progress"
)

// kubeVirtVMGroupResource represents the GroupResource identifier for KubeVirt VMs.
var kubeVirtVMGroupResource = schema.GroupResource{
	Group:    kubeVirtGroup,
	Resource: kubeVirtResource,
}

// VMStatus represents the status information for a virtual machine within the backup operation.
type VMStatus struct {
	VMName              string                     `json:"vmName"`
	Status              string                     `json:"status"` // ACTIVE, COMPLETED, FAILED, PARTIAL
	AdditionalResources []AdditionalResourceStatus `json:"additionalResources"`
	Message             string                     `json:"message,omitempty"`
}

// AdditionalResourceStatus represents the status information for a resource related to the virtual machine.
type AdditionalResourceStatus struct {
	ResourceType string `json:"resourceType"` // GVR of the resource
	ResourceName string `json:"resourceName"` // <namespace>/<name> format
	Status       string `json:"status"`       // ACTIVE, COMPLETED, FAILED, PARTIAL
	Message      string `json:"message,omitempty"`
}

// itemKey represents a unique identifier for an item during restore processing.
type ItemKey struct {
	Resource  string
	Namespace string
	Name      string
}

// IsKubeVirtVMResource checks if a provided GroupResource matches the KubeVirt VM GroupResource.
func IsKubeVirtVMResource(groupResource schema.GroupResource) bool {
	return groupResource == kubeVirtVMGroupResource
}

// Helper function to create initial data structure for the ConfigMap
func createInitialData(jobID, jobType, vmKey string) map[string]interface{} {
	return map[string]interface{}{
		"jobID":   jobID,
		"jobType": jobType,
		"vms": map[string]VMStatus{
			vmKey: createVMStatus(vmKey, jobType),
		},
	}
}

// Helper function to create a VMStatus with optional status, message, and additional resources
// Parameters: VM Key[MANDATORY], jobtype[MANDATORY], OPTIONAL [STATUS, Message, Slice of additionalResources]
// Example: createVMStatus("my-vm", "Backup", "PARTIAL", "Backup Partially Completed", additionalResources)
func createVMStatus(vmKey string, jobType string, opts ...interface{}) VMStatus {
	defaultStatus := "ACTIVE"
	defaultMessage := fmt.Sprintf("%s In Progress", jobType)

	status := defaultStatus
	message := defaultMessage
	var additionalResources []AdditionalResourceStatus
	additionalResourcesProvided := false

	// Handle optional parameters based on type and index
	for i, opt := range opts {
		switch v := opt.(type) {
		case string:
			if i == 0 && v != "" {
				status = v
			} else if i == 1 && v != "" {
				message = v
			}
		case []AdditionalResourceStatus:
			additionalResources = v
			additionalResourcesProvided = true
		}
	}

	// Create VMStatus object
	vmStatus := VMStatus{
		VMName:  vmKey,
		Status:  status,
		Message: message,
	}

	// Set AdditionalResources only if provided
	if additionalResourcesProvided {
		vmStatus.AdditionalResources = additionalResources
	}

	return vmStatus
}

// printConfigMapData logs the Data section of the ConfigMap in JSON format
func printConfigMapData(log logrus.FieldLogger, configMap *corev1.ConfigMap) {
	// Convert the ConfigMap data to JSON format for logging
	dataJSON, err := json.MarshalIndent(configMap.Data, "", "  ")
	if err != nil {
		log.Errorf("Failed to marshal ConfigMap data for logging: %v", err)
		return
	}

	// Log the Data section of the ConfigMap
	log.Infof(" ==> ConfigMap %s/%s Data:\n%s", configMap.Namespace, configMap.Name, string(dataJSON))
}

// extractJobDetails extracts the ID, type, and namespace of a job (either a Backup or Restore) for tracking VM progress.
func extractJobDetails(job interface{}) (string, string, string, error) {
	var jobID, jobType, jobNamespace string

	switch j := job.(type) {
	case *velerov1api.Backup:
		jobID = j.Name
		jobType = "Backup"
		jobNamespace = j.Namespace
	case *velerov1api.Restore:
		jobID = j.Name
		jobType = "Restore"
		jobNamespace = j.Namespace
	default:
		err := fmt.Errorf("unsupported job type: %T", job)
		return "", "", "", err
	}

	return jobID, jobType, jobNamespace, nil
}

// ToError converts a results.Result to an error, or returns nil if there are no errors.
func ToError(r results.Result) error {
	var messages []string

	// Collect Velero-specific errors
	messages = append(messages, r.Velero...)

	// Collect cluster-scoped errors
	messages = append(messages, r.Cluster...)

	// Collect namespace-scoped errors
	for ns, errs := range r.Namespaces {
		for _, err := range errs {
			messages = append(messages, fmt.Sprintf("namespace %s: %s", ns, err))
		}
	}

	// If there are no messages, return nil
	if len(messages) == 0 {
		return nil
	}

	// Join all messages into a single error string
	return fmt.Errorf(strings.Join(messages, "; "))
}

// KubeVirtVMStartOp reports the start of a backup for a KubeVirt VM resource,
// creating or updating a ConfigMap to store progress.
func KubeVirtVMStartOp(
	log logrus.FieldLogger,
	vmName, vmNamespace string,
	job interface{}, // Accepts either *velerov1api.Backup or *velerov1api.Restore
	kubeClient kbClient.Client, // Uses kbClient.Client from itemBackupper
) {
	jobID, jobType, jobNamespace, err := extractJobDetails(job)
	if err != nil {
		log.Error(err)
		return
	}

	configMapName := fmt.Sprintf("%s-%s", jobID, vmProgressSuffix)
	configMapNamespace := jobNamespace
	vmKey := fmt.Sprintf("%s/%s", vmNamespace, vmName)

	// Create the initial VM status using helper function
	vmStatus := createVMStatus(vmKey, jobType)
	initialData := createInitialData(jobID, jobType, vmKey)

	initialDataJSON, err := json.Marshal(initialData)
	if err != nil {
		log.Errorf("Failed to marshal initial data for ConfigMap %s: %v", configMapName, err)
		return
	}

	// Retry loop to create or update the ConfigMap, handling conflicts with exponential backoff
	err = wait.ExponentialBackoff(wait.Backoff{
		Steps:    5,
		Duration: configMapRetryInterval,
		Factor:   1.5,
		Jitter:   0.1,
	}, func() (bool, error) {
		configMap := &corev1.ConfigMap{}
		if err := kubeClient.Get(context.TODO(), kbClient.ObjectKey{Name: configMapName, Namespace: configMapNamespace}, configMap); err != nil {
			if apierrors.IsNotFound(err) {
				// ConfigMap does not exist, so create it with initial data
				newConfigMap := &corev1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{
						Name:      configMapName,
						Namespace: configMapNamespace,
						Labels: map[string]string{
							"cc-job-id": jobID,
						},
					},
					Data: map[string]string{
						"vmStatusData": string(initialDataJSON),
					},
				}
				if err := kubeClient.Create(context.TODO(), newConfigMap); err != nil {
					log.Warnf("Failed to create ConfigMap %s: %v", configMapName, err)
					return false, err // Retry on other errors
				}
				log.Infof("Created ConfigMap %s for KubeVirt VM %s status", configMapName, jobType)
				printConfigMapData(log, newConfigMap)
				return true, nil
			}
			log.Warnf("Error retrieving ConfigMap %s: %v", configMapName, err)
			return false, err // Retry on transient errors
		}

		// ConfigMap exists, attempt to update it with the latest VM status
		existingData, ok := configMap.Data["vmStatusData"]
		if !ok {
			log.Warnf("ConfigMap %s missing expected 'vmStatusData' key, reinitializing", configMapName)
			configMap.Data["vmStatusData"] = string(initialDataJSON)
		} else {
			// Parse existing VM data, checking for errors
			var currentData map[string]interface{}
			if err := json.Unmarshal([]byte(existingData), &currentData); err != nil {
				log.Errorf("Failed to unmarshal existing data in ConfigMap %s: %v", configMapName, err)
				return false, err // Unrecoverable error
			}

			vmsData, ok := currentData["vms"].(map[string]interface{})
			if !ok {
				log.Warnf("ConfigMap %s has an invalid 'vms' structure, reinitializing", configMapName)
				currentData["vms"] = map[string]VMStatus{vmStatus.VMName: vmStatus}
			} else {
				vmsData[vmStatus.VMName] = vmStatus // Update or add VM status
			}

			updatedDataJSON, err := json.Marshal(currentData)
			if err != nil {
				log.Errorf("Failed to marshal updated data for ConfigMap %s: %v", configMapName, err)
				return false, err
			}
			configMap.Data["vmStatusData"] = string(updatedDataJSON)
		}

		// Attempt to update the ConfigMap
		if err := kubeClient.Update(context.TODO(), configMap); err != nil {
			if apierrors.IsConflict(err) {
				log.Warnf("Conflict updating ConfigMap %s, retrying", configMapName)
				return false, nil // Retry on conflicts
			}
			log.Warnf("Failed to update ConfigMap %s: %v", configMapName, err)
			return false, err // Retry on other errors
		}
		log.Infof("Updated ConfigMap %s for KubeVirt VM %s status", configMapName, jobType)
		printConfigMapData(log, configMap)
		return true, nil
	})

	// Log final error if all retries failed
	if err != nil {
		log.Errorf("Error creating/updating ConfigMap %s for VM %s: %v", configMapName, vmStatus.VMName, err)
	}
}

// kubeVirtVMOpResult reports the backup result for the primary KubeVirt VM resource,
// updating the ConfigMap to reflect success or failure.
func KubeVirtVMOpResult(
	log logrus.FieldLogger,
	groupResource schema.GroupResource,
	name string, namespace string,
	job interface{}, // Accepts either *velerov1api.Backup or *velerov1api.Restore
	success bool,
	err error,
	kubeClient kbClient.Client,
) {
	jobID, jobType, jobNamespace, eerr := extractJobDetails(job)
	if eerr != nil {
		log.Error(eerr)
		return
	}

	configMapName := fmt.Sprintf("%s-%s", jobID, vmProgressSuffix)
	configMapNamespace := jobNamespace
	vmKey := fmt.Sprintf("%s/%s", namespace, name) // Full VM key to locate VM status in ConfigMap

	// Determine status and message based on success or failure
	var status, message string
	if success {
		status = "COMPLETED"
		message = fmt.Sprintf("%s completed successfully", jobType)
		log.Infof("%s succeeded for KubeVirt VM resource: %s : %s/%s, %s: %s", jobType, groupResource.String(), namespace, name, jobType, jobID)
	} else {
		status = "FAILED"
		message = fmt.Sprintf("%s failed: %v", jobType, err)
		log.Errorf("%s failed for KubeVirt VM resource: %s : %s/%s, %s: %s, reason: %v", jobType, groupResource.String(), namespace, name, jobType, jobID, err)
	}

	// Retry loop to update the ConfigMap with the result of the backup
	err = wait.ExponentialBackoff(wait.Backoff{
		Steps:    5,
		Duration: configMapRetryInterval,
		Factor:   1.5,
		Jitter:   0.1,
	}, func() (bool, error) {
		configMap := &corev1.ConfigMap{}
		if err := kubeClient.Get(context.TODO(), kbClient.ObjectKey{Name: configMapName, Namespace: configMapNamespace}, configMap); err != nil {
			log.Warnf("Error retrieving ConfigMap %s: %v", configMapName, err)
			return false, err // Retry on transient errors
		}

		// Fetch the existing data from the ConfigMap and update the VM status
		existingData, ok := configMap.Data["vmStatusData"]
		if !ok {
			log.Warnf("ConfigMap %s missing expected 'vmStatusData' key; unable to update VM status", configMapName)
			return false, fmt.Errorf("missing 'vmStatusData' in ConfigMap %s", configMapName)
		}

		var currentData map[string]interface{}
		if err := json.Unmarshal([]byte(existingData), &currentData); err != nil {
			log.Errorf("Failed to unmarshal existing data in ConfigMap %s: %v", configMapName, err)
			return false, err // Unrecoverable error
		}

		// Update VM's backup status and message
		vmsData, ok := currentData["vms"].(map[string]interface{})
		if !ok {
			log.Warnf("ConfigMap %s has an invalid 'vms' structure, cannot update VM status", configMapName)
			return false, fmt.Errorf("invalid 'vms' structure in ConfigMap %s", configMapName)
		}

		vmEntry, ok := vmsData[vmKey].(map[string]interface{})
		if !ok {
			log.Warnf("VM entry %s missing in ConfigMap %s, cannot update VM status", vmKey, configMapName)
			return false, fmt.Errorf("VM entry %s missing in ConfigMap %s", vmKey, configMapName)
		}

		// Set the VM backup result
		vmEntry["status"] = status
		vmEntry["message"] = message

		// Marshal the updated data and save it back to the ConfigMap
		updatedDataJSON, err := json.Marshal(currentData)
		if err != nil {
			log.Errorf("Failed to marshal updated data for ConfigMap %s: %v", configMapName, err)
			return false, err
		}
		configMap.Data["vmStatusData"] = string(updatedDataJSON)

		// Attempt to update the ConfigMap
		if err := kubeClient.Update(context.TODO(), configMap); err != nil {
			if apierrors.IsConflict(err) {
				log.Warnf("Conflict updating ConfigMap %s, retrying", configMapName)
				return false, nil // Retry on conflicts
			}
			log.Warnf("Failed to update ConfigMap %s: %v", configMapName, err)
			return false, err // Retry on other errors
		}

		log.Infof("Updated ConfigMap %s with %s result for KubeVirt VM %s: %s", configMapName, jobType, vmKey, status)
		printConfigMapData(log, configMap)
		return true, nil
	})

	// Log final error if all retries failed
	if err != nil {
		log.Errorf("Error updating ConfigMap %s for VM %s %s result: %v", configMapName, vmKey, jobType, err)
	}
}

// kubeVirtVMAdditionalResourceOpResult reports the backup result for additional resources related to KubeVirt VMs,
// updating the ConfigMap to reflect success or failure for each additional resource.
func KubeVirtVMAdditionalResourceOpResult(
	log logrus.FieldLogger,
	groupResource schema.GroupResource,
	vmName, vmNamespace string,
	resName, resNamespace string,
	job interface{}, // Accepts either *velerov1api.Backup or *velerov1api.Restore
	success bool,
	err error,
	kubeClient kbClient.Client,
) {
	jobID, jobType, jobNamespace, eerr := extractJobDetails(job)
	if eerr != nil {
		log.Error(eerr)
		return
	}

	configMapName := fmt.Sprintf("%s-%s", jobID, vmProgressSuffix)
	configMapNamespace := jobNamespace
	vmKey := fmt.Sprintf("%s/%s", vmNamespace, vmName) // Unique identifier for the VM in the ConfigMap

	// Set the backup result status and message based on success or failure
	var status, message string
	if success {
		status = "COMPLETED"
		message = fmt.Sprintf("%s of additional resource completed successfully", jobType)
		log.Infof("%s succeeded for additional KubeVirt VM %s/%s -related resource: %s/%s in namespace: %s, %s: %s", jobType, vmNamespace, vmName, groupResource.String(), resName, resNamespace, jobType, jobID)
	} else {
		status = "FAILED"
		message = fmt.Sprintf("%s failed: %v", jobType, err)
		log.Errorf("%s failed for additional KubeVirt VM %s/%s related resource: %s/%s in namespace: %s, %s: %s, reason: %v", jobType, vmNamespace, vmName, groupResource.String(), resName, resNamespace, jobType, jobID, err)
	}

	// Retry loop to update the ConfigMap with the result of the additional resource backup
	updateErr := wait.ExponentialBackoff(wait.Backoff{
		Steps:    5,
		Duration: configMapRetryInterval,
		Factor:   1.5,
		Jitter:   0.1,
	}, func() (bool, error) {
		// Retrieve the ConfigMap
		configMap := &corev1.ConfigMap{}
		if err := kubeClient.Get(context.TODO(), kbClient.ObjectKey{Name: configMapName, Namespace: configMapNamespace}, configMap); err != nil {
			log.Warnf("Error retrieving ConfigMap %s: %v", configMapName, err)
			return false, err // Retry on transient errors
		}

		// Fetch and parse existing data from the ConfigMap
		existingData, ok := configMap.Data["vmStatusData"]
		if !ok {
			log.Warnf("ConfigMap %s missing expected 'vmStatusData' key; unable to update additional resource status", configMapName)
			return false, fmt.Errorf("missing 'vmStatusData' in ConfigMap %s", configMapName)
		}

		var currentData map[string]interface{}
		if err := json.Unmarshal([]byte(existingData), &currentData); err != nil {
			log.Errorf("Failed to unmarshal existing data in ConfigMap %s: %v", configMapName, err)
			return false, err // Unrecoverable error
		}

		// Access or initialize the VM entry in the ConfigMap
		vmsData, ok := currentData["vms"].(map[string]interface{})
		if !ok {
			log.Warnf("ConfigMap %s has an invalid 'vms' structure; cannot update additional resource status", configMapName)
			return false, fmt.Errorf("invalid 'vms' structure in ConfigMap %s", configMapName)
		}

		vmEntry, ok := vmsData[vmKey].(map[string]interface{})
		if !ok {
			log.Warnf("VM entry %s missing in ConfigMap %s, cannot update additional resource status", vmKey, configMapName)
			return false, fmt.Errorf("VM entry %s missing in ConfigMap %s", vmKey, configMapName)
		}

		// Check if the specific additional resource already exists
		additionalResources, ok := vmEntry["additionalResources"].([]interface{})
		if !ok {
			additionalResources = []interface{}{}
		}

		// Update or append the additional resource status
		resourceFound := false
		for _, res := range additionalResources {
			if resource, ok := res.(map[string]interface{}); ok && resource["resourceName"] == fmt.Sprintf("%s/%s", resNamespace, resName) && resource["resourceType"] == groupResource.String() {
				// Update existing resource status and message
				resource["status"] = status
				resource["message"] = message
				resourceFound = true
				break
			}
		}

		// Add new resource entry if it wasn't found
		if !resourceFound {
			additionalResources = append(additionalResources, map[string]interface{}{
				"resourceType": groupResource.String(),
				"resourceName": fmt.Sprintf("%s/%s", resNamespace, resName),
				"status":       status,
				"message":      message,
			})
		}

		// Update the VM entry with the new additional resources list
		vmEntry["additionalResources"] = additionalResources
		vmsData[vmKey] = vmEntry
		currentData["vms"] = vmsData

		// Marshal the updated ConfigMap data
		updatedDataJSON, err := json.Marshal(currentData)
		if err != nil {
			log.Errorf("Failed to marshal updated data for ConfigMap %s: %v", configMapName, err)
			return false, err
		}
		configMap.Data["vmStatusData"] = string(updatedDataJSON)

		// Attempt to update the ConfigMap
		if err := kubeClient.Update(context.TODO(), configMap); err != nil {
			if apierrors.IsConflict(err) {
				log.Warnf("Conflict updating ConfigMap %s, retrying", configMapName)
				return false, nil // Retry on conflicts
			}
			log.Warnf("Failed to update ConfigMap %s: %v", configMapName, err)
			return false, err // Retry on other errors
		}

		log.Infof("Updated ConfigMap %s with %s result for additional KubeVirt VM resource %s/%s: %s", configMapName, jobType, groupResource.String(), resName, status)
		printConfigMapData(log, configMap)
		return true, nil
	})

	// Log final error if all retries failed
	if updateErr != nil {
		log.Errorf("Error updating ConfigMap %s for additional resource %s/%s %s result: %v", configMapName, groupResource.String(), resName, jobType, updateErr)
	}
}

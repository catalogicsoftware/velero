package catalogic

type PvcInfo struct {
	Namespace              string `json:"namespace,omitempty"`
	DataTransferSnapshotId string `json:"data_transfer_snapshot_id,omitempty"`
	SnapshotId             string `json:"snapshot_id,omitempty"`
	VolumeId               string `json:"volume_id,omitempty"`
	Name                   string `json:"name,omitempty"`
	PvName                 string `json:"pv_name,omitempty"`
	SnapshotType           string `json:"snapshot_type,omitempty"`
}
type PvcSnapshotProgressData struct {
	JobId            string   `json:"job_id,omitempty"`
	Message          string   `json:"message,omitempty"`
	State            string   `json:"state,omitempty"`
	SnapshotProgress int32    `json:"snapshot_progress,omitempty"`
	Pvc              *PvcInfo `json:"pvc,omitempty"`
}

type PluginConfig struct {
	SnapshotWherePossible       bool
	SnapshotLonghorn            bool
	CsiSnapshotTimeout          int
	StorageClassBackupMethodMap map[string]string
}

const (
	CloudCasaNamespace = "cloudcasa-io"

	// Prefix name of configmap used to to report progress of snapshot
	SnapshotProgressUpdateConfigMapPrefix = "cloudcasa-io-snapshot-updater-"

	// TimeFormat must match the kubeagent's sibling layout exactly. Note the day
	// token is "02": using "06" here renders the two-digit YEAR in the day
	// position (e.g. day "26" throughout 2026), which previously masqueraded as
	// clock skew in customer logs.
	TimeFormat = "2006-01-02 15:04:05 UTC: "

	// VeleroCsiPluginConfigMapPrefix is the name prefix of the configmap used to store configuration parameters
	VeleroCsiPluginConfigMapPrefix = "cloudcasa-io-velero-csi-plugin-"
)

const (
	// These annotation are set for Azure File PVCs. They must match values in azurefilemover/utils/utils.go
	// Our Azure File mover reads these values and picks the right file share and snapshot
	// based on them.
	StaticAzureFilePvcAnnotation                = "cloudcasa-source-azure-file-share-name"
	StaticAzureFilePvcSecretNameAnnotation      = "cloudcasa-source-azure-file-share-secret-name"
	StaticAzureFilePvcSecretNamespaceAnnotation = "cloudcasa-source-azure-file-share-secret-namespace"
)

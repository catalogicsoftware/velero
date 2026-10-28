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

//nolint:gosec // Internal call. No need to check.
package keys

import (
	corev1api "k8s.io/api/core/v1"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"

	"github.com/vmware-tanzu/velero/pkg/builder"
)

const (
	credentialsSecretName = "velero-repo-credentials"
	credentialsKey        = "repository-password"
)

// EnsureCommonRepositoryKey does nothing: CloudCasa does not use Velero's
// backup repositories, so their shared key secret is never created.
func EnsureCommonRepositoryKey(_ corev1client.SecretsGetter, _ string) error {
	return nil
}

// RepoKeySelector returns the SecretKeySelector which can be used to fetch
// the backup repository key.
func RepoKeySelector() *corev1api.SecretKeySelector {
	// For now, all backup repos share the same key so we don't need the repoName to fetch it.
	// When we move to full-backup encryption, we'll likely have a separate key per backup repo
	// (all within the Velero server's namespace) so RepoKeySelector will need to select the key
	// for that repo.
	return builder.ForSecretKeySelector(credentialsSecretName, credentialsKey).Result()
}

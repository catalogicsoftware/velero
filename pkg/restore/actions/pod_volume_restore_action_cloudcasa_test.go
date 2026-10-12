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

package actions

import (
	"context"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	veleroimage "github.com/vmware-tanzu/velero/internal/velero"
	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/builder"
	"github.com/vmware-tanzu/velero/pkg/plugin/velero"
	velerotest "github.com/vmware-tanzu/velero/pkg/test"
)

// A CloudCasa engine runs as a bare pod, with no "velero" Deployment. Every
// restore builds this action, so building it must not fail, and a pod without
// pod volume backups must pass through unchanged.
func TestPodVolumeRestoreActionWithoutTheVeleroDeployment(t *testing.T) {
	forbidden := apierrors.NewForbidden(schema.GroupResource{Group: "apps", Resource: "deployments"}, "velero", nil)
	tests := []struct {
		name     string
		crClient ctrlclient.Client
	}{
		{
			name:     "no deployment",
			crClient: velerotest.NewFakeControllerRuntimeClient(t),
		},
		{
			name: "deployment not readable",
			crClient: ctrlfake.NewClientBuilder().WithScheme(velerotest.NewFakeControllerRuntimeClient(t).Scheme()).
				WithInterceptorFuncs(interceptor.Funcs{
					Get: func(ctx context.Context, client ctrlclient.WithWatch, key ctrlclient.ObjectKey, obj ctrlclient.Object, opts ...ctrlclient.GetOption) error {
						return forbidden
					},
				}).Build(),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a, err := NewPodVolumeRestoreAction(logrus.StandardLogger(), fake.NewSimpleClientset().CoreV1().ConfigMaps("cloudcasa-io"),
				tc.crClient, "cloudcasa-io")
			require.NoError(t, err)
			assert.Equal(t, veleroimage.DefaultVeleroImage(), a.veleroImage)

			pod := builder.ForPod("p6-app", "app").
				Volumes(builder.ForVolume("data").PersistentVolumeClaimSource("data").Result()).Result()
			item, err := runtime.DefaultUnstructuredConverter.ToUnstructured(pod)
			require.NoError(t, err)
			res, err := a.Execute(&velero.RestoreItemActionExecuteInput{
				Item:           &unstructured.Unstructured{Object: item},
				ItemFromBackup: &unstructured.Unstructured{Object: item},
				Restore:        builder.ForRestore("cloudcasa-io", "r-1").Backup("b-1").Phase(velerov1api.RestorePhaseInProgress).Result(),
			})
			require.NoError(t, err)
			assert.Equal(t, item, res.UpdatedItem.(*unstructured.Unstructured).Object)
		})
	}
}

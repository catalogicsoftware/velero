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

package csi

import (
	"testing"

	volumegroupsnapshotv1beta2 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumegroupsnapshot/v1beta2"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/vmware-tanzu/velero/pkg/kuberesource"
)

// The snapshot controller writes the owner reference of a member VolumeSnapshot
// in the API version it serves: v1 since external-snapshotter v8.6.0, v1beta2
// or v1beta1 before. The member belongs to the group whatever that version is.
func TestHasOwnerReferenceAcceptsEveryGroupSnapshotAPIVersion(t *testing.T) {
	vgs := &volumegroupsnapshotv1beta2.VolumeGroupSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "velero-demo-6tcs4", Namespace: "ns", UID: "de0f9a41"},
	}
	for _, version := range []string{"v1", "v1beta2", "v1beta1"} {
		t.Run(version, func(t *testing.T) {
			obj := &metav1.ObjectMeta{OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "groupsnapshot.storage.k8s.io/" + version,
				Kind:       kuberesource.VGSKind,
				Name:       vgs.Name,
				UID:        vgs.UID,
			}}}
			assert.True(t, hasOwnerReference(obj, vgs))
		})
	}
}

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

package instance

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

func labeled(id string) client.Object {
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "obj"}}
	if id != "" {
		cm.Labels = map[string]string{LabelKey: id}
	}
	return cm
}

func TestFromEnv(t *testing.T) {
	t.Setenv(EnvName, "  job-a ")
	s := FromEnv()
	assert.Equal(t, "job-a", s.ID())
	assert.True(t, s.Instanced())

	t.Setenv(EnvName, "")
	s = FromEnv()
	assert.Equal(t, "", s.ID())
	assert.False(t, s.Instanced())
}

func TestOwns(t *testing.T) {
	instanced := New("job-a")
	shared := New("")

	assert.True(t, instanced.Owns(labeled("job-a")))
	assert.False(t, instanced.Owns(labeled("job-b")))
	assert.False(t, instanced.Owns(labeled("")))
	assert.False(t, instanced.Owns(nil))

	assert.True(t, shared.Owns(labeled("")))
	assert.False(t, shared.Owns(labeled("job-a")))
	assert.False(t, shared.Owns(nil))
}

func TestSelectorMatchesOwns(t *testing.T) {
	cases := []struct {
		scope  Scope
		object client.Object
	}{
		{New("job-a"), labeled("job-a")},
		{New("job-a"), labeled("job-b")},
		{New("job-a"), labeled("")},
		{New(""), labeled("")},
		{New(""), labeled("job-a")},
	}
	for _, tc := range cases {
		matches := tc.scope.Selector().Matches(labels.Set(tc.object.GetLabels()))
		assert.Equal(t, tc.scope.Owns(tc.object), matches, "scope %s object labels %v", tc.scope, tc.object.GetLabels())
	}

	assert.Equal(t, LabelKey+"=job-a", New("job-a").Selector().String())
	assert.Equal(t, "!"+LabelKey, New("").Selector().String())
}

func TestPredicateCoversAllEvents(t *testing.T) {
	p := New("job-a").Predicate()
	own, foreign := labeled("job-a"), labeled("job-b")

	assert.True(t, p.Create(event.CreateEvent{Object: own}))
	assert.False(t, p.Create(event.CreateEvent{Object: foreign}))
	assert.True(t, p.Update(event.UpdateEvent{ObjectOld: foreign, ObjectNew: own}))
	assert.False(t, p.Update(event.UpdateEvent{ObjectOld: own, ObjectNew: foreign}))
	assert.True(t, p.Delete(event.DeleteEvent{Object: own}))
	assert.False(t, p.Delete(event.DeleteEvent{Object: foreign}))
	assert.True(t, p.Generic(event.GenericEvent{Object: own}))
	assert.False(t, p.Generic(event.GenericEvent{Object: foreign}))
}

func TestByObject(t *testing.T) {
	cm, secret := &corev1.ConfigMap{}, &corev1.Secret{}
	byObject := New("job-a").ByObject("velero", cm, secret)
	require.Len(t, byObject, 2)
	assert.Equal(t, LabelKey+"=job-a", byObject[cm].Label.String())
	assert.Equal(t, LabelKey+"=job-a", byObject[secret].Label.String())
	// The per-namespace selector is what controller-runtime v0.17 actually honours.
	require.Contains(t, byObject[cm].Namespaces, "velero")
	assert.Equal(t, LabelKey+"=job-a", byObject[cm].Namespaces["velero"].LabelSelector.String())
	assert.Equal(t, "!"+LabelKey, New("").ByObject("velero", cm)[cm].Namespaces["velero"].LabelSelector.String())
}

func TestListOption(t *testing.T) {
	opts := &client.ListOptions{}
	New("job-a").ListOption().ApplyToList(opts)
	assert.Equal(t, LabelKey+"=job-a", opts.LabelSelector.String())
}

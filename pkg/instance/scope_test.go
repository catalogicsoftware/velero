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
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes/fake"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"

	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
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

func TestSelectorForSharedKindIgnoresTheInstance(t *testing.T) {
	labeled := labels.Set{LabelKey: "job-1"}
	other := labels.Set{LabelKey: "job-2"}
	unlabeled := labels.Set{}

	// A shared kind carries no instance label, so every engine — instanced or
	// not — selects exactly the unlabelled objects.
	for _, scope := range []Scope{New(""), New("job-1")} {
		shared := scope.SelectorFor(KindShared)
		if !shared.Matches(unlabeled) {
			t.Fatalf("%s: shared selector must match an unlabelled object", scope)
		}
		if shared.Matches(labeled) || shared.Matches(other) {
			t.Fatalf("%s: shared selector must not match a labelled object", scope)
		}
	}

	// An owned kind still isolates one instance from another.
	owned := New("job-1").SelectorFor(KindOwned)
	if !owned.Matches(labeled) {
		t.Fatal("owned selector must match this instance's object")
	}
	if owned.Matches(other) || owned.Matches(unlabeled) {
		t.Fatal("owned selector must match only this instance's object")
	}
}

func TestByObjectModesAppliesThePerKindSelector(t *testing.T) {
	scope := New("job-1")
	shared := &velerov1api.BackupStorageLocation{}
	owned := &velerov1api.Restore{}

	byObject := scope.ByObjectModes("cloudcasa-io", map[client.Object]KindMode{
		shared: KindShared,
		owned:  KindOwned,
	})

	if len(byObject) != 2 {
		t.Fatalf("expected an entry per kind, got %d", len(byObject))
	}
	for obj, mode := range map[client.Object]KindMode{shared: KindShared, owned: KindOwned} {
		entry, ok := byObject[obj]
		if !ok {
			t.Fatalf("no cache entry for %T", obj)
		}
		want := scope.SelectorFor(mode).String()
		if entry.Label.String() != want {
			t.Fatalf("%T: label selector %q, want %q", obj, entry.Label, want)
		}
		// v0.17 ignores ByObject.Label once DefaultNamespaces is set, so the
		// per-namespace selector has to carry the same restriction.
		ns, ok := entry.Namespaces["cloudcasa-io"]
		if !ok {
			t.Fatalf("%T: no per-namespace config", obj)
		}
		if ns.LabelSelector.String() != want {
			t.Fatalf("%T: namespace selector %q, want %q", obj, ns.LabelSelector, want)
		}
	}
}

func TestAnnouncePodCapabilityStampsThePod(t *testing.T) {
	t.Setenv(PodNameEnv, "cc-helper-job-1")
	t.Setenv(PodNamespaceEnv, "cloudcasa-io")

	client := fake.NewSimpleClientset(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "cc-helper-job-1", Namespace: "cloudcasa-io"},
	})

	if err := AnnouncePodCapability(context.Background(), client, "ignored", New("job-1")); err != nil {
		t.Fatalf("announce: %v", err)
	}

	pod, err := client.CoreV1().Pods("cloudcasa-io").Get(context.Background(), "cc-helper-job-1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get pod: %v", err)
	}
	if got := pod.Annotations[CapabilityAnnotation]; got != "true" {
		t.Fatalf("capability annotation %q, want \"true\"", got)
	}
	if got := pod.Annotations[InstanceAnnotation]; got != "job-1" {
		t.Fatalf("instance annotation %q, want \"job-1\"", got)
	}
}

func TestAnnouncePodCapabilityNeedsThePodName(t *testing.T) {
	t.Setenv(PodNameEnv, "")
	if err := AnnouncePodCapability(context.Background(), fake.NewSimpleClientset(), "cloudcasa-io", New("")); err == nil {
		t.Fatal("expected an error when the pod name is unknown")
	}
}

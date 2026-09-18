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

// Package instance scopes an engine process to the custom resources of one
// CloudCasa job, so several engines can share a namespace without
// processing each other's work.
package instance

import (
	"context"
	"fmt"
	"os"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

const (
	// LabelKey carries the job ID that binds a CR to one engine instance.
	LabelKey = "cloudcasa.io/helper-instance"

	// EnvName is the environment variable holding this engine's instance ID.
	EnvName = "CC_HELPER_INSTANCE"
)

// absentSelector matches objects that carry no instance label at all.
var absentSelector = func() labels.Selector {
	requirement, err := labels.NewRequirement(LabelKey, selection.DoesNotExist, nil)
	if err != nil {
		panic(fmt.Sprintf("instance: invalid label requirement: %v", err))
	}
	return labels.NewSelector().Add(*requirement)
}()

// Scope identifies which CRs an engine owns. An empty ID is the shared
// engine, which owns only CRs without the instance label.
type Scope struct {
	id string
}

// FromEnv builds the scope from EnvName.
func FromEnv() Scope {
	return New(os.Getenv(EnvName))
}

// New builds a scope for the given instance ID; empty means shared engine.
func New(id string) Scope {
	return Scope{id: strings.TrimSpace(id)}
}

// ID returns the instance ID, empty for the shared engine.
func (s Scope) ID() string {
	return s.id
}

// Instanced reports whether this engine serves a single job.
func (s Scope) Instanced() bool {
	return s.id != ""
}

func (s Scope) String() string {
	if s.id == "" {
		return "shared engine (CRs without " + LabelKey + ")"
	}
	return "instance " + s.id + " (" + LabelKey + "=" + s.id + ")"
}

// Owns reports whether the object belongs to this engine.
func (s Scope) Owns(obj client.Object) bool {
	if obj == nil {
		return false
	}
	value, labeled := obj.GetLabels()[LabelKey]
	if s.id == "" {
		return !labeled
	}
	return labeled && value == s.id
}

// Selector is the label selector that matches exactly the objects Owns accepts.
func (s Scope) Selector() labels.Selector {
	if s.id == "" {
		return absentSelector
	}
	return labels.SelectorFromSet(labels.Set{LabelKey: s.id})
}

// ListOption applies Selector to a client.List call.
func (s Scope) ListOption() client.ListOption {
	return client.MatchingLabelsSelector{Selector: s.Selector()}
}

// Predicate filters every event type through Owns.
func (s Scope) Predicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc:  func(e event.CreateEvent) bool { return s.Owns(e.Object) },
		DeleteFunc:  func(e event.DeleteEvent) bool { return s.Owns(e.Object) },
		UpdateFunc:  func(e event.UpdateEvent) bool { return s.Owns(e.ObjectNew) },
		GenericFunc: func(e event.GenericEvent) bool { return s.Owns(e.Object) },
	}
}

// KindMode says how an engine treats one resource kind.
type KindMode int

const (
	// KindOwned is a kind the engine reconciles, so it sees only the objects
	// bound to its own instance.
	KindOwned KindMode = iota
	// KindShared is a kind the engine only reads. Resources describing a
	// recovery point rather than a job — a storage location, a volume
	// snapshot location, the stub backup a restore or delete refers to — are
	// shared by every job that reads that recovery point and therefore carry
	// no instance label at all. An engine sees exactly the unlabelled ones,
	// which is what the shared engine already sees.
	KindShared
)

// SelectorFor is the label selector for a kind held in the given mode.
func (s Scope) SelectorFor(mode KindMode) labels.Selector {
	if mode == KindShared {
		return absentSelector
	}
	return s.Selector()
}

// ByObject returns cache options that restrict the informers of the given
// namespaced kinds, in namespace, to this scope, so cached reads never
// return foreign CRs. Every kind is treated as owned; use ByObjectModes to
// mark the ones this engine only reads. The selector is set per namespace
// explicitly because controller-runtime v0.17 ignores ByObject.Label once
// DefaultNamespaces is configured.
func (s Scope) ByObject(namespace string, objects ...client.Object) map[client.Object]cache.ByObject {
	modes := make(map[client.Object]KindMode, len(objects))
	for _, obj := range objects {
		modes[obj] = KindOwned
	}
	return s.ByObjectModes(namespace, modes)
}

// ByObjectModes is ByObject with a mode chosen per kind.
func (s Scope) ByObjectModes(namespace string, modes map[client.Object]KindMode) map[client.Object]cache.ByObject {
	byObject := make(map[client.Object]cache.ByObject, len(modes))
	for obj, mode := range modes {
		selector := s.SelectorFor(mode)
		byObject[obj] = cache.ByObject{
			Label:      selector,
			Namespaces: map[string]cache.Config{namespace: {LabelSelector: selector}},
		}
	}
	return byObject
}

const (
	// CapabilityAnnotation is set by this engine on its own pod at startup, so
	// the agent can tell an engine that understands instance scoping from one
	// built before it existed. An image without this code never writes it, and
	// that absence is the whole signal — no version string to parse.
	//
	// The agent mirrors these two strings in amdslib/types, which is a
	// separate module. Keep them identical.
	CapabilityAnnotation = "cloudcasa.io/engine-instance-scope"

	// InstanceAnnotation records the scope this engine resolved, for support.
	InstanceAnnotation = "cloudcasa.io/engine-instance-id"

	// PodNameEnv names the pod this engine runs in.
	PodNameEnv = "MY_POD_NAME"

	// PodNamespaceEnv names its namespace.
	PodNamespaceEnv = "MY_POD_NAMESPACE"
)

// AnnouncePodCapability records on this engine's own pod that it understands
// instance scoping, and which scope it resolved.
//
// Best effort by design: an engine that cannot annotate itself should still
// serve its job. The agent treats a missing annotation as an engine too old to
// isolate one job's resources from another's, and refuses to hand it work
// before creating anything bound to an instance.
func AnnouncePodCapability(ctx context.Context, client kubernetes.Interface, namespace string, scope Scope) error {
	podName := os.Getenv(PodNameEnv)
	if podName == "" {
		return fmt.Errorf("instance: %s is not set, cannot announce the engine capability", PodNameEnv)
	}
	if fromEnv := os.Getenv(PodNamespaceEnv); fromEnv != "" {
		namespace = fromEnv
	}

	patch := fmt.Sprintf(
		`{"metadata":{"annotations":{%q:"true",%q:%q}}}`,
		CapabilityAnnotation, InstanceAnnotation, scope.ID())
	_, err := client.CoreV1().Pods(namespace).Patch(
		ctx, podName, types.MergePatchType, []byte(patch), metav1.PatchOptions{})
	if err != nil {
		return fmt.Errorf("instance: could not annotate pod %s/%s: %w", namespace, podName, err)
	}
	return nil
}

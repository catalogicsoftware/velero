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

package controller

import (
	"sync"

	"github.com/sirupsen/logrus"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	"github.com/vmware-tanzu/velero/pkg/instance"
	"github.com/vmware-tanzu/velero/pkg/util/kube"
)

var (
	instanceScopeMu sync.RWMutex
	instanceScope   instance.Scope // zero value: shared engine
)

// SetInstanceScope installs the scope every controller enforces, both as an
// event filter and as a guard at reconcile entry. Call it before
// SetupWithManager.
func SetInstanceScope(scope instance.Scope) {
	instanceScopeMu.Lock()
	defer instanceScopeMu.Unlock()
	instanceScope = scope
}

// InstanceScope returns the scope controllers currently enforce.
func InstanceScope() instance.Scope {
	instanceScopeMu.RLock()
	defer instanceScopeMu.RUnlock()
	return instanceScope
}

// instancePredicate drops events for objects another engine instance owns.
// It consults the scope per event, so it follows SetInstanceScope.
func instancePredicate() predicate.Predicate {
	return kube.NewAllEventPredicate(func(obj client.Object) bool {
		return InstanceScope().Owns(obj)
	})
}

// skipForeign is the second-layer guard: it reports whether the object
// belongs to another engine instance and must not be processed.
func skipForeign(log logrus.FieldLogger, obj client.Object) bool {
	scope := InstanceScope()
	if scope.Owns(obj) {
		return false
	}
	log.WithFields(logrus.Fields{
		"object":         obj.GetNamespace() + "/" + obj.GetName(),
		"objectInstance": obj.GetLabels()[instance.LabelKey],
		"engineScope":    scope.String(),
	}).Info("Object belongs to another engine instance, skipping")
	return true
}

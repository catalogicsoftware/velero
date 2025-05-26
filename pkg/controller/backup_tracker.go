/*
Copyright 2018 the Velero contributors.

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
	"context"
	"fmt"
	"sync"

	"k8s.io/apimachinery/pkg/util/sets"
)

// BackupTracker keeps track of in-progress backups.
type BackupTracker interface {
	// Add informs the tracker that a backup is in progress.
	Add(ns, name string)
	// Delete informs the tracker that a backup is no longer in progress.
	Delete(ns, name string)
	// Contains returns true if the tracker is tracking the backup.
	Contains(ns, name string) bool
	Cancel(ns, name string)
	GetContext(ns, name string) context.Context
}

type backupContext struct {
	ctx    context.Context
	cancel context.CancelFunc
}

type backupTracker struct {
	lock     sync.RWMutex
	backups  sets.Set[string]
	contexts map[string]*backupContext
}

// NewBackupTracker returns a new BackupTracker.
func NewBackupTracker() BackupTracker {
	return &backupTracker{
		backups:  sets.New[string](),
		contexts: make(map[string]*backupContext),
	}
}

func (bt *backupTracker) Add(ns, name string) {
	bt.lock.Lock()
	defer bt.lock.Unlock()
	key := backupTrackerKey(ns, name)

	if bt.contexts == nil {
		bt.contexts = make(map[string]*backupContext)
	}
	bt.backups.Insert(key)

	ctx, cancelFunc := context.WithCancel(context.Background())
	bt.contexts[key] = &backupContext{
		ctx:    ctx,
		cancel: cancelFunc,
	}
}

func (bt *backupTracker) Contains(ns, name string) bool {
	bt.lock.RLock()
	defer bt.lock.RUnlock()

	return bt.backups.Has(backupTrackerKey(ns, name))
}

func (bt *backupTracker) Delete(ns, name string) {
	bt.lock.Lock()
	defer bt.lock.Unlock()
	key := backupTrackerKey(ns, name)

	bt.backups.Delete(key)

	// Safely cancel and delete from contexts map
	if ctx, ok := bt.contexts[key]; ok && ctx.cancel != nil {
		ctx.cancel()
	}
	delete(bt.contexts, key)
}

func (bt *backupTracker) Cancel(ns, name string) {
	bt.lock.Lock()
	defer bt.lock.Unlock()
	key := backupTrackerKey(ns, name)

	if ctx, ok := bt.contexts[key]; ok && ctx.cancel != nil {
		ctx.cancel()
	}
}

func backupTrackerKey(ns, name string) string {
	return fmt.Sprintf("%s/%s", ns, name)
}

func (bt *backupTracker) GetContext(ns, name string) context.Context {
	bt.lock.Lock()
	defer bt.lock.Unlock()

	key := backupTrackerKey(ns, name)
	if ctx, ok := bt.contexts[key]; ok {
		return ctx.ctx
	}
	return nil
}

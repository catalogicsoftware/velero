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
	"context"
	"crypto/sha256"
	"encoding/hex"
	"slices"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	volumegroupsnapshotv1beta2 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumegroupsnapshot/v1beta2"
	snapshotv1api "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	"github.com/sirupsen/logrus"
	corev1api "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/retry"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"

	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/label"
)

// groupContentFinalizer makes the snapshotter delete a group content's group
// snapshot on the storage before the content goes.
const groupContentFinalizer = "groupsnapshot.storage.kubernetes.io/volumegroupsnapshotcontent-bound-protection"

var groupStubPollInterval = time.Second

// GroupMemberSnapshotHandles returns, sorted, the snapshot handles of the
// contents in the cluster that belong to group snapshot groupHandle.
func GroupMemberSnapshotHandles(ctx context.Context, client crclient.Client, groupHandle string) ([]string, error) {
	vscList := new(snapshotv1api.VolumeSnapshotContentList)
	if err := client.List(ctx, vscList); err != nil {
		return nil, errors.Wrap(err, "error listing VolumeSnapshotContents")
	}

	handles := []string{}
	for _, vsc := range vscList.Items {
		if vsc.Status == nil || vsc.Status.VolumeGroupSnapshotHandle == nil ||
			*vsc.Status.VolumeGroupSnapshotHandle != groupHandle {
			continue
		}
		handle := vsc.Status.SnapshotHandle
		if handle == nil {
			handle = vsc.Spec.Source.SnapshotHandle
		}
		if handle != nil && !slices.Contains(handles, *handle) {
			handles = append(handles, *handle)
		}
	}
	slices.Sort(handles)
	return handles, nil
}

// EnsureGroupSnapshotDeleteStub makes sure the deletion of backupName has a
// stub group content for the group of member content vsc. The stub names all
// members (a driver refuses a partial group) and is deleted by
// DeleteGroupSnapshotStubs once every item of the backup was handled.
func EnsureGroupSnapshotDeleteStub(
	ctx context.Context,
	client crclient.Client,
	backupName string,
	vsc *snapshotv1api.VolumeSnapshotContent,
	members []string,
) error {
	groupHandle := *vsc.Status.VolumeGroupSnapshotHandle
	sum := sha256.Sum256([]byte(backupName + groupHandle))
	name := hex.EncodeToString(sum[:])
	refNamespace := vsc.Spec.VolumeSnapshotRef.Namespace
	if refNamespace == "" {
		refNamespace = "default"
	}

	stub := &volumegroupsnapshotv1beta2.VolumeGroupSnapshotContent{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Labels:      map[string]string{velerov1api.BackupNameLabel: label.GetValidName(backupName)},
			Annotations: groupDeletionSecretAnnotations(ctx, client, vsc.Spec.Driver),
			Finalizers:  []string{groupContentFinalizer},
		},
		Spec: volumegroupsnapshotv1beta2.VolumeGroupSnapshotContentSpec{
			DeletionPolicy: snapshotv1api.VolumeSnapshotContentDelete,
			Driver:         vsc.Spec.Driver,
			Source: volumegroupsnapshotv1beta2.VolumeGroupSnapshotContentSource{
				GroupSnapshotHandles: &volumegroupsnapshotv1beta2.GroupSnapshotHandles{
					VolumeGroupSnapshotHandle: groupHandle,
					VolumeSnapshotHandles:     members,
				},
			},
			VolumeGroupSnapshotRef: corev1api.ObjectReference{
				Name:      "stub-vgs-" + name[:8],
				Namespace: refNamespace,
			},
		},
	}
	if err := client.Create(ctx, stub); err != nil && !apierrors.IsAlreadyExists(err) {
		return errors.Wrapf(err, "error creating the stub VolumeGroupSnapshotContent %s for group snapshot %s", name, groupHandle)
	}
	return nil
}

// groupDeletionSecretAnnotations returns the deletion secret annotations a
// group content of driver needs, from the driver's group snapshot class.
func groupDeletionSecretAnnotations(ctx context.Context, client crclient.Client, driver string) map[string]string {
	annotations := map[string]string{}
	classes := &volumegroupsnapshotv1beta2.VolumeGroupSnapshotClassList{}
	if err := client.List(ctx, classes); err != nil {
		return annotations
	}
	for _, class := range classes.Items {
		if class.Driver != driver {
			continue
		}
		if name, ok := class.Parameters["csi.storage.k8s.io/group-snapshotter-secret-name"]; ok {
			annotations["groupsnapshot.storage.kubernetes.io/deletion-secret-name"] = name
		}
		if namespace, ok := class.Parameters["csi.storage.k8s.io/group-snapshotter-secret-namespace"]; ok {
			annotations["groupsnapshot.storage.kubernetes.io/deletion-secret-namespace"] = namespace
		}
		break
	}
	return annotations
}

// DeleteGroupSnapshotStubs deletes the group snapshots on the storage that the
// deletion of backup left to stub group contents, and the stubs.
func DeleteGroupSnapshotStubs(ctx context.Context, client crclient.Client, backup *velerov1api.Backup, log logrus.FieldLogger) []error {
	stubs := &volumegroupsnapshotv1beta2.VolumeGroupSnapshotContentList{}
	if err := client.List(ctx, stubs, crclient.MatchingLabels{velerov1api.BackupNameLabel: label.GetValidName(backup.Name)}); err != nil {
		if meta.IsNoMatchError(err) || apierrors.IsNotFound(err) {
			return nil
		}
		return []error{errors.Wrap(err, "error listing the stub VolumeGroupSnapshotContents")}
	}

	timeout, err := time.ParseDuration(backup.Annotations[velerov1api.ResourceTimeoutAnnotation])
	if err != nil {
		timeout = 10 * time.Minute
	}
	var errs []error
	for i := range stubs.Items {
		stub := &stubs.Items[i]
		if stub.Spec.Source.GroupSnapshotHandles == nil || stub.Spec.DeletionPolicy != snapshotv1api.VolumeSnapshotContentDelete {
			continue
		}
		if err := deleteGroupThroughStub(ctx, client, stub, timeout, log); err != nil {
			errs = append(errs, err)
		}
	}
	return errs
}

// deleteGroupThroughStub deletes the stub once the snapshotter has found its
// group on the storage, and waits for the snapshotter to delete the group.
func deleteGroupThroughStub(
	ctx context.Context,
	client crclient.Client,
	stub *volumegroupsnapshotv1beta2.VolumeGroupSnapshotContent,
	timeout time.Duration,
	log logrus.FieldLogger,
) error {
	groupHandle := stub.Spec.Source.GroupSnapshotHandles.VolumeGroupSnapshotHandle
	log = log.WithField("groupSnapshot", groupHandle).WithField("stub", stub.Name)

	var statusErr string
	err := wait.PollUntilContextTimeout(ctx, groupStubPollInterval, timeout, true, func(ctx context.Context) (bool, error) {
		current := &volumegroupsnapshotv1beta2.VolumeGroupSnapshotContent{}
		if err := client.Get(ctx, crclient.ObjectKeyFromObject(stub), current); err != nil {
			return false, err
		}
		if current.Status == nil {
			return false, nil
		}
		if current.Status.VolumeGroupSnapshotHandle != nil {
			return true, nil
		}
		if current.Status.Error != nil && current.Status.Error.Message != nil {
			statusErr = *current.Status.Error.Message
			return true, nil
		}
		return false, nil
	})
	if err == nil && statusErr == "" {
		log.Info("Deleting the group snapshot through its stub VolumeGroupSnapshotContent")
		return waitForStubDeletion(ctx, client, stub, timeout)
	}

	// Nothing will delete this group on the storage: drop the stub.
	if releaseErr := releaseStub(ctx, client, stub); releaseErr != nil {
		log.WithError(releaseErr).Warn("Failed to remove the stub VolumeGroupSnapshotContent")
	}
	if strings.Contains(statusErr, "code = NotFound") {
		log.Infof("The group snapshot is no longer on the storage: %s", statusErr)
		return nil
	}
	if err != nil {
		return errors.Wrapf(err, "group snapshot %s was not deleted: the snapshotter did not find it on the storage", groupHandle)
	}
	return errors.Errorf("group snapshot %s was not deleted: %s", groupHandle, statusErr)
}

func waitForStubDeletion(
	ctx context.Context,
	client crclient.Client,
	stub *volumegroupsnapshotv1beta2.VolumeGroupSnapshotContent,
	timeout time.Duration,
) error {
	groupHandle := stub.Spec.Source.GroupSnapshotHandles.VolumeGroupSnapshotHandle
	if err := client.Delete(ctx, stub); err != nil && !apierrors.IsNotFound(err) {
		return errors.Wrapf(err, "error deleting the stub VolumeGroupSnapshotContent of group snapshot %s", groupHandle)
	}

	var statusErr string
	err := wait.PollUntilContextTimeout(ctx, groupStubPollInterval, timeout, true, func(ctx context.Context) (bool, error) {
		current := &volumegroupsnapshotv1beta2.VolumeGroupSnapshotContent{}
		if err := client.Get(ctx, crclient.ObjectKeyFromObject(stub), current); err != nil {
			if apierrors.IsNotFound(err) {
				return true, nil
			}
			return false, err
		}
		if current.Status != nil && current.Status.Error != nil && current.Status.Error.Message != nil {
			statusErr = *current.Status.Error.Message
		}
		return false, nil
	})
	if err != nil {
		return errors.Wrapf(err, "group snapshot %s was not deleted from the storage in %s: %s", groupHandle, timeout, statusErr)
	}
	return nil
}

// releaseStub removes the stub without deleting its group snapshot.
func releaseStub(ctx context.Context, client crclient.Client, stub *volumegroupsnapshotv1beta2.VolumeGroupSnapshotContent) error {
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		current := &volumegroupsnapshotv1beta2.VolumeGroupSnapshotContent{}
		if err := client.Get(ctx, crclient.ObjectKeyFromObject(stub), current); err != nil {
			return err
		}
		if !slices.Contains(current.Finalizers, groupContentFinalizer) {
			return nil
		}
		current.Finalizers = slices.DeleteFunc(current.Finalizers, func(f string) bool { return f == groupContentFinalizer })
		return client.Update(ctx, current)
	})
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	if err := client.Delete(ctx, stub); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

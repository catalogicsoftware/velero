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

	"github.com/sirupsen/logrus"

	"github.com/vmware-tanzu/velero/internal/catalogic"
)

// Unit tests run outside a cluster, where the plugin ConfigMap cannot be read.
// By default they get the configuration of a plain snapshot backup.
func init() {
	getPluginConfig = func(string, logrus.FieldLogger) (*catalogic.PluginConfig, error) {
		return &catalogic.PluginConfig{SnapshotWherePossible: true}, nil
	}
}

// withPluginConfig gives one test its own plugin configuration.
func withPluginConfig(t *testing.T, config *catalogic.PluginConfig) {
	t.Helper()
	previous := getPluginConfig
	getPluginConfig = func(string, logrus.FieldLogger) (*catalogic.PluginConfig, error) { return config, nil }
	t.Cleanup(func() { getPluginConfig = previous })
}

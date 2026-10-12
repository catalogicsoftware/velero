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

package restore

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/vmware-tanzu/velero/pkg/builder"
	"github.com/vmware-tanzu/velero/pkg/test"
)

// CloudCasa's catalog (amdslib/velerocatalog/veleroregex.go) turns this
// warning into a WARNING status for the object; it needs Kind "name".
var kasAlreadyExistsPattern = regexp.MustCompile(`could not restore, (\w+) "(.+?)"`)

func TestRestoreAlreadyExistsWarningKeepsTheTextKASParses(t *testing.T) {
	h := newHarness(t)
	h.AddItems(t, test.ConfigMaps(builder.ForConfigMap("ns-1", "app-config").Data("k", "cluster").Result()))
	tarball := test.NewTarWriter(t).
		AddItems("configmaps", builder.ForConfigMap("ns-1", "app-config").Data("k", "backup").Result()).
		Done()

	warnings, errs := h.restorer.Restore(&Request{
		Log:                  h.log,
		Restore:              defaultRestore().Result(),
		Backup:               defaultBackup().Result(),
		BackupReader:         tarball,
		DisableInformerCache: true,
	}, nil, nil)

	assertEmptyResults(t, errs)
	require.Len(t, warnings.Namespaces["ns-1"], 1)
	match := kasAlreadyExistsPattern.FindStringSubmatch(warnings.Namespaces["ns-1"][0])
	require.NotNil(t, match, "warning %q", warnings.Namespaces["ns-1"][0])
	assert.Equal(t, []string{"ConfigMap", "app-config"}, match[1:])
}

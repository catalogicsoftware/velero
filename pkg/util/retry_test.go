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

package util

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRetryOnError(t *testing.T) {
	failure := errors.New("not yet")

	t.Run("stops at the first success", func(t *testing.T) {
		calls := 0
		err := RetryOnError(t.Context(), 5, time.Millisecond, logrus.New(), func() error {
			calls++
			if calls < 3 {
				return failure
			}
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, 3, calls)
	})

	t.Run("returns the last error without sleeping after the last try", func(t *testing.T) {
		calls := 0
		start := time.Now()
		err := RetryOnError(t.Context(), 3, 200*time.Millisecond, logrus.New(), func() error {
			calls++
			return failure
		})
		require.ErrorIs(t, err, failure)
		assert.Equal(t, 3, calls)
		assert.Less(t, time.Since(start), 500*time.Millisecond, "two waits, not three")
	})

	t.Run("stops waiting when the context ends", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		calls := 0
		time.AfterFunc(50*time.Millisecond, cancel)
		start := time.Now()
		err := RetryOnError(ctx, 10, time.Second, logrus.New(), func() error {
			calls++
			return failure
		})
		require.ErrorIs(t, err, context.Canceled)
		assert.NotErrorIs(t, err, failure, "a shutdown is not reported as the object's error")
		assert.Equal(t, 1, calls)
		assert.Less(t, time.Since(start), 500*time.Millisecond)
	})
}

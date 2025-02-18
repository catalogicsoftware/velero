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
	"time"

	"github.com/sirupsen/logrus"
)

func Contains(slice []string, key string) bool {
	for _, i := range slice {
		if i == key {
			return true
		}
	}
	return false
}

// RetryOnError calls the retryFunc after specified retryInterval until it won't return an error
// or then the max number of attempts has been reached.
func RetryOnError(ctx context.Context, maxRetries int, retryInterval time.Duration, logger logrus.FieldLogger, retryFunc func() error) error {
	var err error
	for retries := 0; retries < maxRetries; retries++ {
		err = retryFunc()
		if err == nil {
			return nil
		}

		logger.Infof("Error occurred: %v. Retrying (%d/%d)...", err, retries+1, maxRetries)
		time.Sleep(retryInterval)
	}
	return err
}

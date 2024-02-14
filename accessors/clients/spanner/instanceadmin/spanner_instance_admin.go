// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package spinstanceadmin

import (
	"context"
	"fmt"
	"sync"

	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
)

var once sync.Once
var instanceAdminClient *instance.InstanceAdminClient

// This function is declared as a global variable to make it testable. The unit
// tests update this function, acting like a double.
var newInstanceAdminClient = instance.NewInstanceAdminClient

func GetOrCreateClient(ctx context.Context) (*instance.InstanceAdminClient, error) {
	var err error
	if instanceAdminClient == nil {
		once.Do(func() {
			instanceAdminClient, err = newInstanceAdminClient(ctx)
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create spanner instance admin client: %v", err)
		}
		return instanceAdminClient, nil
	}
	return instanceAdminClient, nil
}

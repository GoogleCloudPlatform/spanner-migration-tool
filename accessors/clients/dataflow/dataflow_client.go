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
package dataflowclient

import (
	"context"
	"fmt"
	"sync"

	dataflow "cloud.google.com/go/dataflow/apiv1beta3"
)

var once sync.Once
var dfClient *dataflow.FlexTemplatesClient

// This function is declared as a global variable to make it testable. The unit
// tests edit this function, acting like a double.
var newFlexTemplatesClient = dataflow.NewFlexTemplatesClient

func GetOrCreateClient(ctx context.Context) (*dataflow.FlexTemplatesClient, error) {
	var err error
	if dfClient == nil {
		once.Do(func() {
			dfClient, err = newFlexTemplatesClient(ctx)
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create dataflow client: %v", err)
		}
		return dfClient, nil
	}
	return dfClient, nil
}

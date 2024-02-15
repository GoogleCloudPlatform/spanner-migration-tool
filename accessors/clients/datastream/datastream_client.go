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
package datastreamclient

import (
	"context"
	"fmt"
	"sync"

	datastream "cloud.google.com/go/datastream/apiv1"
)

var once sync.Once
var dsClient *datastream.Client

// This function is declared as a global variable to make it testable. The unit
// tests update this function, acting like a double.
var newClient = datastream.NewClient

func GetOrCreateClient(ctx context.Context) (*datastream.Client, error) {
	var err error
	if dsClient == nil {
		once.Do(func() {
			dsClient, err = newClient(ctx)
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create datastream client: %v", err)
		}
		return dsClient, nil
	}
	return dsClient, nil
}

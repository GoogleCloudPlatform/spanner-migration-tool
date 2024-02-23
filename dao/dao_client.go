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
package dao

import (
	"context"
	"fmt"
	"sync"

	sp "cloud.google.com/go/spanner"
)

var once sync.Once
var spClient *sp.Client

// This function is declared as a global variable to make it testable. The unit
// tests edit this function, acting like a double.
var newClient = sp.NewClient

func GetOrCreateClient(ctx context.Context, dbURI string) (*sp.Client, error) {
	var err error
	if spClient == nil {
		once.Do(func() {
			spClient, err = newClient(ctx, dbURI)
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create spanner database client: %v", err)
		}
		return spClient, nil
	}
	return spClient, nil
}

// The DAO client must be initiated via GetOrCreateClient() once before using GetClient().
func GetClient() *sp.Client {
	return spClient
}

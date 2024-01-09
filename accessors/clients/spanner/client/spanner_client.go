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
package spannerclient

import (
	"context"
	"fmt"
	"sync"

	sp "cloud.google.com/go/spanner"
)

var once sync.Once
var spannerClient *sp.Client

func GetOrCreateClient(ctx context.Context, dbURI string) (*sp.Client, error) {
	var err error
	if spannerClient == nil || spannerClient.DatabaseName() != dbURI {
		once.Do(func() {
			spannerClient, err = sp.NewClient(ctx, dbURI)
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create spanner database client: %v", err)
		}
		return spannerClient, nil
	}
	return spannerClient, nil
}

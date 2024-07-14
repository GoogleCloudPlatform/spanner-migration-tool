// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package internal

import (
	"testing"
	"sync"

	"github.com/stretchr/testify/assert"
)

func TestGenerateIdSuffix(t *testing.T) {
	var wg sync.WaitGroup
	tests := []struct {
		number     int
		expected string
	}{
		{10000, "10000"},
		{20000, "30000"},
		{10000, "40000"},
	}
	for _, tc := range tests {
		for i := 0; i < tc.number; i++ {
			// Increment the WaitGroup counter.
			wg.Add(1)
			go func() {
				GenerateIdSuffix()
				// Decrement the counter when the goroutine completes.
				defer wg.Done()
			}()
		}
		// Wait for all Go routines in the tc to complete
        wg.Wait()
		assert.Equal(t, tc.expected, Cntr.ObjectId)
	}
}
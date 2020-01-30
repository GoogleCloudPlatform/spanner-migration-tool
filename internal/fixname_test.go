// Copyright 2020 Google LLC
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

	"github.com/stretchr/testify/assert"
)

func TestFixName(t *testing.T) {
	tests := []struct {
		name     string
		in       string
		expected string
		changed  bool
	}{
		{"good1", "mytable", "mytable", false},
		{"good2", "mytable_nNm87", "mytable_nNm87", false},
		{"badstart1", "_mytable", "Amytable", true},
		{"badstart2", "8mytable", "Amytable", true},
		{"verybad", "my\nt\ta&$#ble", "my_t_a___ble", true},
	}
	for _, tc := range tests {
		n, c := FixName(tc.in)
		assert.Equal(t, tc.expected, n, tc.name)
		assert.Equal(t, tc.changed, c, tc.name)
	}
}

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

package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
)


func TestSanitizeDefaultValue(t *testing.T) {
	tests := []struct {
		inputString    string
		ty             string
		generated      bool
		expectedString string
	}{
		{"a", "char", true, "a"},
		{"b", "char", false, "'b'"},
		{"c", "int", true, "c"},
		{"d", "int", false, "d"},
		{"_utf8mb4\\'hello world\\'", "char", false, "'hello world'"},
		{"week(_utf8mb4\\'2024-06-20\\',0)", "char", true, "week('2024-06-20',0)"},
		{"_utf8mb4\\'This is a message \\\\nwith a newline\\\\rand a carriage return.\\'", "char", false, "'This is a message \\nwith a newline\\rand a carriage return.'"},
		{"strcmp(_utf8mb4\\'abc\\',_utf8mb4\\'abcd\\')", "char", true, "strcmp('abc','abcd')"},
		{"_utf8mb4\\'John\\\\\\'s Jack\\'", "char", false, "'John\\'s Jack'"},
		{"_utf8mb4\\'This product has\tmultiple features.\\'", "char", false, "'This product has\tmultiple features.'"},
		{"_utf8mb4\\'C:\\\\\\\\Users\\\\\\\\johndoe\\\\\\\\Documents\\\\\\\\myfile.txt\\'", "char", false, "'C:\\\\Users\\\\johndoe\\\\Documents\\\\myfile.txt'"},
	}
	for _, test := range tests {
		result := SanitizeDefaultValue(test.inputString, test.ty, test.generated)
		assert.Equal(t, test.expectedString, result)
	}
}

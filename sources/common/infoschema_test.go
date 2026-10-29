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
	"errors"
	"testing"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/stretchr/testify/assert"
)

func TestSanitizeExpressionsValue(t *testing.T) {
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
		result := SanitizeExpressionsValue(test.inputString, test.ty, test.generated)
		assert.Equal(t, test.expectedString, result)
	}
}

type mockIsolationInfoSchema struct {
	InfoSchema
	isolation string
	err       error
}

func (m mockIsolationInfoSchema) GetDefaultTransactionIsolation() (string, error) {
	return m.isolation, m.err
}

func TestNormalizeSupportedIsolationLevel(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"repeatable read", "repeatable read"},
		{"REPEATABLE-READ", "repeatable read"},
		{"  Repeatable   Read  ", "repeatable read"},
		{"serializable", "serializable"},
		{"SERIALIZABLE", "serializable"},
		{"REPEATABLE_READ", ""},
		{"read committed", ""},
		{"READ-COMMITTED", ""},
		{"read uncommitted", ""},
		{"", ""},
	}
	for _, tc := range tests {
		assert.Equal(t, tc.expected, NormalizeSupportedIsolationLevel(tc.input), tc.input)
	}
}

func TestAnnotateDatabaseOptions(t *testing.T) {
	t.Run("sets normalized supported isolation level when unset", func(t *testing.T) {
		conv := internal.MakeConv()
		annotateDatabaseOptions(conv, mockIsolationInfoSchema{isolation: "REPEATABLE-READ"})
		assert.Equal(t, "repeatable read", conv.DatabaseOptions.DefaultTransactionIsolation)
	})

	t.Run("preserves existing DefaultTransactionIsolation override", func(t *testing.T) {
		conv := internal.MakeConv()
		conv.DatabaseOptions.DefaultTransactionIsolation = "serializable"
		annotateDatabaseOptions(conv, mockIsolationInfoSchema{isolation: "REPEATABLE-READ"})
		assert.Equal(t, "serializable", conv.DatabaseOptions.DefaultTransactionIsolation)
	})

	t.Run("ignores unsupported source isolation level", func(t *testing.T) {
		conv := internal.MakeConv()
		annotateDatabaseOptions(conv, mockIsolationInfoSchema{isolation: "READ-COMMITTED"})
		assert.Equal(t, "", conv.DatabaseOptions.DefaultTransactionIsolation)
	})

	t.Run("leaves unset when provider returns error", func(t *testing.T) {
		conv := internal.MakeConv()
		annotateDatabaseOptions(conv, mockIsolationInfoSchema{err: errors.New("query failed")})
		assert.Equal(t, "", conv.DatabaseOptions.DefaultTransactionIsolation)
	})

	t.Run("leaves unset when InfoSchema does not implement provider", func(t *testing.T) {
		conv := internal.MakeConv()
		var plainInfoSchema InfoSchema
		annotateDatabaseOptions(conv, plainInfoSchema)
		assert.Equal(t, "", conv.DatabaseOptions.DefaultTransactionIsolation)
	})
}

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

package spanner

import (
	"testing"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/schema"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
	"github.com/stretchr/testify/assert"
)

func TestToType(t *testing.T) {
	testCases := []struct {
		name          string
		dataType      string
		expColumnType schema.Type
	}{
		// Scalar inputs.
		{"bool", "BOOL", schema.Type{Name: "BOOL"}},
		{"int", "INT64", schema.Type{Name: "INT64"}},
		{"float", "FLOAT64", schema.Type{Name: "FLOAT64"}},
		{"date", "DATE", schema.Type{Name: "DATE"}},
		{"numeric", "NUMERIC", schema.Type{Name: "NUMERIC"}},
		{"json", "JSON", schema.Type{Name: "JSON"}},
		{"timestamp", "TIMESTAMP", schema.Type{Name: "TIMESTAMP"}},
		{"bytes", "BYTES(100)", schema.Type{Name: "BYTES", Mods: []int64{100}}},
		{"bytes", "BYTES(MAX)", schema.Type{Name: "BYTES", Mods: []int64{ddl.MaxLength}}},
		{"string", "STRING(100)", schema.Type{Name: "STRING", Mods: []int64{100}}},
		{"string", "STRING(MAX)", schema.Type{Name: "STRING", Mods: []int64{ddl.MaxLength}}},
		// Array types.
		{"string_max_arr", "ARRAY<STRING(MAX)>", schema.Type{Name: "STRING", Mods: []int64{ddl.MaxLength}, ArrayBounds: []int64{-1}}},
		{"string_arr", "ARRAY<STRING(100)>", schema.Type{Name: "STRING", Mods: []int64{100}, ArrayBounds: []int64{-1}}},
		{"float_arr", "ARRAY<FLOAT64>", schema.Type{Name: "FLOAT64", ArrayBounds: []int64{-1}}},
		{"numeric_arr", "ARRAY<NUMERIC>", schema.Type{Name: "NUMERIC", ArrayBounds: []int64{-1}}},
	}
	for _, tc := range testCases {
		ty := toType(tc.dataType)
		assert.Equal(t, tc.expColumnType, ty, tc.name)
	}
}

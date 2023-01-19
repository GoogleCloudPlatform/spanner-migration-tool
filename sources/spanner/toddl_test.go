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

	"github.com/cloudspannerecosystem/harbourbridge/common/constants"
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/schema"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
	"github.com/stretchr/testify/assert"
)

func TestToSpannerType(t *testing.T) {
	conv := internal.MakeConv()
	toDDLImpl := ToDdlImpl{}
	toDDLTests := []struct {
		name       string
		pgTarget   bool
		columnType schema.Type
		expDDLType ddl.Type
	}{
		// Exact inputs.
		{"bool", false, schema.Type{Name: "BOOL"}, ddl.Type{Name: ddl.Bool}},
		{"bytes", false, schema.Type{Name: "BYTES", Mods: []int64{100}}, ddl.Type{Name: ddl.Bytes, Len: 100}},
		{"date", false, schema.Type{Name: "DATE"}, ddl.Type{Name: ddl.Date}},
		{"float", false, schema.Type{Name: "FLOAT64"}, ddl.Type{Name: ddl.Float64}},
		{"int", false, schema.Type{Name: "INT64"}, ddl.Type{Name: ddl.Int64}},
		{"json", false, schema.Type{Name: "JSON"}, ddl.Type{Name: ddl.JSON}},
		{"numeric", false, schema.Type{Name: "NUMERIC"}, ddl.Type{Name: ddl.Numeric}},
		{"string", false, schema.Type{Name: "STRING", Mods: []int64{100}}, ddl.Type{Name: ddl.String, Len: 100}},
		{"timestamp", false, schema.Type{Name: "TIMESTAMP"}, ddl.Type{Name: ddl.Timestamp}},
		// PG target.
		{"pg_numeric", true, schema.Type{Name: "PG.NUMERIC"}, ddl.Type{Name: ddl.Numeric}},
		{"pg_json", true, schema.Type{Name: "PG.JSONB"}, ddl.Type{Name: ddl.JSON}},
	}
	for _, tc := range toDDLTests {
		conv.TargetDb = constants.TargetSpanner
		if tc.pgTarget {
			conv.TargetDb = constants.TargetExperimentalPostgres
		}
		ty, err := toDDLImpl.ToSpannerType(conv, "", tc.columnType.Name, tc.columnType.Mods, tc.columnType.ArrayBounds)
		assert.Nil(t, err, tc.name)
		assert.Equal(t, tc.expDDLType, ty, tc.name)
	}
}

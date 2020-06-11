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

package postgres

import (
	"testing"

	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/schema"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
	"github.com/stretchr/testify/assert"
)

// This is just a very basic smoke-test for toSpannerType.
// The real testing of toSpannerType happens in process_test.go
// via the public API ProcessPgDump (see TestProcessPgDump).
func TestToSpannerType(t *testing.T) {
	conv := internal.MakeConv()
	conv.SetSchemaMode()
	name := "test"
	srcSchema := schema.Table{
		Name:     name,
		ColNames: []string{"a", "b", "c", "d", "e", "f"},
		ColDefs: map[string]schema.Column{
			"a": schema.Column{Name: "a", Type: schema.Type{Name: "int8"}},
			"b": schema.Column{Name: "b", Type: schema.Type{Name: "float4"}},
			"c": schema.Column{Name: "c", Type: schema.Type{Name: "bool"}},
			"d": schema.Column{Name: "d", Type: schema.Type{Name: "varchar", Mods: []int64{6}}},
			"e": schema.Column{Name: "e", Type: schema.Type{Name: "numeric"}},
			"f": schema.Column{Name: "f", Type: schema.Type{Name: "timestamptz"}},
		},
		PrimaryKeys: []schema.Key{schema.Key{Column: "a"}}}
	conv.SrcSchema[name] = srcSchema
	assert.Nil(t, schemaToDDL(conv))
	actual := conv.SpSchema[name]
	dropComments(&actual) // Don't test comment.
	expected := ddl.CreateTable{
		Name:     name,
		ColNames: []string{"a", "b", "c", "d", "e", "f"},
		ColDefs: map[string]ddl.ColumnDef{
			"a": ddl.ColumnDef{Name: "a", T: ddl.Int64{}},
			"b": ddl.ColumnDef{Name: "b", T: ddl.Float64{}},
			"c": ddl.ColumnDef{Name: "c", T: ddl.Bool{}},
			"d": ddl.ColumnDef{Name: "d", T: ddl.String{Len: ddl.Int64Length{Value: 6}}},
			"e": ddl.ColumnDef{Name: "e", T: ddl.Float64{}},
			"f": ddl.ColumnDef{Name: "f", T: ddl.Timestamp{}},
		},
		Pks: []ddl.IndexKey{ddl.IndexKey{Col: "a"}},
	}
	assert.Equal(t, expected, actual)
	expectedIssues := map[string][]internal.SchemaIssue{
		"b": []internal.SchemaIssue{internal.Widened},
		"e": []internal.SchemaIssue{internal.Numeric},
	}
	assert.Equal(t, expectedIssues, conv.Issues[name])
}

func dropComments(t *ddl.CreateTable) {
	t.Comment = ""
	for _, c := range t.ColNames {
		cd := t.ColDefs[c]
		cd.Comment = ""
		t.ColDefs[c] = cd
	}
}

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

package dynamodb

import (
	"testing"

	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/schema"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
	"github.com/stretchr/testify/assert"
)

func TestToSpannerType(t *testing.T) {
	conv := internal.MakeConv()
	conv.SetSchemaMode()
	name := "test"
	srcSchema := schema.Table{
		Name:     name,
		ColNames: []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k"},
		ColDefs: map[string]schema.Column{
			"a": {Name: "a", Type: schema.Type{Name: "String"}},
			"b": {Name: "b", Type: schema.Type{Name: "NumberInt"}},
			"c": {Name: "c", Type: schema.Type{Name: "NumberFloat"}},
			"d": {Name: "d", Type: schema.Type{Name: "Bool"}},
			"e": {Name: "e", Type: schema.Type{Name: "Binary"}},
			"f": {Name: "f", Type: schema.Type{Name: "List"}},
			"g": {Name: "g", Type: schema.Type{Name: "Map"}},
			"h": {Name: "h", Type: schema.Type{Name: "StringSet"}},
			"i": {Name: "i", Type: schema.Type{Name: "BinarySet"}},
			"j": {Name: "j", Type: schema.Type{Name: "NumberIntSet"}},
			"k": {Name: "k", Type: schema.Type{Name: "NumberFloatSet"}},
		},
		PrimaryKeys: []schema.Key{{Column: "a"}, {Column: "b"}}}
	conv.SrcSchema[name] = srcSchema
	assert.Nil(t, schemaToDDL(conv))
	actual := conv.SpSchema[name]
	dropComments(&actual) // Don't test comment.
	expected := ddl.CreateTable{
		Name:     name,
		ColNames: []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k"},
		ColDefs: map[string]ddl.ColumnDef{
			"a": {Name: "a", T: ddl.String{Len: ddl.MaxLength{}}},
			"b": {Name: "b", T: ddl.Int64{}},
			"c": {Name: "c", T: ddl.String{Len: ddl.MaxLength{}}},
			"d": {Name: "d", T: ddl.Bool{}},
			"e": {Name: "e", T: ddl.Bytes{Len: ddl.MaxLength{}}},
			"f": {Name: "f", T: ddl.String{Len: ddl.MaxLength{}}},
			"g": {Name: "g", T: ddl.String{Len: ddl.MaxLength{}}},
			"h": {Name: "h", T: ddl.String{Len: ddl.MaxLength{}}, IsArray: true},
			"i": {Name: "i", T: ddl.Bytes{Len: ddl.MaxLength{}}, IsArray: true},
			"j": {Name: "j", T: ddl.Int64{}, IsArray: true},
			"k": {Name: "k", T: ddl.String{Len: ddl.MaxLength{}}, IsArray: true},
		},
		Pks: []ddl.IndexKey{{Col: "a"}, {Col: "b"}},
	}
	assert.Equal(t, expected, actual)
}

func dropComments(t *ddl.CreateTable) {
	t.Comment = ""
	for _, c := range t.ColNames {
		cd := t.ColDefs[c]
		cd.Comment = ""
		t.ColDefs[c] = cd
	}
}

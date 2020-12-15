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

package web

import (
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/schema"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
)

func buildConvPostgres(conv *internal.Conv) {
	conv.SrcSchema = map[string]schema.Table{
		"t1": schema.Table{
			Name:     "t1",
			ColNames: []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p"},
			ColDefs: map[string]schema.Column{
				"a": schema.Column{Name: "a", Type: schema.Type{Name: "int8"}},
				"b": schema.Column{Name: "b", Type: schema.Type{Name: "float4"}},
				"c": schema.Column{Name: "c", Type: schema.Type{Name: "bool"}},
				"d": schema.Column{Name: "d", Type: schema.Type{Name: "varchar", Mods: []int64{6}}},
				"e": schema.Column{Name: "e", Type: schema.Type{Name: "numeric"}},
				"f": schema.Column{Name: "f", Type: schema.Type{Name: "timestamptz"}},
				"g": schema.Column{Name: "g", Type: schema.Type{Name: "bigserial"}},
				"h": schema.Column{Name: "h", Type: schema.Type{Name: "bpchar"}},
				"i": schema.Column{Name: "i", Type: schema.Type{Name: "bytea"}},
				"j": schema.Column{Name: "j", Type: schema.Type{Name: "date"}},
				"k": schema.Column{Name: "k", Type: schema.Type{Name: "float8"}},
				"l": schema.Column{Name: "l", Type: schema.Type{Name: "int4"}},
				"m": schema.Column{Name: "m", Type: schema.Type{Name: "serial"}},
				"n": schema.Column{Name: "n", Type: schema.Type{Name: "text"}},
				"o": schema.Column{Name: "o", Type: schema.Type{Name: "timestamp"}},
				"p": schema.Column{Name: "p", Type: schema.Type{Name: "bool"}},
			},
			PrimaryKeys: []schema.Key{schema.Key{Column: "a"}}},
		"t2": schema.Table{
			Name:     "t2",
			ColNames: []string{"a", "b", "c"},
			ColDefs: map[string]schema.Column{
				"a": schema.Column{Name: "a", Type: schema.Type{Name: "int8"}},
				"b": schema.Column{Name: "b", Type: schema.Type{Name: "float4"}},
				"c": schema.Column{Name: "c", Type: schema.Type{Name: "bool"}},
			}},
	}
	conv.SpSchema = map[string]ddl.CreateTable{
		"t1": ddl.CreateTable{
			Name:     "t1",
			ColNames: []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p"},
			ColDefs: map[string]ddl.ColumnDef{
				"a": ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.Int64}},
				"b": ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.Float64}},
				"c": ddl.ColumnDef{Name: "c", T: ddl.Type{Name: ddl.Bool}},
				"d": ddl.ColumnDef{Name: "d", T: ddl.Type{Name: ddl.String, Len: int64(6)}},
				"e": ddl.ColumnDef{Name: "e", T: ddl.Type{Name: ddl.Float64}},
				"f": ddl.ColumnDef{Name: "f", T: ddl.Type{Name: ddl.Timestamp}},
				"g": ddl.ColumnDef{Name: "g", T: ddl.Type{Name: ddl.Int64}},
				"h": ddl.ColumnDef{Name: "h", T: ddl.Type{Name: ddl.String, Len: int64(1)}},
				"i": ddl.ColumnDef{Name: "i", T: ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}},
				"j": ddl.ColumnDef{Name: "j", T: ddl.Type{Name: ddl.Date}},
				"k": ddl.ColumnDef{Name: "k", T: ddl.Type{Name: ddl.Float64}},
				"l": ddl.ColumnDef{Name: "l", T: ddl.Type{Name: ddl.Int64}},
				"m": ddl.ColumnDef{Name: "m", T: ddl.Type{Name: ddl.Int64}},
				"n": ddl.ColumnDef{Name: "n", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
				"o": ddl.ColumnDef{Name: "o", T: ddl.Type{Name: ddl.Timestamp}},
				"p": ddl.ColumnDef{Name: "p", T: ddl.Type{Name: ddl.Int64}},
			},
			Pks: []ddl.IndexKey{ddl.IndexKey{Col: "a"}},
		},
		"t2": ddl.CreateTable{
			Name:     "t2",
			ColNames: []string{"a", "b", "c", "synth_id"},
			ColDefs: map[string]ddl.ColumnDef{
				"a":        ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.Int64}},
				"b":        ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.Float64}},
				"c":        ddl.ColumnDef{Name: "c", T: ddl.Type{Name: ddl.Bool}},
				"synth_id": ddl.ColumnDef{Name: "synth_id", T: ddl.Type{Name: ddl.Int64}},
			},
			Pks: []ddl.IndexKey{ddl.IndexKey{Col: "synth_id"}},
		},
	}
	conv.ToSource = map[string]internal.NameAndCols{
		"t1": internal.NameAndCols{
			Name: "t1",
			Cols: map[string]string{
				"a": "a", "b": "b", "c": "c", "d": "d", "e": "e", "f": "f", "g": "g", "h": "h", "i": "i", "j": "j", "k": "k", "l": "l", "m": "m", "n": "n", "o": "o", "p": "p",
			}},
		"t2": internal.NameAndCols{
			Name: "t2",
			Cols: map[string]string{
				"a": "a", "b": "b", "c": "c",
			}},
	}
	conv.ToSpanner = map[string]internal.NameAndCols{
		"t1": internal.NameAndCols{
			Name: "t1",
			Cols: map[string]string{
				"a": "a", "b": "b", "c": "c", "d": "d", "e": "e", "f": "f", "g": "g", "h": "h", "i": "i", "j": "j", "k": "k", "l": "l", "m": "m", "n": "n", "o": "o", "p": "p",
			}},
		"t2": internal.NameAndCols{
			Name: "t2",
			Cols: map[string]string{
				"a": "a", "b": "b", "c": "c",
			}},
	}
	conv.Issues = map[string]map[string][]internal.SchemaIssue{
		"t1": map[string][]internal.SchemaIssue{
			"b": []internal.SchemaIssue{internal.Widened},
			"e": []internal.SchemaIssue{internal.Numeric},
			"g": []internal.SchemaIssue{internal.Serial},
			"l": []internal.SchemaIssue{internal.Widened},
			"m": []internal.SchemaIssue{internal.Serial},
			"o": []internal.SchemaIssue{internal.Timestamp},
		},
		"t2": map[string][]internal.SchemaIssue{
			"b": []internal.SchemaIssue{internal.Widened},
		},
	}
	conv.SyntheticPKeys["t2"] = internal.SyntheticPKey{"synth_id", 0}
}

func buildConvPostgresMultiTable(conv *internal.Conv) {
	conv.SrcSchema = map[string]schema.Table{
		"t1": schema.Table{
			Name:     "t1",
			ColNames: []string{"a", "b"},
			ColDefs: map[string]schema.Column{
				"a": schema.Column{Name: "a", Type: schema.Type{Name: "int8"}},
				"b": schema.Column{Name: "b", Type: schema.Type{Name: "float8"}},
			},
			PrimaryKeys: []schema.Key{schema.Key{Column: "a"}}},
		"t2": schema.Table{
			Name:     "t2",
			ColNames: []string{"a", "b"},
			ColDefs: map[string]schema.Column{
				"a": schema.Column{Name: "a", Type: schema.Type{Name: "int8"}},
				"b": schema.Column{Name: "b", Type: schema.Type{Name: "float8"}},
			},
		},
		"t3": schema.Table{
			Name:     "t3",
			ColNames: []string{"a", "b", "c"},
			ColDefs: map[string]schema.Column{
				"a": schema.Column{Name: "a", Type: schema.Type{Name: "serial"}},
				"b": schema.Column{Name: "b", Type: schema.Type{Name: "serial"}},
				"c": schema.Column{Name: "c", Type: schema.Type{Name: "numeric"}},
			},
		},
	}
	conv.SpSchema = map[string]ddl.CreateTable{
		"t1": ddl.CreateTable{
			Name:     "t1",
			ColNames: []string{"a", "b"},
			ColDefs: map[string]ddl.ColumnDef{
				"a": ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.Int64}},
				"b": ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.Float64}},
			},
			Pks: []ddl.IndexKey{ddl.IndexKey{Col: "a"}},
		},
		"t2": ddl.CreateTable{
			Name:     "t2",
			ColNames: []string{"a", "b", "synth_id"},
			ColDefs: map[string]ddl.ColumnDef{
				"a":        ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.Int64}},
				"b":        ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.Float64}},
				"synth_id": ddl.ColumnDef{Name: "synth_id", T: ddl.Type{Name: ddl.Int64}},
			},
			Pks: []ddl.IndexKey{ddl.IndexKey{Col: "synth_id"}},
		},
		"t3": ddl.CreateTable{
			Name:     "t3",
			ColNames: []string{"a", "b", "c", "synth_id"},
			ColDefs: map[string]ddl.ColumnDef{
				"a":        ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.Int64}},
				"b":        ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.Int64}},
				"c":        ddl.ColumnDef{Name: "c", T: ddl.Type{Name: ddl.Float64}},
				"synth_id": ddl.ColumnDef{Name: "synth_id", T: ddl.Type{Name: ddl.Int64}},
			},
			Pks: []ddl.IndexKey{ddl.IndexKey{Col: "synth_id"}},
		},
	}
	conv.Issues = map[string]map[string][]internal.SchemaIssue{
		"t3": map[string][]internal.SchemaIssue{
			"a": []internal.SchemaIssue{internal.Serial},
			"b": []internal.SchemaIssue{internal.Serial},
			"c": []internal.SchemaIssue{internal.Numeric},
		},
	}
	conv.SyntheticPKeys["t2"] = internal.SyntheticPKey{"synth_id", 0}
	conv.SyntheticPKeys["t3"] = internal.SyntheticPKey{"synth_id", 0}
}

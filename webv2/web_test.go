// Copyright 2022 Google LLC
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

package webv2

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/cloudspannerecosystem/harbourbridge/common/constants"
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/schema"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
	"github.com/cloudspannerecosystem/harbourbridge/webv2/session"
	"github.com/stretchr/testify/assert"
)

func TestGetTypeMapNoDriver(t *testing.T) {
	req, err := http.NewRequest("GET", "/typemap", nil)
	if err != nil {
		t.Fatal(err)
	}
	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(getTypeMap)
	handler.ServeHTTP(rr, req)

	status := rr.Code

	if status != http.StatusNotFound {
		t.Errorf("handler returned wrong status code : got %v want %v",
			status, http.StatusNotFound)
	}

}

func TestGetTypeMapPostgres(t *testing.T) {
	sessionState := session.GetSessionState()
	sessionState.Driver = constants.POSTGRES
	sessionState.Conv = internal.MakeConv()
	buildConvPostgres(sessionState.Conv)
	req, err := http.NewRequest("GET", "/typemap", nil)
	if err != nil {
		t.Fatal(err)
	}
	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(getTypeMap)
	handler.ServeHTTP(rr, req)
	var typemap map[string][]typeIssue
	json.Unmarshal(rr.Body.Bytes(), &typemap)
	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusOK)
	}
	expectedTypemap := map[string][]typeIssue{
		"bool": {
			{T: ddl.Bool},
			{T: ddl.Int64, Brief: internal.IssueDB[internal.Widened].Brief},
			{T: ddl.String, Brief: internal.IssueDB[internal.Widened].Brief}},
		"bigserial": {
			{T: ddl.Int64, Brief: internal.IssueDB[internal.Serial].Brief},
			{T: ddl.String, Brief: internal.IssueDB[internal.Widened].Brief + ", " + internal.IssueDB[internal.Serial].Brief}},
		"bpchar": {
			{T: ddl.Bytes},
			{T: ddl.String}},
		"bytea": {
			{T: ddl.Bytes},
			{T: ddl.String}},
		"date": {
			{T: ddl.Date},
			{T: ddl.String, Brief: internal.IssueDB[internal.Widened].Brief}},
		"float8": {
			{T: ddl.Float64},
			{T: ddl.String, Brief: internal.IssueDB[internal.Widened].Brief}},
		"float4": {
			{T: ddl.Float64, Brief: internal.IssueDB[internal.Widened].Brief},
			{T: ddl.String, Brief: internal.IssueDB[internal.Widened].Brief}},
		"int8": {
			{T: ddl.Int64},
			{T: ddl.String, Brief: internal.IssueDB[internal.Widened].Brief}},
		"int4": {
			{T: ddl.Int64, Brief: internal.IssueDB[internal.Widened].Brief},
			{T: ddl.String, Brief: internal.IssueDB[internal.Widened].Brief}},
		"numeric": {
			{T: ddl.String, Brief: internal.IssueDB[internal.Widened].Brief},
			{T: ddl.Numeric}},
		"serial": {
			{T: ddl.Int64, Brief: internal.IssueDB[internal.Serial].Brief},
			{T: ddl.String, Brief: internal.IssueDB[internal.Widened].Brief + ", " + internal.IssueDB[internal.Serial].Brief}},
		"text": {
			{T: ddl.Bytes},
			{T: ddl.String}},
		"timestamptz": {
			{T: ddl.String, Brief: internal.IssueDB[internal.Widened].Brief},
			{T: ddl.Timestamp}},
		"timestamp": {
			{T: ddl.String, Brief: internal.IssueDB[internal.Widened].Brief},
			{T: ddl.Timestamp, Brief: internal.IssueDB[internal.Timestamp].Brief}},
		"varchar": {
			{T: ddl.Bytes},
			{T: ddl.String}},
	}
	assert.Equal(t, expectedTypemap, typemap)

}

func TestUpdateTableSchema(t *testing.T) {
	tc := []struct {
		name         string
		table        string
		payload      string
		statusCode   int64
		conv         *internal.Conv
		expectedConv *internal.Conv
	}{
		{
			name:  "Test remove fail column part of PK",
			table: "t1",
			payload: `
    {
      "UpdateCols":{
		"a": { "Removed": true }
	}
    }`,
			statusCode: http.StatusBadRequest,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:     "t1",
						ColNames: []string{"a", "b"},
						ColDefs: map[string]ddl.ColumnDef{
							"a": {Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
							"b": {Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
						},
						Pks: []ddl.IndexKey{{Col: "a"}},
					}},
				ToSource: map[string]internal.NameAndCols{
					"t1": {Name: "t1", Cols: map[string]string{"a": "a", "b": "b"}},
				},
			},
		},
		{
			name:  "Test remove fail column part of secondary index",
			table: "t1",
			payload: `
    {
      "UpdateCols":{
		"b": { "Removed": true }
	}
    }`,
			statusCode: http.StatusPreconditionFailed,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:     "t1",
						ColNames: []string{"a", "b"},
						ColDefs: map[string]ddl.ColumnDef{
							"a": {Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
							"b": {Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
						},
						Pks:     []ddl.IndexKey{{Col: "a"}},
						Indexes: []ddl.CreateIndex{{Name: "idx", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}}},
					}},
				ToSource: map[string]internal.NameAndCols{
					"t1": {Name: "t1", Cols: map[string]string{"a": "a", "b": "b"}},
				},
			},
		},
		{
			name:  "Test remove fail column part of FK",
			table: "t1",
			payload: `
    {
      "UpdateCols":{
		"b": { "Removed": true }
	}
    }`,
			statusCode: http.StatusPreconditionFailed,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:     "t1",
						ColNames: []string{"a", "b"},
						ColDefs: map[string]ddl.ColumnDef{
							"a": {Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
							"b": {Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
						},
						Pks: []ddl.IndexKey{{Col: "a"}},
						Fks: []ddl.Foreignkey{{Name: "fk1", Columns: []string{"b"}, ReferTable: "t2", ReferColumns: []string{"b"}}},
					}},
				ToSource: map[string]internal.NameAndCols{
					"t1": {Name: "t1", Cols: map[string]string{"a": "a", "b": "b"}},
				},
			},
		},
		{
			name:  "Test remove fail column referenced by FK",
			table: "t1",
			payload: `
    {
      "UpdateCols":{
		"b": { "Removed": true }
	}
    }`,
			statusCode: http.StatusPreconditionFailed,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:     "t1",
						ColNames: []string{"a", "b"},
						ColDefs: map[string]ddl.ColumnDef{
							"a": {Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
							"b": {Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
						},
						Pks: []ddl.IndexKey{{Col: "a"}},
					},
					"t2": {
						Name:     "t2",
						ColNames: []string{"aa", "bb"},
						ColDefs: map[string]ddl.ColumnDef{
							"aa": {Name: "aa", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
							"bb": {Name: "bb", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
						},
						Pks: []ddl.IndexKey{{Col: "aa"}},
						Fks: []ddl.Foreignkey{{Name: "fk1", Columns: []string{"bb"}, ReferTable: "t1", ReferColumns: []string{"b"}}},
					},
				},
				ToSource: map[string]internal.NameAndCols{
					"t1": {Name: "t1", Cols: map[string]string{"a": "a", "b": "b"}},
				},
			},
		},
		{
			name:  "Test remove success",
			table: "t1",
			payload: `
    {
      "UpdateCols":{
		"c": { "Removed": true }
	}
    }`,
			statusCode: http.StatusOK,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:     "t1",
						ColNames: []string{"a", "b", "c"},
						ColDefs: map[string]ddl.ColumnDef{
							"a": {Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
							"b": {Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
							"c": {Name: "c", T: ddl.Type{Name: ddl.Int64}},
						},
						Pks: []ddl.IndexKey{{Col: "a"}},
					}},
				Issues: map[string]map[string][]internal.SchemaIssue{
					"t1": {
						"c": {internal.Widened},
					},
				},
				ToSource: map[string]internal.NameAndCols{
					"t1": {Name: "t1", Cols: map[string]string{"a": "a", "b": "b", "c": "c"}},
				},
				ToSpanner: map[string]internal.NameAndCols{
					"t1": {Name: "t1", Cols: map[string]string{"a": "a", "b": "b", "c": "c"}},
				},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:     "t1",
						ColNames: []string{"a", "b"},
						ColDefs: map[string]ddl.ColumnDef{
							"a": {Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
							"b": {Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
						},
						Pks: []ddl.IndexKey{{Col: "a"}},
					}},
				Issues: map[string]map[string][]internal.SchemaIssue{
					"t1": {},
				},
				ToSource: map[string]internal.NameAndCols{
					"t1": {Name: "t1", Cols: map[string]string{"a": "a", "b": "b"}},
				},
				ToSpanner: map[string]internal.NameAndCols{
					"t1": {Name: "t1", Cols: map[string]string{"a": "a", "b": "b"}},
				},
			},
		},
		{
			name:  "Test rename fail column part of PK and child table",
			table: "t1",
			payload: `
    {
      "UpdateCols":{
		"b": { "Rename": "bb" }
	}
    }`,
			statusCode: http.StatusBadRequest,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:     "t1",
						ColNames: []string{"a", "b", "c"},
						ColDefs: map[string]ddl.ColumnDef{
							"a": {Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
							"b": {Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
							"c": {Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
						},
						Pks:    []ddl.IndexKey{{Col: "a"}, {Col: "b"}},
						Parent: "t2",
					}},
				ToSource: map[string]internal.NameAndCols{
					"t1": {Name: "t1", Cols: map[string]string{"a": "a", "b": "b", "c": "c"}},
				},
			},
		},
		{
			name:  "Test rename fail column part of PK and parent table",
			table: "t1",
			payload: `
		{
		  "UpdateCols":{
			"a": { "Rename": "aa" }
		}
		}`,
			statusCode: http.StatusBadRequest,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:     "t1",
						ColNames: []string{"a", "b"},
						ColDefs: map[string]ddl.ColumnDef{
							"a": {Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
							"b": {Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
						},
						Pks: []ddl.IndexKey{{Col: "a"}},
					},
					"t2": {
						Name:     "t2",
						ColNames: []string{"a", "b", "c"},
						ColDefs: map[string]ddl.ColumnDef{
							"a": {Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
							"b": {Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
							"c": {Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
						},
						Pks:    []ddl.IndexKey{{Col: "a"}, {Col: "b"}},
						Parent: "t1",
					},
				},
				ToSource: map[string]internal.NameAndCols{
					"t1": {Name: "t1", Cols: map[string]string{"a": "a", "b": "b"}},
					"t2": {Name: "t2", Cols: map[string]string{"a": "a", "b": "b", "c": "c"}},
				},
			},
		},
		{
			name:  "Test rename fail column part of secondary index",
			table: "t1",
			payload: `
		{
		  "UpdateCols":{
			"b": { "Rename": "bb" }
		}
		}`,
			statusCode: http.StatusPreconditionFailed,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:     "t1",
						ColNames: []string{"a", "b"},
						ColDefs: map[string]ddl.ColumnDef{
							"a": {Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
							"b": {Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
						},
						Pks:     []ddl.IndexKey{{Col: "a"}},
						Indexes: []ddl.CreateIndex{{Name: "idx", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}}},
					}},
				ToSource: map[string]internal.NameAndCols{
					"t1": {Name: "t1", Cols: map[string]string{"a": "a", "b": "b"}},
				},
			},
		},
		{
			name:  "Test rename fail column part of FK",
			table: "t1",
			payload: `
		{
		  "UpdateCols":{
			"b": { "Rename": "bb" }
		}
		}`,
			statusCode: http.StatusPreconditionFailed,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:     "t1",
						ColNames: []string{"a", "b"},
						ColDefs: map[string]ddl.ColumnDef{
							"a": {Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
							"b": {Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
						},
						Pks: []ddl.IndexKey{{Col: "a"}},
						Fks: []ddl.Foreignkey{{Name: "fk1", Columns: []string{"b"}, ReferTable: "t2", ReferColumns: []string{"b"}}},
					}},
				ToSource: map[string]internal.NameAndCols{
					"t1": {Name: "t1", Cols: map[string]string{"a": "a", "b": "b"}},
				},
			},
		},
		{
			name:  "Test rename fail column referenced by FK",
			table: "t1",
			payload: `
		{
		  "UpdateCols":{
			"b": { "Rename": "bb" }
		}
		}`,
			statusCode: http.StatusPreconditionFailed,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:     "t1",
						ColNames: []string{"a", "b"},
						ColDefs: map[string]ddl.ColumnDef{
							"a": {Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
							"b": {Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
						},
						Pks: []ddl.IndexKey{{Col: "a"}},
					},
					"t2": {
						Name:     "t2",
						ColNames: []string{"aa", "bb"},
						ColDefs: map[string]ddl.ColumnDef{
							"aa": {Name: "aa", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
							"bb": {Name: "bb", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
						},
						Pks: []ddl.IndexKey{{Col: "aa"}},
						Fks: []ddl.Foreignkey{{Name: "fk1", Columns: []string{"bb"}, ReferTable: "t1", ReferColumns: []string{"b"}}},
					},
				},
				ToSource: map[string]internal.NameAndCols{
					"t1": {Name: "t1", Cols: map[string]string{"a": "a", "b": "b"}},
				},
			},
		},
		{
			name:  "Test rename success",
			table: "t1",
			payload: `
    {
      "UpdateCols":{
		"a": { "Rename": "aa" }
	}
    }`,
			statusCode: http.StatusOK,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:     "t1",
						ColNames: []string{"a", "b", "c"},
						ColDefs: map[string]ddl.ColumnDef{
							"a": {Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
							"b": {Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
							"c": {Name: "c", T: ddl.Type{Name: ddl.Int64}},
						},
						Pks: []ddl.IndexKey{{Col: "a"}},
					}},
				ToSource: map[string]internal.NameAndCols{
					"t1": {Name: "t1", Cols: map[string]string{"a": "a", "b": "b", "c": "c"}},
				},
				ToSpanner: map[string]internal.NameAndCols{
					"t1": {Name: "t1", Cols: map[string]string{"a": "a", "b": "b", "c": "c"}},
				},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:     "t1",
						ColNames: []string{"aa", "b", "c"},
						ColDefs: map[string]ddl.ColumnDef{
							"aa": {Name: "aa", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
							"b":  {Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
							"c":  {Name: "c", T: ddl.Type{Name: ddl.Int64}},
						},
						Pks: []ddl.IndexKey{{Col: "aa"}},
					}},
				ToSource: map[string]internal.NameAndCols{
					"t1": {Name: "t1", Cols: map[string]string{"aa": "a", "b": "b", "c": "c"}},
				},
				ToSpanner: map[string]internal.NameAndCols{
					"t1": {Name: "t1", Cols: map[string]string{"a": "aa", "b": "b", "c": "c"}},
				},
			},
		},
		{
			name:  "Test change type success",
			table: "t1",
			payload: `
    {
      "UpdateCols":{
		"a": { "ToType": "STRING" },
		"b": { "ToType": "BYTES" }
	}
    }`,
			statusCode: http.StatusOK,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:     "t1",
						ColNames: []string{"a", "b"},
						ColDefs: map[string]ddl.ColumnDef{
							"a": {Name: "a", T: ddl.Type{Name: ddl.Int64}},
							"b": {Name: "b", T: ddl.Type{Name: ddl.String, Len: 6}},
						},
						Pks: []ddl.IndexKey{{Col: "a"}},
					}},
				SrcSchema: map[string]schema.Table{
					"t1": {
						Name:     "t1",
						ColNames: []string{"a", "b"},
						ColDefs: map[string]schema.Column{
							"a": {Name: "a", Type: schema.Type{Name: "bigint", Mods: []int64{}}},
							"b": {Name: "b", Type: schema.Type{Name: "varchar", Mods: []int64{6}}},
						},
						PrimaryKeys: []schema.Key{{Column: "a"}},
					}},
				ToSource: map[string]internal.NameAndCols{
					"t1": {Name: "t1", Cols: map[string]string{"a": "a", "b": "b"}},
				},
				ToSpanner: map[string]internal.NameAndCols{
					"t1": {Name: "t1", Cols: map[string]string{"a": "a", "b": "b"}},
				},
				Issues: map[string]map[string][]internal.SchemaIssue{
					"t1": {},
				},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:     "t1",
						ColNames: []string{"a", "b"},
						ColDefs: map[string]ddl.ColumnDef{
							"a": {Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
							"b": {Name: "b", T: ddl.Type{Name: ddl.Bytes, Len: 6}},
						},
						Pks: []ddl.IndexKey{{Col: "a"}},
					}},
				SrcSchema: map[string]schema.Table{
					"t1": {
						Name:     "t1",
						ColNames: []string{"a", "b"},
						ColDefs: map[string]schema.Column{
							"a": {Name: "a", Type: schema.Type{Name: "bigint", Mods: []int64{}}},
							"b": {Name: "b", Type: schema.Type{Name: "varchar", Mods: []int64{6}}},
						},
						PrimaryKeys: []schema.Key{{Column: "a"}},
					}},
				ToSource: map[string]internal.NameAndCols{
					"t1": {Name: "t1", Cols: map[string]string{"a": "a", "b": "b"}},
				},
				ToSpanner: map[string]internal.NameAndCols{
					"t1": {Name: "t1", Cols: map[string]string{"a": "a", "b": "b"}},
				},
				Issues: map[string]map[string][]internal.SchemaIssue{
					"t1": {
						"a": {internal.Widened},
					},
				},
			},
		},
		{
			name:  "Test add or remove not null",
			table: "t1",
			payload: `
    {
      "UpdateCols":{
		"b": { "NotNull": "ADDED" },
		"c": { "NotNull": "REMOVED" }
	}
    }`,
			statusCode: http.StatusOK,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:     "t1",
						ColNames: []string{"a", "b", "c"},
						ColDefs: map[string]ddl.ColumnDef{
							"a": {Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
							"b": {Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
							"c": {Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						Pks: []ddl.IndexKey{{Col: "a"}},
					}},
				ToSource: map[string]internal.NameAndCols{
					"t1": {Name: "t1", Cols: map[string]string{"a": "a", "b": "b", "c": "c"}},
				},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:     "t1",
						ColNames: []string{"a", "b", "c"},
						ColDefs: map[string]ddl.ColumnDef{
							"a": {Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
							"b": {Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
							"c": {Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
						},
						Pks: []ddl.IndexKey{{Col: "a"}},
					}},
				ToSource: map[string]internal.NameAndCols{
					"t1": {Name: "t1", Cols: map[string]string{"a": "a", "b": "b", "c": "c"}},
				},
			},
		},
	}
	for _, tc := range tc {
		sessionState := session.GetSessionState()
		sessionState.Driver = constants.MYSQL
		sessionState.Conv = tc.conv
		payload := tc.payload
		req, err := http.NewRequest("POST", "/typemap/table?table="+tc.table, strings.NewReader(payload))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		handler := http.HandlerFunc(updateTableSchema)
		handler.ServeHTTP(rr, req)
		var res *internal.Conv
		json.Unmarshal(rr.Body.Bytes(), &res)
		if status := rr.Code; int64(status) != tc.statusCode {
			t.Errorf("handler returned wrong status code: got %v want %v",
				status, tc.statusCode)
		}
		if tc.statusCode == http.StatusOK {
			assert.Equal(t, tc.expectedConv, res)
		}
	}
}

func TestSetTypeMapGlobalLevelPostgres(t *testing.T) {
	tc := []struct {
		name           string
		payload        string
		statusCode     int64
		expectedSchema ddl.CreateTable
		expectedIssues map[string][]internal.SchemaIssue
	}{
		{
			name: "Test type change",
			payload: `
    {
      	"bool":"STRING",
		"int8":"STRING",
		"float4":"STRING",
		"varchar":"BYTES",
		"numeric":"STRING",
		"timestamptz":"STRING",
		"bigserial":"STRING",
		"bpchar":"BYTES",
		"bytea":"STRING",
		"date":"STRING",
		"float8":"STRING",
		"int4":"STRING",
		"serial":"STRING",
		"text":"BYTES",
		"timestamp":"STRING"
    }`,
			statusCode: http.StatusOK,
			expectedSchema: ddl.CreateTable{
				Name:     "t1",
				ColNames: []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p"},
				ColDefs: map[string]ddl.ColumnDef{
					"a": {Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"b": {Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"c": {Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"d": {Name: "d", T: ddl.Type{Name: ddl.Bytes, Len: 6}},
					"e": {Name: "e", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"f": {Name: "f", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"g": {Name: "g", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"h": {Name: "h", T: ddl.Type{Name: ddl.Bytes, Len: int64(1)}},
					"i": {Name: "i", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"j": {Name: "j", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"k": {Name: "k", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"l": {Name: "l", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"m": {Name: "m", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"n": {Name: "n", T: ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}},
					"o": {Name: "o", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"p": {Name: "p", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
				},
				Pks: []ddl.IndexKey{{Col: "a"}},
			},
			expectedIssues: map[string][]internal.SchemaIssue{
				"a": {internal.Widened},
				"b": {internal.Widened},
				"c": {internal.Widened},
				"e": {internal.Widened},
				"f": {internal.Widened},
				"g": {internal.Widened, internal.Serial},
				"j": {internal.Widened},
				"k": {internal.Widened},
				"l": {internal.Widened},
				"m": {internal.Widened, internal.Serial},
				"o": {internal.Widened},
				"p": {internal.Widened},
			},
		},
		{
			name: "Test type change 2",
			payload: `
    {
      	"bool":"INT64",
		"int8":"STRING",
		"float4":"STRING"
    }`,
			statusCode: http.StatusOK,
			expectedSchema: ddl.CreateTable{
				Name:     "t1",
				ColNames: []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p"},
				ColDefs: map[string]ddl.ColumnDef{
					"a": {Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"b": {Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"c": {Name: "c", T: ddl.Type{Name: ddl.Int64}},
					"d": {Name: "d", T: ddl.Type{Name: ddl.String, Len: int64(6)}},
					"e": {Name: "e", T: ddl.Type{Name: ddl.Numeric}},
					"f": {Name: "f", T: ddl.Type{Name: ddl.Timestamp}},
					"g": {Name: "g", T: ddl.Type{Name: ddl.Int64}},
					"h": {Name: "h", T: ddl.Type{Name: ddl.String, Len: int64(1)}},
					"i": {Name: "i", T: ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}},
					"j": {Name: "j", T: ddl.Type{Name: ddl.Date}},
					"k": {Name: "k", T: ddl.Type{Name: ddl.Float64}},
					"l": {Name: "l", T: ddl.Type{Name: ddl.Int64}},
					"m": {Name: "m", T: ddl.Type{Name: ddl.Int64}},
					"n": {Name: "n", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"o": {Name: "o", T: ddl.Type{Name: ddl.Timestamp}},
					"p": {Name: "p", T: ddl.Type{Name: ddl.Int64}},
				},
				Pks: []ddl.IndexKey{{Col: "a"}},
			},
			expectedIssues: map[string][]internal.SchemaIssue{
				"a": {internal.Widened},
				"b": {internal.Widened},
				"c": {internal.Widened},
				"g": {internal.Serial},
				"l": {internal.Widened},
				"m": {internal.Serial},
				"o": {internal.Timestamp},
				"p": {internal.Widened},
			},
		},
		{
			name: "Test bad request",
			payload: `
    {
      	"bool":"INT64",
		"int8":"STRING",
		"float4":"STRING",
    }`,
			statusCode: http.StatusBadRequest,
		},
	}
	for _, tc := range tc {

		sessionState := session.GetSessionState()

		sessionState.Driver = constants.POSTGRES
		sessionState.Conv = internal.MakeConv()
		buildConvPostgres(sessionState.Conv)
		payload := tc.payload
		req, err := http.NewRequest("POST", "/typemap/global", strings.NewReader(payload))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		handler := http.HandlerFunc(setTypeMapGlobal)
		handler.ServeHTTP(rr, req)
		var res *internal.Conv
		json.Unmarshal(rr.Body.Bytes(), &res)
		if status := rr.Code; int64(status) != tc.statusCode {
			t.Errorf("handler returned wrong status code: got %v want %v",
				status, tc.statusCode)
		}

		if tc.statusCode == http.StatusOK {
			assert.Equal(t, tc.expectedSchema, res.SpSchema["t1"])
			assert.Equal(t, tc.expectedIssues, res.Issues["t1"])
		}
	}

}

func TestGetConversionPostgres(t *testing.T) {
	sessionState := session.GetSessionState()

	sessionState.Driver = constants.POSTGRES
	sessionState.Conv = internal.MakeConv()
	buildConvPostgres(sessionState.Conv)
	req, err := http.NewRequest("GET", "/conversion", nil)
	if err != nil {
		t.Fatal(err)
	}
	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(getConversionRate)
	handler.ServeHTTP(rr, req)
	var result map[string]string
	json.Unmarshal(rr.Body.Bytes(), &result)
	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusOK)
	}
	assert.Equal(t, 2, len(result))
	assert.Contains(t, result, "t1")
	assert.Contains(t, result, "t2")
}

func TestGetTypeMapMySQL(t *testing.T) {
	sessionState := session.GetSessionState()

	sessionState.Driver = constants.MYSQL
	sessionState.Conv = internal.MakeConv()
	buildConvMySQL(sessionState.Conv)
	req, err := http.NewRequest("GET", "/typemap", nil)
	if err != nil {
		t.Fatal(err)
	}
	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(getTypeMap)
	handler.ServeHTTP(rr, req)
	var typemap map[string][]typeIssue
	json.Unmarshal(rr.Body.Bytes(), &typemap)
	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusOK)
	}
	expectedTypemap := map[string][]typeIssue{
		"bool": {
			{T: ddl.Bool},
			{T: ddl.Int64, Brief: internal.IssueDB[internal.Widened].Brief},
			{T: ddl.String, Brief: internal.IssueDB[internal.Widened].Brief}},
		"varchar": {
			{T: ddl.Bytes},
			{T: ddl.String}},
		"text": {
			{T: ddl.Bytes},
			{T: ddl.String}},
		"enum": {
			{T: ddl.String}},
		"json": {
			{T: ddl.Bytes},
			{T: ddl.String}},
		"binary": {
			{T: ddl.Bytes},
			{T: ddl.String}},
		"blob": {
			{T: ddl.Bytes},
			{T: ddl.String}},
		"integer": {
			{T: ddl.Int64, Brief: internal.IssueDB[internal.Widened].Brief},
			{T: ddl.String, Brief: internal.IssueDB[internal.Widened].Brief}},
		"smallint": {
			{T: ddl.Int64, Brief: internal.IssueDB[internal.Widened].Brief},
			{T: ddl.String, Brief: internal.IssueDB[internal.Widened].Brief}},
		"double": {
			{T: ddl.Float64},
			{T: ddl.String, Brief: internal.IssueDB[internal.Widened].Brief}},
		"float": {
			{T: ddl.Float64, Brief: internal.IssueDB[internal.Widened].Brief},
			{T: ddl.String, Brief: internal.IssueDB[internal.Widened].Brief}},
		"numeric": {
			{T: ddl.String, Brief: internal.IssueDB[internal.Widened].Brief},
			{T: ddl.Numeric}},
		"decimal": {
			{T: ddl.String, Brief: internal.IssueDB[internal.Widened].Brief},
			{T: ddl.Numeric}},
		"date": {
			{T: ddl.Date},
			{T: ddl.String, Brief: internal.IssueDB[internal.Widened].Brief}},
		"timestamp": {
			{T: ddl.String, Brief: internal.IssueDB[internal.Widened].Brief},
			{T: ddl.Timestamp}},
		"time": {
			{T: ddl.String, Brief: internal.IssueDB[internal.Time].Brief}},
	}
	assert.Equal(t, expectedTypemap, typemap)

}

func TestSetTypeMapGlobalLevelMySQL(t *testing.T) {
	tc := []struct {
		name           string
		payload        string
		statusCode     int64
		expectedSchema ddl.CreateTable
		expectedIssues map[string][]internal.SchemaIssue
	}{
		{
			name: "Test type change",
			payload: `
    {
      	"bool":"STRING",
		"smallint":"STRING",
		"float":"STRING",
		"varchar":"BYTES",
		"numeric":"STRING",
		"timestamp":"STRING",
		"decimal":"STRING",
		"json":"BYTES",
		"binary":"STRING",
		"blob":"STRING",
		"double":"STRING",
		"date":"STRING",
		"time":"STRING",
		"enum":"STRING",
		"text":"BYTES"
    }`,
			statusCode: http.StatusOK,
			expectedSchema: ddl.CreateTable{
				Name:     "t1",
				ColNames: []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p"},
				ColDefs: map[string]ddl.ColumnDef{
					"a": {Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"b": {Name: "b", T: ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}},
					"c": {Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"d": {Name: "d", T: ddl.Type{Name: ddl.Bytes, Len: 6}},
					"e": {Name: "e", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"f": {Name: "f", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"g": {Name: "g", T: ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}},
					"h": {Name: "h", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"i": {Name: "i", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"j": {Name: "j", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"k": {Name: "k", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"l": {Name: "l", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"m": {Name: "m", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"n": {Name: "n", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"o": {Name: "o", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"p": {Name: "p", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
				},
				Pks: []ddl.IndexKey{{Col: "a"}},
			},
			expectedIssues: map[string][]internal.SchemaIssue{
				"a": {internal.Widened},
				"c": {internal.Widened},
				"e": {internal.Widened},
				"j": {internal.Widened},
				"k": {internal.Widened},
				"l": {internal.Widened},
				"m": {internal.Widened},
				"n": {internal.Widened},
				"o": {internal.Widened},
				"p": {internal.Time},
			},
		},
		{
			name: "Test type change 2",
			payload: `
    {
      	"bool":"INT64",
		"varchar":"BYTES"
    }`,
			statusCode: http.StatusOK,
			expectedSchema: ddl.CreateTable{
				Name:     "t1",
				ColNames: []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p"},
				ColDefs: map[string]ddl.ColumnDef{
					"a": {Name: "a", T: ddl.Type{Name: ddl.Int64}},
					"b": {Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"c": {Name: "c", T: ddl.Type{Name: ddl.Int64}},
					"d": {Name: "d", T: ddl.Type{Name: ddl.Bytes, Len: 6}},
					"e": {Name: "e", T: ddl.Type{Name: ddl.Numeric}},
					"f": {Name: "f", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"g": {Name: "g", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"h": {Name: "h", T: ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}},
					"i": {Name: "i", T: ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}},
					"j": {Name: "j", T: ddl.Type{Name: ddl.Int64}},
					"k": {Name: "k", T: ddl.Type{Name: ddl.Float64}},
					"l": {Name: "l", T: ddl.Type{Name: ddl.Float64}},
					"m": {Name: "m", T: ddl.Type{Name: ddl.Numeric}},
					"n": {Name: "n", T: ddl.Type{Name: ddl.Date}},
					"o": {Name: "o", T: ddl.Type{Name: ddl.Timestamp}},
					"p": {Name: "p", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
				},
				Pks: []ddl.IndexKey{{Col: "a"}},
			},
			expectedIssues: map[string][]internal.SchemaIssue{
				"a": {internal.Widened},
				"c": {internal.Widened},
				"j": {internal.Widened},
				"l": {internal.Widened},
				"o": {internal.Time},
			},
		},
		{
			name: "Test bad request",
			payload: `
    {
      	"bool":"INT64",
		"smallint":"STRING",
    }`,
			statusCode: http.StatusBadRequest,
		},
	}
	for _, tc := range tc {
		sessionState := session.GetSessionState()

		sessionState.Driver = constants.MYSQL
		sessionState.Conv = internal.MakeConv()
		buildConvMySQL(sessionState.Conv)
		payload := tc.payload
		req, err := http.NewRequest("POST", "/typemap/global", strings.NewReader(payload))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		handler := http.HandlerFunc(setTypeMapGlobal)
		handler.ServeHTTP(rr, req)
		var res *internal.Conv
		json.Unmarshal(rr.Body.Bytes(), &res)
		if status := rr.Code; int64(status) != tc.statusCode {
			t.Errorf("handler returned wrong status code: got %v want %v",
				status, tc.statusCode)
		}

		if tc.statusCode == http.StatusOK {
			assert.Equal(t, tc.expectedSchema, res.SpSchema["t1"])
			assert.Equal(t, tc.expectedIssues, res.Issues["t1"])
		}
	}
}

func TestGetConversionMySQL(t *testing.T) {
	sessionState := session.GetSessionState()

	sessionState.Driver = constants.MYSQL
	sessionState.Conv = internal.MakeConv()
	buildConvMySQL(sessionState.Conv)
	req, err := http.NewRequest("GET", "/conversion", nil)
	if err != nil {
		t.Fatal(err)
	}
	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(getConversionRate)
	handler.ServeHTTP(rr, req)
	var result map[string]string
	json.Unmarshal(rr.Body.Bytes(), &result)
	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusOK)
	}
	assert.Equal(t, 2, len(result))
	assert.Contains(t, result, "t1")
	assert.Contains(t, result, "t2")
}

//todo update SetParentTable with case III suggest interleve table column.
func TestSetParentTable(t *testing.T) {
	tests := []struct {
		name             string
		ct               *internal.Conv
		table            string
		statusCode       int64
		expectedResponse *TableInterleaveStatus
		expectedFKs      []ddl.Foreignkey
		parentTable      string
	}{
		{
			name:       "no conv provided",
			statusCode: http.StatusNotFound,
		},
		{
			name:       "no table name provided",
			statusCode: http.StatusBadRequest,
			ct: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{"t1": ddl.CreateTable{
					Name:     "t1",
					ColNames: []string{"a", "b", "c"},
					ColDefs: map[string]ddl.ColumnDef{"a": ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
						"b": ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
						"c": ddl.ColumnDef{Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true}},
					Pks: []ddl.IndexKey{ddl.IndexKey{Col: "a", Desc: false}},
					Fks: []ddl.Foreignkey{ddl.Foreignkey{Name: "fk1", Columns: []string{"a"}, ReferTable: "ref_t1", ReferColumns: []string{"ref_c1"}},
						ddl.Foreignkey{Name: "fk2", Columns: []string{"c"}, ReferTable: "ref_t2", ReferColumns: []string{"ref_c2"}}},
				}}},
		},
		{
			name: "table with synthetic PK",
			ct: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{"t1": ddl.CreateTable{
					Name:     "t1",
					ColNames: []string{"a", "b", "c"},
					ColDefs: map[string]ddl.ColumnDef{"a": ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
						"b":        ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
						"c":        ddl.ColumnDef{Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						"synth_id": ddl.ColumnDef{Name: "synth_id", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
					},
					Pks: []ddl.IndexKey{ddl.IndexKey{Col: "synth_id", Desc: false}},
					Fks: []ddl.Foreignkey{ddl.Foreignkey{Name: "fk1", Columns: []string{"a"}, ReferTable: "ref_t1", ReferColumns: []string{"ref_c1"}},
						ddl.Foreignkey{Name: "fk2", Columns: []string{"c"}, ReferTable: "ref_t2", ReferColumns: []string{"ref_c2"}}},
				}},
				SyntheticPKeys: map[string]internal.SyntheticPKey{"t1": internal.SyntheticPKey{Col: "synth_id"}},
			},
			table:            "t1",
			statusCode:       http.StatusOK,
			expectedResponse: &TableInterleaveStatus{Possible: false, Comment: "Has synthetic pk"},
		},
		{
			name: "no valid prefix 1",
			ct: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": ddl.CreateTable{
						Name:     "t1",
						ColNames: []string{"a", "b", "c"},
						ColDefs: map[string]ddl.ColumnDef{"a": ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"b": ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c": ddl.ColumnDef{Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						Pks: []ddl.IndexKey{ddl.IndexKey{Col: "a", Desc: false}},
						Fks: []ddl.Foreignkey{ddl.Foreignkey{Name: "fk1", Columns: []string{"a"}, ReferTable: "t2", ReferColumns: []string{"a"}}},
					},
					"t2": ddl.CreateTable{
						Name:     "t2",
						ColNames: []string{"a", "b", "c"},
						ColDefs: map[string]ddl.ColumnDef{"a": ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"b":        ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c":        ddl.ColumnDef{Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
							"synth_id": ddl.ColumnDef{Name: "synth_id", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
						},
						Pks: []ddl.IndexKey{ddl.IndexKey{Col: "synth_id", Desc: false}},
					},
				},
				SyntheticPKeys: map[string]internal.SyntheticPKey{"t2": internal.SyntheticPKey{Col: "synth_id"}},
			},
			table:            "t1",
			statusCode:       http.StatusOK,
			expectedResponse: &TableInterleaveStatus{Possible: false, Comment: "No valid prefix"},
			expectedFKs:      []ddl.Foreignkey{ddl.Foreignkey{Name: "fk1", Columns: []string{"a"}, ReferTable: "t2", ReferColumns: []string{"a"}}},
		},
		{
			name: "no valid prefix 2",
			ct: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": ddl.CreateTable{
						Name:     "t1",
						ColNames: []string{"a", "b", "c"},
						ColDefs: map[string]ddl.ColumnDef{"a": ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"b": ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c": ddl.ColumnDef{Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						Pks: []ddl.IndexKey{ddl.IndexKey{Col: "a", Desc: false}},
						Fks: []ddl.Foreignkey{ddl.Foreignkey{Name: "fk1", Columns: []string{"a"}, ReferTable: "t2", ReferColumns: []string{"a"}}},
					},
					"t2": ddl.CreateTable{
						Name:     "t2",
						ColNames: []string{"a", "b", "c"},
						ColDefs: map[string]ddl.ColumnDef{"a": ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"b": ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c": ddl.ColumnDef{Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						Pks: []ddl.IndexKey{ddl.IndexKey{Col: "a", Desc: false}, ddl.IndexKey{Col: "b", Desc: false}},
					},
				},
			},
			table:            "t1",
			statusCode:       http.StatusOK,
			expectedResponse: &TableInterleaveStatus{Possible: true, Parent: "t2", Comment: ""},
			expectedFKs:      []ddl.Foreignkey{ddl.Foreignkey{}},
		},
		{
			name: "no valid prefix 3",
			ct: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": ddl.CreateTable{
						Name:     "t1",
						ColNames: []string{"a", "b", "c"},
						ColDefs: map[string]ddl.ColumnDef{"a": ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"b": ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c": ddl.ColumnDef{Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						Pks: []ddl.IndexKey{ddl.IndexKey{Col: "a", Desc: false}, ddl.IndexKey{Col: "b", Desc: false}},
						Fks: []ddl.Foreignkey{ddl.Foreignkey{Name: "fk1", Columns: []string{"c"}, ReferTable: "t2", ReferColumns: []string{"c"}}},
					},
					"t2": ddl.CreateTable{
						Name:     "t2",
						ColNames: []string{"a", "b", "c"},
						ColDefs: map[string]ddl.ColumnDef{"a": ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"b": ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c": ddl.ColumnDef{Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						Pks: []ddl.IndexKey{ddl.IndexKey{Col: "a", Desc: false}},
					},
				},
			},
			table:            "t1",
			statusCode:       http.StatusOK,
			expectedResponse: &TableInterleaveStatus{Possible: false, Comment: "No valid prefix"},
			expectedFKs:      []ddl.Foreignkey{ddl.Foreignkey{Name: "fk1", Columns: []string{"c"}, ReferTable: "t2", ReferColumns: []string{"c"}}},
		},
		{
			name: "successful interleave",
			ct: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": ddl.CreateTable{
						Name:     "t1",
						ColNames: []string{"a", "b", "c"},
						ColDefs: map[string]ddl.ColumnDef{"a": ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"b": ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c": ddl.ColumnDef{Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						Pks: []ddl.IndexKey{ddl.IndexKey{Col: "a", Desc: false}, ddl.IndexKey{Col: "b", Desc: false}},
						Fks: []ddl.Foreignkey{ddl.Foreignkey{Name: "fk1", Columns: []string{"a"}, ReferTable: "t2", ReferColumns: []string{"a"}}},
					},
					"t2": ddl.CreateTable{
						Name:     "t2",
						ColNames: []string{"a", "b", "c"},
						ColDefs: map[string]ddl.ColumnDef{"a": ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"b": ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c": ddl.ColumnDef{Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						Pks: []ddl.IndexKey{ddl.IndexKey{Col: "a", Desc: false}},
					},
				},
			},
			table:            "t1",
			statusCode:       http.StatusOK,
			expectedResponse: &TableInterleaveStatus{Possible: true, Parent: "t2"},
			expectedFKs:      []ddl.Foreignkey{},
			parentTable:      "t2",
		},
		{
			name: "successful interleave with same primary key",
			ct: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": ddl.CreateTable{
						Name:     "t1",
						ColNames: []string{"a", "b", "c"},
						ColDefs: map[string]ddl.ColumnDef{"a": ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"b": ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c": ddl.ColumnDef{Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						Pks: []ddl.IndexKey{ddl.IndexKey{Col: "a", Desc: false}, ddl.IndexKey{Col: "b", Desc: false}},
						Fks: []ddl.Foreignkey{ddl.Foreignkey{Name: "fk1", Columns: []string{"a", "b"}, ReferTable: "t2", ReferColumns: []string{"a", "b"}}},
					},
					"t2": ddl.CreateTable{
						Name:     "t2",
						ColNames: []string{"a", "b", "c"},
						ColDefs: map[string]ddl.ColumnDef{"a": ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"b": ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c": ddl.ColumnDef{Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						Pks: []ddl.IndexKey{ddl.IndexKey{Col: "a", Desc: false}, ddl.IndexKey{Col: "b", Desc: false}},
					},
				},
			},
			table:            "t1",
			statusCode:       http.StatusOK,
			expectedResponse: &TableInterleaveStatus{Possible: true, Parent: "t2"},
			expectedFKs:      []ddl.Foreignkey{},
			parentTable:      "t2",
		},
		{
			name: "successful interleave with multiple fks refering multiple tables",
			ct: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": ddl.CreateTable{
						Name:     "t1",
						ColNames: []string{"a", "b", "c"},
						ColDefs: map[string]ddl.ColumnDef{"a": ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"b": ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c": ddl.ColumnDef{Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						Pks: []ddl.IndexKey{ddl.IndexKey{Col: "a", Desc: false}, ddl.IndexKey{Col: "b", Desc: false}},
						Fks: []ddl.Foreignkey{
							ddl.Foreignkey{Name: "fk1", Columns: []string{"c"}, ReferTable: "t3", ReferColumns: []string{"c"}},
							ddl.Foreignkey{Name: "fk1", Columns: []string{"a", "b"}, ReferTable: "t2", ReferColumns: []string{"a", "b"}}},
					},
					"t2": ddl.CreateTable{
						Name:     "t2",
						ColNames: []string{"a", "b", "c"},
						ColDefs: map[string]ddl.ColumnDef{"a": ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"b": ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c": ddl.ColumnDef{Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						Pks: []ddl.IndexKey{ddl.IndexKey{Col: "a", Desc: false}, ddl.IndexKey{Col: "b", Desc: false}},
					},
					"t3": ddl.CreateTable{
						Name:     "t3",
						ColNames: []string{"a", "b", "c"},
						ColDefs: map[string]ddl.ColumnDef{"a": ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"b": ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c": ddl.ColumnDef{Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						Pks: []ddl.IndexKey{ddl.IndexKey{Col: "c", Desc: false}},
					},
				},
			},
			table:            "t1",
			statusCode:       http.StatusOK,
			expectedResponse: &TableInterleaveStatus{Possible: true, Parent: "t2"},
			expectedFKs: []ddl.Foreignkey{
				ddl.Foreignkey{Name: "fk1", Columns: []string{"c"}, ReferTable: "t3", ReferColumns: []string{"c"}}},
			parentTable: "t2",
		},
	}
	for _, tc := range tests {
		sessionState := session.GetSessionState()

		sessionState.Driver = constants.MYSQL
		sessionState.Conv = tc.ct
		update := true
		req, err := http.NewRequest("GET", fmt.Sprintf("/setparent?table=%s&update=%v", tc.table, update), nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		handler := http.HandlerFunc(setParentTable)
		handler.ServeHTTP(rr, req)

		type ParentTableSetResponse struct {
			TableInterleaveStatus *TableInterleaveStatus `json:"tableInterleaveStatus"`
			SessionState          *internal.Conv         `json:"sessionState"`
		}

		var res *TableInterleaveStatus

		if update {
			parentTableResponse := &ParentTableSetResponse{}
			json.Unmarshal(rr.Body.Bytes(), parentTableResponse)
			res = parentTableResponse.TableInterleaveStatus
		} else {
			res = &TableInterleaveStatus{}
			json.Unmarshal(rr.Body.Bytes(), res)
		}

		if status := rr.Code; int64(status) != tc.statusCode {
			t.Errorf("%s\nhandler returned wrong status code: got %v want %v",
				tc.name, status, tc.statusCode)
		}
		if tc.statusCode == http.StatusOK {
			assert.Equal(t, tc.expectedResponse, res, tc.name)
		}
		if tc.parentTable != "" {
			assert.Equal(t, tc.parentTable, sessionState.Conv.SpSchema[tc.table].Parent, tc.name)
			assert.Equal(t, tc.expectedFKs, sessionState.Conv.SpSchema[tc.table].Fks, tc.name)
		}
	}
}
func TestDropForeignKey(t *testing.T) {
	tc := []struct {
		name         string
		table        string
		position     string
		statusCode   int64
		conv         *internal.Conv
		expectedConv *internal.Conv
	}{
		{
			name:       "Test drop valid FK success",
			table:      "t1",
			position:   "1",
			statusCode: http.StatusOK,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Fks: []ddl.Foreignkey{{Name: "fk1", Columns: []string{"b"}, ReferTable: "reft1", ReferColumns: []string{"ref_b"}},
							{Name: "fk2", Columns: []string{"c", "d"}, ReferTable: "reft2", ReferColumns: []string{"ref_c", "ref_d"}}},
					}},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Fks: []ddl.Foreignkey{{Name: "fk1", Columns: []string{"b"}, ReferTable: "reft1", ReferColumns: []string{"ref_b"}}},
					}},
			},
		},
		{
			name:       "Test drop FK invalid position",
			table:      "t1",
			position:   "1",
			statusCode: http.StatusBadRequest,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Fks: []ddl.Foreignkey{{Name: "fk1", Columns: []string{"b"}, ReferTable: "reft1", ReferColumns: []string{"ref_b"}}},
					}},
			},
		},
		{
			name:       "Test drop FK invalid position 2",
			table:      "t1",
			position:   "AB",
			statusCode: http.StatusBadRequest,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Fks: []ddl.Foreignkey{{Name: "fk1", Columns: []string{"b"}, ReferTable: "reft1", ReferColumns: []string{"ref_b"}}},
					}},
			},
		},
	}
	for _, tc := range tc {
		sessionState := session.GetSessionState()

		sessionState.Driver = constants.MYSQL
		sessionState.Conv = tc.conv
		req, err := http.NewRequest("GET", "/drop/fk?table="+tc.table+"&pos="+tc.position, nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		handler := http.HandlerFunc(dropForeignKey)
		handler.ServeHTTP(rr, req)
		var res *internal.Conv
		json.Unmarshal(rr.Body.Bytes(), &res)
		if status := rr.Code; int64(status) != tc.statusCode {
			t.Errorf("handler returned wrong status code: got %v want %v",
				status, tc.statusCode)
		}
		if tc.statusCode == http.StatusOK {
			assert.Equal(t, tc.expectedConv, res)
		}
	}
}

func TestRenameIndexes(t *testing.T) {
	tc := []struct {
		name         string
		table        string
		input        interface{}
		statusCode   int64
		conv         *internal.Conv
		expectedConv *internal.Conv
	}{
		{
			name:  "Test rename indexes",
			table: "t1",
			input: map[string]string{
				"idx": "idx_new",
			},
			statusCode: http.StatusOK,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}}},
					}},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx_new", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}}},
					}},
			},
		},
		{
			name:  "Test rename multiple indexes",
			table: "t1",
			input: map[string]string{
				"idx_1": "idx_new_1",
				"idx_2": "idx_new_2",
			},
			statusCode: http.StatusOK,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx_1", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}},
							{Name: "idx_2", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}}},
					}},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx_new_1", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}},
							{Name: "idx_new_2", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}}},
					}},
			},
		},
		{
			name:  "New name conflicts with an existing table",
			table: "t1",
			input: map[string]string{
				"idx_1": "t1",
			},
			statusCode: http.StatusBadRequest,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx_1", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}},
							{Name: "idx_2", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}}},
					}},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx_1", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}},
							{Name: "idx_2", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}}},
					}},
			},
		},
		{
			name:  "New name conflicts with an existing index",
			table: "t1",
			input: map[string]string{
				"idx_1": "idx_2",
			},
			statusCode: http.StatusBadRequest,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx_1", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}},
							{Name: "idx_2", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}}},
					}},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx_new_1", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}},
							{Name: "idx_new_2", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}}},
					}},
			},
		},
		{
			name:  "New name conflicts with an existing foreign key",
			table: "t1",
			input: map[string]string{
				"idx_1": "fk1",
			},
			statusCode: http.StatusBadRequest,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx_1", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}},
							{Name: "idx_2", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}}},
						Fks: []ddl.Foreignkey{{Name: "fk1", Columns: []string{"b"}, ReferTable: "reft1", ReferColumns: []string{"ref_b"}},
							{Name: "fk2", Columns: []string{"c", "d"}, ReferTable: "reft2", ReferColumns: []string{"ref_c", "ref_d"}}},
					}},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx_new_1", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}},
							{Name: "idx_new_2", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}}},
					}},
			},
		},
		{
			name:  "Given Index not available",
			table: "t1",
			input: map[string]string{
				"idx_new": "idx",
			},
			statusCode: http.StatusBadRequest,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}}},
					}},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}}},
					}},
			},
		},
		{
			name:  "Conflicts within new name array",
			table: "t1",
			input: map[string]string{
				"idx1": "idx_100",
				"idx2": "idx_100",
			},
			statusCode: http.StatusBadRequest,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx1", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}},
							{Name: "idx2", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}}},
					}},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx1", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}},
							{Name: "idx2", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}}},
					}},
			},
		},
		{
			name:       "Input Empty Map ",
			table:      "t1",
			input:      map[string]string{},
			statusCode: http.StatusOK,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}}},
					}},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}}},
					}},
			},
		},
		{
			name:       "Invalid input",
			table:      "t1",
			input:      []string{"test1", "test2"},
			statusCode: http.StatusBadRequest,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}}},
					}},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}}},
					}},
			},
		},
	}

	for _, tc := range tc {
		sessionState := session.GetSessionState()

		sessionState.Driver = constants.MYSQL
		sessionState.Conv = tc.conv

		inputBytes, err := json.Marshal(tc.input)
		if err != nil {
			t.Fatal(err)
		}
		buffer := bytes.NewBuffer(inputBytes)

		req, err := http.NewRequest("POST", "/rename/indexes?table="+tc.table, buffer)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		handler := http.HandlerFunc(renameIndexes)
		handler.ServeHTTP(rr, req)
		var res *internal.Conv
		json.Unmarshal(rr.Body.Bytes(), &res)
		if status := rr.Code; int64(status) != tc.statusCode {
			t.Errorf("%s : handler returned wrong status code: got %v want %v",
				tc.name, status, tc.statusCode)
		}
		if tc.statusCode == http.StatusOK {
			assert.Equal(t, tc.expectedConv, res)
		}
	}
}

func TestRenameForeignKeys(t *testing.T) {
	tc := []struct {
		name         string
		table        string
		input        interface{}
		statusCode   int64
		conv         *internal.Conv
		expectedConv *internal.Conv
	}{
		{
			name:  "Test rename foreignkey",
			table: "t1",
			input: map[string]string{
				"fk1": "foreignkey1",
			},
			statusCode: http.StatusOK,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Fks: []ddl.Foreignkey{{Name: "fk1", Columns: []string{"b"}, ReferTable: "reft1", ReferColumns: []string{"ref_b"}},
							{Name: "fk2", Columns: []string{"c", "d"}, ReferTable: "reft2", ReferColumns: []string{"ref_c", "ref_d"}}},
					}},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Fks: []ddl.Foreignkey{{Name: "foreignkey1", Columns: []string{"b"}, ReferTable: "reft1", ReferColumns: []string{"ref_b"}},
							{Name: "fk2", Columns: []string{"c", "d"}, ReferTable: "reft2", ReferColumns: []string{"ref_c", "ref_d"}}},
					}},
			},
		},
		{
			name:  "Test rename multiple foreignkeys",
			table: "t1",
			input: map[string]string{
				"fk1": "foreignkey1",
				"fk2": "foreignkey2",
			},
			statusCode: http.StatusOK,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Fks: []ddl.Foreignkey{{Name: "fk1", Columns: []string{"b"}, ReferTable: "reft1", ReferColumns: []string{"ref_b"}},
							{Name: "fk2", Columns: []string{"c", "d"}, ReferTable: "reft2", ReferColumns: []string{"ref_c", "ref_d"}}},
					}},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Fks: []ddl.Foreignkey{{Name: "foreignkey1", Columns: []string{"b"}, ReferTable: "reft1", ReferColumns: []string{"ref_b"}},
							{Name: "foreignkey2", Columns: []string{"c", "d"}, ReferTable: "reft2", ReferColumns: []string{"ref_c", "ref_d"}}},
					}},
			},
		},
		{
			name:  "New name conflicts with an existing table",
			table: "t1",
			input: map[string]string{
				"fk1": "t1",
			},
			statusCode: http.StatusBadRequest,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Fks: []ddl.Foreignkey{{Name: "fk1", Columns: []string{"b"}, ReferTable: "reft1", ReferColumns: []string{"ref_b"}},
							{Name: "fk2", Columns: []string{"c", "d"}, ReferTable: "reft2", ReferColumns: []string{"ref_c", "ref_d"}}},
					}},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Fks: []ddl.Foreignkey{{Name: "foreignkey1", Columns: []string{"b"}, ReferTable: "reft1", ReferColumns: []string{"ref_b"}},
							{Name: "fk2", Columns: []string{"c", "d"}, ReferTable: "reft2", ReferColumns: []string{"ref_c", "ref_d"}}},
					}},
			},
		},
		{
			name:  "New name conflicts with an existing foreignkey",
			table: "t1",
			input: map[string]string{
				"fk1": "fk2",
			},
			statusCode: http.StatusBadRequest,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Fks: []ddl.Foreignkey{{Name: "fk1", Columns: []string{"b"}, ReferTable: "reft1", ReferColumns: []string{"ref_b"}},
							{Name: "fk2", Columns: []string{"c", "d"}, ReferTable: "reft2", ReferColumns: []string{"ref_c", "ref_d"}}},
					}},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Fks: []ddl.Foreignkey{{Name: "foreignkey1", Columns: []string{"b"}, ReferTable: "reft1", ReferColumns: []string{"ref_b"}},
							{Name: "fk2", Columns: []string{"c", "d"}, ReferTable: "reft2", ReferColumns: []string{"ref_c", "ref_d"}}},
					}},
			},
		},
		{
			name:  "New name conflicts with an existing indexes",
			table: "t1",
			input: map[string]string{
				"fk1": "idx_1",
			},
			statusCode: http.StatusBadRequest,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx_1", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}},
							{Name: "idx_2", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}}},
						Fks: []ddl.Foreignkey{{Name: "fk1", Columns: []string{"b"}, ReferTable: "reft1", ReferColumns: []string{"ref_b"}},
							{Name: "fk2", Columns: []string{"c", "d"}, ReferTable: "reft2", ReferColumns: []string{"ref_c", "ref_d"}}},
					}},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx_new_1", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}},
							{Name: "idx_new_2", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}}},
					}},
			},
		},
		{
			name:  "Conflicts within new name array",
			table: "t1",
			input: map[string]string{
				"idx1": "idx_100",
				"idx2": "idx_100",
			},
			statusCode: http.StatusBadRequest,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx1", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}},
							{Name: "idx2", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}}},
					}},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx1", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}},
							{Name: "idx2", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}}},
					}},
			},
		},
		{
			name:  "Given Foreignkey not available ",
			table: "t1",
			input: map[string]string{
				"fkx": "foreignkeyx",
			},
			statusCode: http.StatusOK,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Fks: []ddl.Foreignkey{{Name: "fk1", Columns: []string{"b"}, ReferTable: "reft1", ReferColumns: []string{"ref_b"}},
							{Name: "fk2", Columns: []string{"c", "d"}, ReferTable: "reft2", ReferColumns: []string{"ref_c", "ref_d"}}},
					}},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Fks: []ddl.Foreignkey{{Name: "fk1", Columns: []string{"b"}, ReferTable: "reft1", ReferColumns: []string{"ref_b"}},
							{Name: "fk2", Columns: []string{"c", "d"}, ReferTable: "reft2", ReferColumns: []string{"ref_c", "ref_d"}}},
					}},
			},
		},
		{
			name:       "Input Empty Map ",
			table:      "t1",
			input:      map[string]string{},
			statusCode: http.StatusOK,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Fks: []ddl.Foreignkey{{Name: "fk1", Columns: []string{"b"}, ReferTable: "reft1", ReferColumns: []string{"ref_b"}},
							{Name: "fk2", Columns: []string{"c", "d"}, ReferTable: "reft2", ReferColumns: []string{"ref_c", "ref_d"}}},
					}},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Fks: []ddl.Foreignkey{{Name: "fk1", Columns: []string{"b"}, ReferTable: "reft1", ReferColumns: []string{"ref_b"}},
							{Name: "fk2", Columns: []string{"c", "d"}, ReferTable: "reft2", ReferColumns: []string{"ref_c", "ref_d"}}},
					}},
			},
		},
		{
			name:       "Invalid input",
			table:      "t1",
			input:      []string{"test1", "test2"},
			statusCode: http.StatusBadRequest,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Fks: []ddl.Foreignkey{{Name: "fk1", Columns: []string{"b"}, ReferTable: "reft1", ReferColumns: []string{"ref_b"}},
							{Name: "fk2", Columns: []string{"c", "d"}, ReferTable: "reft2", ReferColumns: []string{"ref_c", "ref_d"}}},
					}},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Fks: []ddl.Foreignkey{{Name: "fk1", Columns: []string{"b"}, ReferTable: "reft1", ReferColumns: []string{"ref_b"}},
							{Name: "fk2", Columns: []string{"c", "d"}, ReferTable: "reft2", ReferColumns: []string{"ref_c", "ref_d"}}},
					}},
			},
		},
		{
			name:       "Check non usage in another table",
			table:      "t1",
			input:      map[string]string{"fk1": "t2_fk2"},
			statusCode: http.StatusBadRequest,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Fks: []ddl.Foreignkey{{Name: "fk1", Columns: []string{"b"}, ReferTable: "reft1", ReferColumns: []string{"ref_b"}},
							{Name: "fk2", Columns: []string{"c", "d"}, ReferTable: "reft2", ReferColumns: []string{"ref_c", "ref_d"}}},
					},
					"t2": {
						Fks: []ddl.Foreignkey{{Name: "t2_fk1", Columns: []string{"b"}, ReferTable: "reft1", ReferColumns: []string{"ref_b"}},
							{Name: "t2_fk2", Columns: []string{"c", "d"}, ReferTable: "reft2", ReferColumns: []string{"ref_c", "ref_d"}}},
					}},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Fks: []ddl.Foreignkey{{Name: "fk1", Columns: []string{"b"}, ReferTable: "reft1", ReferColumns: []string{"ref_b"}},
							{Name: "fk2", Columns: []string{"c", "d"}, ReferTable: "reft2", ReferColumns: []string{"ref_c", "ref_d"}}},
					},
					"t2": {
						Fks: []ddl.Foreignkey{{Name: "t2_fk1", Columns: []string{"b"}, ReferTable: "reft1", ReferColumns: []string{"ref_b"}},
							{Name: "t2_fk2", Columns: []string{"c", "d"}, ReferTable: "reft2", ReferColumns: []string{"ref_c", "ref_d"}}},
					}},
			},
		},
	}
	for _, tc := range tc {
		sessionState := session.GetSessionState()

		sessionState.Driver = constants.MYSQL
		sessionState.Conv = tc.conv

		inputBytes, err := json.Marshal(tc.input)
		if err != nil {
			t.Fatal(err)
		}
		buffer := bytes.NewBuffer(inputBytes)

		req, err := http.NewRequest("POST", "/rename/fks?table="+tc.table, buffer)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		handler := http.HandlerFunc(renameForeignKeys)
		handler.ServeHTTP(rr, req)
		var res *internal.Conv
		json.Unmarshal(rr.Body.Bytes(), &res)
		if status := rr.Code; int64(status) != tc.statusCode {
			t.Errorf("%s : handler returned wrong status code: got %v want %v",
				tc.name, status, tc.statusCode)
		}
		if tc.statusCode == http.StatusOK {
			assert.Equal(t, tc.expectedConv, res)
		}
	}
}

func TestAddIndexes(t *testing.T) {
	tc := []struct {
		name         string
		table        string
		input        interface{}
		statusCode   int64
		conv         *internal.Conv
		expectedConv *internal.Conv
	}{
		{
			name:       "Test Empty input",
			table:      "t1",
			input:      []ddl.CreateIndex{},
			statusCode: http.StatusOK,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx1", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}},
							{Name: "idx2", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "c", Desc: false}, {Col: "d", Desc: false}}}},
					}},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx1", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}},
							{Name: "idx2", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "c", Desc: false}, {Col: "d", Desc: false}}}},
					}},
			},
		},
		{
			name:  "Add Index with unique name",
			table: "t1",
			input: []ddl.CreateIndex{
				{Name: "idx3", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}},
			},
			statusCode: http.StatusOK,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx1", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}},
							{Name: "idx2", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "c", Desc: false}, {Col: "d", Desc: false}}}},
					}},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx1", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}},
							{Name: "idx2", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "c", Desc: false}, {Col: "d", Desc: false}}},
							{Name: "idx3", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}},
						},
					}},
			},
		},
		{
			name:  "Add multiple indexes",
			table: "t1",
			input: []ddl.CreateIndex{
				{Name: "idx3", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}},
				{Name: "idx4", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}},
			},
			statusCode: http.StatusOK,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx1", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}},
							{Name: "idx2", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "c", Desc: false}, {Col: "d", Desc: false}}}},
					}},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx1", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}},
							{Name: "idx2", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "c", Desc: false}, {Col: "d", Desc: false}}},
							{Name: "idx3", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}},
							{Name: "idx4", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}},
						},
					}},
			},
		},
		{
			name:  "New name conflicts with an existing table",
			table: "t1",
			input: []ddl.CreateIndex{
				{Name: "t1", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}},
				{Name: "idx4", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}},
			},
			statusCode: http.StatusBadRequest,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx1", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}},
							{Name: "idx2", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "c", Desc: false}, {Col: "d", Desc: false}}}},
					}},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx1", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}},
							{Name: "idx2", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "c", Desc: false}, {Col: "d", Desc: false}}},
							{Name: "t1", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}},
							{Name: "idx4", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}},
						},
					}},
			},
		},
		{
			name:  "New name conflicts with an existing index",
			table: "t1",
			input: []ddl.CreateIndex{
				{Name: "idx2", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}},
			},
			statusCode: http.StatusBadRequest,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx1", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}},
							{Name: "idx2", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "c", Desc: false}, {Col: "d", Desc: false}}}},
					}},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx1", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}},
							{Name: "idx2", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "c", Desc: false}, {Col: "d", Desc: false}}},
							{Name: "idx3", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}},
							{Name: "idx4", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}},
						},
					}},
			},
		},
		{
			name:  "Conflicts within new name array",
			table: "t1",
			input: []ddl.CreateIndex{
				{Name: "idx2", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}},
				{Name: "idx2", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}},
			},
			statusCode: http.StatusBadRequest,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx1", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}},
							{Name: "idx2", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}}},
					}},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx1", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}},
							{Name: "idx2", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}}},
					}},
			},
		},
		{
			name:  "Add Index with same name",
			table: "t1",
			input: []ddl.CreateIndex{
				{Name: "idx2", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}},
			},
			statusCode: http.StatusBadRequest,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx1", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}},
							{Name: "idx2", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "c", Desc: false}, {Col: "d", Desc: false}}}},
					}},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx1", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}},
							{Name: "idx2", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "c", Desc: false}, {Col: "d", Desc: false}}},
						},
					}},
			},
		},
		{
			name:       "Invalid input",
			table:      "t1",
			input:      []string{"test1"},
			statusCode: http.StatusBadRequest,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx1", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}},
							{Name: "idx2", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "c", Desc: false}, {Col: "d", Desc: false}}}},
					}},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx1", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}},
							{Name: "idx2", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "c", Desc: false}, {Col: "d", Desc: false}}},
						},
					}},
			},
		},
	}

	for _, tc := range tc {
		sessionState := session.GetSessionState()

		sessionState.Driver = constants.MYSQL
		sessionState.Conv = tc.conv

		inputBytes, err := json.Marshal(tc.input)
		if err != nil {
			t.Fatal(err)
		}
		buffer := bytes.NewBuffer(inputBytes)

		req, err := http.NewRequest("POST", "/add/indexes?table="+tc.table, buffer)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		handler := http.HandlerFunc(addIndexes)
		handler.ServeHTTP(rr, req)
		var res *internal.Conv
		json.Unmarshal(rr.Body.Bytes(), &res)
		if status := rr.Code; int64(status) != tc.statusCode {
			t.Errorf("%s : handler returned wrong status code: got %v want %v",
				tc.name, status, tc.statusCode)
		}
		if tc.statusCode == http.StatusOK {
			assert.Equal(t, tc.expectedConv, res)
		}
	}
}

func TestDropSecondaryIndex(t *testing.T) {
	tc := []struct {
		name         string
		table        string
		position     string
		statusCode   int64
		conv         *internal.Conv
		expectedConv *internal.Conv
	}{
		{
			name:       "Test drop valid secondary index success",
			table:      "t1",
			position:   "1",
			statusCode: http.StatusOK,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx1", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}},
							{Name: "idx2", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "c", Desc: false}, {Col: "d", Desc: false}}}},
					}},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx1", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}}},
					}},
			},
		},
		{
			name:       "Test drop secondary index invalid position",
			table:      "t1",
			position:   "1",
			statusCode: http.StatusBadRequest,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx1", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}}},
					}},
			},
		},
		{
			name:       "Test drop secondary index invalid position 2",
			table:      "t1",
			position:   "AB",
			statusCode: http.StatusBadRequest,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx1", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}}},
					}},
			},
		},
	}
	for _, tc := range tc {
		sessionState := session.GetSessionState()

		sessionState.Driver = constants.MYSQL
		sessionState.Conv = tc.conv
		req, err := http.NewRequest("GET", "/drop/secondaryindex?table="+tc.table+"&pos="+tc.position, nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		handler := http.HandlerFunc(dropSecondaryIndex)
		handler.ServeHTTP(rr, req)
		var res *internal.Conv
		json.Unmarshal(rr.Body.Bytes(), &res)
		if status := rr.Code; int64(status) != tc.statusCode {
			t.Errorf("handler returned wrong status code: got %v want %v",
				status, tc.statusCode)
		}
		if tc.statusCode == http.StatusOK {
			assert.Equal(t, tc.expectedConv, res)
		}
	}
}

func buildConvMySQL(conv *internal.Conv) {
	conv.SrcSchema = map[string]schema.Table{
		"t1": {
			Name:     "t1",
			ColNames: []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p"},
			ColDefs: map[string]schema.Column{
				"a": {Name: "a", Type: schema.Type{Name: "bool"}},
				"b": {Name: "b", Type: schema.Type{Name: "text"}},
				"c": {Name: "c", Type: schema.Type{Name: "bool"}},
				"d": {Name: "d", Type: schema.Type{Name: "varchar", Mods: []int64{6}}},
				"e": {Name: "e", Type: schema.Type{Name: "numeric"}},
				"f": {Name: "f", Type: schema.Type{Name: "enum"}},
				"g": {Name: "g", Type: schema.Type{Name: "json"}},
				"h": {Name: "h", Type: schema.Type{Name: "binary"}},
				"i": {Name: "i", Type: schema.Type{Name: "blob"}},
				"j": {Name: "j", Type: schema.Type{Name: "smallint"}},
				"k": {Name: "k", Type: schema.Type{Name: "double"}},
				"l": {Name: "l", Type: schema.Type{Name: "float"}},
				"m": {Name: "m", Type: schema.Type{Name: "decimal"}},
				"n": {Name: "n", Type: schema.Type{Name: "date"}},
				"o": {Name: "o", Type: schema.Type{Name: "timestamp"}},
				"p": {Name: "p", Type: schema.Type{Name: "time"}},
			},
			PrimaryKeys: []schema.Key{{Column: "a"}}},
		"t2": {
			Name:     "t2",
			ColNames: []string{"a", "b", "c"},
			ColDefs: map[string]schema.Column{
				"a": {Name: "a", Type: schema.Type{Name: "integer"}},
				"b": {Name: "b", Type: schema.Type{Name: "double"}},
				"c": {Name: "c", Type: schema.Type{Name: "bool"}},
			}},
	}
	conv.SpSchema = map[string]ddl.CreateTable{
		"t1": {
			Name:     "t1",
			ColNames: []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p"},
			ColDefs: map[string]ddl.ColumnDef{
				"a": {Name: "a", T: ddl.Type{Name: ddl.Bool}},
				"b": {Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
				"c": {Name: "c", T: ddl.Type{Name: ddl.Bool}},
				"d": {Name: "d", T: ddl.Type{Name: ddl.String, Len: int64(6)}},
				"e": {Name: "e", T: ddl.Type{Name: ddl.Numeric}},
				"f": {Name: "f", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
				"g": {Name: "g", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
				"h": {Name: "h", T: ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}},
				"i": {Name: "i", T: ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}},
				"j": {Name: "j", T: ddl.Type{Name: ddl.Int64}},
				"k": {Name: "k", T: ddl.Type{Name: ddl.Float64}},
				"l": {Name: "l", T: ddl.Type{Name: ddl.Float64}},
				"m": {Name: "m", T: ddl.Type{Name: ddl.Numeric}},
				"n": {Name: "n", T: ddl.Type{Name: ddl.Date}},
				"o": {Name: "o", T: ddl.Type{Name: ddl.Timestamp}},
				"p": {Name: "p", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
			},
			Pks: []ddl.IndexKey{{Col: "a"}},
		},
		"t2": {
			Name:     "t2",
			ColNames: []string{"a", "b", "c", "synth_id"},
			ColDefs: map[string]ddl.ColumnDef{
				"a":        {Name: "a", T: ddl.Type{Name: ddl.Int64}},
				"b":        {Name: "b", T: ddl.Type{Name: ddl.Float64}},
				"c":        {Name: "c", T: ddl.Type{Name: ddl.Bool}},
				"synth_id": {Name: "synth_id", T: ddl.Type{Name: ddl.Int64}},
			},
			Pks: []ddl.IndexKey{{Col: "synth_id"}},
		},
	}
	conv.ToSource = map[string]internal.NameAndCols{
		"t1": {
			Name: "t1",
			Cols: map[string]string{
				"a": "a", "b": "b", "c": "c", "d": "d", "e": "e", "f": "f", "g": "g", "h": "h", "i": "i", "j": "j", "k": "k", "l": "l", "m": "m", "n": "n", "o": "o", "p": "p",
			}},
		"t2": {
			Name: "t2",
			Cols: map[string]string{
				"a": "a", "b": "b", "c": "c",
			}},
	}
	conv.ToSpanner = map[string]internal.NameAndCols{
		"t1": {
			Name: "t1",
			Cols: map[string]string{
				"a": "a", "b": "b", "c": "c", "d": "d", "e": "e", "f": "f", "g": "g", "h": "h", "i": "i", "j": "j", "k": "k", "l": "l", "m": "m", "n": "n", "o": "o", "p": "p",
			}},
		"t2": {
			Name: "t2",
			Cols: map[string]string{
				"a": "a", "b": "b", "c": "c",
			}},
	}
	conv.Issues = map[string]map[string][]internal.SchemaIssue{
		"t1": {
			"j": {internal.Widened},
			"l": {internal.Widened},
			"o": {internal.Time},
		},
		"t2": {
			"a": {internal.Widened},
		},
	}
	conv.SyntheticPKeys["t2"] = internal.SyntheticPKey{"synth_id", 0}
}

func buildConvPostgres(conv *internal.Conv) {
	conv.SrcSchema = map[string]schema.Table{
		"t1": {
			Name:     "t1",
			ColNames: []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p"},
			ColDefs: map[string]schema.Column{
				"a": {Name: "a", Type: schema.Type{Name: "int8"}},
				"b": {Name: "b", Type: schema.Type{Name: "float4"}},
				"c": {Name: "c", Type: schema.Type{Name: "bool"}},
				"d": {Name: "d", Type: schema.Type{Name: "varchar", Mods: []int64{6}}},
				"e": {Name: "e", Type: schema.Type{Name: "numeric"}},
				"f": {Name: "f", Type: schema.Type{Name: "timestamptz"}},
				"g": {Name: "g", Type: schema.Type{Name: "bigserial"}},
				"h": {Name: "h", Type: schema.Type{Name: "bpchar"}},
				"i": {Name: "i", Type: schema.Type{Name: "bytea"}},
				"j": {Name: "j", Type: schema.Type{Name: "date"}},
				"k": {Name: "k", Type: schema.Type{Name: "float8"}},
				"l": {Name: "l", Type: schema.Type{Name: "int4"}},
				"m": {Name: "m", Type: schema.Type{Name: "serial"}},
				"n": {Name: "n", Type: schema.Type{Name: "text"}},
				"o": {Name: "o", Type: schema.Type{Name: "timestamp"}},
				"p": {Name: "p", Type: schema.Type{Name: "bool"}},
			},
			PrimaryKeys: []schema.Key{{Column: "a"}}},
		"t2": {
			Name:     "t2",
			ColNames: []string{"a", "b", "c"},
			ColDefs: map[string]schema.Column{
				"a": {Name: "a", Type: schema.Type{Name: "int8"}},
				"b": {Name: "b", Type: schema.Type{Name: "float4"}},
				"c": {Name: "c", Type: schema.Type{Name: "bool"}},
			}},
	}
	conv.SpSchema = map[string]ddl.CreateTable{
		"t1": {
			Name:     "t1",
			ColNames: []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p"},
			ColDefs: map[string]ddl.ColumnDef{
				"a": {Name: "a", T: ddl.Type{Name: ddl.Int64}},
				"b": {Name: "b", T: ddl.Type{Name: ddl.Float64}},
				"c": {Name: "c", T: ddl.Type{Name: ddl.Bool}},
				"d": {Name: "d", T: ddl.Type{Name: ddl.String, Len: int64(6)}},
				"e": {Name: "e", T: ddl.Type{Name: ddl.Numeric}},
				"f": {Name: "f", T: ddl.Type{Name: ddl.Timestamp}},
				"g": {Name: "g", T: ddl.Type{Name: ddl.Int64}},
				"h": {Name: "h", T: ddl.Type{Name: ddl.String, Len: int64(1)}},
				"i": {Name: "i", T: ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}},
				"j": {Name: "j", T: ddl.Type{Name: ddl.Date}},
				"k": {Name: "k", T: ddl.Type{Name: ddl.Float64}},
				"l": {Name: "l", T: ddl.Type{Name: ddl.Int64}},
				"m": {Name: "m", T: ddl.Type{Name: ddl.Int64}},
				"n": {Name: "n", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
				"o": {Name: "o", T: ddl.Type{Name: ddl.Timestamp}},
				"p": {Name: "p", T: ddl.Type{Name: ddl.Int64}},
			},
			Pks: []ddl.IndexKey{{Col: "a"}},
		},
		"t2": {
			Name:     "t2",
			ColNames: []string{"a", "b", "c", "synth_id"},
			ColDefs: map[string]ddl.ColumnDef{
				"a":        {Name: "a", T: ddl.Type{Name: ddl.Int64}},
				"b":        {Name: "b", T: ddl.Type{Name: ddl.Float64}},
				"c":        {Name: "c", T: ddl.Type{Name: ddl.Bool}},
				"synth_id": {Name: "synth_id", T: ddl.Type{Name: ddl.Int64}},
			},
			Pks: []ddl.IndexKey{{Col: "synth_id"}},
		},
	}
	conv.ToSource = map[string]internal.NameAndCols{
		"t1": {
			Name: "t1",
			Cols: map[string]string{
				"a": "a", "b": "b", "c": "c", "d": "d", "e": "e", "f": "f", "g": "g", "h": "h", "i": "i", "j": "j", "k": "k", "l": "l", "m": "m", "n": "n", "o": "o", "p": "p",
			}},
		"t2": {
			Name: "t2",
			Cols: map[string]string{
				"a": "a", "b": "b", "c": "c",
			}},
	}
	conv.ToSpanner = map[string]internal.NameAndCols{
		"t1": {
			Name: "t1",
			Cols: map[string]string{
				"a": "a", "b": "b", "c": "c", "d": "d", "e": "e", "f": "f", "g": "g", "h": "h", "i": "i", "j": "j", "k": "k", "l": "l", "m": "m", "n": "n", "o": "o", "p": "p",
			}},
		"t2": {
			Name: "t2",
			Cols: map[string]string{
				"a": "a", "b": "b", "c": "c",
			}},
	}
	conv.Issues = map[string]map[string][]internal.SchemaIssue{
		"t1": {
			"b": {internal.Widened},
			"g": {internal.Serial},
			"l": {internal.Widened},
			"m": {internal.Serial},
			"o": {internal.Timestamp},
		},
		"t2": {
			"b": {internal.Widened},
		},
	}
	conv.SyntheticPKeys["t2"] = internal.SyntheticPKey{"synth_id", 0}
}

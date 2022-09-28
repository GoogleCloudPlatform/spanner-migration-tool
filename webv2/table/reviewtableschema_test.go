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

package table

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bmizerany/assert"
	"github.com/cloudspannerecosystem/harbourbridge/common/constants"
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/proto/migration"
	"github.com/cloudspannerecosystem/harbourbridge/schema"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
	"github.com/cloudspannerecosystem/harbourbridge/webv2/session"
)

func TestReviewTableSchemachangetype(t *testing.T) {

	tc := []struct {
		name         string
		table        string
		payload      string
		statusCode   int64
		conv         *internal.Conv
		expectedConv *internal.Conv
	}{
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
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_MIGRATION_TYPE_UNSPECIFIED.Enum(),
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
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_MIGRATION_TYPE_UNSPECIFIED.Enum(),
				},
			},
		},
	}

	for _, tc := range tc {

		sessionState := session.GetSessionState()
		sessionState.Conv = tc.conv
		sessionState.Driver = constants.MYSQL

		payload := tc.payload

		req, err := http.NewRequest("POST", "/typemap/reviewtableschema?table="+tc.table, strings.NewReader(payload))
		if err != nil {
			t.Fatal(err)
		}

		req.Header.Set("Content-Type", "application/json")

		rr := httptest.NewRecorder()

		handler := http.HandlerFunc(ReviewTableSchema)

		handler.ServeHTTP(rr, req)

		res := ReviewTableSchemaResponse{}

		json.Unmarshal(rr.Body.Bytes(), &res)

		if status := rr.Code; int64(status) != tc.statusCode {
			t.Errorf("handler returned wrong status code: got %v want %v",
				status, tc.statusCode)
		}

		expectedddl := GetSpannerTableDDL(tc.expectedConv.SpSchema[tc.table])

		if tc.statusCode == http.StatusOK {
			assert.Equal(t, expectedddl, res.DDL)
		}
	}
}

func TestReviewTableSchemaAddsuccess(t *testing.T) {

	tc := []struct {
		name         string
		table        string
		payload      string
		statusCode   int64
		conv         *internal.Conv
		expectedConv *internal.Conv
	}{
		{
			name:  "Test Add success",
			table: "t1",
			payload: `
    {
      "UpdateCols":{
		"c": { "Add": true, "ToType": "STRING"}
	}
    }`,
			statusCode: http.StatusOK,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:     "t1",
						Id:       "t1",
						ColNames: []string{"a", "b"},
						ColDefs: map[string]ddl.ColumnDef{
							"a": {Id: "c2", Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
							"b": {Id: "c3", Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
						},
						Pks: []ddl.IndexKey{{Col: "a"}},
					}},
				SrcSchema: map[string]schema.Table{
					"t1": {
						Id:       "t1",
						Name:     "t1",
						ColNames: []string{"a", "b"},
						ColDefs: map[string]schema.Column{
							"a": {Id: "c2", Name: "a", Type: schema.Type{Name: "varchar", Mods: []int64{6}}},
							"b": {Id: "c3", Name: "b", Type: schema.Type{Name: "varchar", Mods: []int64{6}}},
							"c": {Id: "c4", Name: "c", Type: schema.Type{Name: "varchar", Mods: []int64{}}},
						},
						PrimaryKeys: []schema.Key{{Column: "a"}},
					}},

				ToSource: map[string]internal.NameAndCols{
					"t1": {Name: "t1", Cols: map[string]string{"a": "a", "b": "b"}},
				},
				ToSpanner: map[string]internal.NameAndCols{
					"t1": {Name: "t1", Cols: map[string]string{"a": "a", "b": "b"}},
				},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_MIGRATION_TYPE_UNSPECIFIED.Enum(),
				},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Id:       "t1",
						Name:     "t1",
						ColNames: []string{"a", "b", "c"},
						ColDefs: map[string]ddl.ColumnDef{
							"a": {Id: "c2", Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
							"b": {Id: "c3", Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
							"c": {Id: "c4", Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
						},
						Pks: []ddl.IndexKey{{Col: "a"}},
					}},
				SrcSchema: map[string]schema.Table{
					"t1": {
						Id:       "t1",
						Name:     "t1",
						ColNames: []string{"a", "b"},
						ColDefs: map[string]schema.Column{
							"a": {Id: "c2", Name: "a", Type: schema.Type{Name: "varchar", Mods: []int64{6}}},
							"b": {Id: "c3", Name: "b", Type: schema.Type{Name: "varchar", Mods: []int64{6}}},
							"c": {Id: "c4", Name: "c", Type: schema.Type{Name: "varchar", Mods: []int64{}}},
						},
						PrimaryKeys: []schema.Key{{Column: "a"}},
					}},

				ToSource: map[string]internal.NameAndCols{
					"t1": {Name: "t1", Cols: map[string]string{"a": "a", "b": "b", "c": "c"}},
				},
				ToSpanner: map[string]internal.NameAndCols{
					"t1": {Name: "t1", Cols: map[string]string{"a": "a", "b": "b", "c": "c"}},
				},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_MIGRATION_TYPE_UNSPECIFIED.Enum(),
				},
			},
		},
	}

	for _, tc := range tc {

		sessionState := session.GetSessionState()
		sessionState.Conv = tc.conv
		sessionState.Driver = constants.MYSQL

		payload := tc.payload

		req, err := http.NewRequest("POST", "/typemap/reviewtableschema?table="+tc.table, strings.NewReader(payload))
		if err != nil {
			t.Fatal(err)
		}

		req.Header.Set("Content-Type", "application/json")

		rr := httptest.NewRecorder()

		handler := http.HandlerFunc(ReviewTableSchema)

		handler.ServeHTTP(rr, req)

		res := ReviewTableSchemaResponse{}

		json.Unmarshal(rr.Body.Bytes(), &res)

		if status := rr.Code; int64(status) != tc.statusCode {
			t.Errorf("handler returned wrong status code: got %v want %v",
				status, tc.statusCode)
		}

		expectedddl := GetSpannerTableDDL(tc.expectedConv.SpSchema[tc.table])

		if tc.statusCode == http.StatusOK {
			assert.Equal(t, expectedddl, res.DDL)
		}
	}
}

func TestReviewTableSchemaRemove(t *testing.T) {

	tc := []struct {
		name         string
		table        string
		payload      string
		statusCode   int64
		conv         *internal.Conv
		expectedConv *internal.Conv
	}{
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
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_MIGRATION_TYPE_UNSPECIFIED.Enum(),
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
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_MIGRATION_TYPE_UNSPECIFIED.Enum(),
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
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_MIGRATION_TYPE_UNSPECIFIED.Enum(),
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
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_MIGRATION_TYPE_UNSPECIFIED.Enum(),
				},
			},
		},
	}
	for _, tc := range tc {

		sessionState := session.GetSessionState()
		sessionState.Conv = tc.conv
		sessionState.Driver = constants.MYSQL

		payload := tc.payload

		req, err := http.NewRequest("POST", "/typemap/reviewtableschema?table="+tc.table, strings.NewReader(payload))
		if err != nil {
			t.Fatal(err)
		}

		req.Header.Set("Content-Type", "application/json")

		rr := httptest.NewRecorder()

		handler := http.HandlerFunc(ReviewTableSchema)

		handler.ServeHTTP(rr, req)

		res := ReviewTableSchemaResponse{}

		json.Unmarshal(rr.Body.Bytes(), &res)

		if status := rr.Code; int64(status) != tc.statusCode {
			t.Errorf("handler returned wrong status code: got %v want %v",
				status, tc.statusCode)
		}

		expectedddl := GetSpannerTableDDL(tc.expectedConv.SpSchema[tc.table])

		if tc.statusCode == http.StatusOK {
			assert.Equal(t, expectedddl, res.DDL)
		}
	}
}

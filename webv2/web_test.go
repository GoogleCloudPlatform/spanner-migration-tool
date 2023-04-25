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
	"github.com/cloudspannerecosystem/harbourbridge/internal/reports"
	"github.com/cloudspannerecosystem/harbourbridge/proto/migration"
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
			{T: ddl.Bool, DisplayT: ddl.Bool},
			{T: ddl.Int64, Brief: reports.IssueDB[internal.Widened].Brief, DisplayT: ddl.Int64},
			{T: ddl.String, Brief: reports.IssueDB[internal.Widened].Brief, DisplayT: ddl.String}},
		"bigserial": {
			{T: ddl.Int64, Brief: reports.IssueDB[internal.Serial].Brief, DisplayT: ddl.Int64},
			{T: ddl.String, Brief: reports.IssueDB[internal.Widened].Brief + ", " + reports.IssueDB[internal.Serial].Brief, DisplayT: ddl.String}},
		"bpchar": {
			{T: ddl.Bytes, DisplayT: ddl.Bytes},
			{T: ddl.String, DisplayT: ddl.String}},
		"bytea": {
			{T: ddl.Bytes, DisplayT: ddl.Bytes},
			{T: ddl.String, DisplayT: ddl.String}},
		"date": {
			{T: ddl.Date, DisplayT: ddl.Date},
			{T: ddl.String, Brief: reports.IssueDB[internal.Widened].Brief, DisplayT: ddl.String}},
		"float8": {
			{T: ddl.Float64, DisplayT: ddl.Float64},
			{T: ddl.String, Brief: reports.IssueDB[internal.Widened].Brief, DisplayT: ddl.String}},
		"float4": {
			{T: ddl.Float64, Brief: reports.IssueDB[internal.Widened].Brief, DisplayT: ddl.Float64},
			{T: ddl.String, Brief: reports.IssueDB[internal.Widened].Brief, DisplayT: ddl.String}},
		"int8": {
			{T: ddl.Int64, DisplayT: ddl.Int64},
			{T: ddl.String, Brief: reports.IssueDB[internal.Widened].Brief, DisplayT: ddl.String}},
		"int4": {
			{T: ddl.Int64, Brief: reports.IssueDB[internal.Widened].Brief, DisplayT: ddl.Int64},
			{T: ddl.String, Brief: reports.IssueDB[internal.Widened].Brief, DisplayT: ddl.String}},
		"numeric": {
			{T: ddl.String, Brief: reports.IssueDB[internal.Widened].Brief, DisplayT: ddl.String},
			{T: ddl.Numeric, DisplayT: ddl.Numeric}},
		"serial": {
			{T: ddl.Int64, Brief: reports.IssueDB[internal.Serial].Brief, DisplayT: ddl.Int64},
			{T: ddl.String, Brief: reports.IssueDB[internal.Widened].Brief + ", " + reports.IssueDB[internal.Serial].Brief, DisplayT: ddl.String}},
		"text": {
			{T: ddl.Bytes, DisplayT: ddl.Bytes},
			{T: ddl.String, DisplayT: ddl.String}},
		"timestamptz": {
			{T: ddl.String, Brief: reports.IssueDB[internal.Widened].Brief, DisplayT: ddl.String},
			{T: ddl.Timestamp, DisplayT: ddl.Timestamp}},
		"timestamp": {
			{T: ddl.String, Brief: reports.IssueDB[internal.Widened].Brief, DisplayT: ddl.String},
			{T: ddl.Timestamp, Brief: reports.IssueDB[internal.Timestamp].Brief, DisplayT: ddl.Timestamp}},
		"varchar": {
			{T: ddl.Bytes, DisplayT: ddl.Bytes},
			{T: ddl.String, DisplayT: ddl.String}},
	}
	assert.Equal(t, expectedTypemap, typemap)

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
			{T: ddl.Bool, DisplayT: ddl.Bool},
			{T: ddl.Int64, Brief: reports.IssueDB[internal.Widened].Brief, DisplayT: ddl.Int64},
			{T: ddl.String, Brief: reports.IssueDB[internal.Widened].Brief, DisplayT: ddl.String}},
		"varchar": {
			{T: ddl.Bytes, DisplayT: ddl.Bytes},
			{T: ddl.String, DisplayT: ddl.String}},
		"text": {
			{T: ddl.Bytes, DisplayT: ddl.Bytes},
			{T: ddl.String, DisplayT: ddl.String}},
		"enum": {
			{T: ddl.String, DisplayT: ddl.String}},
		"json": {
			{T: ddl.Bytes, DisplayT: ddl.Bytes},
			{T: ddl.JSON, DisplayT: ddl.JSON}},
		"binary": {
			{T: ddl.Bytes, DisplayT: ddl.Bytes},
			{T: ddl.String, DisplayT: ddl.String}},
		"blob": {
			{T: ddl.Bytes, DisplayT: ddl.Bytes},
			{T: ddl.String, DisplayT: ddl.String}},
		"integer": {
			{T: ddl.Int64, Brief: reports.IssueDB[internal.Widened].Brief, DisplayT: ddl.Int64},
			{T: ddl.String, Brief: reports.IssueDB[internal.Widened].Brief, DisplayT: ddl.String}},
		"smallint": {
			{T: ddl.Int64, Brief: reports.IssueDB[internal.Widened].Brief, DisplayT: ddl.Int64},
			{T: ddl.String, Brief: reports.IssueDB[internal.Widened].Brief, DisplayT: ddl.String}},
		"double": {
			{T: ddl.Float64, DisplayT: ddl.Float64},
			{T: ddl.String, Brief: reports.IssueDB[internal.Widened].Brief, DisplayT: ddl.String}},
		"float": {
			{T: ddl.Float64, Brief: reports.IssueDB[internal.Widened].Brief, DisplayT: ddl.Float64},
			{T: ddl.String, Brief: reports.IssueDB[internal.Widened].Brief, DisplayT: ddl.String}},
		"numeric": {
			{T: ddl.String, Brief: reports.IssueDB[internal.Widened].Brief, DisplayT: ddl.String},
			{T: ddl.Numeric, DisplayT: ddl.Numeric}},
		"decimal": {
			{T: ddl.String, Brief: reports.IssueDB[internal.Widened].Brief, DisplayT: ddl.String},
			{T: ddl.Numeric, DisplayT: ddl.Numeric}},
		"date": {
			{T: ddl.Date, DisplayT: ddl.Date},
			{T: ddl.String, Brief: reports.IssueDB[internal.Widened].Brief, DisplayT: ddl.String}},
		"timestamp": {
			{T: ddl.String, Brief: reports.IssueDB[internal.Widened].Brief, DisplayT: ddl.String},
			{T: ddl.Timestamp, DisplayT: ddl.Timestamp}},
		"time": {
			{T: ddl.String, Brief: reports.IssueDB[internal.Time].Brief, DisplayT: ddl.String}},
	}
	assert.Equal(t, expectedTypemap, typemap)

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

// todo update SetParentTable with case III suggest interleve table column.
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
				SpSchema: map[string]ddl.CreateTable{"t1": {
					Name:   "t1",
					ColIds: []string{"c1", "c2", "c3"},
					ColDefs: map[string]ddl.ColumnDef{"c1": ddl.ColumnDef{Name: "c1", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
						"c2": ddl.ColumnDef{Name: "c2", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
						"c3": ddl.ColumnDef{Name: "c3", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true}},
					PrimaryKeys: []ddl.IndexKey{ddl.IndexKey{ColId: "c1", Desc: false}},
					ForeignKeys: []ddl.Foreignkey{ddl.Foreignkey{Name: "fk1", ColIds: []string{"c1"}, ReferTableId: "t1", ReferColumnIds: []string{"c1"}},
						ddl.Foreignkey{Name: "fk2", ColIds: []string{"c3"}, ReferTableId: "t2", ReferColumnIds: []string{"c2"}}},
				}},
				SchemaIssues: map[string]map[string][]internal.SchemaIssue{
					"t1": {},
				},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
			},
		},
		{
			name: "table with synthetic PK",
			ct: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{"t1": {
					Name:   "t1",
					ColIds: []string{"c1", "c2", "c3"},
					ColDefs: map[string]ddl.ColumnDef{"c1": {Name: "c1", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
						"c2":       {Name: "c2", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
						"c3":       {Name: "c3", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						"synth_id": {Name: "synth_id", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
					},
					PrimaryKeys: []ddl.IndexKey{{ColId: "synth_id", Desc: false}},
					ForeignKeys: []ddl.Foreignkey{{Name: "fk1", ColIds: []string{"c1"}, ReferTableId: "t1", ReferColumnIds: []string{"c1"}},
						{Name: "fk2", ColIds: []string{"c3"}, ReferTableId: "t2", ReferColumnIds: []string{"c2"}}},
				}},
				SyntheticPKeys: map[string]internal.SyntheticPKey{"t1": internal.SyntheticPKey{ColId: "synth_id"}},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				SchemaIssues: map[string]map[string][]internal.SchemaIssue{
					"t1": {},
				},
			},
			table:            "t1",
			statusCode:       http.StatusOK,
			expectedResponse: &TableInterleaveStatus{Possible: false, Comment: "Has synthetic pk"},
		},
		{
			name: "no valid prefix 1",
			ct: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:   "t1",
						ColIds: []string{"c1", "c2", "c3"},
						ColDefs: map[string]ddl.ColumnDef{"c1": {Name: "c1", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c2": {Name: "c2", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c3": {Name: "c3", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c1", Desc: false}},
						ForeignKeys: []ddl.Foreignkey{{Name: "fk1", ColIds: []string{"c1"}, ReferTableId: "t2", ReferColumnIds: []string{"c1"}}},
					},
					"t2": {
						Name:   "t2",
						ColIds: []string{"c1", "c2", "c3"},
						ColDefs: map[string]ddl.ColumnDef{"c1": {Name: "c1", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c2":       {Name: "c2", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c3":       {Name: "c3", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
							"synth_id": {Name: "synth_id", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "synth_id", Desc: false}},
					},
				},
				SyntheticPKeys: map[string]internal.SyntheticPKey{"t2": internal.SyntheticPKey{ColId: "synth_id"}},
				SchemaIssues: map[string]map[string][]internal.SchemaIssue{
					"t1": {},
				},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
			},
			table:            "t1",
			statusCode:       http.StatusOK,
			expectedResponse: &TableInterleaveStatus{Possible: false, Comment: "No valid prefix"},
			expectedFKs:      []ddl.Foreignkey{{Name: "fk1", ColIds: []string{"c1"}, ReferTableId: "t2", ReferColumnIds: []string{"c1"}}},
		},
		{
			name: "no valid prefix 2",
			ct: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:   "t1",
						ColIds: []string{"c1", "c2", "c3"},
						ColDefs: map[string]ddl.ColumnDef{"c1": {Name: "c1", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c2": {Name: "c2", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c3": {Name: "c3", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c1", Desc: false, Order: 1}},
						ForeignKeys: []ddl.Foreignkey{{Name: "fk1", ColIds: []string{"c1"}, ReferTableId: "t2", ReferColumnIds: []string{"c1"}}},
					},
					"t2": {
						Name:   "t2",
						ColIds: []string{"c1", "c2", "c3"},
						ColDefs: map[string]ddl.ColumnDef{"c1": {Name: "c1", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c2": {Name: "c2", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c3": {Name: "c3", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c1", Desc: false, Order: 1}, {ColId: "c2", Desc: false, Order: 2}},
					},
				},
				SchemaIssues: map[string]map[string][]internal.SchemaIssue{
					"t1": {},
				},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
			},
			table:            "t1",
			statusCode:       http.StatusOK,
			expectedResponse: &TableInterleaveStatus{Possible: true, Parent: "t2", Comment: ""},
			expectedFKs:      []ddl.Foreignkey{{}},
		},
		{
			name: "no valid prefix 3",
			ct: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:   "t1",
						ColIds: []string{"c1", "c2", "c3"},
						ColDefs: map[string]ddl.ColumnDef{"c1": {Name: "c1", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c2": {Name: "c2", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c3": {Name: "c3", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c1", Desc: false}, {ColId: "c2", Desc: false}},
						ForeignKeys: []ddl.Foreignkey{{Name: "fk1", ColIds: []string{"c3"}, ReferTableId: "t2", ReferColumnIds: []string{"c3"}}},
					},
					"t2": {
						Name:   "t2",
						ColIds: []string{"c1", "c2", "c3"},
						ColDefs: map[string]ddl.ColumnDef{"c1": {Name: "c1", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c2": {Name: "c2", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c3": {Name: "c3", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c1", Desc: false}},
					},
				},
				SchemaIssues: map[string]map[string][]internal.SchemaIssue{
					"t1": {},
				},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
			},
			table:            "t1",
			statusCode:       http.StatusOK,
			expectedResponse: &TableInterleaveStatus{Possible: false, Comment: "No valid prefix"},
			expectedFKs:      []ddl.Foreignkey{{Name: "fk1", ColIds: []string{"c3"}, ReferTableId: "t2", ReferColumnIds: []string{"c3"}}},
		},
		{
			name: "successful interleave",
			ct: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:   "t1",
						ColIds: []string{"c1", "c2", "c3"},
						ColDefs: map[string]ddl.ColumnDef{"c1": {Name: "c1", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c2": {Name: "c2", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c3": {Name: "c3", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c1", Desc: false, Order: 1}, {ColId: "c2", Desc: false, Order: 2}},
						ForeignKeys: []ddl.Foreignkey{{Name: "fk1", ColIds: []string{"c1"}, ReferTableId: "t2", ReferColumnIds: []string{"c1"}}},
					},
					"t2": {
						Name:   "t2",
						ColIds: []string{"c1", "c2", "c3"},
						ColDefs: map[string]ddl.ColumnDef{"c1": {Name: "c1", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c2": {Name: "c2", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c3": {Name: "c3", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c1", Desc: false, Order: 1}},
					},
				},
				SchemaIssues: map[string]map[string][]internal.SchemaIssue{
					"t1": {},
				},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
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
					"t1": {
						Name:   "t1",
						ColIds: []string{"c1", "c2", "c3"},
						ColDefs: map[string]ddl.ColumnDef{"c1": {Name: "c1", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c2": {Name: "c2", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c3": {Name: "c3", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c1", Desc: false, Order: 1}, {ColId: "c2", Desc: false, Order: 2}},
						ForeignKeys: []ddl.Foreignkey{{Name: "fk1", ColIds: []string{"c1", "c2"}, ReferTableId: "t2", ReferColumnIds: []string{"c1", "c2"}}},
					},
					"t2": {
						Name:   "t2",
						ColIds: []string{"c1", "c2", "c3"},
						ColDefs: map[string]ddl.ColumnDef{"c1": {Name: "c1", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c2": {Name: "c2", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c3": {Name: "c3", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c1", Desc: false, Order: 1}, {ColId: "c2", Desc: false, Order: 2}},
					},
				},
				SchemaIssues: map[string]map[string][]internal.SchemaIssue{
					"t1": {},
				},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
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
					"t1": {
						Name:   "t1",
						ColIds: []string{"c1", "c2", "c3"},
						ColDefs: map[string]ddl.ColumnDef{"c1": {Name: "c1", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c2": {Name: "c2", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c3": {Name: "c3", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c1", Desc: false, Order: 1}, {ColId: "c2", Desc: false, Order: 2}},
						ForeignKeys: []ddl.Foreignkey{
							{Name: "fk1", ColIds: []string{"c3"}, ReferTableId: "t3", ReferColumnIds: []string{"c3"}},
							{Name: "fk1", ColIds: []string{"c1", "c2"}, ReferTableId: "t2", ReferColumnIds: []string{"c1", "c2"}}},
					},
					"t2": {
						Name:   "t2",
						ColIds: []string{"c1", "c2", "c3"},
						ColDefs: map[string]ddl.ColumnDef{"c1": {Name: "c1", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c2": {Name: "c2", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c3": {Name: "c3", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c1", Desc: false, Order: 1}, {ColId: "c2", Desc: false, Order: 2}},
					},
					"t3": {
						Name:   "t3",
						ColIds: []string{"c1", "c2", "c3"},
						ColDefs: map[string]ddl.ColumnDef{"c1": {Name: "c1", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c2": {Name: "c2", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c3": {Name: "c3", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c3", Desc: false, Order: 1}},
					},
				},
				SchemaIssues: map[string]map[string][]internal.SchemaIssue{
					"t1": {},
				},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
			},
			table:            "t1",
			statusCode:       http.StatusOK,
			expectedResponse: &TableInterleaveStatus{Possible: true, Parent: "t2"},
			expectedFKs:      []ddl.Foreignkey{ddl.Foreignkey{Name: "fk1", ColIds: []string{"c1", "c2"}, ReferTableId: "t2", ReferColumnIds: []string{"c1", "c2"}, Id: ""}},
			parentTable:      "t2",
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
			assert.Equal(t, tc.parentTable, sessionState.Conv.SpSchema[tc.table].ParentId, tc.name)
			assert.Equal(t, tc.expectedFKs, sessionState.Conv.SpSchema[tc.table].ForeignKeys, tc.name)
		}
	}
}

func TestDropForeignKey(t *testing.T) {
	tc := []struct {
		name         string
		table        string
		input        interface{}
		statusCode   int64
		conv         *internal.Conv
		expectedConv *internal.Conv
	}{
		{
			name:  "Test drop valid FK success",
			table: "t1",
			input: []ddl.Foreignkey{{Name: "fk1", ColIds: []string{"c2"}, ReferTableId: "reft1", ReferColumnIds: []string{"ref_c1"}, Id: "f1"},
				{Name: "", ColIds: []string{}, ReferTableId: "", ReferColumnIds: []string{}, Id: "f2"}},
			statusCode: http.StatusOK,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						ForeignKeys: []ddl.Foreignkey{{Name: "fk1", ColIds: []string{"c2"}, ReferTableId: "reft1", ReferColumnIds: []string{"ref_c1"}, Id: "f1"},
							{Name: "fk2", ColIds: []string{"c3", "c4"}, ReferTableId: "reft2", ReferColumnIds: []string{"ref_c2", "ref_c3"}, Id: "f2"}},
					}},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						ForeignKeys: []ddl.Foreignkey{{Name: "fk1", ColIds: []string{"c2"}, ReferTableId: "reft1", ReferColumnIds: []string{"ref_c1"}, Id: "f1"}},
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

		req, err := http.NewRequest("POST", "/update/fks?table="+tc.table, buffer)

		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		handler := http.HandlerFunc(updateForeignKeys)
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
				"i1": "idx_new",
			},
			statusCode: http.StatusOK,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx", Id: "i1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}}},
					}},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"t1": true, "idx": true},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx_new", Id: "i1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}}},
					}},
			},
		},
		{
			name:  "Test rename multiple indexes",
			table: "t1",
			input: map[string]string{
				"i1": "idx_new_1",
				"i2": "idx_new_2",
			},
			statusCode: http.StatusOK,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx_1", Id: "i1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}},
							{Name: "idx_2", Id: "i2", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}}},
					}},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"t1": true, "idx_1": true, "idx_2": true},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx_new_1", Id: "i1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}},
							{Name: "idx_new_2", Id: "i2", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}}},
					}},
			},
		},
		{
			name:  "New name conflicts with an existing table",
			table: "t1",
			input: map[string]string{
				"i1": "t1",
			},
			statusCode: http.StatusBadRequest,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx_1", Id: "i1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}},
							{Name: "idx_2", Id: "i2", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}}},
					}},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"t1": true, "idx_1": true, "idx_2": true},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx_1", Id: "i1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}},
							{Name: "idx_2", Id: "i2", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}}},
					}},
			},
		},
		{
			name:  "New name conflicts with an existing index",
			table: "t1",
			input: map[string]string{
				"i1": "idx_2",
			},
			statusCode: http.StatusBadRequest,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx_1", Id: "i1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}},
							{Name: "idx_2", Id: "i2", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}}},
					}},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"t1": true, "idx_1": true, "idx_2": true},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx_new_1", Id: "i1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}},
							{Name: "idx_new_2", Id: "i2", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}}},
					}},
			},
		},
		{
			name:  "New name conflicts with an existing foreign key",
			table: "t1",
			input: map[string]string{
				"i1": "fk1",
			},
			statusCode: http.StatusBadRequest,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx_1", Id: "i1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}},
							{Name: "idx_2", Id: "i2", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}}},
						ForeignKeys: []ddl.Foreignkey{{Name: "fk1", Id: "fkId1", ColIds: []string{"c2"}, ReferTableId: "reft1", ReferColumnIds: []string{"ref_b"}},
							{Name: "fk2", Id: "fkId2", ColIds: []string{"c3", "d"}, ReferTableId: "reft2", ReferColumnIds: []string{"ref_c", "ref_d"}}},
					}},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"t1": true, "idx_1": true, "idx_2": true, "fk1": true, "fk2": true},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx_new_1", Id: "i1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}},
							{Name: "idx_new_2", Id: "i2", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}}},
					}},
			},
		},
		{
			name:  "Given Index not available",
			table: "t1",
			input: map[string]string{
				"i1": "idx",
			},
			statusCode: http.StatusBadRequest,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx", Id: "i1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}}},
					}},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"t1": true, "idx": true},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx", Id: "i1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}}},
					}},
			},
		},
		{
			name:  "Conflicts within new name array",
			table: "t1",
			input: map[string]string{
				"i1": "idx_100",
				"i2": "idx_100",
			},
			statusCode: http.StatusBadRequest,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx1", Id: "i1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}},
							{Name: "idx2", Id: "i2", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}}},
					}},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"t1": true, "idx1": true, "idx2": true},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx1", Id: "i1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}},
							{Name: "idx2", Id: "i2", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}}},
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
						Indexes: []ddl.CreateIndex{{Name: "idx", Id: "i1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}}},
					}},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"t1": true, "idx": true},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx", Id: "i1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}}},
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
						Indexes: []ddl.CreateIndex{{Name: "idx", Id: "i1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}}},
					}},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"t1": true, "idx": true},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx", Id: "i2", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}}},
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
			input: []ddl.Foreignkey{{Name: "foreignkey1", ColIds: []string{"c2"}, ReferTableId: "reft1", ReferColumnIds: []string{"ref_b"}, Id: "f1"},
				{Name: "fk2", ColIds: []string{"c", "d"}, ReferTableId: "reft2", ReferColumnIds: []string{"ref_c", "ref_d"}, Id: "f2"}},
			statusCode: http.StatusOK,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						ForeignKeys: []ddl.Foreignkey{{Name: "fk1", Id: "f1", ColIds: []string{"c2"}, ReferTableId: "reft1", ReferColumnIds: []string{"ref_b"}},
							{Name: "fk2", Id: "f2", ColIds: []string{"c", "d"}, ReferTableId: "reft2", ReferColumnIds: []string{"ref_c", "ref_d"}}},
					}},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"t1": true, "fk1": true, "fk2": true},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						ForeignKeys: []ddl.Foreignkey{{Name: "foreignkey1", ColIds: []string{"c2"}, ReferTableId: "reft1", ReferColumnIds: []string{"ref_b"}, Id: "f1"},
							{Name: "fk2", ColIds: []string{"c", "d"}, ReferTableId: "reft2", ReferColumnIds: []string{"ref_c", "ref_d"}, Id: "f2"}},
					}},
			},
		},
		{
			name:  "Test rename multiple foreignkeys",
			table: "t1",
			input: []ddl.Foreignkey{{Name: "foreignkey1", ColIds: []string{"b"}, ReferTableId: "reft1", ReferColumnIds: []string{"ref_b"}, Id: "f1"},
				{Name: "foreignkey2", ColIds: []string{"c", "d"}, ReferTableId: "reft2", ReferColumnIds: []string{"ref_c", "ref_d"}, Id: "f2"}},
			statusCode: http.StatusOK,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						ForeignKeys: []ddl.Foreignkey{{Name: "fk1", ColIds: []string{"b"}, ReferTableId: "reft1", ReferColumnIds: []string{"ref_b"}, Id: "f1"},
							{Name: "fk2", ColIds: []string{"c", "d"}, ReferTableId: "reft2", ReferColumnIds: []string{"ref_c", "ref_d"}, Id: "f2"}},
					}},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"t1": true, "fk1": true, "fk2": true, "reft1": true, "reft2": true},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						ForeignKeys: []ddl.Foreignkey{{Name: "foreignkey1", ColIds: []string{"b"}, ReferTableId: "reft1", ReferColumnIds: []string{"ref_b"}, Id: "f1"},
							{Name: "foreignkey2", ColIds: []string{"c", "d"}, ReferTableId: "reft2", ReferColumnIds: []string{"ref_c", "ref_d"}, Id: "f2"}},
					}},
			},
		},
		{
			name:  "New name conflicts with an existing table",
			table: "t1",
			input: []ddl.Foreignkey{{Name: "t1", ColIds: []string{"b"}, ReferTableId: "reft1", ReferColumnIds: []string{"ref_b"}, Id: "f1"},
				{Name: "fk2", ColIds: []string{"c", "d"}, ReferTableId: "reft2", ReferColumnIds: []string{"ref_c", "ref_d"}, Id: "f2"}},
			statusCode: http.StatusBadRequest,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						ForeignKeys: []ddl.Foreignkey{{Name: "fk1", ColIds: []string{"b"}, ReferTableId: "reft1", ReferColumnIds: []string{"ref_b"}, Id: "f1"},
							{Name: "fk2", ColIds: []string{"c", "d"}, ReferTableId: "reft2", ReferColumnIds: []string{"ref_c", "ref_d"}, Id: "f2"}},
					}},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"t1": true, "fk1": true, "fk2": true, "reft1": true, "reft2": true},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						ForeignKeys: []ddl.Foreignkey{{Name: "foreignkey1", ColIds: []string{"b"}, ReferTableId: "reft1", ReferColumnIds: []string{"ref_b"}, Id: "f1"},
							{Name: "fk2", ColIds: []string{"c", "d"}, ReferTableId: "reft2", ReferColumnIds: []string{"ref_c", "ref_d"}, Id: "f2"}},
					}},
			},
		},
		{
			name:  "New name conflicts with an existing foreignkey",
			table: "t1",
			input: []ddl.Foreignkey{{Name: "fk2", ColIds: []string{"b"}, ReferTableId: "reft1", ReferColumnIds: []string{"ref_b"}, Id: "f1"},
				{Name: "fk2", ColIds: []string{"c", "d"}, ReferTableId: "reft2", ReferColumnIds: []string{"ref_c", "ref_d"}, Id: "f2"}},
			statusCode: http.StatusBadRequest,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						ForeignKeys: []ddl.Foreignkey{{Name: "fk1", ColIds: []string{"b"}, ReferTableId: "reft1", ReferColumnIds: []string{"ref_b"}, Id: "f1"},
							{Name: "fk2", ColIds: []string{"c", "d"}, ReferTableId: "reft2", ReferColumnIds: []string{"ref_c", "ref_d"}, Id: "f2"}},
					}},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"t1": true, "fk1": true, "fk2": true, "reft1": true, "reft2": true},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						ForeignKeys: []ddl.Foreignkey{{Name: "foreignkey1", ColIds: []string{"b"}, ReferTableId: "reft1", ReferColumnIds: []string{"ref_b"}, Id: "f1"},
							{Name: "fk2", ColIds: []string{"c", "d"}, ReferTableId: "reft2", ReferColumnIds: []string{"ref_c", "ref_d"}, Id: "f2"}},
					}},
			},
		},
		{
			name:  "New name conflicts with an existing indexes",
			table: "t1",
			input: []ddl.Foreignkey{{Name: "idx_1", ColIds: []string{"b"}, ReferTableId: "reft1", ReferColumnIds: []string{"ref_b"}, Id: "f1"},
				{Name: "fk2", ColIds: []string{"c", "d"}, ReferTableId: "reft2", ReferColumnIds: []string{"ref_c", "ref_d"}, Id: "f2"}},
			statusCode: http.StatusBadRequest,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx_1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "b", Desc: false}}},
							{Name: "idx_2", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "b", Desc: false}}}},
						ForeignKeys: []ddl.Foreignkey{{Name: "fk1", ColIds: []string{"b"}, ReferTableId: "reft1", ReferColumnIds: []string{"ref_b"}, Id: "f1"},
							{Name: "fk2", ColIds: []string{"c", "d"}, ReferTableId: "reft2", ReferColumnIds: []string{"ref_c", "ref_d"}, Id: "f2"}},
					}},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"t1": true, "idx_1": true, "idx_2": true, "fk1": true, "fk2": true, "reft1": true, "reft2": true},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx_new_1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}},
							{Name: "idx_new_2", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}}},
						ForeignKeys: []ddl.Foreignkey{{Name: "fk1", Id: "fkId1", ColIds: []string{"c2"}, ReferTableId: "reft1", ReferColumnIds: []string{"ref_b"}},
							{Name: "fk2", Id: "fkId2", ColIds: []string{"c3", "d"}, ReferTableId: "reft2", ReferColumnIds: []string{"ref_c", "ref_d"}}},
					}},
			},
		},
		{
			name:  "Conflicts within new name array",
			table: "t1",
			input: map[string]string{
				"fkId1": "fk_100",
				"fkId2": "fk_100",
			},
			statusCode: http.StatusBadRequest,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}},
							{Name: "idx2", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}}},
						ForeignKeys: []ddl.Foreignkey{{Name: "fk1", Id: "fkId1", ColIds: []string{"c2"}, ReferTableId: "reft1", ReferColumnIds: []string{"ref_b"}},
							{Name: "fk2", Id: "fkId2", ColIds: []string{"c3", "d"}, ReferTableId: "reft2", ReferColumnIds: []string{"ref_c", "ref_d"}}},
					}},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"t1": true, "idx1": true, "idx2": true},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}},
							{Name: "idx2", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}}},
						ForeignKeys: []ddl.Foreignkey{{Name: "fk1", Id: "fkId1", ColIds: []string{"c2"}, ReferTableId: "reft1", ReferColumnIds: []string{"ref_b"}},
							{Name: "fk2", Id: "fkId2", ColIds: []string{"c3", "d"}, ReferTableId: "reft2", ReferColumnIds: []string{"ref_c", "ref_d"}}},
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
						ForeignKeys: []ddl.Foreignkey{{Name: "fk1", Id: "fkId1", ColIds: []string{"c2"}, ReferTableId: "reft1", ReferColumnIds: []string{"ref_b"}},
							{Name: "fk2", Id: "fkId2", ColIds: []string{"c3", "d"}, ReferTableId: "reft2", ReferColumnIds: []string{"ref_c", "ref_d"}}},
					}},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"t1": true, "fk1": true, "fk2": true, "reft1": true, "reft2": true},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						ForeignKeys: []ddl.Foreignkey{{Name: "fk1", Id: "fkId1", ColIds: []string{"c2"}, ReferTableId: "reft1", ReferColumnIds: []string{"ref_b"}},
							{Name: "fk2", Id: "fkId2", ColIds: []string{"c3", "d"}, ReferTableId: "reft2", ReferColumnIds: []string{"ref_c", "ref_d"}}},
					}},
			},
		},
		{
			name:  "Check non usage in another table",
			table: "t1",
			input: []ddl.Foreignkey{{Name: "t2_fk2", ColIds: []string{"b"}, ReferTableId: "reft1", ReferColumnIds: []string{"ref_b"}, Id: "f1"},
				{Name: "fk2", ColIds: []string{"c", "d"}, ReferTableId: "reft2", ReferColumnIds: []string{"ref_c", "ref_d"}, Id: "f2"}},
			statusCode: http.StatusBadRequest,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						ForeignKeys: []ddl.Foreignkey{{Name: "fk1", ColIds: []string{"b"}, ReferTableId: "reft1", ReferColumnIds: []string{"ref_b"}, Id: "f1"},
							{Name: "fk2", ColIds: []string{"c", "d"}, ReferTableId: "reft2", ReferColumnIds: []string{"ref_c", "ref_d"}, Id: "f2"}},
					},
					"t2": {
						ForeignKeys: []ddl.Foreignkey{{Name: "t2_fk1", ColIds: []string{"b"}, ReferTableId: "reft1", ReferColumnIds: []string{"ref_b"}, Id: "f3"},
							{Name: "t2_fk2", ColIds: []string{"c", "d"}, ReferTableId: "reft2", ReferColumnIds: []string{"ref_c", "ref_d"}, Id: "f4"}},
					}},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"t1": true, "t2": true, "fk1": true, "fk2": true, "reft1": true, "reft2": true, "t2_fk1": true, "t2_fk2": true},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						ForeignKeys: []ddl.Foreignkey{{Name: "fk1", ColIds: []string{"b"}, ReferTableId: "reft1", ReferColumnIds: []string{"ref_b"}, Id: "f1"},
							{Name: "fk2", ColIds: []string{"c", "d"}, ReferTableId: "reft2", ReferColumnIds: []string{"ref_c", "ref_d"}, Id: "f2"}},
					},
					"t2": {
						ForeignKeys: []ddl.Foreignkey{{Name: "t2_fk1", ColIds: []string{"b"}, ReferTableId: "reft1", ReferColumnIds: []string{"ref_b"}, Id: "f3"},
							{Name: "t2_fk2", ColIds: []string{"c", "d"}, ReferTableId: "reft2", ReferColumnIds: []string{"ref_c", "ref_d"}, Id: "f4"}},
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

		req, err := http.NewRequest("POST", "/update/fks?table="+tc.table, buffer)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		handler := http.HandlerFunc(updateForeignKeys)
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
		payload      string
		statusCode   int64
		conv         *internal.Conv
		expectedConv *internal.Conv
	}{
		{
			name:       "Test drop valid secondary index success",
			table:      "t1",
			payload:    `{"Id":"i2"}`,
			statusCode: http.StatusOK,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx1", Id: "i1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}},
							{Name: "idx2", Id: "i2", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c3", Desc: false}, {ColId: "d", Desc: false}}}},
					}},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx1", Id: "i1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}}},
					}},
			},
		},
		{
			name:       "Test drop secondary index invalid Id",
			table:      "t1",
			payload:    `{"Id":""}`,
			statusCode: http.StatusBadRequest,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx1", Id: "i1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}}},
					}},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
			},
		},
		{
			name:       "Test drop secondary index invalid Id 2",
			table:      "t1",
			payload:    `{"Id":"AB"}`,
			statusCode: http.StatusBadRequest,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx1", Id: "i1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}}},
					}},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
			},
		},
	}
	for _, tc := range tc {
		sessionState := session.GetSessionState()

		sessionState.Driver = constants.MYSQL
		sessionState.Conv = tc.conv
		payload := tc.payload
		req, err := http.NewRequest("POST", "/drop/secondaryindex?table="+tc.table, strings.NewReader(payload))
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

func TestRestoreSecondaryIndex(t *testing.T) {
	tc := []struct {
		name         string
		tableId      string
		indexId      string
		statusCode   int64
		conv         *internal.Conv
		expectedConv *internal.Conv
	}{
		{
			name:       "Test restore valid secondary index success",
			tableId:    "t1",
			indexId:    "i1",
			statusCode: http.StatusOK,
			conv: &internal.Conv{
				SrcSchema: map[string]schema.Table{
					"t1": {
						Name:   "table1",
						ColIds: []string{"c1", "c2", "c3"},
						Indexes: []schema.Index{
							{Name: "idx1", Unique: false, Keys: []schema.Key{{ColId: "c2", Desc: false, Order: 1}}, Id: "i1"},
							{Name: "idx2", Unique: false, Keys: []schema.Key{{ColId: "c3", Desc: false, Order: 1}}, Id: "i2"},
						},
						Id: "t1",
					},
				},
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:   "table1",
						ColIds: []string{"c1", "c2", "c3"},
						Indexes: []ddl.CreateIndex{
							{Name: "idx2", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c3", Desc: false, Order: 1}}, Id: "i2"},
						},
						Id: "t1",
					},
				},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"t1": true, "idx2": true},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:   "table1",
						ColIds: []string{"c1", "c2", "c3"},
						Indexes: []ddl.CreateIndex{
							{Name: "idx2", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c3", Desc: false, Order: 1}}, Id: "i2"},
							{Name: "idx1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false, Order: 1}}, Id: "i1"},
						},
						Id: "t1",
					},
				},
			},
		},

		{
			name:       "Test restore secondary index invalid index id",
			tableId:    "t1",
			indexId:    "A",
			statusCode: http.StatusBadRequest,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:    "table1",
						Id:      "t1",
						ColIds:  []string{"c1", "c2", "c3"},
						Indexes: []ddl.CreateIndex{{Name: "idx1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false, Order: 1}}, Id: "i1"}},
					}},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
			},
			expectedConv: &internal.Conv{},
		},
		{
			name:       "Test drop secondary index invalid table id",
			tableId:    "X",
			indexId:    "i1",
			statusCode: http.StatusBadRequest,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:    "table1",
						Id:      "t1",
						ColIds:  []string{"c1", "c2", "c3"},
						Indexes: []ddl.CreateIndex{{Name: "idx1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false, Order: 1}}, Id: "i1"}},
					}},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
			},
			expectedConv: &internal.Conv{},
		},
	}
	for _, tc := range tc {
		sessionState := session.GetSessionState()

		sessionState.Driver = constants.MYSQL
		sessionState.Conv = tc.conv
		payload := `{}`
		req, err := http.NewRequest("POST", "/restore/secondaryIndex?tableId="+tc.tableId+"&indexId="+tc.indexId, strings.NewReader(payload))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		handler := http.HandlerFunc(restoreSecondaryIndex)
		handler.ServeHTTP(rr, req)
		var res *internal.Conv
		json.Unmarshal(rr.Body.Bytes(), &res)
		if status := rr.Code; int64(status) != tc.statusCode {
			t.Errorf("handler returned wrong status code: got %v want %v",
				status, tc.statusCode)
		}
		if tc.statusCode == http.StatusOK {
			assert.Equal(t, tc.expectedConv.SpSchema, res.SpSchema)
		}
	}
}

func TestDropTable(t *testing.T) {
	sessionState := session.GetSessionState()
	sessionState.Driver = constants.MYSQL

	c3 := &internal.Conv{
		SchemaIssues: map[string]map[string][]internal.SchemaIssue{
			"t1": {},
			"t2": {},
		},
		SrcSchema: map[string]schema.Table{
			"t1": {
				Name:   "tn1",
				ColIds: []string{"c1", "c2", "c3"},
				ColDefs: map[string]schema.Column{
					"c1": {Name: "cn1", Type: schema.Type{Name: "bigint"}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: "c1"},
					"c2": {Name: "cn2", Type: schema.Type{Name: "bigint"}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: "c2"},
					"c3": {Name: "cn3", Type: schema.Type{Name: "varchar"}, NotNull: false, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: "c3"},
				},
				PrimaryKeys: []schema.Key{{ColId: "c1", Desc: false, Order: 1}},
				Id:          "t1",
			},

			"t2": {
				Name:   "tn2",
				ColIds: []string{"c4", "c5", "c6"},
				ColDefs: map[string]schema.Column{
					"c4": {Name: "cn4", Type: schema.Type{Name: "bigint"}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: "c4"},
					"c5": {Name: "cn5", Type: schema.Type{Name: "varchar"}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: "c5"},
					"c6": {Name: "cn6", Type: schema.Type{Name: "bigint"}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: "c6"},
				},
				Id: "t2",
			},
		},
		SpSchema: map[string]ddl.CreateTable{
			"t1": {
				Name:   "tn1",
				ColIds: []string{"c1", "c2", "c3"},
				ColDefs: map[string]ddl.ColumnDef{"c1": {Name: "cn1", T: ddl.Type{Name: ddl.Int64}, NotNull: true, Id: "c1"},
					"c2": {Name: "cn2", T: ddl.Type{Name: ddl.Int64}, NotNull: true, Id: "c2"},
					"c3": {Name: "cn3", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true, Id: "c3"},
				},
				PrimaryKeys: []ddl.IndexKey{{ColId: "c1", Desc: false}},
				Id:          "t1",
			},
			"t2": {
				Name:   "tn2",
				ColIds: []string{"c4", "c5", "c6", "c7"},
				ColDefs: map[string]ddl.ColumnDef{"c4": {Name: "cn4", T: ddl.Type{Name: ddl.Int64}, NotNull: true, Id: "c4"},
					"c5": {Name: "cn5", T: ddl.Type{Name: ddl.Int64}, NotNull: true, Id: "c5"},
					"c6": {Name: "cn6", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true, Id: "c6"},
					"c7": {Name: "synth_id", T: ddl.Type{Name: ddl.Int64}, NotNull: true, Id: "c7"},
				},
				PrimaryKeys: []ddl.IndexKey{{ColId: "c7", Desc: false}},
				Id:          "t2",
			}},
		Audit: internal.Audit{
			MigrationType: migration.MigrationData_MIGRATION_TYPE_UNSPECIFIED.Enum(),
		},
	}

	sessionState.Conv = c3

	payload := `{}`

	req, err := http.NewRequest("POST", "/drop/table?table=t1", strings.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}

	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	handler := http.HandlerFunc(dropTable)
	handler.ServeHTTP(rr, req)

	res := &internal.Conv{}

	json.Unmarshal(rr.Body.Bytes(), &res)

	expectedConv := &internal.Conv{

		SpSchema: map[string]ddl.CreateTable{
			"t2": {
				Name:   "tn2",
				ColIds: []string{"c4", "c5", "c6", "c7"},
				ColDefs: map[string]ddl.ColumnDef{"c4": {Name: "cn4", T: ddl.Type{Name: ddl.Int64}, NotNull: true, Id: "c4"},
					"c5": {Name: "cn5", T: ddl.Type{Name: ddl.Int64}, NotNull: true, Id: "c5"},
					"c6": {Name: "cn6", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true, Id: "c6"},
					"c7": {Name: "synth_id", T: ddl.Type{Name: ddl.Int64}, NotNull: true, Id: "c7"},
				},
				PrimaryKeys: []ddl.IndexKey{{ColId: "c7", Desc: false, Order: 0}},
				ForeignKeys: []ddl.Foreignkey{},
				Indexes:     []ddl.CreateIndex(nil),
				Id:          "t2",
			}},
	}

	assert.Equal(t, expectedConv.SpSchema, res.SpSchema)
}

func TestRestoreTable(t *testing.T) {
	sessionState := session.GetSessionState()

	sessionState.Driver = constants.MYSQL

	conv := &internal.Conv{

		SpSchema: map[string]ddl.CreateTable{
			"t1": {
				Name:   "tn1",
				ColIds: []string{"c1", "c2"},
				ColDefs: map[string]ddl.ColumnDef{"c1": {Name: "cn1", T: ddl.Type{Name: "STRING", IsArray: false}, NotNull: true, Comment: "", Id: "c1"},
					"c2": {Name: "cn2", T: ddl.Type{Name: "STRING", IsArray: false}, NotNull: true, Comment: "", Id: "c2"},
				},
				PrimaryKeys: []ddl.IndexKey{{ColId: "c1", Desc: false, Order: 1}},
				Id:          "t1",
			}},
		SrcSchema: map[string]schema.Table{
			"t2": {
				Name:   "tn2",
				ColIds: []string{"c3", "c4", "c5"},
				ColDefs: map[string]schema.Column{
					"c3": {Name: "cn3", Type: schema.Type{Name: "varchar"}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: "c3"},
					"c4": {Name: "cn4", Type: schema.Type{Name: "varchar"}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: "c4"},
					"c5": {Name: "cn5", Type: schema.Type{Name: "bigint"}, NotNull: false, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: "c5"},
				},
				PrimaryKeys: []schema.Key{{ColId: "c3", Desc: false, Order: 1}},
				Id:          "t2",
			},

			"t1": {
				Name:   "tn1",
				ColIds: []string{"c1", "c2"},
				ColDefs: map[string]schema.Column{
					"c1": {Name: "cn1", Type: schema.Type{Name: "varchar"}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: "c1"},
					"c2": {Name: "cn2", Type: schema.Type{Name: "char"}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: "c2"},
				},
				PrimaryKeys: []schema.Key{{ColId: "c1", Desc: false, Order: 1}},
				Id:          "t1",
			},
		},

		UsedNames: map[string]bool{
			"t1": true,
		},

		Audit: internal.Audit{
			MigrationType: migration.MigrationData_MIGRATION_TYPE_UNSPECIFIED.Enum(),
		},

		SchemaIssues: map[string]map[string][]internal.SchemaIssue{},
	}

	sessionState.Conv = conv

	payload := `{}`

	req, err := http.NewRequest("POST", "/restore/table?table=t2", strings.NewReader(payload))

	if err != nil {
		t.Fatal(err)
	}

	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	handler := http.HandlerFunc(restoreTable)
	handler.ServeHTTP(rr, req)

	res := &internal.Conv{}

	json.Unmarshal(rr.Body.Bytes(), &res)

	expectedConv := &internal.Conv{

		SpSchema: map[string]ddl.CreateTable{
			"t1": {
				Name:   "tn1",
				ColIds: []string{"c1", "c2"},
				ColDefs: map[string]ddl.ColumnDef{"c1": {Name: "cn1", T: ddl.Type{Name: "STRING", Len: 0, IsArray: false}, NotNull: true, Comment: "", Id: "c1"},
					"c2": {Name: "cn2", T: ddl.Type{Name: "STRING", Len: 0, IsArray: false}, NotNull: true, Comment: "", Id: "c2"},
				},
				PrimaryKeys: []ddl.IndexKey{{ColId: "c1", Desc: false, Order: 1}},
				Id:          "t1",
			},

			"t2": {
				Name:   "tn2",
				ColIds: []string{"c3", "c4", "c5"},
				ColDefs: map[string]ddl.ColumnDef{
					"c3": {Name: "cn3", T: ddl.Type{Name: "STRING", Len: 9223372036854775807, IsArray: false}, NotNull: true, Comment: "From: cn3 varchar", Id: "c3"},
					"c4": {Name: "cn4", T: ddl.Type{Name: "STRING", Len: 9223372036854775807, IsArray: false}, NotNull: true, Comment: "From: cn4 varchar", Id: "c4"},
					"c5": {Name: "cn5", T: ddl.Type{Name: "INT64", Len: 0, IsArray: false}, NotNull: false, Comment: "From: cn5 bigint", Id: "c5"},
				},
				PrimaryKeys: []ddl.IndexKey{{ColId: "c3", Desc: false, Order: 1}},
				Id:          "t2",
				Comment:     "Spanner schema for source table tn2",
			},
		},
	}
	assert.Equal(t, expectedConv.SpSchema, res.SpSchema)

}

func TestRemoveParentTable(t *testing.T) {
	tc := []struct {
		name             string
		tableId          string
		statusCode       int64
		conv             *internal.Conv
		expectedSpSchema ddl.Schema
	}{
		{
			name:       "Remove interleaving with valid table id",
			tableId:    "t1",
			statusCode: http.StatusOK,
			conv: &internal.Conv{
				SchemaIssues: map[string]map[string][]internal.SchemaIssue{
					"t1": {},
					"t2": {},
				},
				SrcSchema: map[string]schema.Table{
					"t1": {
						Name:   "table1",
						ColIds: []string{"c1", "c2", "c3"},
						ColDefs: map[string]schema.Column{
							"c1": {Name: "a", Type: schema.Type{Name: "bigint"}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: "c1"},
							"c2": {Name: "b", Type: schema.Type{Name: "bigint"}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: "c2"},
							"c3": {Name: "c", Type: schema.Type{Name: "varchar"}, NotNull: false, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: "c3"},
						},
						PrimaryKeys: []schema.Key{{ColId: "c1", Desc: false, Order: 1}, {ColId: "c2", Desc: false, Order: 2}},
						ForeignKeys: []schema.ForeignKey{{Name: "fk1", ColIds: []string{"c1"}, ReferTableId: "t2", ReferColumnIds: []string{"c4"}, Id: "f1"}},
						Id:          "t1",
					},

					"t2": {
						Name:   "table2",
						ColIds: []string{"c4", "c5"},
						ColDefs: map[string]schema.Column{
							"c4": {Name: "a", Type: schema.Type{Name: "bigint"}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: true, AutoIncrement: false}, Id: "c4"},
							"c5": {Name: "d", Type: schema.Type{Name: "varchar"}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: "c5"},
						},
						Id:          "t2",
						PrimaryKeys: []schema.Key{{ColId: "c4", Desc: false, Order: 1}},
					},
				},
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:   "table1",
						ColIds: []string{"c1", "c2", "c3"},
						ColDefs: map[string]ddl.ColumnDef{
							"c1": {Name: "a", Id: "c1", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c2": {Name: "b", Id: "c2", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c3": {Name: "c", Id: "c3", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c1", Desc: false, Order: 1}, {ColId: "c2", Desc: false, Order: 2}},
						Id:          "t1",
						ParentId:    "t2",
					},
					"t2": {
						Name:   "table2",
						ColIds: []string{"c4", "c5"},
						ColDefs: map[string]ddl.ColumnDef{
							"c4": {Name: "a", Id: "c4", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c5": {Name: "d", Id: "c5", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c4", Desc: false, Order: 1}},
						Id:          "t2",
					}},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_MIGRATION_TYPE_UNSPECIFIED.Enum(),
				},
				UsedNames: map[string]bool{"table1": true, "table2": true},
			},
			expectedSpSchema: ddl.Schema{
				"t1": {
					Name:   "table1",
					ColIds: []string{"c1", "c2", "c3"},
					ColDefs: map[string]ddl.ColumnDef{
						"c1": {Name: "a", Id: "c1", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
						"c2": {Name: "b", Id: "c2", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
						"c3": {Name: "c", Id: "c3", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
					},
					PrimaryKeys: []ddl.IndexKey{{ColId: "c1", Desc: false, Order: 1}, {ColId: "c2", Desc: false, Order: 2}},
					ForeignKeys: []ddl.Foreignkey{{Name: "fk1", ColIds: []string{"c1"}, ReferTableId: "t2", ReferColumnIds: []string{"c4"}, Id: "f1"}},
					Id:          "t1",
					ParentId:    "",
				},
				"t2": {
					Name:   "table2",
					ColIds: []string{"c4", "c5"},
					ColDefs: map[string]ddl.ColumnDef{
						"c4": {Name: "a", Id: "c4", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
						"c5": {Name: "d", Id: "c5", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
					},
					PrimaryKeys: []ddl.IndexKey{{ColId: "c4", Desc: false, Order: 1}},
					Id:          "t2",
				},
			},
		},

		{name: "Remove interleaving with invalid table id",
			tableId:    "A",
			statusCode: http.StatusBadRequest,
			conv: &internal.Conv{
				SchemaIssues: map[string]map[string][]internal.SchemaIssue{
					"t1": {},
					"t2": {},
				},
				SrcSchema: map[string]schema.Table{
					"t1": {
						Name:   "table1",
						ColIds: []string{"c1", "c2", "c3"},
						ColDefs: map[string]schema.Column{
							"c1": {Name: "a", Type: schema.Type{Name: "bigint"}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: "c1"},
							"c2": {Name: "b", Type: schema.Type{Name: "bigint"}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: "c2"},
							"c3": {Name: "c", Type: schema.Type{Name: "varchar"}, NotNull: false, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: "c3"},
						},
						PrimaryKeys: []schema.Key{{ColId: "c1", Desc: false, Order: 1}, {ColId: "c2", Desc: false, Order: 2}},
						ForeignKeys: []schema.ForeignKey{{Name: "fk1", ColIds: []string{"c1"}, ReferTableId: "t2", ReferColumnIds: []string{"c4"}, Id: "f1"}},
						Id:          "t1",
					},

					"t2": {
						Name:   "table2",
						ColIds: []string{"c4", "c5"},
						ColDefs: map[string]schema.Column{
							"c4": {Name: "a", Type: schema.Type{Name: "bigint"}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: true, AutoIncrement: false}, Id: "c4"},
							"c5": {Name: "d", Type: schema.Type{Name: "varchar"}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: "c5"},
						},
						Id:          "t2",
						PrimaryKeys: []schema.Key{{ColId: "c4", Desc: false, Order: 1}},
					},
				},
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:   "table1",
						ColIds: []string{"c1", "c2", "c3"},
						ColDefs: map[string]ddl.ColumnDef{
							"c1": {Name: "a", Id: "c1", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c2": {Name: "b", Id: "c2", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c3": {Name: "c", Id: "c3", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c1", Desc: false, Order: 1}, {ColId: "c2", Desc: false, Order: 2}},
						Id:          "t1",
						ParentId:    "t2",
					},
					"t2": {
						Name:   "table2",
						ColIds: []string{"c4", "c5"},
						ColDefs: map[string]ddl.ColumnDef{
							"c4": {Name: "a", Id: "c4", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c5": {Name: "d", Id: "c5", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c4", Desc: false, Order: 1}},
						Id:          "t2",
					}},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_MIGRATION_TYPE_UNSPECIFIED.Enum(),
				},
				UsedNames: map[string]bool{"table1": true, "table2": true},
			},
			expectedSpSchema: ddl.Schema{},
		},
	}

	for _, tc := range tc {
		sessionState := session.GetSessionState()
		sessionState.Driver = constants.MYSQL

		sessionState.Conv = tc.conv
		payload := `{}`
		req, err := http.NewRequest("POST", "/drop/removeParent?tableId="+tc.tableId, strings.NewReader(payload))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		handler := http.HandlerFunc(removeParentTable)
		handler.ServeHTTP(rr, req)
		var res *internal.Conv
		json.Unmarshal(rr.Body.Bytes(), &res)
		if status := rr.Code; int64(status) != tc.statusCode {
			t.Errorf("handler returned wrong status code: got %v want %v",
				status, tc.statusCode)
		}
		if tc.statusCode == http.StatusOK {
			assert.Equal(t, tc.expectedSpSchema, res.SpSchema)
		}
	}
}

func TestApplyRule(t *testing.T) {
	tcAddIndex := []struct {
		name         string
		input        internal.Rule
		statusCode   int64
		conv         *internal.Conv
		expectedConv *internal.Conv
	}{
		{
			name: "Add Index with unique name",
			input: internal.Rule{
				Name:              "rule-index1",
				ObjectType:        "Table",
				AssociatedObjects: "t1",
				Enabled:           true,
				Type:              constants.AddIndex,
				Data: ddl.CreateIndex{
					Name:    "idx3",
					TableId: "t1",
					Unique:  false,
					Keys:    []ddl.IndexKey{{ColId: "c2", Desc: false, Order: 1}},
				},
			},
			statusCode: http.StatusOK,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name: "table1",
						Id:   "t1",
						Indexes: []ddl.CreateIndex{
							{Name: "idx1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}},
							{Name: "idx2", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c3", Desc: false}, {ColId: "c4", Desc: false}}}},
					}},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"table1": true, "idx1": true, "idx2": true},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name: "table1",
						Id:   "t1",
						Indexes: []ddl.CreateIndex{
							{Name: "idx1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}},
							{Name: "idx2", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c3", Desc: false}, {ColId: "c4", Desc: false}}},
							{Id: "i1", Name: "idx3", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false, Order: 1}}},
						},
					}},
			},
		},
		{
			name: "New name conflicts with an existing table",
			input: internal.Rule{
				Name:              "rule-index1",
				ObjectType:        "Table",
				AssociatedObjects: "t1",
				Enabled:           true,
				Type:              constants.AddIndex,
				Data: map[string]interface{}{
					"Name":    "table1",
					"TableId": "t1",
					"Unique":  false,
					"Keys":    []interface{}{map[string]interface{}{"ColId": "c2", "Desc": false}},
				},
			},
			statusCode: http.StatusInternalServerError,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name: "table1",
						Id:   "t1",
						Indexes: []ddl.CreateIndex{{Name: "idx1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}},
							{Name: "idx2", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c3", Desc: false}, {ColId: "c4", Desc: false}}}},
					}},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"table1": true, "idx1": true, "idx2": true},
			},
		},
		{
			name: "New name conflicts with an existing index",
			input: internal.Rule{
				Name:              "rule-index1",
				ObjectType:        "Table",
				AssociatedObjects: "t1",
				Enabled:           true,
				Type:              constants.AddIndex,
				Data: map[string]interface{}{
					"Name":    "idx2",
					"TableId": "t1",
					"Unique":  false,
					"Keys":    []interface{}{map[string]interface{}{"ColId": "c2", "Desc": false}},
				},
			},
			statusCode: http.StatusInternalServerError,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name: "table1",
						Id:   "t1",
						Indexes: []ddl.CreateIndex{{Name: "idx1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}},
							{Name: "idx2", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c3", Desc: false}, {ColId: "c4", Desc: false}}}},
					}},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"table1": true, "idx1": true, "idx2": true},
			},
		},
		{
			name: "Invalid input",
			input: internal.Rule{
				Name:              "rule-index1",
				ObjectType:        "Table",
				AssociatedObjects: "t1",
				Enabled:           true,
				Type:              constants.AddIndex,
				Data:              []string{"test1"},
			},
			statusCode: http.StatusInternalServerError,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name: "table1",
						Id:   "t1",
						Indexes: []ddl.CreateIndex{{Name: "idx1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}},
							{Name: "idx2", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c3", Desc: false}, {ColId: "c4", Desc: false}}}},
					}},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"table1": true, "idx1": true, "idx2": true},
			},
		},
	}
	for _, tc := range tcAddIndex {
		sessionState := session.GetSessionState()

		sessionState.Driver = constants.MYSQL
		sessionState.Conv = tc.conv

		inputBytes, err := json.Marshal(tc.input)
		if err != nil {
			t.Fatal(err)
		}
		buffer := bytes.NewBuffer(inputBytes)

		req, err := http.NewRequest("POST", "/applyrule", buffer)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		handler := http.HandlerFunc(applyRule)
		handler.ServeHTTP(rr, req)
		var res *internal.Conv
		json.Unmarshal(rr.Body.Bytes(), &res)
		if status := rr.Code; int64(status) != tc.statusCode {
			t.Errorf("%s : handler returned wrong status code: got %v want %v",
				tc.name, status, tc.statusCode)
		}
		if tc.statusCode == http.StatusOK {
			tc.expectedConv.Rules = internal.MakeConv().Rules
			tc.expectedConv.Rules = append(tc.expectedConv.Rules, tc.input)

			// Marshall and unmarshall the data field of rule with its proper type i.e ddl.CreateIndex.
			// Else unmarshalling data field of rule as interface convert int to float64.
			// In this particular case, order of index-key would be unmarshall to float64 instead of int.
			dataBytes, err := json.Marshal(res.Rules[0].Data)
			assert.Equal(t, err, nil)
			var data ddl.CreateIndex
			json.Unmarshal(dataBytes, &data)

			// Removing random ids before comparison.
			addedRule := res.Rules[0]
			data.Id = ""
			addedRule.Data = data
			addedRule.Id = ""
			res.Rules[0] = addedRule

			assert.Equal(t, tc.expectedConv, res)
		}
	}

	tcSetGlobalDataTypePostgres := []struct {
		name           string
		payload        string
		statusCode     int64
		expectedSchema ddl.CreateTable
		expectedIssues map[string][]internal.SchemaIssue
	}{
		{
			name: "Test type change",
			payload: `{
				"Name":              "rule1",
				"Type":              "global_datatype_change",
				"ObjectType":        "Column",
				"AssociatedObjects": "All Columns",
				"Enabled":           true,
				"Data":
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
	}
		}`,
			statusCode: http.StatusOK,
			expectedSchema: ddl.CreateTable{
				Name:   "table1",
				Id:     "t1",
				ColIds: []string{"c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11", "c12", "c13", "c14", "c15", "c16"},
				ColDefs: map[string]ddl.ColumnDef{
					"c1":  {Name: "a", Id: "c1", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"c2":  {Name: "b", Id: "c2", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"c3":  {Name: "c", Id: "c3", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"c4":  {Name: "d", Id: "c4", T: ddl.Type{Name: ddl.Bytes, Len: 6}},
					"c5":  {Name: "e", Id: "c5", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"c6":  {Name: "f", Id: "c6", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"c7":  {Name: "g", Id: "c7", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"c8":  {Name: "h", Id: "c8", T: ddl.Type{Name: ddl.Bytes, Len: int64(1)}},
					"c9":  {Name: "i", Id: "c9", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"c10": {Name: "j", Id: "c10", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"c11": {Name: "k", Id: "c11", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"c12": {Name: "l", Id: "c12", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"c13": {Name: "m", Id: "c13", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"c14": {Name: "n", Id: "c14", T: ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}},
					"c15": {Name: "o", Id: "c15", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"c16": {Name: "p", Id: "c16", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
				},
				PrimaryKeys: []ddl.IndexKey{{ColId: "c1"}},
			},
			expectedIssues: map[string][]internal.SchemaIssue{
				"c1":  {internal.Widened},
				"c2":  {internal.Widened},
				"c3":  {internal.Widened},
				"c5":  {internal.Widened},
				"c6":  {internal.Widened},
				"c7":  {internal.Widened, internal.Serial},
				"c10": {internal.Widened},
				"c11": {internal.Widened},
				"c12": {internal.Widened},
				"c13": {internal.Widened, internal.Serial},
				"c15": {internal.Widened},
				"c16": {internal.Widened},
			},
		},
		{
			name: "Test type change 2",
			payload: `{
				"Name":              "rule1",
				"Type":              "global_datatype_change",
				"ObjectType":        "Column",
				"AssociatedObjects": "All Columns",
				"Enabled":           true,
				"Data":
		{
		  	"bool":"INT64",
			"int8":"STRING",
			"float4":"STRING"
		}
			}`,
			statusCode: http.StatusOK,
			expectedSchema: ddl.CreateTable{
				Name:   "table1",
				Id:     "t1",
				ColIds: []string{"c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11", "c12", "c13", "c14", "c15", "c16"},
				ColDefs: map[string]ddl.ColumnDef{
					"c1":  {Name: "a", Id: "c1", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"c2":  {Name: "b", Id: "c2", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"c3":  {Name: "c", Id: "c3", T: ddl.Type{Name: ddl.Int64}},
					"c4":  {Name: "d", Id: "c4", T: ddl.Type{Name: ddl.String, Len: int64(6)}},
					"c5":  {Name: "e", Id: "c5", T: ddl.Type{Name: ddl.Numeric}},
					"c6":  {Name: "f", Id: "c6", T: ddl.Type{Name: ddl.Timestamp}},
					"c7":  {Name: "g", Id: "c7", T: ddl.Type{Name: ddl.Int64}},
					"c8":  {Name: "h", Id: "c8", T: ddl.Type{Name: ddl.String, Len: int64(1)}},
					"c9":  {Name: "i", Id: "c9", T: ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}},
					"c10": {Name: "j", Id: "c10", T: ddl.Type{Name: ddl.Date}},
					"c11": {Name: "k", Id: "c11", T: ddl.Type{Name: ddl.Float64}},
					"c12": {Name: "l", Id: "c12", T: ddl.Type{Name: ddl.Int64}},
					"c13": {Name: "m", Id: "c13", T: ddl.Type{Name: ddl.Int64}},
					"c14": {Name: "n", Id: "c14", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"c15": {Name: "o", Id: "c15", T: ddl.Type{Name: ddl.Timestamp}},
					"c16": {Name: "p", Id: "c16", T: ddl.Type{Name: ddl.Int64}},
				},
				PrimaryKeys: []ddl.IndexKey{{ColId: "c1"}},
			},
			expectedIssues: map[string][]internal.SchemaIssue{
				"c1":  {internal.Widened},
				"c2":  {internal.Widened},
				"c3":  {internal.Widened},
				"c7":  {internal.Serial},
				"c12": {internal.Widened},
				"c13": {internal.Serial},
				"c15": {internal.Timestamp},
				"c16": {internal.Widened},
			},
		},
		{
			name: "Test bad payload data request",
			payload: `{
				"Name":              "rule1",
				"Type":              "global_datatype_change",
				"ObjectType":        "Column",
				"AssociatedObjects": "All Columns",
				"Enabled":           true,
				"Data":
		{
		  	"bool":"INT64",
			"int8":"STRING",
			"float4":"STRING",
		}
			}`,
			statusCode: http.StatusBadRequest,
		},
	}
	for _, tc := range tcSetGlobalDataTypePostgres {

		sessionState := session.GetSessionState()

		sessionState.Driver = constants.POSTGRES
		sessionState.Conv = internal.MakeConv()
		buildConvPostgres(sessionState.Conv)
		payload := tc.payload
		req, err := http.NewRequest("POST", "/applyrule", strings.NewReader(payload))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		handler := http.HandlerFunc(applyRule)
		handler.ServeHTTP(rr, req)
		var res *internal.Conv
		json.Unmarshal(rr.Body.Bytes(), &res)
		if status := rr.Code; int64(status) != tc.statusCode {
			t.Errorf("handler returned wrong status code: got %v want %v",
				status, tc.statusCode)
		}

		if tc.statusCode == http.StatusOK {
			assert.Equal(t, tc.expectedSchema, res.SpSchema["t1"])
			assert.Equal(t, tc.expectedIssues, res.SchemaIssues["t1"])
		}
	}

	tcSetGlobalDataTypeMysql := []struct {
		name           string
		payload        string
		statusCode     int64
		expectedSchema ddl.CreateTable
		expectedIssues map[string][]internal.SchemaIssue
	}{
		{
			name: "Test type change",
			payload: `{
			"Name":              "rule1",
			"Type":              "global_datatype_change",
			"ObjectType":        "Column",
			"AssociatedObjects": "All Columns",
			"Enabled":           true,
			"Data":
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
	}
		}`,
			statusCode: http.StatusOK,
			expectedSchema: ddl.CreateTable{
				Name:   "table1",
				Id:     "t1",
				ColIds: []string{"c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11", "c12", "c13", "c14", "c15", "c16"},
				ColDefs: map[string]ddl.ColumnDef{
					"c1":  {Name: "a", Id: "c1", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"c2":  {Name: "b", Id: "c2", T: ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}},
					"c3":  {Name: "c", Id: "c3", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"c4":  {Name: "d", Id: "c4", T: ddl.Type{Name: ddl.Bytes, Len: 6}},
					"c5":  {Name: "e", Id: "c5", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"c6":  {Name: "f", Id: "c6", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"c7":  {Name: "g", Id: "c7", T: ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}},
					"c8":  {Name: "h", Id: "c8", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"c9":  {Name: "i", Id: "c9", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"c10": {Name: "j", Id: "c10", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"c11": {Name: "k", Id: "c11", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"c12": {Name: "l", Id: "c12", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"c13": {Name: "m", Id: "c13", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"c14": {Name: "n", Id: "c14", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"c15": {Name: "o", Id: "c15", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"c16": {Name: "p", Id: "c16", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
				},
				PrimaryKeys: []ddl.IndexKey{{ColId: "c1"}},
			},
			expectedIssues: map[string][]internal.SchemaIssue{
				"c1":  {internal.Widened},
				"c3":  {internal.Widened},
				"c5":  {internal.Widened},
				"c10": {internal.Widened},
				"c11": {internal.Widened},
				"c12": {internal.Widened},
				"c13": {internal.Widened},
				"c14": {internal.Widened},
				"c15": {internal.Widened},
				"c16": {internal.Time},
			},
		},
		{
			name: "Test type change 2",
			payload: `{
				"Name":              "rule1",
				"Type":              "global_datatype_change",
				"ObjectType":        "Column",
				"AssociatedObjects": "All Columns",
				"Enabled":           true,
				"Data":
		{
		  	"bool":"INT64",
			"varchar":"BYTES"
		}
			}`,
			statusCode: http.StatusOK,
			expectedSchema: ddl.CreateTable{
				Name:   "table1",
				Id:     "t1",
				ColIds: []string{"c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11", "c12", "c13", "c14", "c15", "c16"},
				ColDefs: map[string]ddl.ColumnDef{
					"c1":  {Name: "a", Id: "c1", T: ddl.Type{Name: ddl.Int64}},
					"c2":  {Name: "b", Id: "c2", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"c3":  {Name: "c", Id: "c3", T: ddl.Type{Name: ddl.Int64}},
					"c4":  {Name: "d", Id: "c4", T: ddl.Type{Name: ddl.Bytes, Len: 6}},
					"c5":  {Name: "e", Id: "c5", T: ddl.Type{Name: ddl.Numeric}},
					"c6":  {Name: "f", Id: "c6", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"c7":  {Name: "g", Id: "c7", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"c8":  {Name: "h", Id: "c8", T: ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}},
					"c9":  {Name: "i", Id: "c9", T: ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}},
					"c10": {Name: "j", Id: "c10", T: ddl.Type{Name: ddl.Int64}},
					"c11": {Name: "k", Id: "c11", T: ddl.Type{Name: ddl.Float64}},
					"c12": {Name: "l", Id: "c12", T: ddl.Type{Name: ddl.Float64}},
					"c13": {Name: "m", Id: "c13", T: ddl.Type{Name: ddl.Numeric}},
					"c14": {Name: "n", Id: "c14", T: ddl.Type{Name: ddl.Date}},
					"c15": {Name: "o", Id: "c15", T: ddl.Type{Name: ddl.Timestamp}},
					"c16": {Name: "p", Id: "c16", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
				},
				PrimaryKeys: []ddl.IndexKey{{ColId: "c1"}},
			},
			expectedIssues: map[string][]internal.SchemaIssue{
				"c1":  {internal.Widened},
				"c3":  {internal.Widened},
				"c10": {internal.Widened},
				"c12": {internal.Widened},
				"c15": {internal.Time},
			},
		},
		{
			name: "Test bad request",
			payload: `{
				"Name":              "rule1",
				"Type":              "global_datatype_change",
				"ObjectType":        "Column",
				"AssociatedObjects": "All Columns",
				"Enabled":           true,
				"Data":
		{
		  	"bool":"INT64",
			"smallint":"STRING",
		}
			}`,
			statusCode: http.StatusBadRequest,
		},
	}
	for _, tc := range tcSetGlobalDataTypeMysql {
		sessionState := session.GetSessionState()

		sessionState.Driver = constants.MYSQL
		sessionState.Conv = internal.MakeConv()
		buildConvMySQL(sessionState.Conv)
		payload := tc.payload
		req, err := http.NewRequest("POST", "/applyrule", strings.NewReader(payload))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		handler := http.HandlerFunc(applyRule)
		handler.ServeHTTP(rr, req)
		var res *internal.Conv
		json.Unmarshal(rr.Body.Bytes(), &res)
		if status := rr.Code; int64(status) != tc.statusCode {
			t.Errorf("handler returned wrong status code: got %v want %v",
				status, tc.statusCode)
		}

		if tc.statusCode == http.StatusOK {
			assert.Equal(t, tc.expectedSchema, res.SpSchema["t1"])
			assert.Equal(t, tc.expectedIssues, res.SchemaIssues["t1"])
		}
	}
}

func TestDropRule(t *testing.T) {
	tc := []struct {
		name         string
		ruleId       string
		statusCode   int64
		conv         *internal.Conv
		expectedConv *internal.Conv
	}{
		{
			name:       "drop a valid add index rule",
			ruleId:     "r101",
			statusCode: http.StatusOK,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name: "table1",
						Id:   "t1",
						Indexes: []ddl.CreateIndex{
							{Name: "idx1", Id: "i1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c1", Desc: false}}},
							{Name: "idx2", Id: "i2", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c3", Desc: false}, {ColId: "c4", Desc: false}}},
							{Name: "idx3", Id: "i3", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false, Order: 1}}},
						},
					}},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"table1": true, "idx1": true, "idx2": true, "idx3": true},
				Rules: []internal.Rule{{
					Id:                "r101",
					Name:              "add_index",
					Type:              constants.AddIndex,
					ObjectType:        "table",
					AssociatedObjects: "t1",
					Enabled:           true,
					Data:              ddl.CreateIndex{Name: "idx3", Id: "i3", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false, Order: 1}}},
				}},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name: "table1",
						Id:   "t1",
						Indexes: []ddl.CreateIndex{
							{Name: "idx1", Id: "i1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c1", Desc: false}}},
							{Name: "idx2", Id: "i2", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c3", Desc: false}, {ColId: "c4", Desc: false}}},
						},
					}},
			},
		},
		{
			name:       "drop a vaild add global data type rule",
			ruleId:     "r101",
			statusCode: http.StatusOK,
			conv: &internal.Conv{
				SchemaIssues: map[string]map[string][]internal.SchemaIssue{
					"t1": {},
				},
				SrcSchema: map[string]schema.Table{
					"t1": {
						Name:   "table1",
						ColIds: []string{"c1", "c2", "c3"},
						ColDefs: map[string]schema.Column{
							"c1": {Name: "a", Type: schema.Type{Name: "bigint"}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: "c1"},
							"c2": {Name: "b", Type: schema.Type{Name: "bigint"}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: "c2"},
							"c3": {Name: "c", Type: schema.Type{Name: "varchar"}, NotNull: false, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: "c3"},
						},
						PrimaryKeys: []schema.Key{{ColId: "c1", Desc: false, Order: 1}},
						Id:          "t1",
					},
				},
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:   "table1",
						ColIds: []string{"c1", "c2", "c3"},
						ColDefs: map[string]ddl.ColumnDef{
							"c1": {Name: "a", Id: "c1", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c2": {Name: "b", Id: "c2", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
							"c3": {Name: "c", Id: "c3", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c1", Desc: false}},
						Id:          "t1",
					},
				},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_MIGRATION_TYPE_UNSPECIFIED.Enum(),
				},
				Rules: []internal.Rule{
					{
						Id:                "r101",
						Name:              "bigint to BTYES",
						Type:              constants.GlobalDataTypeChange,
						ObjectType:        "Column",
						AssociatedObjects: "All Columns",
						Enabled:           true,
						Data: map[string]string{
							"bigint": ddl.String,
						},
					},
				},
			},
			expectedConv: &internal.Conv{
				SchemaIssues: map[string]map[string][]internal.SchemaIssue{
					"t1": {},
				},
				SrcSchema: map[string]schema.Table{
					"t1": {
						Name:   "table1",
						ColIds: []string{"c1", "c2", "c3"},
						ColDefs: map[string]schema.Column{
							"c1": {Name: "a", Type: schema.Type{Name: "bigint"}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: "c1"},
							"c2": {Name: "b", Type: schema.Type{Name: "bigint"}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: "c2"},
							"c3": {Name: "c", Type: schema.Type{Name: "varchar"}, NotNull: false, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: "c3"},
						},
						PrimaryKeys: []schema.Key{{ColId: "c1", Desc: false, Order: 1}},
						Id:          "t1",
					},
				},
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:   "table1",
						ColIds: []string{"c1", "c2", "c3"},
						ColDefs: map[string]ddl.ColumnDef{
							"c1": {Name: "a", Id: "c1", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c2": {Name: "b", Id: "c2", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c3": {Name: "c", Id: "c3", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c1", Desc: false}},
						Id:          "t1",
					},
				},
			},
		},
		{
			name:       "drop rule with an invalid rule-id",
			ruleId:     "ABC",
			statusCode: http.StatusBadRequest,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name: "table1",
						Id:   "t1",
						Indexes: []ddl.CreateIndex{
							{Name: "idx1", Id: "i1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c1", Desc: false}}},
							{Name: "idx2", Id: "i2", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c3", Desc: false}, {ColId: "c4", Desc: false}}},
							{Name: "idx3", Id: "i3", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false, Order: 1}}},
						},
					}},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"table1": true, "idx1": true, "idx2": true, "idx3": true},
				Rules: []internal.Rule{{
					Id:                "r101",
					Name:              "add_index",
					Type:              constants.AddIndex,
					ObjectType:        "table",
					AssociatedObjects: "t1",
					Enabled:           true,
					Data:              ddl.CreateIndex{Name: "idx3", Id: "i3", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false, Order: 1}}},
				}},
			},
		},
		{
			name:       "drop a disabled valid add index rule",
			ruleId:     "r101",
			statusCode: http.StatusOK,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name: "table1",
						Id:   "t1",
						Indexes: []ddl.CreateIndex{
							{Name: "idx1", Id: "i1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c1", Desc: false}}},
							{Name: "idx2", Id: "i2", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c3", Desc: false}, {ColId: "c4", Desc: false}}},
						},
					}},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"table1": true, "idx1": true, "idx2": true},
				Rules: []internal.Rule{{
					Id:                "r101",
					Name:              "add_index",
					Type:              constants.AddIndex,
					ObjectType:        "table",
					AssociatedObjects: "t1",
					Enabled:           false,
					Data:              ddl.CreateIndex{Name: "idx3", Id: "i3", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false, Order: 1}}},
				}},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name: "table1",
						Id:   "t1",
						Indexes: []ddl.CreateIndex{
							{Name: "idx1", Id: "i1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c1", Desc: false}}},
							{Name: "idx2", Id: "i2", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c3", Desc: false}, {ColId: "c4", Desc: false}}},
						},
					}},
			},
		},
	}
	for _, tc := range tc {
		sessionState := session.GetSessionState()
		sessionState.Driver = constants.MYSQL
		sessionState.Conv = tc.conv
		payload := `{}`
		req, err := http.NewRequest("POST", "/dropRule?id="+tc.ruleId, strings.NewReader(payload))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		handler := http.HandlerFunc(dropRule)
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

func buildConvMySQL(conv *internal.Conv) {
	conv.SrcSchema = map[string]schema.Table{
		"t1": {
			Name:   "table1",
			Id:     "t1",
			ColIds: []string{"c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11", "c12", "c13", "c14", "c15", "c16"},
			ColDefs: map[string]schema.Column{
				"c1":  {Name: "a", Id: "c1", Type: schema.Type{Name: "bool"}},
				"c2":  {Name: "b", Id: "c2", Type: schema.Type{Name: "text"}},
				"c3":  {Name: "c", Id: "c3", Type: schema.Type{Name: "bool"}},
				"c4":  {Name: "d", Id: "c4", Type: schema.Type{Name: "varchar", Mods: []int64{6}}},
				"c5":  {Name: "e", Id: "c5", Type: schema.Type{Name: "numeric"}},
				"c6":  {Name: "f", Id: "c6", Type: schema.Type{Name: "enum"}},
				"c7":  {Name: "g", Id: "c7", Type: schema.Type{Name: "json"}},
				"c8":  {Name: "h", Id: "c8", Type: schema.Type{Name: "binary"}},
				"c9":  {Name: "i", Id: "c9", Type: schema.Type{Name: "blob"}},
				"c10": {Name: "j", Id: "c10", Type: schema.Type{Name: "smallint"}},
				"c11": {Name: "k", Id: "c11", Type: schema.Type{Name: "double"}},
				"c12": {Name: "l", Id: "c12", Type: schema.Type{Name: "float"}},
				"c13": {Name: "m", Id: "c13", Type: schema.Type{Name: "decimal"}},
				"c14": {Name: "n", Id: "c14", Type: schema.Type{Name: "date"}},
				"c15": {Name: "o", Id: "c15", Type: schema.Type{Name: "timestamp"}},
				"c16": {Name: "p", Id: "c16", Type: schema.Type{Name: "time"}},
			},
			PrimaryKeys: []schema.Key{{ColId: "c1"}}},
		"t2": {
			Name:   "table2",
			Id:     "t2",
			ColIds: []string{"c17", "c18", "c19"},
			ColDefs: map[string]schema.Column{
				"c17": {Name: "a", Id: "c17", Type: schema.Type{Name: "integer"}},
				"c18": {Name: "b", Id: "c18", Type: schema.Type{Name: "double"}},
				"c19": {Name: "c", Id: "c19", Type: schema.Type{Name: "bool"}},
			}},
	}
	conv.SpSchema = map[string]ddl.CreateTable{
		"t1": {
			Name:   "table1",
			Id:     "t1",
			ColIds: []string{"c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11", "c12", "c13", "c14", "c15", "c16"},
			ColDefs: map[string]ddl.ColumnDef{
				"c1":  {Name: "a", Id: "c1", T: ddl.Type{Name: ddl.Bool}},
				"c2":  {Name: "b", Id: "c2", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
				"c3":  {Name: "c", Id: "c3", T: ddl.Type{Name: ddl.Bool}},
				"c4":  {Name: "d", Id: "c4", T: ddl.Type{Name: ddl.String, Len: int64(6)}},
				"c5":  {Name: "e", Id: "c5", T: ddl.Type{Name: ddl.Numeric}},
				"c6":  {Name: "f", Id: "c6", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
				"c7":  {Name: "g", Id: "c7", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
				"c8":  {Name: "h", Id: "c8", T: ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}},
				"c9":  {Name: "i", Id: "c9", T: ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}},
				"c10": {Name: "j", Id: "c10", T: ddl.Type{Name: ddl.Int64}},
				"c11": {Name: "k", Id: "c11", T: ddl.Type{Name: ddl.Float64}},
				"c12": {Name: "l", Id: "c12", T: ddl.Type{Name: ddl.Float64}},
				"c13": {Name: "m", Id: "c13", T: ddl.Type{Name: ddl.Numeric}},
				"c14": {Name: "n", Id: "c14", T: ddl.Type{Name: ddl.Date}},
				"c15": {Name: "o", Id: "c15", T: ddl.Type{Name: ddl.Timestamp}},
				"c16": {Name: "p", Id: "c16", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
			},
			PrimaryKeys: []ddl.IndexKey{{ColId: "c1"}},
		},
		"t2": {
			Name:   "t2",
			ColIds: []string{"c17", "c18", "c19", "c20"},
			ColDefs: map[string]ddl.ColumnDef{
				"c17": {Name: "a", Id: "c17", T: ddl.Type{Name: ddl.Int64}},
				"c18": {Name: "b", Id: "c18", T: ddl.Type{Name: ddl.Float64}},
				"c19": {Name: "c", Id: "c19", T: ddl.Type{Name: ddl.Bool}},
				"c20": {Name: "synth_id", Id: "c20", T: ddl.Type{Name: ddl.Int64}},
			},
			PrimaryKeys: []ddl.IndexKey{{ColId: "c20"}},
		},
	}

	conv.SchemaIssues = map[string]map[string][]internal.SchemaIssue{
		"t1": {
			"c10": {internal.Widened},
			"c12": {internal.Widened},
			"c15": {internal.Time},
		},
		"t2": {
			"c17": {internal.Widened},
		},
	}
	conv.SyntheticPKeys["t2"] = internal.SyntheticPKey{"c20", 0}
	conv.Audit.MigrationType = migration.MigrationData_SCHEMA_AND_DATA.Enum()
}

func buildConvPostgres(conv *internal.Conv) {
	conv.SrcSchema = map[string]schema.Table{
		"t1": {
			Name:   "table1",
			Id:     "t1",
			ColIds: []string{"c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11", "c12", "c13", "c14", "c15", "c16"},
			ColDefs: map[string]schema.Column{
				"c1":  {Name: "a", Id: "c1", Type: schema.Type{Name: "int8"}},
				"c2":  {Name: "b", Id: "c2", Type: schema.Type{Name: "float4"}},
				"c3":  {Name: "c", Id: "c3", Type: schema.Type{Name: "bool"}},
				"c4":  {Name: "d", Id: "c4", Type: schema.Type{Name: "varchar", Mods: []int64{6}}},
				"c5":  {Name: "e", Id: "c5", Type: schema.Type{Name: "numeric"}},
				"c6":  {Name: "f", Id: "c6", Type: schema.Type{Name: "timestamptz"}},
				"c7":  {Name: "g", Id: "c7", Type: schema.Type{Name: "bigserial"}},
				"c8":  {Name: "h", Id: "c8", Type: schema.Type{Name: "bpchar"}},
				"c9":  {Name: "i", Id: "c9", Type: schema.Type{Name: "bytea"}},
				"c10": {Name: "j", Id: "c10", Type: schema.Type{Name: "date"}},
				"c11": {Name: "k", Id: "c11", Type: schema.Type{Name: "float8"}},
				"c12": {Name: "l", Id: "c12", Type: schema.Type{Name: "int4"}},
				"c13": {Name: "m", Id: "c13", Type: schema.Type{Name: "serial"}},
				"c14": {Name: "n", Id: "c14", Type: schema.Type{Name: "text"}},
				"c15": {Name: "o", Id: "c15", Type: schema.Type{Name: "timestamp"}},
				"c16": {Name: "p", Id: "c16", Type: schema.Type{Name: "bool"}},
			},
			PrimaryKeys: []schema.Key{{ColId: "c1"}}},
		"t2": {
			Name:   "t2",
			Id:     "t2",
			ColIds: []string{"c17", "c18", "c19"},
			ColDefs: map[string]schema.Column{
				"c17": {Name: "a", Id: "c17", Type: schema.Type{Name: "int8"}},
				"c18": {Name: "b", Id: "c18", Type: schema.Type{Name: "float4"}},
				"c19": {Name: "c", Id: "c19", Type: schema.Type{Name: "bool"}},
			}},
	}
	conv.SpSchema = map[string]ddl.CreateTable{
		"t1": {
			Name:   "table1",
			Id:     "t1",
			ColIds: []string{"c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11", "c12", "c13", "c14", "c15", "c16"},
			ColDefs: map[string]ddl.ColumnDef{
				"c1":  {Name: "a", Id: "c1", T: ddl.Type{Name: ddl.Int64}},
				"c2":  {Name: "b", Id: "c2", T: ddl.Type{Name: ddl.Float64}},
				"c3":  {Name: "c", Id: "c3", T: ddl.Type{Name: ddl.Bool}},
				"c4":  {Name: "d", Id: "c4", T: ddl.Type{Name: ddl.String, Len: int64(6)}},
				"c5":  {Name: "e", Id: "c5", T: ddl.Type{Name: ddl.Numeric}},
				"c6":  {Name: "f", Id: "c6", T: ddl.Type{Name: ddl.Timestamp}},
				"c7":  {Name: "g", Id: "c7", T: ddl.Type{Name: ddl.Int64}},
				"c8":  {Name: "h", Id: "c8", T: ddl.Type{Name: ddl.String, Len: int64(1)}},
				"c9":  {Name: "i", Id: "c9", T: ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}},
				"c10": {Name: "j", Id: "c10", T: ddl.Type{Name: ddl.Date}},
				"c11": {Name: "k", Id: "c11", T: ddl.Type{Name: ddl.Float64}},
				"c12": {Name: "l", Id: "c12", T: ddl.Type{Name: ddl.Int64}},
				"c13": {Name: "m", Id: "c13", T: ddl.Type{Name: ddl.Int64}},
				"c14": {Name: "n", Id: "c14", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
				"c15": {Name: "o", Id: "c15", T: ddl.Type{Name: ddl.Timestamp}},
				"c16": {Name: "p", Id: "c16", T: ddl.Type{Name: ddl.Int64}},
			},
			PrimaryKeys: []ddl.IndexKey{{ColId: "c1"}},
		},
		"t2": {
			Name:   "table2",
			Id:     "t2",
			ColIds: []string{"c17", "c18", "c19", "c20"},
			ColDefs: map[string]ddl.ColumnDef{
				"c17": {Name: "a", Id: "c17", T: ddl.Type{Name: ddl.Int64}},
				"c18": {Name: "b", Id: "c18", T: ddl.Type{Name: ddl.Float64}},
				"c19": {Name: "c", Id: "c19", T: ddl.Type{Name: ddl.Bool}},
				"c20": {Name: "synth_id", Id: "c20", T: ddl.Type{Name: ddl.Int64}},
			},
			PrimaryKeys: []ddl.IndexKey{{ColId: "c20"}},
		},
	}

	conv.SchemaIssues = map[string]map[string][]internal.SchemaIssue{
		"t1": {
			"c2":  {internal.Widened},   //b
			"c7":  {internal.Serial},    //g
			"c12": {internal.Widened},   //l
			"c13": {internal.Serial},    //m
			"c15": {internal.Timestamp}, //o
		},
		"t2": {
			"c18": {internal.Widened},
		},
	}
	conv.SyntheticPKeys["t2"] = internal.SyntheticPKey{"c20", 0}
	conv.Audit.MigrationType = migration.MigrationData_SCHEMA_AND_DATA.Enum()
}

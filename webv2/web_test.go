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
				SpSchema: map[string]ddl.CreateTable{"t1": {
					Name:     "t1",
					ColNames: []string{"a", "b", "c"},
					ColDefs: map[string]ddl.ColumnDef{"a": ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
						"b": ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
						"c": ddl.ColumnDef{Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true}},
					Pks: []ddl.IndexKey{ddl.IndexKey{Col: "a", Desc: false, Order: 1}},
					Fks: []ddl.Foreignkey{ddl.Foreignkey{Name: "fk1", Columns: []string{"a"}, ReferTable: "ref_t1", ReferColumns: []string{"ref_c1"}},
						ddl.Foreignkey{Name: "fk2", Columns: []string{"c"}, ReferTable: "ref_t2", ReferColumns: []string{"ref_c2"}}},
				}},
				Issues: map[string]map[string][]internal.SchemaIssue{
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
					Name:     "t1",
					ColNames: []string{"a", "b", "c"},
					ColDefs: map[string]ddl.ColumnDef{"a": {Name: "a", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
						"b":        {Name: "b", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
						"c":        {Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						"synth_id": {Name: "synth_id", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
					},
					Pks: []ddl.IndexKey{{Col: "synth_id", Desc: false}},
					Fks: []ddl.Foreignkey{{Name: "fk1", Columns: []string{"a"}, ReferTable: "ref_t1", ReferColumns: []string{"ref_c1"}},
						{Name: "fk2", Columns: []string{"c"}, ReferTable: "ref_t2", ReferColumns: []string{"ref_c2"}}},
				}},
				SyntheticPKeys: map[string]internal.SyntheticPKey{"t1": internal.SyntheticPKey{Col: "synth_id"}},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				Issues: map[string]map[string][]internal.SchemaIssue{
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
						Name:     "t1",
						ColNames: []string{"a", "b", "c"},
						ColDefs: map[string]ddl.ColumnDef{"a": {Name: "a", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"b": {Name: "b", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c": {Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						Pks: []ddl.IndexKey{{Col: "a", Desc: false, Order: 1}},
						Fks: []ddl.Foreignkey{{Name: "fk1", Columns: []string{"a"}, ReferTable: "t2", ReferColumns: []string{"a"}}},
					},
					"t2": {
						Name:     "t2",
						ColNames: []string{"a", "b", "c"},
						ColDefs: map[string]ddl.ColumnDef{"a": {Name: "a", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"b":        {Name: "b", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c":        {Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
							"synth_id": {Name: "synth_id", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
						},
						Pks: []ddl.IndexKey{{Col: "synth_id", Desc: false}},
					},
				},
				SyntheticPKeys: map[string]internal.SyntheticPKey{"t2": internal.SyntheticPKey{Col: "synth_id"}},
				Issues: map[string]map[string][]internal.SchemaIssue{
					"t1": {},
				},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
			},
			table:            "t1",
			statusCode:       http.StatusOK,
			expectedResponse: &TableInterleaveStatus{Possible: false, Comment: "No valid prefix"},
			expectedFKs:      []ddl.Foreignkey{{Name: "fk1", Columns: []string{"a"}, ReferTable: "t2", ReferColumns: []string{"a"}}},
		},
		{
			name: "no valid prefix 2",
			ct: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:     "t1",
						ColNames: []string{"a", "b", "c"},
						ColDefs: map[string]ddl.ColumnDef{"a": {Name: "a", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"b": {Name: "b", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c": {Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						Pks: []ddl.IndexKey{{Col: "a", Desc: false, Order: 1}},
						Fks: []ddl.Foreignkey{{Name: "fk1", Columns: []string{"a"}, ReferTable: "t2", ReferColumns: []string{"a"}}},
					},
					"t2": {
						Name:     "t2",
						ColNames: []string{"a", "b", "c"},
						ColDefs: map[string]ddl.ColumnDef{"a": {Name: "a", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"b": {Name: "b", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c": {Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						Pks: []ddl.IndexKey{{Col: "a", Desc: false, Order: 1}, {Col: "b", Desc: false}},
					},
				},
				Issues: map[string]map[string][]internal.SchemaIssue{
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
						Name:     "t1",
						ColNames: []string{"a", "b", "c"},
						ColDefs: map[string]ddl.ColumnDef{"a": {Name: "a", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"b": {Name: "b", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c": {Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						Pks: []ddl.IndexKey{{Col: "a", Desc: false, Order: 1}, {Col: "b", Desc: false, Order: 2}},
						Fks: []ddl.Foreignkey{{Name: "fk1", Columns: []string{"c"}, ReferTable: "t2", ReferColumns: []string{"c"}}},
					},
					"t2": {
						Name:     "t2",
						ColNames: []string{"a", "b", "c"},
						ColDefs: map[string]ddl.ColumnDef{"a": {Name: "a", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"b": {Name: "b", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c": {Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						Pks: []ddl.IndexKey{{Col: "a", Desc: false, Order: 1}},
					},
				},
				Issues: map[string]map[string][]internal.SchemaIssue{
					"t1": {},
				},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
			},
			table:            "t1",
			statusCode:       http.StatusOK,
			expectedResponse: &TableInterleaveStatus{Possible: false, Comment: "No valid prefix"},
			expectedFKs:      []ddl.Foreignkey{{Name: "fk1", Columns: []string{"c"}, ReferTable: "t2", ReferColumns: []string{"c"}}},
		},
		{
			name: "successful interleave",
			ct: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:     "t1",
						ColNames: []string{"a", "b", "c"},
						ColDefs: map[string]ddl.ColumnDef{"a": {Name: "a", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"b": {Name: "b", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c": {Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						Pks: []ddl.IndexKey{{Col: "a", Desc: false, Order: 1}, {Col: "b", Desc: false, Order: 2}},
						Fks: []ddl.Foreignkey{{Name: "fk1", Columns: []string{"a"}, ReferTable: "t2", ReferColumns: []string{"a"}}},
					},
					"t2": {
						Name:     "t2",
						ColNames: []string{"a", "b", "c"},
						ColDefs: map[string]ddl.ColumnDef{"a": {Name: "a", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"b": {Name: "b", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c": {Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						Pks: []ddl.IndexKey{{Col: "a", Desc: false, Order: 1}},
					},
				},
				Issues: map[string]map[string][]internal.SchemaIssue{
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
						Name:     "t1",
						ColNames: []string{"a", "b", "c"},
						ColDefs: map[string]ddl.ColumnDef{"a": {Name: "a", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"b": {Name: "b", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c": {Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						Pks: []ddl.IndexKey{{Col: "a", Desc: false, Order: 1}, {Col: "b", Desc: false, Order: 2}},
						Fks: []ddl.Foreignkey{{Name: "fk1", Columns: []string{"a", "b"}, ReferTable: "t2", ReferColumns: []string{"a", "b"}}},
					},
					"t2": {
						Name:     "t2",
						ColNames: []string{"a", "b", "c"},
						ColDefs: map[string]ddl.ColumnDef{"a": {Name: "a", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"b": {Name: "b", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c": {Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						Pks: []ddl.IndexKey{{Col: "a", Desc: false, Order: 1}, {Col: "b", Desc: false, Order: 2}},
					},
				},
				Issues: map[string]map[string][]internal.SchemaIssue{
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
						Name:     "t1",
						ColNames: []string{"a", "b", "c"},
						ColDefs: map[string]ddl.ColumnDef{"a": {Name: "a", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"b": {Name: "b", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c": {Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						Pks: []ddl.IndexKey{{Col: "a", Desc: false, Order: 1}, {Col: "b", Desc: false, Order: 2}},
						Fks: []ddl.Foreignkey{
							{Name: "fk1", Columns: []string{"c"}, ReferTable: "t3", ReferColumns: []string{"c"}},
							{Name: "fk1", Columns: []string{"a", "b"}, ReferTable: "t2", ReferColumns: []string{"a", "b"}}},
					},
					"t2": {
						Name:     "t2",
						ColNames: []string{"a", "b", "c"},
						ColDefs: map[string]ddl.ColumnDef{"a": {Name: "a", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"b": {Name: "b", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c": {Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						Pks: []ddl.IndexKey{{Col: "a", Desc: false, Order: 1}, {Col: "b", Desc: false, Order: 2}},
					},
					"t3": {
						Name:     "t3",
						ColNames: []string{"a", "b", "c"},
						ColDefs: map[string]ddl.ColumnDef{"a": {Name: "a", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"b": {Name: "b", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c": {Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						Pks: []ddl.IndexKey{{Col: "c", Desc: false, Order: 1}},
					},
				},
				Issues: map[string]map[string][]internal.SchemaIssue{
					"t1": {},
				},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
			},
			table:            "t1",
			statusCode:       http.StatusOK,
			expectedResponse: &TableInterleaveStatus{Possible: true, Parent: "t2"},
			expectedFKs: []ddl.Foreignkey{
				{Name: "fk1", Columns: []string{"c"}, ReferTable: "t3", ReferColumns: []string{"c"}}},
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
		payload      string
		statusCode   int64
		conv         *internal.Conv
		expectedConv *internal.Conv
	}{
		{
			name:       "Test drop valid FK success",
			table:      "t1",
			payload:    `{"Name":"fk2"}`,
			statusCode: http.StatusOK,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Fks: []ddl.Foreignkey{{Name: "fk1", Columns: []string{"b"}, ReferTable: "reft1", ReferColumns: []string{"ref_b"}},
							{Name: "fk2", Columns: []string{"c", "d"}, ReferTable: "reft2", ReferColumns: []string{"ref_c", "ref_d"}}},
					}},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Fks: []ddl.Foreignkey{{Name: "fk1", Columns: []string{"b"}, ReferTable: "reft1", ReferColumns: []string{"ref_b"}}},
					}},
			},
		},
		{
			name:       "Test drop FK invalid fkName",
			table:      "t1",
			payload:    `{"Name":""}`,
			statusCode: http.StatusBadRequest,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Fks: []ddl.Foreignkey{{Name: "fk1", Columns: []string{"b"}, ReferTable: "reft1", ReferColumns: []string{"ref_b"}}},
					}},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
			},
		},
		{
			name:       "Test drop FK invalid fkName 2",
			table:      "t1",
			payload:    `{"Name":"AB"}`,
			statusCode: http.StatusBadRequest,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Fks: []ddl.Foreignkey{{Name: "fk1", Columns: []string{"b"}, ReferTable: "reft1", ReferColumns: []string{"ref_b"}}},
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
		req, err := http.NewRequest("POST", "/drop/fk?table="+tc.table, strings.NewReader(payload))
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
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"t1": true, "idx": true},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx_new", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}}},
					}},
				UsedNames: map[string]bool{"t1": true, "idx": true},
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
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"t1": true, "idx_1": true, "idx_2": true},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx_new_1", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}},
							{Name: "idx_new_2", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}}},
					}},
				UsedNames: map[string]bool{"t1": true, "idx_1": true, "idx_2": true},
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
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"t1": true, "idx_1": true, "idx_2": true},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx_1", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}},
							{Name: "idx_2", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}}},
					}},
				UsedNames: map[string]bool{"t1": true, "idx_1": true, "idx_2": true},
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
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"t1": true, "idx_1": true, "idx_2": true},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx_new_1", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}},
							{Name: "idx_new_2", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}}},
					}},
				UsedNames: map[string]bool{"t1": true, "idx_1": true, "idx_2": true},
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
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"t1": true, "idx_1": true, "idx_2": true, "fk1": true, "fk2": true},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx_new_1", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}},
							{Name: "idx_new_2", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}}},
					}},
				UsedNames: map[string]bool{"t1": true, "idx_1": true, "idx_2": true, "fk1": true, "fk2": true},
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
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"t1": true, "idx": true},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}}},
					}},
				UsedNames: map[string]bool{"t1": true, "idx": true},
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
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"t1": true, "idx1": true, "idx2": true},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx1", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}},
							{Name: "idx2", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}}},
					}},
				UsedNames: map[string]bool{"t1": true, "idx1": true, "idx2": true},
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
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"t1": true, "idx": true},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}}},
					}},
				UsedNames: map[string]bool{"t1": true, "idx": true},
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
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"t1": true, "idx": true},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}}},
					}},
				UsedNames: map[string]bool{"t1": true, "idx": true},
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
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"t1": true, "fk1": true, "fk2": true, "reft1": true, "reft2": true},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Fks: []ddl.Foreignkey{{Name: "foreignkey1", Columns: []string{"b"}, ReferTable: "reft1", ReferColumns: []string{"ref_b"}},
							{Name: "fk2", Columns: []string{"c", "d"}, ReferTable: "reft2", ReferColumns: []string{"ref_c", "ref_d"}}},
					}},
				UsedNames: map[string]bool{"t1": true, "fk1": true, "fk2": true, "reft1": true, "reft2": true},
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
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"t1": true, "fk1": true, "fk2": true, "reft1": true, "reft2": true},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Fks: []ddl.Foreignkey{{Name: "foreignkey1", Columns: []string{"b"}, ReferTable: "reft1", ReferColumns: []string{"ref_b"}},
							{Name: "foreignkey2", Columns: []string{"c", "d"}, ReferTable: "reft2", ReferColumns: []string{"ref_c", "ref_d"}}},
					}},
				UsedNames: map[string]bool{"t1": true, "fk1": true, "fk2": true, "reft1": true, "reft2": true},
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
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"t1": true, "fk1": true, "fk2": true, "reft1": true, "reft2": true},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Fks: []ddl.Foreignkey{{Name: "foreignkey1", Columns: []string{"b"}, ReferTable: "reft1", ReferColumns: []string{"ref_b"}},
							{Name: "fk2", Columns: []string{"c", "d"}, ReferTable: "reft2", ReferColumns: []string{"ref_c", "ref_d"}}},
					}},
				UsedNames: map[string]bool{"t1": true, "fk1": true, "fk2": true, "reft1": true, "reft2": true},
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
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"t1": true, "fk1": true, "fk2": true, "reft1": true, "reft2": true},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Fks: []ddl.Foreignkey{{Name: "foreignkey1", Columns: []string{"b"}, ReferTable: "reft1", ReferColumns: []string{"ref_b"}},
							{Name: "fk2", Columns: []string{"c", "d"}, ReferTable: "reft2", ReferColumns: []string{"ref_c", "ref_d"}}},
					}},
				UsedNames: map[string]bool{"t1": true, "fk1": true, "fk2": true, "reft1": true, "reft2": true},
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
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"t1": true, "idx_1": true, "idx_2": true, "fk1": true, "fk2": true, "reft1": true, "reft2": true},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx_new_1", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}},
							{Name: "idx_new_2", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}}},
					}},
				UsedNames: map[string]bool{"t1": true, "idx_1": true, "idx_2": true, "fk1": true, "fk2": true, "reft1": true, "reft2": true},
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
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"t1": true, "idx1": true, "idx2": true},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx1", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}},
							{Name: "idx2", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}}},
					}},
				UsedNames: map[string]bool{"t1": true, "idx1": true, "idx2": true},
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
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"t1": true, "fk1": true, "fk2": true, "reft1": true, "reft2": true},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Fks: []ddl.Foreignkey{{Name: "fk1", Columns: []string{"b"}, ReferTable: "reft1", ReferColumns: []string{"ref_b"}},
							{Name: "fk2", Columns: []string{"c", "d"}, ReferTable: "reft2", ReferColumns: []string{"ref_c", "ref_d"}}},
					}},
				UsedNames: map[string]bool{"t1": true, "fk1": true, "fk2": true, "reft1": true, "reft2": true},
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
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"t1": true, "fk1": true, "fk2": true, "reft1": true, "reft2": true},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Fks: []ddl.Foreignkey{{Name: "fk1", Columns: []string{"b"}, ReferTable: "reft1", ReferColumns: []string{"ref_b"}},
							{Name: "fk2", Columns: []string{"c", "d"}, ReferTable: "reft2", ReferColumns: []string{"ref_c", "ref_d"}}},
					}},
				UsedNames: map[string]bool{"t1": true, "fk1": true, "fk2": true, "reft1": true, "reft2": true},
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
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"t1": true, "fk1": true, "fk2": true, "reft1": true, "reft2": true},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Fks: []ddl.Foreignkey{{Name: "fk1", Columns: []string{"b"}, ReferTable: "reft1", ReferColumns: []string{"ref_b"}},
							{Name: "fk2", Columns: []string{"c", "d"}, ReferTable: "reft2", ReferColumns: []string{"ref_c", "ref_d"}}},
					}},
				UsedNames: map[string]bool{"t1": true, "fk1": true, "fk2": true, "reft1": true, "reft2": true},
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
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"t1": true, "t2": true, "fk1": true, "fk2": true, "reft1": true, "reft2": true, "t2_fk1": true, "t2_fk2": true},
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
				UsedNames: map[string]bool{"t1": true, "t2": true, "fk1": true, "fk2": true, "reft1": true, "reft2": true, "t2_fk1": true, "t2_fk2": true},
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
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"t1": true, "idx1": true, "idx2": true},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx1", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}},
							{Name: "idx2", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "c", Desc: false}, {Col: "d", Desc: false}}}},
					}},
				UsedNames: map[string]bool{"t1": true, "idx1": true, "idx2": true},
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
						Indexes: []ddl.CreateIndex{
							{Name: "idx1", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}},
							{Name: "idx2", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "c", Desc: false}, {Col: "d", Desc: false}}}},
					}},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"t1": true, "idx1": true, "idx2": true},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{
							{Name: "idx1", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}},
							{Name: "idx2", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "c", Desc: false}, {Col: "d", Desc: false}}},
							{Id: "i1", Name: "idx3", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}},
						},
					}},
				UsedNames: map[string]bool{"t1": true, "idx1": true, "idx2": true},
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
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"t1": true, "idx1": true, "idx2": true},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx1", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}},
							{Name: "idx2", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "c", Desc: false}, {Col: "d", Desc: false}}},
							{Id: "i2", Name: "idx3", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}},
							{Id: "i3", Name: "idx4", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}},
						},
					}},
				UsedNames: map[string]bool{"t1": true, "idx1": true, "idx2": true},
			},
		}, {
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
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"t1": true, "idx1": true, "idx2": true},
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
				UsedNames: map[string]bool{"t1": true, "idx1": true, "idx2": true},
			},
		}, {
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
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"t1": true, "idx1": true, "idx2": true},
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
				UsedNames: map[string]bool{"t1": true, "idx1": true, "idx2": true},
			},
		}, {
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
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"t1": true, "idx1": true, "idx2": true},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx1", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}},
							{Name: "idx2", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}}},
					}},
				UsedNames: map[string]bool{"t1": true, "idx1": true, "idx2": true},
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
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"t1": true, "idx1": true, "idx2": true},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx1", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}},
							{Name: "idx2", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "c", Desc: false}, {Col: "d", Desc: false}}},
						},
					}},
				UsedNames: map[string]bool{"t1": true, "idx1": true, "idx2": true},
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
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"t1": true, "idx1": true, "idx2": true},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx1", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}},
							{Name: "idx2", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "c", Desc: false}, {Col: "d", Desc: false}}},
						},
					}},
				UsedNames: map[string]bool{"t1": true, "idx1": true, "idx2": true},
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
		payload      string
		statusCode   int64
		conv         *internal.Conv
		expectedConv *internal.Conv
	}{
		{
			name:       "Test drop valid secondary index success",
			table:      "t1",
			payload:    `{"Name":"idx2"}`,
			statusCode: http.StatusOK,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx1", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}},
							{Name: "idx2", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "c", Desc: false}, {Col: "d", Desc: false}}}},
					}},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx1", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}}},
					}},
			},
		},
		{
			name:       "Test drop secondary index invalid name",
			table:      "t1",
			payload:    `{"Name":""}`,
			statusCode: http.StatusBadRequest,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx1", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}}},
					}},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
			},
		},
		{
			name:       "Test drop secondary index invalid name 2",
			table:      "t1",
			payload:    `{"Name":"AB"}`,
			statusCode: http.StatusBadRequest,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx1", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}}},
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
						Name: "t1",
						Indexes: []schema.Index{
							{Name: "idx1", Unique: false, Keys: []schema.Key{{Column: "b", Desc: false, Order: 1}}, Id: "i1"},
							{Name: "idx2", Unique: false, Keys: []schema.Key{{Column: "c", Desc: false, Order: 1}}, Id: "i2"},
						},
						Id: "t1",
					},
				},
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name: "t1",
						Indexes: []ddl.CreateIndex{
							{Name: "idx2", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "c", Desc: false, Order: 1}}, Id: "i2"},
						},
						Id: "t1",
					},
				},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
					ToSourceFkIdx: map[string]internal.FkeyAndIdxs{
						"t1": {
							Name:       "t1",
							ForeignKey: map[string]string{},
							Index:      map[string]string{"idx2": "idx2"},
						},
					},
					ToSpannerFkIdx: map[string]internal.FkeyAndIdxs{
						"t1": {
							Name:       "t1",
							ForeignKey: map[string]string{},
							Index:      map[string]string{"idx2": "idx2"},
						},
					},
				},
				ToSource: map[string]internal.NameAndCols{
					"t1": {Name: "t1", Cols: map[string]string{"a": "a", "b": "b", "c": "c"}},
				},
				ToSpanner: map[string]internal.NameAndCols{
					"t1": {Name: "t1", Cols: map[string]string{"a": "a", "b": "b", "c": "c"}},
				},
				UsedNames: map[string]bool{"t1": true, "idx2": true},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name: "t1",
						Indexes: []ddl.CreateIndex{
							{Name: "idx2", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "c", Desc: false, Order: 1}}, Id: "i2"},
							{Name: "idx1", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false, Order: 1}}, Id: "i1"},
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
						Indexes: []ddl.CreateIndex{{Name: "idx1", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false, Order: 1}}, Id: "i1"}},
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
						Indexes: []ddl.CreateIndex{{Name: "idx1", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false, Order: 1}}, Id: "i1"}},
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

	c := &internal.Conv{
		Issues: map[string]map[string][]internal.SchemaIssue{
			"t1": {},
			"t2": {},
		},
		SrcSchema: map[string]schema.Table{
			"t1": {
				Name:     "t1",
				ColNames: []string{"a", "b", "c"},
				ColDefs: map[string]schema.Column{
					"a": {Name: "a", Type: schema.Type{Name: "bigint"}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: "c4"},
					"b": {Name: "b", Type: schema.Type{Name: "bigint"}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: "c2"},
					"c": {Name: "c", Type: schema.Type{Name: "varchar"}, NotNull: false, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: "c3"},
				},
				PrimaryKeys: []schema.Key{{Column: "a", Desc: false, Order: 1}},
				Id:          "id1",
			},

			"t2": {
				Name:     "t2",
				ColNames: []string{"a", "b", "c"},
				ColDefs: map[string]schema.Column{
					"a": {Name: "a", Type: schema.Type{Name: "bigint"}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: "c7"},
					"b": {Name: "b", Type: schema.Type{Name: "varchar"}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: "c8"},
					"c": {Name: "c", Type: schema.Type{Name: "bigint"}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: "c9"},
				},
				Id: "id2",
			},
		},
		SpSchema: map[string]ddl.CreateTable{
			"t1": {
				Name:     "t1",
				ColNames: []string{"a", "b", "c"},
				ColDefs: map[string]ddl.ColumnDef{"a": {Name: "a", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
					"b": {Name: "b", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
					"c": {Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
				},
				Pks: []ddl.IndexKey{{Col: "a", Desc: false}},
				Id:  "id1",
			},
			"t2": {
				Name:     "t2",
				ColNames: []string{"a", "b", "c"},
				ColDefs: map[string]ddl.ColumnDef{"a": {Name: "a", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
					"b":        {Name: "b", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
					"c":        {Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
					"synth_id": {Name: "synth_id", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
				},
				Pks: []ddl.IndexKey{{Col: "synth_id", Desc: false}},
				Id:  "id2",
			}},
		Audit: internal.Audit{
			MigrationType: migration.MigrationData_MIGRATION_TYPE_UNSPECIFIED.Enum(),
		},
	}

	sessionState.Conv = c

	payload := `{}`

	req, err := http.NewRequest("POST", "/drop/table?tableId=id1", strings.NewReader(payload))
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
				Name:     "t2",
				ColNames: []string{"a", "b", "c"},
				ColDefs: map[string]ddl.ColumnDef{"a": {Name: "a", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
					"b":        {Name: "b", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
					"c":        {Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
					"synth_id": {Name: "synth_id", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
				},
				Pks:     []ddl.IndexKey{{Col: "synth_id", Desc: false}},
				Fks:     []ddl.Foreignkey{},
				Indexes: []ddl.CreateIndex(nil),
				Id:      "id2",
			}},
	}

	assert.Equal(t, expectedConv.SpSchema, res.SpSchema)
}

func TestRestoreTable(t *testing.T) {
	sessionState := session.GetSessionState()

	sessionState.Driver = constants.MYSQL

	c := &internal.Conv{

		SpSchema: map[string]ddl.CreateTable{
			"t2": {
				Name:     "t2",
				ColNames: []string{"a", "b"},
				ColDefs: map[string]ddl.ColumnDef{"a": {Name: "a", T: ddl.Type{Name: "STRING", IsArray: false}, NotNull: true, Comment: "", Id: "c7"},
					"b": {Name: "b", T: ddl.Type{Name: "STRING", IsArray: false}, NotNull: true, Comment: "", Id: "c8"},
				},
				Pks: []ddl.IndexKey{{Col: "a", Desc: false, Order: 1}},
				Id:  "t6",
			}},
		SrcSchema: map[string]schema.Table{
			"t1": {
				Name:     "t1",
				ColNames: []string{"a", "x", "y"},
				ColDefs: map[string]schema.Column{
					"a": {Name: "a", Type: schema.Type{Name: "varchar"}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: "c4"},
					"x": {Name: "x", Type: schema.Type{Name: "varchar"}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: "c2"},
					"y": {Name: "y", Type: schema.Type{Name: "bigint"}, NotNull: false, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: "c3"},
				},
				PrimaryKeys: []schema.Key{{Column: "a", Desc: false, Order: 1}},
				Id:          "t1",
			},

			"t2": {
				Name:     "t2",
				ColNames: []string{"a", "b"},
				ColDefs: map[string]schema.Column{
					"a": {Name: "a", Type: schema.Type{Name: "varchar"}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: "c7"},
					"b": {Name: "b", Type: schema.Type{Name: "char"}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: "c8"},
				},
				PrimaryKeys: []schema.Key{{Column: "a", Desc: false, Order: 1}},
				Id:          "t6",
			},
		},

		UsedNames: map[string]bool{
			"t2": true,
		},

		ToSource: map[string]internal.NameAndCols{
			"t2": {Name: "t2", Cols: map[string]string{"a": "a", "b": "b"}},
		},
		ToSpanner: map[string]internal.NameAndCols{
			"t2": {Name: "t2", Cols: map[string]string{"a": "a", "b": "b"}},
		},

		Audit: internal.Audit{
			MigrationType: migration.MigrationData_MIGRATION_TYPE_UNSPECIFIED.Enum(),
			ToSourceFkIdx: map[string]internal.FkeyAndIdxs{
				"t2": {
					Name:       "t2",
					ForeignKey: map[string]string{},
					Index:      map[string]string{},
				},
			},
			ToSpannerFkIdx: map[string]internal.FkeyAndIdxs{
				"t2": {
					Name:       "t2",
					ForeignKey: map[string]string{},
					Index:      map[string]string{},
				},
			},
		},

		Issues: map[string]map[string][]internal.SchemaIssue{},
	}

	sessionState.Conv = c

	payload := `{}`

	req, err := http.NewRequest("POST", "/restore/table?tableId=t1", strings.NewReader(payload))

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
				Name:     "t1",
				ColNames: []string{"a", "x", "y"},
				ColDefs: map[string]ddl.ColumnDef{
					"a": {Name: "a", T: ddl.Type{Name: "STRING", Len: 9223372036854775807, IsArray: false}, NotNull: true, Comment: "From: a varchar", Id: "c4"},
					"x": {Name: "x", T: ddl.Type{Name: "STRING", Len: 9223372036854775807, IsArray: false}, NotNull: true, Comment: "From: x varchar", Id: "c2"},
					"y": {Name: "y", T: ddl.Type{Name: "INT64", Len: 0, IsArray: false}, NotNull: false, Comment: "From: y bigint", Id: "c3"},
				},
				Pks:     []ddl.IndexKey{{Col: "a", Desc: false, Order: 1}},
				Id:      "t1",
				Comment: "Spanner schema for source table t1",
			},

			"t2": {
				Name:     "t2",
				ColNames: []string{"a", "b"},
				ColDefs: map[string]ddl.ColumnDef{"a": {Name: "a", T: ddl.Type{Name: "STRING", Len: 0, IsArray: false}, NotNull: true, Comment: "", Id: "c7"},
					"b": {Name: "b", T: ddl.Type{Name: "STRING", Len: 0, IsArray: false}, NotNull: true, Comment: "", Id: "c8"},
				},
				Pks: []ddl.IndexKey{{Col: "a", Desc: false, Order: 1}},
				Id:  "t6",
			}},
	}
	assert.Equal(t, expectedConv.SpSchema, res.SpSchema)

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
	conv.Audit.MigrationType = migration.MigrationData_SCHEMA_AND_DATA.Enum()
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
	conv.Audit.MigrationType = migration.MigrationData_SCHEMA_AND_DATA.Enum()
}

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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/schema"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
	"github.com/stretchr/testify/assert"
)

func TestGetTypeMap(t *testing.T) {
	// Create a request to pass to our handler. We don't have any query parameters for now, so we'll
	// pass 'nil' as the third parameter.
	req, err := http.NewRequest("GET", "/typemap", nil)
	if err != nil {
		t.Fatal(err)
	}

	// We create a ResponseRecorder (which satisfies http.ResponseWriter) to record the response.
	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(getTypeMap)

	// Our handlers satisfy http.Handler, so we can call their ServeHTTP method
	// directly and pass in our Request and ResponseRecorder.
	handler.ServeHTTP(rr, req)

	// Check the status code is what we expect.
	if status := rr.Code; status != http.StatusNotFound {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusNotFound)
	}

}

func TestGetTypeMapSuccess(t *testing.T) {
	// Create a request to pass to our handler. We don't have any query parameters for now, so we'll
	// pass 'nil' as the third parameter.
	app.driver = "postgres"
	app.conv = internal.MakeConv()
	fillConvPostgres(app.conv)
	req, err := http.NewRequest("GET", "/typemap", nil)
	if err != nil {
		t.Fatal(err)
	}

	// We create a ResponseRecorder (which satisfies http.ResponseWriter) to record the response.
	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(getTypeMap)

	// Our handlers satisfy http.Handler, so we can call their ServeHTTP method
	// directly and pass in our Request and ResponseRecorder.
	handler.ServeHTTP(rr, req)
	var typemap map[string][]typeIssue
	json.Unmarshal(rr.Body.Bytes(), &typemap)
	// Check the status code is what we expect.
	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusOK)
	}
	expectedTypemap := map[string][]typeIssue{
		"bool": []typeIssue{
			typeIssue{T: ddl.Bool},
			typeIssue{T: ddl.String, Issue: internal.Widened},
			typeIssue{T: ddl.Int64, Issue: internal.Widened}},
		"bigserial": []typeIssue{
			typeIssue{T: ddl.String, Issue: internal.Widened},
			typeIssue{T: ddl.Int64, Issue: internal.Serial}},
		"bpchar": []typeIssue{
			typeIssue{T: ddl.String},
			typeIssue{T: ddl.Bytes}},
		"bytea": []typeIssue{
			typeIssue{T: ddl.String, Issue: internal.Widened},
			typeIssue{T: ddl.Bytes}},
		"date": []typeIssue{
			typeIssue{T: ddl.String, Issue: internal.Widened},
			typeIssue{T: ddl.Date}},
		"float8": []typeIssue{
			typeIssue{T: ddl.String, Issue: internal.Widened},
			typeIssue{T: ddl.Float64}},
		"float4": []typeIssue{
			typeIssue{T: ddl.String, Issue: internal.Widened},
			typeIssue{T: ddl.Float64, Issue: internal.Widened}},
		"int8": []typeIssue{
			typeIssue{T: ddl.String, Issue: internal.Widened},
			typeIssue{T: ddl.Int64}},
		"int4": []typeIssue{
			typeIssue{T: ddl.String, Issue: internal.Widened},
			typeIssue{T: ddl.Int64, Issue: internal.Widened}},
		"numeric": []typeIssue{
			typeIssue{T: ddl.String, Issue: internal.Widened},
			typeIssue{T: ddl.Float64, Issue: internal.Numeric}},
		"serial": []typeIssue{
			typeIssue{T: ddl.String, Issue: internal.Widened},
			typeIssue{T: ddl.Int64, Issue: internal.Serial}},
		"text": []typeIssue{
			typeIssue{T: ddl.Bytes, Issue: internal.Widened},
			typeIssue{T: ddl.String}},
		"timestamptz": []typeIssue{
			typeIssue{T: ddl.String, Issue: internal.Widened},
			typeIssue{T: ddl.Timestamp}},
		"timestamp": []typeIssue{
			typeIssue{T: ddl.String, Issue: internal.Widened},
			typeIssue{T: ddl.Timestamp, Issue: internal.Timestamp}},
		"varchar": []typeIssue{
			typeIssue{T: ddl.String},
			typeIssue{T: ddl.Bytes, Issue: internal.Widened}},
	}
	assert.Equal(t, expectedTypemap, typemap)

}

func TestSetTypeMapTableLevel(t *testing.T) {
	// Create a request to pass to our handler. We don't have any query parameters for now, so we'll
	// pass 'nil' as the third parameter.

	tc := []struct {
		name              string
		table             string
		payload           string
		statusCode        int64
		expectedSchema    ddl.CreateTable
		expectedIssues    map[string][]internal.SchemaIssue
		expectedToSource  internal.NameAndCols
		expectedToSpanner internal.NameAndCols
	}{
		{
			name:  "Test type change",
			table: "t1",
			payload: `
    {
      "UpdateCols":{
		"a": { "Removed": false, "Rename":"", "PK":"", "NotNull":"", "ToType":"STRING"},
		"b": { "Removed": false, "Rename":"", "PK":"", "NotNull":"", "ToType":"STRING"},
		"c": { "Removed": false, "Rename":"", "PK":"", "NotNull":"", "ToType":"STRING"},
		"d": { "Removed": false, "Rename":"", "PK":"", "NotNull":"", "ToType":"BYTES"},
		"e": { "Removed": false, "Rename":"", "PK":"", "NotNull":"", "ToType":"STRING"},
		"f": { "Removed": false, "Rename":"", "PK":"", "NotNull":"", "ToType":"STRING"},
		"g": { "Removed": false, "Rename":"", "PK":"", "NotNull":"", "ToType":"STRING"},
		"h": { "Removed": false, "Rename":"", "PK":"", "NotNull":"", "ToType":"BYTES"},
		"i": { "Removed": false, "Rename":"", "PK":"", "NotNull":"", "ToType":"STRING"},
		"j": { "Removed": false, "Rename":"", "PK":"", "NotNull":"", "ToType":"STRING"},
		"k": { "Removed": false, "Rename":"", "PK":"", "NotNull":"", "ToType":"STRING"},
		"l": { "Removed": false, "Rename":"", "PK":"", "NotNull":"", "ToType":"STRING"},
		"m": { "Removed": false, "Rename":"", "PK":"", "NotNull":"", "ToType":"STRING"},
		"n": { "Removed": false, "Rename":"", "PK":"", "NotNull":"", "ToType":"BYTES"},
		"o": { "Removed": false, "Rename":"", "PK":"", "NotNull":"", "ToType":"STRING"},
		"p": { "Removed": false, "Rename":"", "PK":"", "NotNull":"", "ToType":"INT64"}
	}
    }`,
			statusCode: http.StatusOK,
			expectedSchema: ddl.CreateTable{
				Name:     "t1",
				ColNames: []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p"},
				ColDefs: map[string]ddl.ColumnDef{
					"a": ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"b": ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"c": ddl.ColumnDef{Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"d": ddl.ColumnDef{Name: "d", T: ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}},
					"e": ddl.ColumnDef{Name: "e", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"f": ddl.ColumnDef{Name: "f", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"g": ddl.ColumnDef{Name: "g", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"h": ddl.ColumnDef{Name: "h", T: ddl.Type{Name: ddl.Bytes, Len: int64(1)}},
					"i": ddl.ColumnDef{Name: "i", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"j": ddl.ColumnDef{Name: "j", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"k": ddl.ColumnDef{Name: "k", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"l": ddl.ColumnDef{Name: "l", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"m": ddl.ColumnDef{Name: "m", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"n": ddl.ColumnDef{Name: "n", T: ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}},
					"o": ddl.ColumnDef{Name: "o", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"p": ddl.ColumnDef{Name: "p", T: ddl.Type{Name: ddl.Int64}},
				},
				Pks: []ddl.IndexKey{ddl.IndexKey{Col: "a"}},
			},
			expectedIssues: map[string][]internal.SchemaIssue{
				"a": []internal.SchemaIssue{internal.Widened},
				"b": []internal.SchemaIssue{internal.Widened},
				"c": []internal.SchemaIssue{internal.Widened},
				"d": []internal.SchemaIssue{internal.Widened},
				"e": []internal.SchemaIssue{internal.Widened},
				"f": []internal.SchemaIssue{internal.Widened},
				"g": []internal.SchemaIssue{internal.Widened},
				"i": []internal.SchemaIssue{internal.Widened},
				"j": []internal.SchemaIssue{internal.Widened},
				"k": []internal.SchemaIssue{internal.Widened},
				"l": []internal.SchemaIssue{internal.Widened},
				"m": []internal.SchemaIssue{internal.Widened},
				"o": []internal.SchemaIssue{internal.Widened},
				"p": []internal.SchemaIssue{internal.Widened},
			},
			expectedToSource: internal.NameAndCols{
				Name: "t1",
				Cols: map[string]string{
					"a": "a", "b": "b", "c": "c", "d": "d", "e": "e", "f": "f", "g": "g", "h": "h", "i": "i", "j": "j", "k": "k", "l": "l", "m": "m", "n": "n", "o": "o", "p": "p",
				}},
			expectedToSpanner: internal.NameAndCols{
				Name: "t1",
				Cols: map[string]string{
					"a": "a", "b": "b", "c": "c", "d": "d", "e": "e", "f": "f", "g": "g", "h": "h", "i": "i", "j": "j", "k": "k", "l": "l", "m": "m", "n": "n", "o": "o", "p": "p",
				}},
		},
		{
			name:  "Test column removal",
			table: "t1",
			payload: `
    {
      "UpdateCols":{
		"a": { "Removed": true, "Rename":"", "PK":"", "NotNull":"", "ToType":""},
		"b": { "Removed": true, "Rename":"", "PK":"", "NotNull":"", "ToType":""}
	}
    }`,
			statusCode: http.StatusOK,
			expectedSchema: ddl.CreateTable{
				Name:     "t1",
				ColNames: []string{"c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "synth_id"},
				ColDefs: map[string]ddl.ColumnDef{
					"c":        ddl.ColumnDef{Name: "c", T: ddl.Type{Name: ddl.Bool}},
					"d":        ddl.ColumnDef{Name: "d", T: ddl.Type{Name: ddl.String, Len: int64(6)}},
					"e":        ddl.ColumnDef{Name: "e", T: ddl.Type{Name: ddl.Float64}},
					"f":        ddl.ColumnDef{Name: "f", T: ddl.Type{Name: ddl.Timestamp}},
					"g":        ddl.ColumnDef{Name: "g", T: ddl.Type{Name: ddl.Int64}},
					"h":        ddl.ColumnDef{Name: "h", T: ddl.Type{Name: ddl.String, Len: int64(1)}},
					"i":        ddl.ColumnDef{Name: "i", T: ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}},
					"j":        ddl.ColumnDef{Name: "j", T: ddl.Type{Name: ddl.Date}},
					"k":        ddl.ColumnDef{Name: "k", T: ddl.Type{Name: ddl.Float64}},
					"l":        ddl.ColumnDef{Name: "l", T: ddl.Type{Name: ddl.Int64}},
					"m":        ddl.ColumnDef{Name: "m", T: ddl.Type{Name: ddl.Int64}},
					"n":        ddl.ColumnDef{Name: "n", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"o":        ddl.ColumnDef{Name: "o", T: ddl.Type{Name: ddl.Timestamp}},
					"p":        ddl.ColumnDef{Name: "p", T: ddl.Type{Name: ddl.Int64}},
					"synth_id": ddl.ColumnDef{Name: "synth_id", T: ddl.Type{Name: ddl.Int64}},
				},
				Pks: []ddl.IndexKey{ddl.IndexKey{Col: "synth_id"}},
			},
			expectedIssues: map[string][]internal.SchemaIssue{
				"e": []internal.SchemaIssue{internal.Numeric},
				"g": []internal.SchemaIssue{internal.Serial},
				"l": []internal.SchemaIssue{internal.Widened},
				"m": []internal.SchemaIssue{internal.Serial},
				"o": []internal.SchemaIssue{internal.Timestamp},
			},
			expectedToSource: internal.NameAndCols{
				Name: "t1",
				Cols: map[string]string{
					"c": "c", "d": "d", "e": "e", "f": "f", "g": "g", "h": "h", "i": "i", "j": "j", "k": "k", "l": "l", "m": "m", "n": "n", "o": "o", "p": "p",
				}},
			expectedToSpanner: internal.NameAndCols{
				Name: "t1",
				Cols: map[string]string{
					"c": "c", "d": "d", "e": "e", "f": "f", "g": "g", "h": "h", "i": "i", "j": "j", "k": "k", "l": "l", "m": "m", "n": "n", "o": "o", "p": "p",
				}},
		},
		{
			name:  "Test column rename",
			table: "t1",
			payload: `
    {
      "UpdateCols":{
		"a": { "Removed": false, "Rename":"aa", "PK":"", "NotNull":"", "ToType":""},
		"b": { "Removed": false, "Rename":"bb", "PK":"", "NotNull":"", "ToType":""}
	}
    }`,
			statusCode: http.StatusOK,
			expectedSchema: ddl.CreateTable{
				Name:     "t1",
				ColNames: []string{"aa", "bb", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p"},
				ColDefs: map[string]ddl.ColumnDef{
					"aa": ddl.ColumnDef{Name: "aa", T: ddl.Type{Name: ddl.Int64}},
					"bb": ddl.ColumnDef{Name: "bb", T: ddl.Type{Name: ddl.Float64}},
					"c":  ddl.ColumnDef{Name: "c", T: ddl.Type{Name: ddl.Bool}},
					"d":  ddl.ColumnDef{Name: "d", T: ddl.Type{Name: ddl.String, Len: int64(6)}},
					"e":  ddl.ColumnDef{Name: "e", T: ddl.Type{Name: ddl.Float64}},
					"f":  ddl.ColumnDef{Name: "f", T: ddl.Type{Name: ddl.Timestamp}},
					"g":  ddl.ColumnDef{Name: "g", T: ddl.Type{Name: ddl.Int64}},
					"h":  ddl.ColumnDef{Name: "h", T: ddl.Type{Name: ddl.String, Len: int64(1)}},
					"i":  ddl.ColumnDef{Name: "i", T: ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}},
					"j":  ddl.ColumnDef{Name: "j", T: ddl.Type{Name: ddl.Date}},
					"k":  ddl.ColumnDef{Name: "k", T: ddl.Type{Name: ddl.Float64}},
					"l":  ddl.ColumnDef{Name: "l", T: ddl.Type{Name: ddl.Int64}},
					"m":  ddl.ColumnDef{Name: "m", T: ddl.Type{Name: ddl.Int64}},
					"n":  ddl.ColumnDef{Name: "n", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"o":  ddl.ColumnDef{Name: "o", T: ddl.Type{Name: ddl.Timestamp}},
					"p":  ddl.ColumnDef{Name: "p", T: ddl.Type{Name: ddl.Int64}},
				},
				Pks: []ddl.IndexKey{ddl.IndexKey{Col: "aa"}},
			},
			expectedIssues: map[string][]internal.SchemaIssue{
				"b": []internal.SchemaIssue{internal.Widened},
				"e": []internal.SchemaIssue{internal.Numeric},
				"g": []internal.SchemaIssue{internal.Serial},
				"l": []internal.SchemaIssue{internal.Widened},
				"m": []internal.SchemaIssue{internal.Serial},
				"o": []internal.SchemaIssue{internal.Timestamp},
			},
			expectedToSource: internal.NameAndCols{
				Name: "t1",
				Cols: map[string]string{
					"aa": "a", "bb": "b", "c": "c", "d": "d", "e": "e", "f": "f", "g": "g", "h": "h", "i": "i", "j": "j", "k": "k", "l": "l", "m": "m", "n": "n", "o": "o", "p": "p",
				}},
			expectedToSpanner: internal.NameAndCols{
				Name: "t1",
				Cols: map[string]string{
					"a": "aa", "b": "bb", "c": "c", "d": "d", "e": "e", "f": "f", "g": "g", "h": "h", "i": "i", "j": "j", "k": "k", "l": "l", "m": "m", "n": "n", "o": "o", "p": "p",
				}},
		},
		{
			name:  "Test PK removed",
			table: "t1",
			payload: `
    {
      "UpdateCols":{
		"a": { "Removed": false, "Rename":"", "PK":"REMOVED", "NotNull":"", "ToType":""}
	}
    }`,
			statusCode: http.StatusOK,
			expectedSchema: ddl.CreateTable{
				Name:     "t1",
				ColNames: []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "synth_id"},
				ColDefs: map[string]ddl.ColumnDef{
					"a":        ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.Int64}},
					"b":        ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.Float64}},
					"c":        ddl.ColumnDef{Name: "c", T: ddl.Type{Name: ddl.Bool}},
					"d":        ddl.ColumnDef{Name: "d", T: ddl.Type{Name: ddl.String, Len: int64(6)}},
					"e":        ddl.ColumnDef{Name: "e", T: ddl.Type{Name: ddl.Float64}},
					"f":        ddl.ColumnDef{Name: "f", T: ddl.Type{Name: ddl.Timestamp}},
					"g":        ddl.ColumnDef{Name: "g", T: ddl.Type{Name: ddl.Int64}},
					"h":        ddl.ColumnDef{Name: "h", T: ddl.Type{Name: ddl.String, Len: int64(1)}},
					"i":        ddl.ColumnDef{Name: "i", T: ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}},
					"j":        ddl.ColumnDef{Name: "j", T: ddl.Type{Name: ddl.Date}},
					"k":        ddl.ColumnDef{Name: "k", T: ddl.Type{Name: ddl.Float64}},
					"l":        ddl.ColumnDef{Name: "l", T: ddl.Type{Name: ddl.Int64}},
					"m":        ddl.ColumnDef{Name: "m", T: ddl.Type{Name: ddl.Int64}},
					"n":        ddl.ColumnDef{Name: "n", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"o":        ddl.ColumnDef{Name: "o", T: ddl.Type{Name: ddl.Timestamp}},
					"p":        ddl.ColumnDef{Name: "p", T: ddl.Type{Name: ddl.Int64}},
					"synth_id": ddl.ColumnDef{Name: "synth_id", T: ddl.Type{Name: ddl.Int64}},
				},
				Pks: []ddl.IndexKey{ddl.IndexKey{Col: "synth_id"}},
			},
			expectedIssues: map[string][]internal.SchemaIssue{
				"b": []internal.SchemaIssue{internal.Widened},
				"e": []internal.SchemaIssue{internal.Numeric},
				"g": []internal.SchemaIssue{internal.Serial},
				"l": []internal.SchemaIssue{internal.Widened},
				"m": []internal.SchemaIssue{internal.Serial},
				"o": []internal.SchemaIssue{internal.Timestamp},
			},
			expectedToSource: internal.NameAndCols{
				Name: "t1",
				Cols: map[string]string{
					"a": "a", "b": "b", "c": "c", "d": "d", "e": "e", "f": "f", "g": "g", "h": "h", "i": "i", "j": "j", "k": "k", "l": "l", "m": "m", "n": "n", "o": "o", "p": "p",
				}},
			expectedToSpanner: internal.NameAndCols{
				Name: "t1",
				Cols: map[string]string{
					"a": "a", "b": "b", "c": "c", "d": "d", "e": "e", "f": "f", "g": "g", "h": "h", "i": "i", "j": "j", "k": "k", "l": "l", "m": "m", "n": "n", "o": "o", "p": "p",
				}},
		},
		{
			name:  "Test PK changed",
			table: "t1",
			payload: `
    {
      "UpdateCols":{
		"a": { "Removed": false, "Rename":"", "PK":"REMOVED", "NotNull":"", "ToType":""},
		"b": { "Removed": false, "Rename":"", "PK":"ADDED", "NotNull":"", "ToType":""}
	}
    }`,
			statusCode: http.StatusOK,
			expectedSchema: ddl.CreateTable{
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
				Pks: []ddl.IndexKey{ddl.IndexKey{Col: "b"}},
			},
			expectedIssues: map[string][]internal.SchemaIssue{
				"b": []internal.SchemaIssue{internal.Widened},
				"e": []internal.SchemaIssue{internal.Numeric},
				"g": []internal.SchemaIssue{internal.Serial},
				"l": []internal.SchemaIssue{internal.Widened},
				"m": []internal.SchemaIssue{internal.Serial},
				"o": []internal.SchemaIssue{internal.Timestamp},
			},
			expectedToSource: internal.NameAndCols{
				Name: "t1",
				Cols: map[string]string{
					"a": "a", "b": "b", "c": "c", "d": "d", "e": "e", "f": "f", "g": "g", "h": "h", "i": "i", "j": "j", "k": "k", "l": "l", "m": "m", "n": "n", "o": "o", "p": "p",
				}},
			expectedToSpanner: internal.NameAndCols{
				Name: "t1",
				Cols: map[string]string{
					"a": "a", "b": "b", "c": "c", "d": "d", "e": "e", "f": "f", "g": "g", "h": "h", "i": "i", "j": "j", "k": "k", "l": "l", "m": "m", "n": "n", "o": "o", "p": "p",
				}},
		},
		{
			name:  "Test PK Added",
			table: "t2",
			payload: `
    {
      "UpdateCols":{
		"b": { "Removed": false, "Rename":"", "PK":"ADDED", "NotNull":"", "ToType":""}
	}
    }`,
			statusCode: http.StatusOK,
			expectedSchema: ddl.CreateTable{
				Name:     "t2",
				ColNames: []string{"a", "b", "c"},
				ColDefs: map[string]ddl.ColumnDef{
					"a": ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.Int64}},
					"b": ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.Float64}},
					"c": ddl.ColumnDef{Name: "c", T: ddl.Type{Name: ddl.Bool}},
				},
				Pks: []ddl.IndexKey{ddl.IndexKey{Col: "b"}},
			},
			expectedIssues: map[string][]internal.SchemaIssue{
				"b": []internal.SchemaIssue{internal.Widened},
			},
			expectedToSource: internal.NameAndCols{
				Name: "t2",
				Cols: map[string]string{
					"a": "a", "b": "b", "c": "c",
				}},
			expectedToSpanner: internal.NameAndCols{
				Name: "t2",
				Cols: map[string]string{
					"a": "a", "b": "b", "c": "c",
				}},
		},
		{
			name:  "Test bad json",
			table: "t1",
			payload: `
    {
      "UpdateCols":{
		"a": { "Removed": false, "Rename":"", "PK":"", "NotNull":"", "ToType":"STRING"},
		}
    }`,
			statusCode: http.StatusBadRequest,
		},
	}
	for _, tc := range tc {
		app.driver = "postgres"
		app.conv = internal.MakeConv()
		fillConvPostgres(app.conv)
		payload := tc.payload
		req, err := http.NewRequest("POST", "/typemap/table?table="+tc.table, strings.NewReader(payload))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		// We create a ResponseRecorder (which satisfies http.ResponseWriter) to record the response.
		rr := httptest.NewRecorder()
		handler := http.HandlerFunc(setTypeMapTableLevel)

		// Our handlers satisfy http.Handler, so we can call their ServeHTTP method
		// directly and pass in our Request and ResponseRecorder.
		handler.ServeHTTP(rr, req)
		var res *internal.Conv
		json.Unmarshal(rr.Body.Bytes(), &res)
		// Check the status code is what we expect.
		if status := rr.Code; int64(status) != tc.statusCode {
			t.Errorf("handler returned wrong status code: got %v want %v",
				status, tc.statusCode)
		}
		if tc.statusCode == http.StatusOK {
			assert.Equal(t, tc.expectedSchema, res.SpSchema[tc.table])
			assert.Equal(t, tc.expectedIssues, res.Issues[tc.table])
			assert.Equal(t, tc.expectedToSource, res.ToSource[tc.table])
			assert.Equal(t, tc.expectedToSpanner, res.ToSpanner[tc.table])
		}
	}

}

func TestSetTypeMapGlobalLevel(t *testing.T) {
	// Create a request to pass to our handler. We don't have any query parameters for now, so we'll
	// pass 'nil' as the third parameter.

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
					"a": ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"b": ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"c": ddl.ColumnDef{Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"d": ddl.ColumnDef{Name: "d", T: ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}},
					"e": ddl.ColumnDef{Name: "e", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"f": ddl.ColumnDef{Name: "f", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"g": ddl.ColumnDef{Name: "g", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"h": ddl.ColumnDef{Name: "h", T: ddl.Type{Name: ddl.Bytes, Len: int64(1)}},
					"i": ddl.ColumnDef{Name: "i", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"j": ddl.ColumnDef{Name: "j", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"k": ddl.ColumnDef{Name: "k", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"l": ddl.ColumnDef{Name: "l", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"m": ddl.ColumnDef{Name: "m", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"n": ddl.ColumnDef{Name: "n", T: ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}},
					"o": ddl.ColumnDef{Name: "o", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"p": ddl.ColumnDef{Name: "p", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
				},
				Pks: []ddl.IndexKey{ddl.IndexKey{Col: "a"}},
			},
			expectedIssues: map[string][]internal.SchemaIssue{
				"a": []internal.SchemaIssue{internal.Widened},
				"b": []internal.SchemaIssue{internal.Widened},
				"c": []internal.SchemaIssue{internal.Widened},
				"d": []internal.SchemaIssue{internal.Widened},
				"e": []internal.SchemaIssue{internal.Widened},
				"f": []internal.SchemaIssue{internal.Widened},
				"g": []internal.SchemaIssue{internal.Widened},
				"i": []internal.SchemaIssue{internal.Widened},
				"j": []internal.SchemaIssue{internal.Widened},
				"k": []internal.SchemaIssue{internal.Widened},
				"l": []internal.SchemaIssue{internal.Widened},
				"m": []internal.SchemaIssue{internal.Widened},
				"o": []internal.SchemaIssue{internal.Widened},
				"p": []internal.SchemaIssue{internal.Widened},
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
					"a": ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"b": ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"c": ddl.ColumnDef{Name: "c", T: ddl.Type{Name: ddl.Int64}},
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
			expectedIssues: map[string][]internal.SchemaIssue{
				"a": []internal.SchemaIssue{internal.Widened},
				"b": []internal.SchemaIssue{internal.Widened},
				"c": []internal.SchemaIssue{internal.Widened},
				"e": []internal.SchemaIssue{internal.Numeric},
				"g": []internal.SchemaIssue{internal.Serial},
				"l": []internal.SchemaIssue{internal.Widened},
				"m": []internal.SchemaIssue{internal.Serial},
				"o": []internal.SchemaIssue{internal.Timestamp},
				"p": []internal.SchemaIssue{internal.Widened},
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
		app.driver = "postgres"
		app.conv = internal.MakeConv()
		fillConvPostgres(app.conv)
		payload := tc.payload
		req, err := http.NewRequest("POST", "/typemap/global", strings.NewReader(payload))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		// We create a ResponseRecorder (which satisfies http.ResponseWriter) to record the response.
		rr := httptest.NewRecorder()
		handler := http.HandlerFunc(setTypeMapGlobal)

		// Our handlers satisfy http.Handler, so we can call their ServeHTTP method
		// directly and pass in our Request and ResponseRecorder.
		handler.ServeHTTP(rr, req)
		var res *internal.Conv
		json.Unmarshal(rr.Body.Bytes(), &res)
		// Check the status code is what we expect.
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

func TestGetConversion(t *testing.T) {
	// Create a request to pass to our handler. We don't have any query parameters for now, so we'll
	// pass 'nil' as the third parameter.
	app.driver = "postgres"
	app.conv = internal.MakeConv()
	fillConvPostgresMultiTable(app.conv)
	req, err := http.NewRequest("GET", "/conversion", nil)
	if err != nil {
		t.Fatal(err)
	}

	// We create a ResponseRecorder (which satisfies http.ResponseWriter) to record the response.
	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(getConversionRate)

	// Our handlers satisfy http.Handler, so we can call their ServeHTTP method
	// directly and pass in our Request and ResponseRecorder.
	handler.ServeHTTP(rr, req)
	var result map[string]string
	json.Unmarshal(rr.Body.Bytes(), &result)
	// Check the status code is what we expect.
	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusOK)
	}
	expectedConversion := map[string]string{
		"t1": "GREEN",
		"t2": "BLUE",
		"t3": "RED",
	}
	assert.Equal(t, expectedConversion, result)

}
func fillConvPostgres(conv *internal.Conv) {
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

func fillConvPostgresMultiTable(conv *internal.Conv) {
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

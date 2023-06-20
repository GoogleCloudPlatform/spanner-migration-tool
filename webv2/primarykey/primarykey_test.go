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

package primarykey

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/proto/migration"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
	"github.com/cloudspannerecosystem/harbourbridge/webv2/session"
	"github.com/stretchr/testify/assert"
)

func TestUpdatePrimaryKey(t *testing.T) {

	sessionState := session.GetSessionState()

	c := &internal.Conv{

		SpSchema: map[string]ddl.CreateTable{
			"t1": {
				Name:   "film_actor",
				ColIds: []string{"c1", "c2", "c3"},
				ColDefs: map[string]ddl.ColumnDef{
					"c1": {Name: "film_id", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, Id: "c1"},
					"c2": {Name: "actor_id", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, Id: "c2"},
					"c3": {Name: "last_update", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, Id: "c3"},
				},
				PrimaryKeys: []ddl.IndexKey{{ColId: "c1", Order: 1, Desc: true}},
				Id:          "t1",
			}},
		Audit: internal.Audit{
			MigrationType: migration.MigrationData_MIGRATION_TYPE_UNSPECIFIED.Enum(),
		},
		SchemaIssues: make(map[string]internal.TableIssues),
	}

	sessionState.Conv = c

	input := PrimaryKeyRequest{
		TableId: "t1",
		Columns: []Column{{ColumnId: "c1", Desc: false, Order: 1}},
	}

	inputBytes, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	buffer := bytes.NewBuffer(inputBytes)

	req, err := http.NewRequest("POST", "/primarykey", buffer)
	if err != nil {
		t.Fatal(err)
	}

	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	handler := http.HandlerFunc(PrimaryKey)
	handler.ServeHTTP(rr, req)

	res := &internal.Conv{}

	json.Unmarshal(rr.Body.Bytes(), &res)

	expectedConv := &internal.Conv{

		SpSchema: map[string]ddl.CreateTable{
			"t1": {
				Name:   "film_actor",
				ColIds: []string{"c1", "c2", "c3"},
				ColDefs: map[string]ddl.ColumnDef{
					"c1": {Name: "film_id", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, Id: "c1"},
					"c2": {Name: "actor_id", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, Id: "c2"},
					"c3": {Name: "last_update", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, Id: "c3"},
				},
				PrimaryKeys: []ddl.IndexKey{{ColId: "c1", Order: 1, Desc: false}},
				Id:          "t1",
			}},
		SchemaIssues: map[string]internal.TableIssues{
			"t1": {},
		},
	}

	assert.Equal(t, expectedConv, res)
}

func TestAddPrimaryKey(t *testing.T) {

	sessionState := session.GetSessionState()

	c := &internal.Conv{

		SpSchema: map[string]ddl.CreateTable{
			"t1": {
				Name:   "film_actor",
				ColIds: []string{"c1", "c2", "c3"},
				ColDefs: map[string]ddl.ColumnDef{
					"c1": {Name: "film_id", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, Id: "c1"},
					"c2": {Name: "actor_id", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, Id: "c2"},
					"c3": {Name: "last_update", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, Id: "c3"},
				},
				PrimaryKeys: []ddl.IndexKey{{ColId: "c1", Order: 1, Desc: true}},
				Id:          "t1",
			}},
		Audit: internal.Audit{
			MigrationType: migration.MigrationData_MIGRATION_TYPE_UNSPECIFIED.Enum(),
		},
		SchemaIssues: make(map[string]internal.TableIssues),
	}

	sessionState.Conv = c

	input := PrimaryKeyRequest{
		TableId: "t1",
		Columns: []Column{{ColumnId: "c1", Desc: true, Order: 1}, {ColumnId: "c2", Desc: false, Order: 2}},
	}

	inputBytes, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	buffer := bytes.NewBuffer(inputBytes)

	req, err := http.NewRequest("POST", "/primarykey", buffer)
	if err != nil {
		t.Fatal(err)
	}

	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	handler := http.HandlerFunc(PrimaryKey)
	handler.ServeHTTP(rr, req)

	res := &internal.Conv{}
	json.Unmarshal(rr.Body.Bytes(), &res)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusOK)
	}

	expectedConv := &internal.Conv{

		SpSchema: map[string]ddl.CreateTable{

			"t1": {
				Name:   "film_actor",
				ColIds: []string{"c1", "c2", "c3"},
				ColDefs: map[string]ddl.ColumnDef{
					"c1": {Name: "film_id", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, Id: "c1"},
					"c2": {Name: "actor_id", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, Id: "c2"},
					"c3": {Name: "last_update", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, Id: "c3"},
				},
				PrimaryKeys: []ddl.IndexKey{{ColId: "c1", Order: 1, Desc: true}, {ColId: "c2", Desc: false, Order: 2}},
				Id:          "t1",
			},
		},
		SchemaIssues: map[string]internal.TableIssues{
			"t1": {},
		},
	}

	assert.Equal(t, expectedConv, res)
}

func TestRemovePrimaryKey(t *testing.T) {

	sessionState := session.GetSessionState()

	c := &internal.Conv{

		SpSchema: map[string]ddl.CreateTable{

			"t1": {
				Name:   "film_actor",
				ColIds: []string{"c1", "c2", "c3"},
				ColDefs: map[string]ddl.ColumnDef{
					"c1": {Name: "film_id", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, Id: "c1"},
					"c2": {Name: "actor_id", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, Id: "c2"},
					"c3": {Name: "last_update", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, Id: "c3"},
				},
				PrimaryKeys: []ddl.IndexKey{{ColId: "c1", Order: 1, Desc: true}, {ColId: "c2", Desc: false, Order: 2}},
				Id:          "t1",
			},
		},
		Audit: internal.Audit{
			MigrationType: migration.MigrationData_MIGRATION_TYPE_UNSPECIFIED.Enum(),
		},
		SchemaIssues: make(map[string]internal.TableIssues),
	}

	sessionState.Conv = c

	input := PrimaryKeyRequest{
		TableId: "t1",
		Columns: []Column{{ColumnId: "c1", Desc: true, Order: 1}},
	}

	inputBytes, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	buffer := bytes.NewBuffer(inputBytes)

	req, err := http.NewRequest("POST", "/primarykey", buffer)
	if err != nil {
		t.Fatal(err)
	}

	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	handler := http.HandlerFunc(PrimaryKey)
	handler.ServeHTTP(rr, req)

	res := &internal.Conv{}
	json.Unmarshal(rr.Body.Bytes(), &res)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusOK)
	}

	expectedConv := &internal.Conv{

		SpSchema: map[string]ddl.CreateTable{
			"t1": {
				Name:   "film_actor",
				ColIds: []string{"c1", "c2", "c3"},
				ColDefs: map[string]ddl.ColumnDef{
					"c1": {Name: "film_id", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, Id: "c1"},
					"c2": {Name: "actor_id", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, Id: "c2"},
					"c3": {Name: "last_update", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, Id: "c3"},
				},
				PrimaryKeys: []ddl.IndexKey{{ColId: "c1", Order: 1, Desc: true}},
				Id:          "t1",
			}},
		SchemaIssues: map[string]internal.TableIssues{
			"t1": {},
		},
	}
	assert.Equal(t, expectedConv, res)
}

func TestPrimarykey(t *testing.T) {

	sessionState := session.GetSessionState()

	c := &internal.Conv{

		SpSchema: map[string]ddl.CreateTable{
			"t1": {
				Name:   "film_actor",
				ColIds: []string{"c1", "c2", "c3"},
				ColDefs: map[string]ddl.ColumnDef{
					"c1": {Name: "film_id", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, Id: "c1"},
					"c2": {Name: "actor_id", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, Id: "c2"},
					"c3": {Name: "last_update", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, Id: "c3"},
				},
				PrimaryKeys: []ddl.IndexKey{{ColId: "c1", Order: 1, Desc: true}, {ColId: "c2", Order: 2, Desc: true}},
				Id:          "t1",
			}},
		Audit: internal.Audit{
			MigrationType: migration.MigrationData_MIGRATION_TYPE_UNSPECIFIED.Enum(),
		},
		SchemaIssues: make(map[string]internal.TableIssues),
	}

	sessionState.Conv = c

	tc := []struct {
		name        string
		input       PrimaryKeyRequest
		statusCode  int
		res         PrimaryKeyResponse
		expectedRes PrimaryKeyResponse
	}{
		{
			name: "Table Id Not found",
			input: PrimaryKeyRequest{
				TableId: "t2",
				Columns: []Column{{ColumnId: "c1", Desc: true, Order: 1}, {ColumnId: "c2", Desc: true, Order: 2}},
			},
			statusCode: http.StatusNotFound,
		},
		{
			name: "Column are empty",
			input: PrimaryKeyRequest{
				TableId: "t1",
				Columns: []Column{}},
			statusCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tc {

		inputBytes, err := json.Marshal(tt.input)
		if err != nil {
			t.Fatal(err)
		}
		buffer := bytes.NewBuffer(inputBytes)

		req, err := http.NewRequest("POST", "/primarykey", buffer)
		if err != nil {
			t.Fatal(err)
		}

		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()

		handler := http.HandlerFunc(PrimaryKey)
		handler.ServeHTTP(rr, req)

		json.Unmarshal(rr.Body.Bytes(), &tt.res)
		assert.Equal(t, tt.statusCode, rr.Code)
	}
}

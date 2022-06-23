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

// Package web defines web APIs to be used with harbourbridge frontend.
// Apart from schema conversion, this package involves API to update
// converted schema.
package primarykey

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
	"github.com/cloudspannerecosystem/harbourbridge/webv2/session"
	"github.com/stretchr/testify/assert"
)

func TestUpdatePrimaryKey(t *testing.T) {

	sessionState := session.GetSessionState()

	c := &internal.Conv{

		SpSchema: map[string]ddl.CreateTable{
			"film_actor": {
				Name:     "film_actor",
				ColNames: []string{"film_id", "actor_id", "last_update"},
				ColDefs: map[string]ddl.ColumnDef{
					"film_id":     {Name: "film_id", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, Id: 1},
					"actor_id":    {Name: "actor_id", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, Id: 2},
					"last_update": {Name: "last_update", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, Id: 3},
				},
				Pks:          []ddl.IndexKey{{Col: "film_id", Order: 1, Desc: true}},
				Id:           1,
				PrimaryKeyId: 1,
			}},
	}

	sessionState.Conv = c

	input := PrimaryKeyRequest{
		TableId:      1,
		PrimaryKeyId: 1,
		Columns:      []Column{{ColumnId: 1, Desc: true, Order: 1}},
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
			"film_actor": {
				Name:     "film_actor",
				ColNames: []string{"film_id", "actor_id", "last_update"},
				ColDefs: map[string]ddl.ColumnDef{
					"film_id":     {Name: "film_id", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, Id: 1},
					"actor_id":    {Name: "actor_id", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, Id: 2},
					"last_update": {Name: "last_update", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, Id: 3},
				},
				Pks:          []ddl.IndexKey{{Col: "film_id", Order: 1, Desc: true}},
				Id:           1,
				PrimaryKeyId: 1,
			}},
	}

	assert.Equal(t, expectedConv, res)
}

func TestAddPrimaryKey(t *testing.T) {

	sessionState := session.GetSessionState()

	c := &internal.Conv{

		SpSchema: map[string]ddl.CreateTable{
			"film_actor": {
				Name:     "film_actor",
				ColNames: []string{"film_id", "actor_id", "last_update"},
				ColDefs: map[string]ddl.ColumnDef{
					"film_id":     {Name: "film_id", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, Id: 1},
					"actor_id":    {Name: "actor_id", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, Id: 2},
					"last_update": {Name: "last_update", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, Id: 3},
				},
				Pks:          []ddl.IndexKey{{Col: "film_id", Order: 1, Desc: true}},
				Id:           1,
				PrimaryKeyId: 1,
			}},
	}

	sessionState.Conv = c

	input := PrimaryKeyRequest{
		TableId:      1,
		PrimaryKeyId: 1,
		Columns:      []Column{{ColumnId: 1, Desc: true, Order: 1}, {ColumnId: 2, Desc: false, Order: 2}},
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
			"film_actor": {
				Name:     "film_actor",
				ColNames: []string{"film_id", "actor_id", "last_update"},
				ColDefs: map[string]ddl.ColumnDef{
					"film_id":     {Name: "film_id", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, Id: 1},
					"actor_id":    {Name: "actor_id", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, Id: 2},
					"last_update": {Name: "last_update", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, Id: 3},
				},
				Pks:          []ddl.IndexKey{{Col: "film_id", Order: 1, Desc: true}, {Col: "actor_id", Order: 2, Desc: false}},
				Id:           1,
				PrimaryKeyId: 1,
			}},
	}

	assert.Equal(t, expectedConv, res)
}

func TestRemovePrimaryKey(t *testing.T) {

	sessionState := session.GetSessionState()

	c := &internal.Conv{

		SpSchema: map[string]ddl.CreateTable{
			"film_actor": {
				Name:     "film_actor",
				ColNames: []string{"film_id", "actor_id", "last_update"},
				ColDefs: map[string]ddl.ColumnDef{
					"film_id":     {Name: "film_id", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, Id: 1},
					"actor_id":    {Name: "actor_id", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, Id: 2},
					"last_update": {Name: "last_update", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, Id: 3},
				},
				Pks:          []ddl.IndexKey{{Col: "film_id", Order: 1, Desc: true}, {Col: "actor_id", Order: 2, Desc: true}, {Col: "last_update", Order: 3, Desc: true}},
				Id:           1,
				PrimaryKeyId: 1,
			}},
	}

	sessionState.Conv = c

	input := PrimaryKeyRequest{
		TableId:      1,
		PrimaryKeyId: 1,
		Columns:      []Column{{ColumnId: 1, Desc: true, Order: 1}, {ColumnId: 2, Desc: true, Order: 2}},
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
			"film_actor": {
				Name:     "film_actor",
				ColNames: []string{"film_id", "actor_id", "last_update"},
				ColDefs: map[string]ddl.ColumnDef{
					"film_id":     {Name: "film_id", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, Id: 1},
					"actor_id":    {Name: "actor_id", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, Id: 2},
					"last_update": {Name: "last_update", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, Id: 3},
				},
				Pks:          []ddl.IndexKey{{Col: "film_id", Order: 1, Desc: true}, {Col: "actor_id", Order: 2, Desc: true}},
				Id:           1,
				PrimaryKeyId: 1,
			}},
	}
	assert.Equal(t, expectedConv, res)
}

func TestPrimarykey(t *testing.T) {

	sessionState := session.GetSessionState()

	c := &internal.Conv{

		SpSchema: map[string]ddl.CreateTable{
			"film_actor": {
				Name:     "film_actor",
				ColNames: []string{"film_id", "actor_id", "last_update"},
				ColDefs: map[string]ddl.ColumnDef{
					"film_id":     {Name: "film_id", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, Id: 1},
					"actor_id":    {Name: "actor_id", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, Id: 2},
					"last_update": {Name: "last_update", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, Id: 3},
				},
				Pks:          []ddl.IndexKey{{Col: "film_id", Order: 1, Desc: true}, {Col: "actor_id", Order: 2, Desc: true}},
				Id:           1,
				PrimaryKeyId: 1,
			}},
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
				TableId:      99,
				PrimaryKeyId: 99,
				Columns:      []Column{{ColumnId: 1, ColName: "film_id", Desc: true, Order: 1}, {ColumnId: 2, ColName: "actor_id", Desc: true, Order: 2}},
			},
			statusCode: http.StatusNotFound,
		},
		{
			name: "Column are empty",
			input: PrimaryKeyRequest{
				TableId:      1,
				PrimaryKeyId: 1,
				Columns:      []Column{}},
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

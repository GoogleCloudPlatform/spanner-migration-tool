package webv2

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cloudspannerecosystem/harbourbridge/common/constants"
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
	"github.com/cloudspannerecosystem/harbourbridge/webv2/session"
	"github.com/stretchr/testify/assert"
)

func TestUpdatePrimaryKey(t *testing.T) {

	sessionState := session.GetSessionState()

	sessionState.Driver = constants.MYSQL

	c := &internal.Conv{

		SpSchema: map[string]ddl.CreateTable{
			"film_actor": ddl.CreateTable{
				Name:     "film_actor",
				ColNames: []string{"film_id", "actor_id", "last_update"},
				ColDefs: map[string]ddl.ColumnDef{
					"film_id":     ddl.ColumnDef{Name: "film_id", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, Id: 1},
					"actor_id":    ddl.ColumnDef{Name: "actor_id", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, Id: 2},
					"last_update": ddl.ColumnDef{Name: "last_update", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, Id: 3},
				},
				Pks:          []ddl.IndexKey{ddl.IndexKey{Col: "film_id", Order: 0, Desc: false}},
				Id:           1,
				PrimaryKeyId: 1,
			}},
	}

	sessionState.Conv = c

	input := PrimaryKeyRequest{
		TableId:      1,
		PrimaryKeyId: 1,
		Columns:      []Column{Column{ColumnId: 1, Desc: true, Order: 1}},
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

	handler := http.HandlerFunc(primaryKey)
	handler.ServeHTTP(rr, req)

	var res PrimaryKeyResponse

	json.Unmarshal(rr.Body.Bytes(), &res)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusOK)
	}

	expected := PrimaryKeyResponse{
		TableId:      1,
		Columns:      []Column{Column{ColumnId: 1, ColName: "film_id", Desc: true, Order: 1}},
		PrimaryKeyId: 1,
	}

	assert.Equal(t, expected.TableId, res.TableId)

	assert.Equal(t, expected.Columns, res.Columns)

	assert.Equal(t, expected, res)
}

func TestAddPrimaryKey(t *testing.T) {

	sessionState := session.GetSessionState()

	sessionState.Driver = constants.MYSQL

	c := &internal.Conv{

		SpSchema: map[string]ddl.CreateTable{
			"film_actor": ddl.CreateTable{
				Name:     "film_actor",
				ColNames: []string{"film_id", "actor_id", "last_update"},
				ColDefs: map[string]ddl.ColumnDef{
					"film_id":     ddl.ColumnDef{Name: "film_id", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, Id: 1},
					"actor_id":    ddl.ColumnDef{Name: "actor_id", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, Id: 2},
					"last_update": ddl.ColumnDef{Name: "last_update", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, Id: 3},
				},
				Pks:          []ddl.IndexKey{ddl.IndexKey{Col: "film_id", Order: 1, Desc: true}},
				Id:           1,
				PrimaryKeyId: 1,
			}},
	}

	sessionState.Conv = c

	input := PrimaryKeyRequest{
		TableId:      1,
		PrimaryKeyId: 1,
		Columns:      []Column{Column{ColumnId: 1, Desc: true, Order: 1}, Column{ColumnId: 2, Desc: false, Order: 2}},
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

	handler := http.HandlerFunc(primaryKey)
	handler.ServeHTTP(rr, req)

	var res PrimaryKeyResponse

	json.Unmarshal(rr.Body.Bytes(), &res)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusOK)
	}

	expected := PrimaryKeyResponse{
		TableId:      1,
		Columns:      []Column{Column{ColumnId: 1, ColName: "film_id", Desc: true, Order: 1}, Column{ColumnId: 2, ColName: "actor_id", Desc: false, Order: 2}},
		PrimaryKeyId: 1,
	}

	assert.Equal(t, expected.TableId, res.TableId)

	assert.Equal(t, expected.Columns, res.Columns)

	assert.Equal(t, expected, res)
}

func TestRemovePrimaryKey(t *testing.T) {

	sessionState := session.GetSessionState()

	sessionState.Driver = constants.MYSQL

	c := &internal.Conv{

		SpSchema: map[string]ddl.CreateTable{
			"film_actor": ddl.CreateTable{
				Name:     "film_actor",
				ColNames: []string{"film_id", "actor_id", "last_update"},
				ColDefs: map[string]ddl.ColumnDef{
					"film_id":     ddl.ColumnDef{Name: "film_id", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, Id: 1},
					"actor_id":    ddl.ColumnDef{Name: "actor_id", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, Id: 2},
					"last_update": ddl.ColumnDef{Name: "last_update", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, Id: 3},
				},
				Pks:          []ddl.IndexKey{ddl.IndexKey{Col: "film_id", Order: 1, Desc: true}, ddl.IndexKey{Col: "actor_id", Order: 2, Desc: true}, ddl.IndexKey{Col: "last_update", Order: 3, Desc: true}},
				Id:           1,
				PrimaryKeyId: 1,
			}},
	}

	sessionState.Conv = c

	input := PrimaryKeyRequest{
		TableId:      1,
		PrimaryKeyId: 1,
		Columns:      []Column{Column{ColumnId: 1, Desc: true, Order: 1}, Column{ColumnId: 2, Desc: true, Order: 2}},
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

	handler := http.HandlerFunc(primaryKey)
	handler.ServeHTTP(rr, req)

	var res PrimaryKeyResponse

	json.Unmarshal(rr.Body.Bytes(), &res)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusOK)
	}

	expected := PrimaryKeyResponse{
		TableId:      1,
		Columns:      []Column{Column{ColumnId: 1, ColName: "film_id", Desc: true, Order: 1}, Column{ColumnId: 2, ColName: "actor_id", Desc: true, Order: 2}},
		PrimaryKeyId: 1,
	}

	assert.Equal(t, expected.TableId, res.TableId)

	assert.Equal(t, expected.Columns, res.Columns)

	assert.Equal(t, expected, res)
}

func TestPrimarykey(t *testing.T) {

	sessionState := session.GetSessionState()

	sessionState.Driver = constants.MYSQL

	c := &internal.Conv{

		SpSchema: map[string]ddl.CreateTable{
			"film_actor": ddl.CreateTable{
				Name:     "film_actor",
				ColNames: []string{"film_id", "actor_id", "last_update"},
				ColDefs: map[string]ddl.ColumnDef{
					"film_id":     ddl.ColumnDef{Name: "film_id", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, Id: 1},
					"actor_id":    ddl.ColumnDef{Name: "actor_id", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, Id: 2},
					"last_update": ddl.ColumnDef{Name: "last_update", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, Id: 3},
				},
				Pks:          []ddl.IndexKey{ddl.IndexKey{Col: "film_id", Order: 1, Desc: true}, ddl.IndexKey{Col: "actor_id", Order: 2, Desc: true}},
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
				Columns:      []Column{Column{ColumnId: 1, ColName: "film_id", Desc: true, Order: 1}, Column{ColumnId: 2, ColName: "actor_id", Desc: true, Order: 2}},
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

		handler := http.HandlerFunc(primaryKey)
		handler.ServeHTTP(rr, req)

		json.Unmarshal(rr.Body.Bytes(), &tt.res)

		assert.Equal(t, tt.statusCode, rr.Code)
	}
}

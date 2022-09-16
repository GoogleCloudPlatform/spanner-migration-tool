package updateTableSchema

import (
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bmizerany/assert"
	"github.com/cloudspannerecosystem/harbourbridge/common/constants"
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/schema"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
	"github.com/cloudspannerecosystem/harbourbridge/webv2/session"
)

func TestUpdateTableSchemaV2(t *testing.T) {

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
	}

	for _, tc := range tc {

		sessionState := session.GetSessionState()
		sessionState.Conv = tc.conv
		sessionState.Driver = constants.MYSQL

		payload := tc.payload

		req, err := http.NewRequest("POST", "/typemap/table?table="+tc.table, strings.NewReader(payload))
		if err != nil {
			t.Fatal(err)
		}

		req.Header.Set("Content-Type", "application/json")

		rr := httptest.NewRecorder()

		handler := http.HandlerFunc(UpdateTableSchema)

		handler.ServeHTTP(rr, req)

		res := &internal.Conv{}

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

func TestAddUpdateTableSchemaV2(t *testing.T) {

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
			},
		},
	}

	for _, tc := range tc {

		sessionState := session.GetSessionState()
		sessionState.Conv = tc.conv
		sessionState.Driver = constants.MYSQL

		payload := tc.payload

		req, err := http.NewRequest("POST", "/typemap/table?table="+tc.table, strings.NewReader(payload))
		if err != nil {
			t.Fatal(err)
		}

		req.Header.Set("Content-Type", "application/json")

		rr := httptest.NewRecorder()

		handler := http.HandlerFunc(UpdateTableSchema)

		handler.ServeHTTP(rr, req)

		res := &internal.Conv{}

		json.Unmarshal(rr.Body.Bytes(), &res)

		log.Println("res :", res.SpSchema["t1"])

		if status := rr.Code; int64(status) != tc.statusCode {
			t.Errorf("handler returned wrong status code: got %v want %v",
				status, tc.statusCode)
		}

		if tc.statusCode == http.StatusOK {
			assert.Equal(t, tc.expectedConv, res)
		}
	}

}

func TestChangetypeUpdateTableSchemaV2(t *testing.T) {

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
	}

	for _, tc := range tc {

		sessionState := session.GetSessionState()
		sessionState.Conv = tc.conv
		sessionState.Driver = constants.MYSQL

		payload := tc.payload

		req, err := http.NewRequest("POST", "/typemap/table?table="+tc.table, strings.NewReader(payload))
		if err != nil {
			t.Fatal(err)
		}

		req.Header.Set("Content-Type", "application/json")

		rr := httptest.NewRecorder()

		handler := http.HandlerFunc(UpdateTableSchema)

		handler.ServeHTTP(rr, req)

		res := &internal.Conv{}

		json.Unmarshal(rr.Body.Bytes(), &res)

		log.Println("res :", res.SpSchema["t1"])

		if status := rr.Code; int64(status) != tc.statusCode {
			t.Errorf("handler returned wrong status code: got %v want %v",
				status, tc.statusCode)
		}

		if tc.statusCode == http.StatusOK {
			assert.Equal(t, tc.expectedConv, res)
		}
	}
}
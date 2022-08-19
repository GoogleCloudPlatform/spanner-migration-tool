package updateTableSchema

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bmizerany/assert"
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
	"github.com/cloudspannerecosystem/harbourbridge/webv2/session"
)

func TestUpdateTableSchemainterleaved(t *testing.T) {

	tc := []struct {
		name         string
		table        string
		payload      string
		statusCode   int64
		conv         *internal.Conv
		expectedConv *internal.Conv
	}{

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
							"a": {Name: "a", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"b": {Name: "b", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c": {Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						Pks:    []ddl.IndexKey{{Col: "a", Desc: false}, {Col: "b", Desc: false}},
						Parent: "t2",
					},
					"t2": {
						Name:     "t2",
						ColNames: []string{"a", "b", "c"},
						ColDefs: map[string]ddl.ColumnDef{
							"a": {Name: "a", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"b": {Name: "b", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c": {Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						Pks: []ddl.IndexKey{{Col: "a", Desc: false}},
					},
				},
				ToSource: map[string]internal.NameAndCols{
					"t1": {Name: "t1", Cols: map[string]string{"a": "a", "b": "b", "c": "c"}},
					"t2": {Name: "t2", Cols: map[string]string{"a": "a", "b": "b", "c": "c"}},
				},
				ToSpanner: map[string]internal.NameAndCols{
					"t1": {Name: "t2", Cols: map[string]string{"a": "a", "b": "b", "c": "c"}},
					"t2": {Name: "t2", Cols: map[string]string{"a": "a", "b": "b", "c": "c"}},
				},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:     "t1",
						ColNames: []string{"aa", "b", "c"},
						ColDefs: map[string]ddl.ColumnDef{
							"aa": {Name: "aa", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"b":  {Name: "b", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c":  {Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						Pks:    []ddl.IndexKey{{Col: "aa", Desc: false}, {Col: "b", Desc: false}},
						Parent: "t2",
					},
					"t2": {
						Name:     "t2",
						ColNames: []string{"aa", "b", "c"},
						ColDefs: map[string]ddl.ColumnDef{
							"aa": {Name: "aa", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"b":  {Name: "b", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c":  {Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						Pks: []ddl.IndexKey{{Col: "aa", Desc: false}},
					},
				},
				ToSource: map[string]internal.NameAndCols{
					"t1": {Name: "t1", Cols: map[string]string{"aa": "a", "b": "b", "c": "c"}},
					"t2": {Name: "t2", Cols: map[string]string{"aa": "a", "b": "b", "c": "c"}},
				},
				ToSpanner: map[string]internal.NameAndCols{
					"t1": {Name: "t2", Cols: map[string]string{"a": "aa", "b": "b", "c": "c"}},
					"t2": {Name: "t2", Cols: map[string]string{"a": "aa", "b": "b", "c": "c"}},
				},
			},
		},
	}

	for _, tc := range tc {

		sessionState := session.GetSessionState()
		sessionState.Conv = tc.conv

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

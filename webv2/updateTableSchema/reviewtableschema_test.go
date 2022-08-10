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

func TestReviewTableSchema(t *testing.T) {

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

		handler := http.HandlerFunc(ReviewTableSchema)

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

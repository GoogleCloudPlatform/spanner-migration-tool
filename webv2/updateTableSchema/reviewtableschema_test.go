package updateTableSchema

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bmizerany/assert"
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/proto/migration"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
	"github.com/cloudspannerecosystem/harbourbridge/webv2/session"
)

func TestUpdatePrimaryKey(t *testing.T) {

	sessionState := session.GetSessionState()

	c := &internal.Conv{

		SpSchema: map[string]ddl.CreateTable{
			"film_actor": {
				Name:     "film_actor",
				ColNames: []string{"film_id", "actor_id", "last_update"},
				ColDefs: map[string]ddl.ColumnDef{
					"film_id":     {Name: "film_id", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, Id: "1"},
					"actor_id":    {Name: "actor_id", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, Id: "2"},
					"last_update": {Name: "last_update", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, Id: "3"},
				},
				Pks: []ddl.IndexKey{{Col: "film_id", Order: 1, Desc: true}},
				Id:  "1",
			}},
		Audit: internal.Audit{
			MigrationType: migration.MigrationData_MIGRATION_TYPE_UNSPECIFIED.Enum(),
		},
	}

	sessionState.Conv = c

	payload := `
    {
      "UpdateCols":{
		"a": { "Rename": "aa" }
	}
    }`

	table := "t1"

	req, err := http.NewRequest("POST", "/typemap/table?table="+table, strings.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}

	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	handler := http.HandlerFunc(ReviewTableSchema)
	handler.ServeHTTP(rr, req)

	res := &internal.Conv{}

	json.Unmarshal(rr.Body.Bytes(), &res)

	expectedConv := &internal.Conv{}

	assert.Equal(t, expectedConv, res)
}

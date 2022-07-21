package updateTableSchema

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/cloudspannerecosystem/harbourbridge/internal"
	updatesessionfiles "github.com/cloudspannerecosystem/harbourbridge/webv2/updatesessionfiles"
	utilities "github.com/cloudspannerecosystem/harbourbridge/webv2/utilities"

	"github.com/cloudspannerecosystem/harbourbridge/webv2/session"
)

// Actions to be performed on a column.
// (1) Add : Add column if true
// (2) Removed: Remove column if true
// (3) Rename: New name or empty string
// (4) NotNull: "ADDED", "REMOVED" or ""
// (5) ToType: New type or empty string
type updateCol struct {
	Add     bool   `json:"Add"`
	Removed bool   `json:"Removed"`
	Rename  string `json:"Rename"`
	NotNull string `json:"NotNull"`
	ToType  string `json:"ToType"`
}

type updateTable struct {
	UpdateCols map[string]updateCol `json:"UpdateCols"`
	Update     bool                 `json:"Update"`
}

// updateTableSchema updates the Spanner schema.
// Following actions can be performed on a specified table:
// (1) Add column
// (2) Remove column
// (3) Rename column
// (4) Add or Remove NotNull constraint
// (5) Update Spanner type
func UpdateTableSchema(w http.ResponseWriter, r *http.Request) {

	reqBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("Body Read Error : %v", err), http.StatusInternalServerError)
		return
	}
	var t updateTable

	table := r.FormValue("table")

	fmt.Println("updateTableSchema getting called")

	err = json.Unmarshal(reqBody, &t)
	if err != nil {
		http.Error(w, fmt.Sprintf("Request Body parse error : %v", err), http.StatusBadRequest)
		return
	}

	fmt.Println("updateTable :", t)

	sessionState := session.GetSessionState()

	var Conv *internal.Conv

	Conv = sessionState.Conv

	for colName, v := range t.UpdateCols {

		if v.Add {

			addColumn(table, colName, Conv)

			continue
		}

		if v.Removed {

			removeColumn(table, colName, Conv)

			continue
		}

		if v.Rename != "" && v.Rename != colName {

			renameColumn(v.Rename, table, colName, Conv)

		}

		if v.ToType != "" {

			typeChange, err := utilities.IsTypeChanged(v.ToType, table, colName, Conv)

			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			if typeChange {

				UpdatecolNameType(v.ToType, table, colName, Conv, w)
			}
		}

		if v.NotNull != "" {
			UpdateNotNull(v.NotNull, table, colName, Conv)
		}
	}

	if t.Update {
		updatesessionfiles.UpdateSessionFile()
		sessionState.Conv = Conv

	}

	//todo fill TableSchemaChanges
	Changes := []TableSchemaChanges{}

	convts := ConvWithUpdateTableSchema{
		DDL:     getDDL(table, Conv),
		Changes: Changes,
		Conv:    *Conv,
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(convts)
}

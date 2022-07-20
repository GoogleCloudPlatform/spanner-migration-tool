package updateTableSchema

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	updatesessionfiles "github.com/cloudspannerecosystem/harbourbridge/webv2/updatesessionfiles"
	utilities "github.com/cloudspannerecosystem/harbourbridge/webv2/utilities"

	"github.com/cloudspannerecosystem/harbourbridge/webv2/session"
)

// Actions to be performed on a column.
// (1) Removed: true/false
// (2) Rename: New name or empty string
// (3) PK: "ADDED", "REMOVED" or ""
// (4) NotNull: "ADDED", "REMOVED" or ""
// (5) ToType: New type or empty string
type updateCol struct {
	Add     bool   `json:"Add"`
	Removed bool   `json:"Removed"`
	Rename  string `json:"Rename"`
	PK      string `json:"PK"`
	NotNull string `json:"NotNull"`
	ToType  string `json:"ToType"`
}

type updateTable struct {
	UpdateCols map[string]updateCol `json:"UpdateCols"`
	table      string
}

// updateTableSchema updates the Spanner schema.
// Following actions can be performed on a specified table:
// (1) Remove column
// (2) Rename column
// (3) Add or Remove Primary Key
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
	srcTableName := sessionState.Conv.ToSource[table].Name

	for colName, v := range t.UpdateCols {

		if v.Removed {

			addColumn(table, colName, srcTableName)

			continue
		}

		if v.Removed {

			removeColumn(table, colName, srcTableName)

			continue
		}

		if v.Rename != "" && v.Rename != colName {

			renameColumn(v.Rename, table, colName, srcTableName)
			colName = v.Rename
		}

		if v.ToType != "" {

			typeChange, err := utilities.IsTypeChanged(v.ToType, table, colName, srcTableName)

			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			if typeChange {

				UpdatecolNameType(v.ToType, table, colName, srcTableName, w)
			}
		}

		if v.NotNull != "" {
			UpdateNotNull(v.NotNull, table, colName)
		}
	}

	updatesessionfiles.UpdateSessionFile()

	convm := session.ConvWithMetadata{
		SessionMetadata: sessionState.SessionMetadata,
		Conv:            *sessionState.Conv,
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(convm)
}

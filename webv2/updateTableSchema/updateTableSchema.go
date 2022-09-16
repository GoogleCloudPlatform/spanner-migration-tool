package updateTableSchema

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/cloudspannerecosystem/harbourbridge/internal"

	"github.com/cloudspannerecosystem/harbourbridge/webv2/session"
	"github.com/cloudspannerecosystem/harbourbridge/webv2/updatesessionfiles"
	utilities "github.com/cloudspannerecosystem/harbourbridge/webv2/utilities"
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

	sessionState := session.GetSessionState()

	var Conv *internal.Conv
	Conv = nil
	Conv = sessionState.Conv

	for colName, v := range t.UpdateCols {

		if v.Add {

			addColumn(table, colName, Conv)

			fmt.Println("after addColumn")

			fmt.Println("Conv.SpSchema[table] : ", Conv.SpSchema[table])

			fmt.Println("Conv.ToSpanner : ", Conv.ToSpanner)

			fmt.Println("Conv.ToSource : ", Conv.ToSource)

		}

		if v.Removed {

			removeColumn(table, colName, Conv)

		}

		if v.Rename != "" && v.Rename != colName {

			renameColumn(v.Rename, table, colName, Conv)
			colName = v.Rename
		}

		if v.ToType != "" {

			fmt.Println("before IsTypeChanged")

			typeChange, err := utilities.IsTypeChanged(v.ToType, table, colName, Conv)

			fmt.Println("after IsTypeChanged")

			if err != nil {
				fmt.Println("err", err)

				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			fmt.Println("typeChange :", typeChange)

			if typeChange {

				fmt.Println("before UpdatecolNameType")

				UpdateColNameType(v.ToType, table, colName, Conv, w)

				fmt.Println("after UpdatecolNameType")

				fmt.Println("Conv.SpSchema[table] : ", Conv.SpSchema[table])

				fmt.Println("Conv.ToSpanner : ", Conv.ToSpanner)

				fmt.Println("Conv.ToSource : ", Conv.ToSource)
			}
		}

		if v.NotNull != "" {
			UpdateNotNull(v.NotNull, table, colName, Conv)
		}
	}

	sessionState.Conv = Conv

	updatesessionfiles.UpdateSessionFile()

	convm := session.ConvWithMetadata{
		SessionMetadata: sessionState.SessionMetadata,
		Conv:            *sessionState.Conv,
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(convm)
}
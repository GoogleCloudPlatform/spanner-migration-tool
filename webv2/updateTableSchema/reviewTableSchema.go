package updateTableSchema

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/webv2/session"
	updatesessionfiles "github.com/cloudspannerecosystem/harbourbridge/webv2/updatesessionfiles"
	utilities "github.com/cloudspannerecosystem/harbourbridge/webv2/utilities"
)

type ReviewTableSchemaResponse struct {
	DDL     string
	Changes []InterleaveTableSchema
}

type InterleaveTableSchema struct {
	table         string
	Columnchanges []InterleaveColumn
}

type InterleaveColumn struct {
	ColumnName       string
	Type             string
	UpdateColumnName string
	UpdateType       string
}

func ReviewTableSchema(w http.ResponseWriter, r *http.Request) {

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

	fmt.Println(" before updateTable :")

	for k, v := range t.UpdateCols {

		fmt.Println("column :", k)
		fmt.Println("updateCol:", v)
	}

	sessionState := session.GetSessionState()

	var Conv *internal.Conv

	Conv = nil

	Conv = sessionState.Conv

	//todo work on TableSchemaChanges

	Changes := []InterleaveTableSchema{}

	interleaveColumn := []InterleaveColumn{}

	for colName, v := range t.UpdateCols {

		if v.Add {

			err := addColumn(table, colName, Conv, w)

			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
		}

		if v.Removed {

			removeColumn(table, colName, Conv)

		}

		if v.Rename != "" && v.Rename != colName {

			interleaveColumn = reviewRenameColumn(v.Rename, table, colName, Conv, interleaveColumn)

			colName = v.Rename
		}

		if v.ToType != "" {

			typeChange, err := utilities.IsTypeChanged(v.ToType, table, colName, Conv)

			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			if typeChange {

				InterleaveColumn, err := ReviewcolNameType(v.ToType, table, colName, Conv, interleaveColumn, w)
				if err != nil {
					return
				}
				fmt.Println(InterleaveColumn)
			}
		}

		if v.NotNull != "" {
			UpdateNotNull(v.NotNull, table, colName, Conv)
		}
	}

	fmt.Println(" before updateTable :")

	for k, v := range t.UpdateCols {

		fmt.Println("column :", k)
		fmt.Println("updateCol:", v)
	}

	updatesessionfiles.UpdateSessionFile()

	fmt.Println("before getDDL table", table)

	ddl := getDDL(table, Conv)

	fmt.Println("")
	fmt.Println("")

	fmt.Println("Changes :", Changes)

	resp := ReviewTableSchemaResponse{
		DDL:     ddl,
		Changes: Changes,
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(resp)
}

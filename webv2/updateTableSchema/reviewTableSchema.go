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
	Changes []TableSchemaChanges
}

type TableSchemaChanges struct {
	table         string
	Columnchanges []Columnchange
}

type Columnchange struct {
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

	fmt.Println("updateTable :", t)

	sessionState := session.GetSessionState()

	var Conv *internal.Conv

	Conv = sessionState.Conv

	//todo work on TableSchemaChanges

	Changes := []TableSchemaChanges{}

	columnchange := []Columnchange{}

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

			columnchange = reviewRenameColumn(v.Rename, table, colName, Conv, columnchange)

			colName = v.Rename
		}

		if v.ToType != "" {

			typeChange, err := utilities.IsTypeChanged(v.ToType, table, colName, Conv)

			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			if typeChange {

				columnchange, err := ReviewcolNameType(v.ToType, table, colName, Conv, columnchange, w)
				if err != nil {
					return
				}
				fmt.Println(columnchange)
			}
		}

		if v.NotNull != "" {
			UpdateNotNull(v.NotNull, table, colName, Conv)
		}
	}

	updatesessionfiles.UpdateSessionFile()

	fmt.Println("table", table)

	resp := ReviewTableSchemaResponse{
		DDL:     getDDL(table, Conv),
		Changes: Changes,
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(resp)
}

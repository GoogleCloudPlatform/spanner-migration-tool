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
	Table                   string
	InterleaveColumnChanges []InterleaveColumn
}

type InterleaveColumn struct {
	ColumnName       string
	Type             string
	UpdateColumnName string
	UpdateType       string
	ColumnId         string
}

func ReviewTableSchema(w http.ResponseWriter, r *http.Request) {

	reqBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("Body Read Error : %v", err), http.StatusInternalServerError)
		return
	}
	var t updateTable

	table := r.FormValue("table")

	err = json.Unmarshal(reqBody, &t)
	if err != nil {
		http.Error(w, fmt.Sprintf("Request Body parse error : %v", err), http.StatusBadRequest)
		return
	}

	sessionState := session.GetSessionState()

	var Conv *internal.Conv

	Conv = nil

	Conv = sessionState.Conv

	interleaveTableSchema := []InterleaveTableSchema{}

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

			interleaveTableSchema = reviewRenameColumn(v.Rename, table, colName, Conv, interleaveTableSchema)

			colName = v.Rename
		}

		if v.ToType != "" {

			typeChange, err := utilities.IsTypeChanged(v.ToType, table, colName, Conv)

			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			if typeChange {

				interleaveTableSchema, err = ReviewcolNameType(v.ToType, table, colName, Conv, interleaveTableSchema, w)
				if err != nil {
					return
				}
			}
		}

		if v.NotNull != "" {
			UpdateNotNull(v.NotNull, table, colName, Conv)
		}
	}

	updatesessionfiles.UpdateSessionFile()

	ddl := getDDL(Conv.SpSchema[table])

	fmt.Println("interleaveTableSchema :", interleaveTableSchema)
	fmt.Println("")

	resp := ReviewTableSchemaResponse{
		DDL:     ddl,
		Changes: interleaveTableSchema,
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(resp)
}

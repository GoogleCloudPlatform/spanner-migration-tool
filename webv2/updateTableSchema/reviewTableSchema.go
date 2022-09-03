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

//ReviewTableSchema review Spanner Table Schema.
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

	convByte, err := json.Marshal(sessionState.Conv)
	if err != nil {
		http.Error(w, fmt.Sprintf("Conversion object parse error : %v", err), http.StatusInternalServerError)
		return
	}
	if err := json.Unmarshal(convByte, &Conv); err != nil {
		http.Error(w, fmt.Sprintf("Conversion object parse error : %v", err), http.StatusInternalServerError)
		return
	}

	interleaveTableSchema := []InterleaveTableSchema{}

	for colName, v := range t.UpdateCols {

		if v.Add {

			addColumn(table, colName, Conv)

			/*
				fmt.Println("err", err)

				if err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}

			*/

			fmt.Println("after addColumn")

			fmt.Println("Conv.SpSchema[table] : ", Conv.SpSchema[table])

			fmt.Println("Conv.ToSpanner : ", Conv.ToSpanner)

			fmt.Println("Conv.ToSource : ", Conv.ToSource)

		}

		if v.Removed {

			removeColumn(table, colName, Conv)

		}

		if v.Rename != "" && v.Rename != colName {

			for _, c := range Conv.SpSchema[table].ColNames {
				if c == v.Rename {
					http.Error(w, fmt.Sprintf("Multiple columns with similar name cannot exist for column : %v", v.Rename), http.StatusBadRequest)
					return
				}
			}

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

				interleaveTableSchema, err = ReviewColumnNameType(v.ToType, table, colName, Conv, interleaveTableSchema, w)
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

	ddl := GetSpannerTableDDL(Conv.SpSchema[table])

	fmt.Println("interleaveTableSchema :", interleaveTableSchema)
	fmt.Println("")

	resp := ReviewTableSchemaResponse{
		DDL:     ddl,
		Changes: interleaveTableSchema,
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(resp)
}

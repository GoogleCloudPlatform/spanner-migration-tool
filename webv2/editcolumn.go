package webv2

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	helpers "github.com/cloudspannerecosystem/harbourbridge/webv2/helpers"
	"github.com/cloudspannerecosystem/harbourbridge/webv2/session"
)

// Actions to be performed on a column.
// (1) Removed: true/false
// (2) Rename: New name or empty string
// (3) PK: "ADDED", "REMOVED" or ""
// (4) NotNull: "ADDED", "REMOVED" or ""
// (5) ToType: New type or empty string
type updateCol struct {
	Removed bool   `json:"Removed"`
	Rename  string `json:"Rename"`
	PK      string `json:"PK"`
	NotNull string `json:"NotNull"`
	ToType  string `json:"ToType"`
}

type updateTable struct {
	UpdateCols map[string]updateCol `json:"UpdateCols"`
}

// updateTableSchema updates the Spanner schema.
// Following actions can be performed on a specified table:
// (1) Remove column
// (2) Rename column
// (3) Add or Remove Primary Key
// (4) Add or Remove NotNull constraint
// (5) Update Spanner type
func updateTableSchema(w http.ResponseWriter, r *http.Request) {

	reqBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("Body Read Error : %v", err), http.StatusInternalServerError)
		return
	}
	var t updateTable

	table := r.FormValue("table")

	fmt.Println("\n\n\n")

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

			/*
				status, err := canRemoveColumn(colName, table)

				if err != nil {
					http.Error(w, fmt.Sprintf("%v", err), status)
					return
				}

			*/
			removeColumn(table, colName, srcTableName)

			continue
		}

		if v.Rename != "" && v.Rename != colName {

			renameColumn(v.Rename, table, colName, srcTableName)
			colName = v.Rename
		}

		if v.ToType != "" {

			typeChange, err := isTypeChanged(v.ToType, table, colName, srcTableName)

			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			if typeChange {

				/*
					if status, err := canRenameOrChangeType(colName, table); err != nil {

						http.Error(w, fmt.Sprintf("%v", err), status)
						return
					}
				*/
				updateType(v.ToType, table, colName, srcTableName, w)
			}
		}

		if v.NotNull != "" {
			updateNotNull(v.NotNull, table, colName)
		}
	}

	fmt.Println("\n\n\n")
	fmt.Println("\n\n\n")
	fmt.Println("\n\n\n")

	helpers.UpdateSessionFile()
	fmt.Println("\n\n\n")
	fmt.Println("\n\n\n")
	fmt.Println("\n\n\n")

	convm := session.ConvWithMetadata{
		SessionMetadata: sessionState.SessionMetadata,
		Conv:            *sessionState.Conv,
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(convm)
}

func canRenameOrChangeType(colName, table string) (int, error) {

	sessionState := session.GetSessionState()

	isPartOfPK := isPartOfPK(colName, table)

	isParent, childSchema := isParent(table)

	isChild := sessionState.Conv.SpSchema[table].Parent != ""

	if isPartOfPK && (isParent || isChild) {
		return http.StatusBadRequest, fmt.Errorf("column : '%s' in table : '%s' is part of parent-child relation with schema : '%s'", colName, table, childSchema)
	}

	if isPartOfSecondaryIndex, indexName := isPartOfSecondaryIndex(colName, table); isPartOfSecondaryIndex {
		return http.StatusPreconditionFailed, fmt.Errorf("column : '%s' in table : '%s' is part of secondary index : '%s', remove secondary index before making the update",
			colName, table, indexName)
	}

	isPartOfFK := isPartOfFK(colName, table)

	isReferencedByFK, relationTable := isReferencedByFK(colName, table)

	if isPartOfFK || isReferencedByFK {
		if isReferencedByFK {
			return http.StatusPreconditionFailed, fmt.Errorf("column : '%s' in table : '%s' is part of foreign key relation with table : '%s', remove foreign key constraint before making the update",
				colName, table, relationTable)
		}
		return http.StatusPreconditionFailed, fmt.Errorf("column : '%s' in table : '%s' is part of foreign keys, remove foreign key constraint before making the update",
			colName, table)
	}

	return http.StatusOK, nil
}

func canRemoveColumn(colName, table string) (int, error) {

	if isPartOfPK := isPartOfPK(colName, table); isPartOfPK {
		return http.StatusBadRequest, fmt.Errorf("column is part of primary key")
	}

	if isPartOfSecondaryIndex, _ := isPartOfSecondaryIndex(colName, table); isPartOfSecondaryIndex {
		return http.StatusPreconditionFailed, fmt.Errorf("column is part of secondary index, remove secondary index before making the update")
	}

	isPartOfFK := isPartOfFK(colName, table)

	isReferencedByFK, _ := isReferencedByFK(colName, table)

	if isPartOfFK || isReferencedByFK {
		return http.StatusPreconditionFailed, fmt.Errorf("column is part of foreign key relation, remove foreign key constraint before making the update")
	}

	return http.StatusOK, nil
}

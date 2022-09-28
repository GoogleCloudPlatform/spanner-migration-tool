// Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package table

import (
	"net/http"

	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
	"github.com/cloudspannerecosystem/harbourbridge/webv2/session"
	utilities "github.com/cloudspannerecosystem/harbourbridge/webv2/utilities"
)

//UpdateColNameType updates type of given columnname to newType.
func UpdateColNameType(newType, table, colName string, Conv *internal.Conv, w http.ResponseWriter) {

	srcTableName := Conv.ToSource[table].Name

	sp, ty, err := utilities.GetType(Conv, newType, table, colName, srcTableName)

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	colDef := sp.ColDefs[colName]
	colDef.T = ty

	sp.ColDefs[colName] = colDef

	Conv.SpSchema[table] = sp

	for i, _ := range sp.Fks {

		err = UpdateColNameTypeForeignkeyTableSchema(Conv, sp, i, colName, newType, w)

		if err != nil {
			return
		}
	}

	for _, sp := range Conv.SpSchema {

		for j := 0; j < len(sp.Fks); j++ {
			if sp.Fks[j].ReferTable == table {
				UpdateColNameTypeForeignkeyReferTableSchema(Conv, sp, sp.Name, colName, newType, w)
			}
		}
	}

	// update interleave table relation
	isParent, parentschemaTable := IsParent(table)

	if isParent {

		err = UpdateColNameTypeParentschemaTable(Conv, parentschemaTable, colName, newType, w)
		if err != nil {
			return
		}
	}

	childSchemaTable := Conv.SpSchema[table].Parent

	if childSchemaTable != "" {

		err = UpdateColNameTypeChildschemaTable(Conv, childSchemaTable, colName, newType, w)
		if err != nil {
			return
		}
	}
}

//UpdateColNameTypeForeignkeyTableSchema updates column type to newtype in from Foreignkey Table Schema.
func UpdateColNameTypeForeignkeyTableSchema(Conv *internal.Conv, sp ddl.CreateTable, index int, colName string, newType string, w http.ResponseWriter) error {

	relationTable := sp.Fks[index].ReferTable

	srcTableName := Conv.ToSource[relationTable].Name

	rsp, ty, err := utilities.GetType(Conv, newType, relationTable, colName, srcTableName)

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return err
	}

	colDef := rsp.ColDefs[colName]
	colDef.T = ty

	rsp.ColDefs[colName] = colDef

	Conv.SpSchema[relationTable] = rsp

	return nil

}

//UpdateColNameTypeForeignkeyReferTableSchema updates column type to newtype in from Foreignkey Refer Table Schema.
func UpdateColNameTypeForeignkeyReferTableSchema(Conv *internal.Conv, sp ddl.CreateTable, table string, colName string, newType string, w http.ResponseWriter) error {

	srcTableName := Conv.ToSource[table].Name

	sp, ty, err := utilities.GetType(Conv, newType, table, colName, srcTableName)

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return err
	}

	colDef := sp.ColDefs[colName]
	colDef.T = ty

	sp.ColDefs[colName] = colDef

	return nil
}

//UpdateColNameTypeParentschemaTable updates column type to newtype in from Parent Table Schema.
func UpdateColNameTypeParentschemaTable(Conv *internal.Conv, parentschemaTable string, colName string, newType string, w http.ResponseWriter) error {

	srcTableName := Conv.ToSource[parentschemaTable].Name

	parentSp, ty, err := utilities.GetType(Conv, newType, parentschemaTable, colName, srcTableName)

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return err
	}

	colDef := parentSp.ColDefs[colName]
	colDef.T = ty

	parentSp.ColDefs[colName] = colDef

	Conv.SpSchema[parentschemaTable] = parentSp

	return nil
}

//UpdateColNameTypechildschemaTable updates column type to newtype in from child Table Schema.
func UpdateColNameTypeChildschemaTable(Conv *internal.Conv, childSchemaTable string, colName string, newType string, w http.ResponseWriter) error {

	srcTableName := Conv.ToSource[childSchemaTable].Name

	childSp, ty, err := utilities.GetType(Conv, newType, childSchemaTable, colName, srcTableName)

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return err
	}

	colDef := childSp.ColDefs[colName]
	colDef.T = ty

	childSp.ColDefs[colName] = colDef

	Conv.SpSchema[childSchemaTable] = childSp

	return nil
}

func UpdateNotNull(notNullChange, table, colName string, Conv *internal.Conv) {

	sp := Conv.SpSchema[table]

	switch notNullChange {
	case "ADDED":
		spColDef := sp.ColDefs[colName]
		spColDef.NotNull = true
		sp.ColDefs[colName] = spColDef
	case "REMOVED":
		spColDef := sp.ColDefs[colName]
		spColDef.NotNull = false
		sp.ColDefs[colName] = spColDef
	}
}

func IsParent(table string) (bool, string) {
	sessionState := session.GetSessionState()

	for _, spSchema := range sessionState.Conv.SpSchema {
		if spSchema.Parent == table {
			return true, spSchema.Name
		}
	}
	return false, ""
}

func IsPartOfPK(col, table string) bool {
	sessionState := session.GetSessionState()

	for _, pk := range sessionState.Conv.SpSchema[table].Pks {
		if pk.Col == col {
			return true
		}
	}
	return false
}

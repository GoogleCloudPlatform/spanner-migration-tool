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
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
	utilities "github.com/cloudspannerecosystem/harbourbridge/webv2/utilities"
)

// removeColumn remove given column from schema.
func removeColumn(table string, colName string, conv *internal.Conv) {

	sp := conv.SpSchema[table]

	removeColumnFromTableSchema(conv, table, colName)

	// update foreignKey relationship Table column names
	for _, fk := range sp.Fks {

		removeColumnFromTableSchema(conv, fk.ReferTable, colName)

	}

	for _, sp := range conv.SpSchema {

		for j := 0; j < len(sp.Fks); j++ {
			if sp.Fks[j].ReferTable == table {
				removeColumnFromTableSchema(conv, sp.Name, colName)
			}

		}

	}

	isParent, childTableName := IsParent(table)

	if isParent {

		removeColumnFromTableSchema(conv, childTableName, colName)

	}

	if conv.SpSchema[table].Parent != "" {

		removeColumnFromTableSchema(conv, conv.SpSchema[table].Parent, colName)
	}
}

// removeColumnFromCurrentTableSchema remove given column from table schema.
func removeColumnFromTableSchema(conv *internal.Conv, table string, colName string) {
	sp := conv.SpSchema[table]

	sp = removeColumnFromSpannerColDefs(sp, colName)

	sp = removeColumnFromSpannerPK(sp, colName)

	sp = removeColumnFromSpannerSecondaryIndex(sp, colName)

	sp = removeColumnFromSpannerForeignkeyColumns(sp, colName)

	sp = removeColumnFromSpannerForeignkeyReferColumns(sp, colName)

	sp = removeColumnFromSpannerColNames(sp, colName)

	removeSpannerSchemaIssue(table, colName, conv)

	removeColumnFromToSpannerToSource(table, colName, conv)

	conv.SpSchema[table] = sp
}

// removeColumnFromSpannerColNames remove given column from ColNames.
func removeColumnFromSpannerColNames(sp ddl.CreateTable, colName string) ddl.CreateTable {

	for i, col := range sp.ColNames {
		if col == colName {
			sp.ColNames = utilities.Remove(sp.ColNames, i)
			break
		}
	}
	delete(sp.ColDefs, colName)
	return sp
}

// removeColumnFromSpannerPK remove given column from Primary Key List.
func removeColumnFromSpannerPK(sp ddl.CreateTable, colName string) ddl.CreateTable {

	for i, pk := range sp.Pks {
		if pk.Col == colName {
			sp.Pks = utilities.RemovePk(sp.Pks, i)
			break
		}
	}
	return sp
}

// removeColumnFromSpannerColDefs remove given column from Spanner ColDefs List.
func removeColumnFromSpannerColDefs(sp ddl.CreateTable, colName string) ddl.CreateTable {
	delete(sp.ColDefs, colName)
	return sp
}

// removeColumnFromSpannerSecondaryIndex remove given column from Spanner SecondaryIndex List.
func removeColumnFromSpannerSecondaryIndex(sp ddl.CreateTable, colName string) ddl.CreateTable {

	for i, index := range sp.Indexes {
		for j, key := range index.Keys {
			if key.Col == colName {
				sp.Indexes[i].Keys = utilities.RemoveColumnFromSecondaryIndexKey(sp.Indexes[i].Keys, j)
				break
			}
		}
	}
	return sp
}

// removeColumnFromSecondaryIndexKey remove given column from Spanner Secondary Schema Issue List.
func removeSpannerSchemaIssue(table string, colName string, conv *internal.Conv) {
	if conv.Issues != nil {
		if conv.Issues[table] != nil && conv.Issues[table][colName] != nil {
			delete(conv.Issues[table], colName)
		}
	}
}

// removeColumnFromToSpannerToSource remove given column from ToSpanner and ToSource List.
func removeColumnFromToSpannerToSource(table string, colName string, conv *internal.Conv) {

	srcTableName := conv.ToSource[table].Name

	srcColName := conv.ToSource[table].Cols[colName]
	delete(conv.ToSource[table].Cols, colName)
	delete(conv.ToSpanner[srcTableName].Cols, srcColName)
}

// removeColumnFromSpannerForeignkeyColumns remove given column from Spanner Foreignkey Columns List.
func removeColumnFromSpannerForeignkeyColumns(sp ddl.CreateTable, colName string) ddl.CreateTable {

	for i, fk := range sp.Fks {
		j := 0
		for _, column := range fk.Columns {
			if column == colName {
				sp.Fks[i].Columns = utilities.RemoveFkColumn(fk.Columns, j)
			} else {
				j = j + 1
			}
		}
	}

	//drop foreing key if the foreign key doesn't have any column left after the update
	i := 0
	for _, fk := range sp.Fks {
		if len(fk.Columns) <= 0 {
			sp.Fks = append(sp.Fks[:i], sp.Fks[i+1:]...)
		}
	}
	return sp
}

// removeColumnFromSpannerForeignkeyReferColumns remove given column from Spanner Foreignkey Refer Columns List.
func removeColumnFromSpannerForeignkeyReferColumns(sp ddl.CreateTable, colName string) ddl.CreateTable {

	for i, fk := range sp.Fks {
		j := 0
		for _, column := range fk.ReferColumns {
			if column == colName {
				sp.Fks[i].ReferColumns = utilities.RemoveFkReferColumns(sp.Fks[i].ReferColumns, j)
			} else {
				j = j + 1
			}
		}
	}

	//drop foreing key if the foreign key doesn't have any refer-column left after the update
	i := 0
	for _, fk := range sp.Fks {
		if len(fk.ReferColumns) <= 0 {
			sp.Fks = append(sp.Fks[:i], sp.Fks[i+1:]...)
		}
	}
	return sp
}

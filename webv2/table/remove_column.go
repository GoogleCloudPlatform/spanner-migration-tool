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
func removeColumn(table string, colName string, Conv *internal.Conv) {

	sp := Conv.SpSchema[table]

	removeColumnFromTableSchema(Conv, table, colName)

	// update foreignKey relationship Table column names
	for _, fk := range sp.Fks {

		removeColumnFromTableSchema(Conv, fk.ReferTable, colName)

	}

	for _, sp := range Conv.SpSchema {

		for j := 0; j < len(sp.Fks); j++ {
			if sp.Fks[j].ReferTable == table {
				removeColumnFromTableSchema(Conv, sp.Name, colName)
			}

		}

	}

	isParent, childTableName := IsParent(table)

	if isParent {

		removeColumnFromTableSchema(Conv, childTableName, colName)

	}

	if Conv.SpSchema[table].Parent != "" {

		removeColumnFromTableSchema(Conv, Conv.SpSchema[table].Parent, colName)
	}
}

// removeColumnFromCurrentTableSchema remove given column from table schema.
func removeColumnFromTableSchema(Conv *internal.Conv, table string, colName string) {
	sp := Conv.SpSchema[table]

	sp = removeColumnFromSpannerColDefs(sp, colName)

	sp = removeColumnFromSpannerPK(sp, colName)

	sp = removeColumnFromSpannerSecondaryIndex(sp, colName)

	sp = removeColumnFromSpannerForeignkeyColumns(sp, colName)

	sp = removeColumnFromSpannerForeignkeyReferColumns(sp, colName)

	sp = removeColumnFromSpannerColNames(sp, colName)

	removeSpannerSchemaIssue(table, colName, Conv)

	removeColumnFromToSpannerToSource(table, colName, Conv)

	Conv.SpSchema[table] = sp
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
				sp.Indexes[i].Keys = removeColumnFromSecondaryIndexKey(sp.Indexes[i].Keys, j)
				break
			}
		}
	}
	return sp
}

// removeColumnFromSecondaryIndexKey remove given column from SpannerSecondary Index Key List.
func removeColumnFromSecondaryIndexKey(slice []ddl.IndexKey, s int) []ddl.IndexKey {
	return append(slice[:s], slice[s+1:]...)
}

// removeColumnFromSecondaryIndexKey remove given column from Spanner Secondary Schema Issue List.
func removeSpannerSchemaIssue(table string, colName string, Conv *internal.Conv) {
	if Conv.Issues != nil {
		if Conv.Issues[table] != nil && Conv.Issues[table][colName] != nil {
			delete(Conv.Issues[table], colName)
		}
	}
}

// removeColumnFromToSpannerToSource remove given column from ToSpanner and ToSource List.
func removeColumnFromToSpannerToSource(table string, colName string, Conv *internal.Conv) {

	srcTableName := Conv.ToSource[table].Name

	srcColName := Conv.ToSource[table].Cols[colName]
	delete(Conv.ToSource[table].Cols, colName)
	delete(Conv.ToSpanner[srcTableName].Cols, srcColName)
}

// removeColumnFromSpannerForeignkeyColumns remove given column from Spanner Foreignkey Columns List.
func removeColumnFromSpannerForeignkeyColumns(sp ddl.CreateTable, colName string) ddl.CreateTable {

	for i, fk := range sp.Fks {
		for j, column := range fk.Columns {
			if column == colName {
				sp.Fks[i].Columns = removeFkColumns(fk.Columns, j)
			}
		}
	}
	return sp
}

// removeFkColumns remove given column from Spanner Foreignkey Columns List.
func removeFkColumns(slice []string, s int) []string {
	return append(slice[:s], slice[s+1:]...)
}

// removeColumnFromSpannerForeignkeyReferColumns remove given column from Spanner Foreignkey Refer Columns List.
func removeColumnFromSpannerForeignkeyReferColumns(sp ddl.CreateTable, colName string) ddl.CreateTable {

	for i, fk := range sp.Fks {
		for j, column := range fk.ReferColumns {
			if column == colName {
				sp.Fks[i].ReferColumns = removeFkReferColumns(sp.Fks[i].ReferColumns, j)
			}
		}
	}
	return sp
}

// removeFkReferColumns remove given column from Spanner FkReferColumns Columns List.
func removeFkReferColumns(slice []string, s int) []string {
	return append(slice[:s], slice[s+1:]...)
}

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
	"fmt"

	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
	utilities "github.com/cloudspannerecosystem/harbourbridge/webv2/utilities"
)

// removeColumn remove given column from schema.
func removeColumn(table string, colName string, Conv *internal.Conv) {

	sp := Conv.SpSchema[table]

	removeColumnFromCurrentTableSchema(Conv, sp, table, colName)

	// update foreignKey relationship Table column names
	for i, _ := range sp.Fks {

		removeColumnFromForeignkeyTableSchema(Conv, sp, i, colName)

	}

	for _, sp := range Conv.SpSchema {

		for j := 0; j < len(sp.Fks); j++ {
			if sp.Fks[j].ReferTable == table {
				removeColumnFromForeignkeyReferTableSchema(Conv, sp, sp.Name, colName)
			}

		}

	}

	isParent, parentSchemaTable := IsParent(table)

	if isParent {

		removeColumnFromParentTableSchema(Conv, parentSchemaTable, colName)

	}

	childSchemaTable := Conv.SpSchema[table].Parent

	if childSchemaTable != "" {

		removeColumnFromChildTableSchema(Conv, parentSchemaTable, colName)
	}
}

// removeColumnFromCurrentTableSchema remove given column from current table schema.
func removeColumnFromCurrentTableSchema(Conv *internal.Conv, sp ddl.CreateTable, table string, colName string) {
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

// removeColumnFromForeignkeyTableSchema remove given column from Foreignkey Table relationship Schema.
func removeColumnFromForeignkeyTableSchema(Conv *internal.Conv, sp ddl.CreateTable, index int, colName string) {

	relationTable := sp.Fks[index].ReferTable
	relationTableSp := Conv.SpSchema[relationTable]

	relationTableSp = removeColumnFromSpannerColNames(relationTableSp, colName)
	relationTableSp = removeColumnFromSpannerColDefs(relationTableSp, colName)
	relationTableSp = removeColumnFromSpannerPK(relationTableSp, colName)
	relationTableSp = removeColumnFromSpannerSecondaryIndex(relationTableSp, colName)
	relationTableSp = removeColumnFromSpannerForeignkeyColumns(relationTableSp, colName)
	relationTableSp = removeColumnFromSpannerForeignkeyReferColumns(relationTableSp, colName)

	removeSpannerSchemaIssue(relationTable, colName, Conv)
	removeColumnFromToSpannerToSource(relationTable, colName, Conv)

	Conv.SpSchema[relationTable] = relationTableSp
}

// removeColumnFromForeignkeyReferTableSchema remove given column from Foreign key Refer Table Schema.
func removeColumnFromForeignkeyReferTableSchema(Conv *internal.Conv, referTable ddl.CreateTable, table string, colName string) {

	referTable = removeColumnFromSpannerColDefs(referTable, colName)

	referTable = removeColumnFromSpannerPK(referTable, colName)

	referTable = removeColumnFromSpannerSecondaryIndex(referTable, colName)

	referTable = removeColumnFromSpannerForeignkeyColumns(referTable, colName)

	referTable = removeColumnFromSpannerForeignkeyReferColumns(referTable, colName)

	referTable = removeColumnFromSpannerColNames(referTable, colName)

	removeSpannerSchemaIssue(table, colName, Conv)

	removeColumnFromToSpannerToSource(table, colName, Conv)

	Conv.SpSchema[table] = referTable

}

// removeColumnFromParentTableSchema remove given column from interleaved Parent Table Schema.
func removeColumnFromParentTableSchema(Conv *internal.Conv, parentSchemaTable string, colName string) {

	childSchemaSp := Conv.SpSchema[parentSchemaTable]

	childSchemaSp = removeColumnFromSpannerColNames(childSchemaSp, colName)
	childSchemaSp = removeColumnFromSpannerColDefs(childSchemaSp, colName)
	childSchemaSp = removeColumnFromSpannerPK(childSchemaSp, colName)
	childSchemaSp = removeColumnFromSpannerSecondaryIndex(childSchemaSp, colName)
	childSchemaSp = removeColumnFromSpannerForeignkeyColumns(childSchemaSp, colName)
	childSchemaSp = removeColumnFromSpannerForeignkeyReferColumns(childSchemaSp, colName)

	removeSpannerSchemaIssue(parentSchemaTable, colName, Conv)
	removeColumnFromToSpannerToSource(parentSchemaTable, colName, Conv)

	Conv.SpSchema[parentSchemaTable] = childSchemaSp

}

// removeColumnFromChildTableSchema remove given column from interleaved Child Table Schema.
func removeColumnFromChildTableSchema(Conv *internal.Conv, childSchemaTable string, colName string) {

	childSchemaSp := Conv.SpSchema[childSchemaTable]

	childSchemaSp = removeColumnFromSpannerColNames(childSchemaSp, colName)
	childSchemaSp = removeColumnFromSpannerColDefs(childSchemaSp, colName)
	childSchemaSp = removeColumnFromSpannerPK(childSchemaSp, colName)
	childSchemaSp = removeColumnFromSpannerSecondaryIndex(childSchemaSp, colName)
	childSchemaSp = removeColumnFromSpannerForeignkeyColumns(childSchemaSp, colName)
	childSchemaSp = removeColumnFromSpannerForeignkeyReferColumns(childSchemaSp, colName)

	removeSpannerSchemaIssue(childSchemaTable, colName, Conv)
	removeColumnFromToSpannerToSource(childSchemaTable, colName, Conv)

	Conv.SpSchema[childSchemaTable] = childSchemaSp

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

	if _, found := sp.ColDefs[colName]; found {

		delete(sp.ColDefs, colName)

	}

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

				fmt.Println("removing sp.Fks[i].Columns[j] : ", sp.Fks[i].Columns[j])

				sp.Fks[i].Columns = removeFkColumns(fk.Columns, j)

				fmt.Println("removed sp.Fks[i].Columns[j] :", sp.Fks[i].Columns)

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

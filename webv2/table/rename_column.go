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
)

// renameColumn renames given column to newname and update in schema.
func renameColumn(newName, table, colName string, Conv *internal.Conv) {

	sp := Conv.SpSchema[table]

	renameColumnNameTableSchema(Conv, table, colName, newName)

	// update foreignKey relationship Table
	for _, fk := range sp.Fks {
		renameColumnNameTableSchema(Conv, fk.ReferTable, colName, newName)
	}

	for _, sp := range Conv.SpSchema {
		for j := 0; j < len(sp.Fks); j++ {
			if sp.Fks[j].ReferTable == table {
				renameColumnNameTableSchema(Conv, sp.Name, colName, newName)
			}
		}
	}

	// update interleave table relation
	isParent, childTableName := IsParent(table)

	if isParent {
		renameColumnNameTableSchema(Conv, childTableName, colName, newName)
	}

	if Conv.SpSchema[table].Parent != "" {
		renameColumnNameTableSchema(Conv, Conv.SpSchema[table].Parent, colName, newName)
	}
}

// renameColumnNameInCurrentTableSchema renames given column in Table Schema.
func renameColumnNameTableSchema(Conv *internal.Conv, table string, colName string, newName string) {
	sp := Conv.SpSchema[table]

	sp = renameColumnNameInSpannerColDefs(sp, colName, newName)

	sp = renameColumnNameInSpannerPK(sp, colName, newName)

	sp = renameColumnNameInSpannerSecondaryIndex(sp, colName, newName)

	sp = renameColumnNameInSpannerForeignkeyColumns(sp, colName, newName)

	sp = renameColumnNameInSpannerForeignkeyReferColumns(sp, colName, newName)

	sp = renameColumnNameInSpannerColNames(sp, colName, newName)

	renameColumnNameInSpannerSchemaIssue(table, colName, newName, Conv)

	renameColumnNameInToSpannerToSource(table, colName, newName, Conv)

	Conv.SpSchema[table] = sp
}

// renameColumnNameInSpannerColNames renames given column in ColNames.
func renameColumnNameInSpannerColNames(sp ddl.CreateTable, colName string, newName string) ddl.CreateTable {
	for i, col := range sp.ColNames {
		if col == colName {
			sp.ColNames[i] = newName
			break
		}
	}
	return sp
}

// renameColumnNameInSpannerColDefs renames given column in Spanner Table ColDefs.
func renameColumnNameInSpannerColDefs(sp ddl.CreateTable, colName string, newName string) ddl.CreateTable {
	if _, found := sp.ColDefs[colName]; found {
		sp.ColDefs[newName] = ddl.ColumnDef{
			Name:    newName,
			T:       sp.ColDefs[colName].T,
			NotNull: sp.ColDefs[colName].NotNull,
			Comment: sp.ColDefs[colName].Comment,
			Id:      sp.ColDefs[colName].Id,
		}
		delete(sp.ColDefs, colName)
	}
	return sp
}

// renameColumnNameInSpannerPK renames given column in Spanner Table Primary Key List.
func renameColumnNameInSpannerPK(sp ddl.CreateTable, colName string, newName string) ddl.CreateTable {
	for i, pk := range sp.Pks {
		if pk.Col == colName {
			sp.Pks[i].Col = newName
			break
		}
	}
	return sp
}

// renameColumnNameInSpannerSecondaryIndex renames given column in Spanner Table Secondary Index List.
func renameColumnNameInSpannerSecondaryIndex(sp ddl.CreateTable, colName string, newName string) ddl.CreateTable {
	for i, index := range sp.Indexes {
		for j, key := range index.Keys {
			if key.Col == colName {
				sp.Indexes[i].Keys[j].Col = newName
				break
			}
		}
	}
	return sp
}

// renameColumnNameInSpannerForeignkeyColumns renames given column in Spanner Table Foreignkey Columns List.
func renameColumnNameInSpannerForeignkeyColumns(sp ddl.CreateTable, colName string, newName string) ddl.CreateTable {
	for i, fk := range sp.Fks {
		for j, column := range fk.Columns {
			if column == colName {
				sp.Fks[i].Columns[j] = newName
			}
		}
	}
	return sp
}

// renameColumnNameInSpannerForeignkeyReferColumns renames given column in Spanner Table Foreignkey Refer Columns List.
func renameColumnNameInSpannerForeignkeyReferColumns(sp ddl.CreateTable, colName string, newName string) ddl.CreateTable {
	for i, fk := range sp.Fks {
		for j, column := range fk.ReferColumns {
			if column == colName {
				sp.Fks[i].ReferColumns[j] = newName
			}
		}
	}
	return sp
}

// renameColumnNameInToSpannerToSource renames given column in ToSpanner and ToSource List.
func renameColumnNameInToSpannerToSource(table string, colName string, newName string, Conv *internal.Conv) {
	srcTableName := Conv.ToSource[table].Name

	srcColName := Conv.ToSource[table].Cols[colName]

	Conv.ToSpanner[srcTableName].Cols[srcColName] = newName
	Conv.ToSource[table].Cols[newName] = srcColName
	delete(Conv.ToSource[table].Cols, colName)
}

// renameColumnNameInSpannerSchemaIssue renames given column in ToSpanner and ToSource List.
func renameColumnNameInSpannerSchemaIssue(table string, colName string, newName string, Conv *internal.Conv) {
	if Conv.Issues != nil {
		if Conv.Issues[table] != nil && Conv.Issues[table][colName] != nil {
			schemaissue := Conv.Issues[table][colName]
			s := map[string][]internal.SchemaIssue{
				newName: schemaissue,
			}
			Conv.Issues[table] = s
		}
	}
	delete(Conv.Issues[table], colName)
}

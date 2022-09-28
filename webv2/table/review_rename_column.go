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

// reviewRenameColumn review  rename Columnname in schmema.
func reviewRenameColumn(newName, table, colName string, Conv *internal.Conv, interleaveTableSchema []InterleaveTableSchema) []InterleaveTableSchema {

	sp := Conv.SpSchema[table]

	columnId := sp.ColDefs[colName].Id

	// update foreignKey relationship Table column names
	for i, _ := range sp.Fks {

		reviewRenameColumnNameInForeignkeyTableSchema(Conv, sp, i, colName, newName)

	}

	for _, sp := range Conv.SpSchema {

		for j := 0; j < len(sp.Fks); j++ {
			if sp.Fks[j].ReferTable == table {

				reviewRenameColumnNameInForeignkeyReferTableSchema(Conv, sp, sp.Name, colName, newName)
			}

		}

	}

	// update interleave table relation
	isParent, parentSchemaTable := IsParent(table)

	if isParent {

		interleaveTableSchema = reviewRenameColumnNameInparentTableSchema(Conv, parentSchemaTable, interleaveTableSchema, colName, newName)
	}

	childSchemaTable := Conv.SpSchema[table].Parent

	if childSchemaTable != "" {

		interleaveTableSchema = reviewRenameColumnNameInchildTableSchema(Conv, childSchemaTable, interleaveTableSchema, colName, newName)

	}

	interleaveTableSchema = reviewreanmeColumnNameInCurrentTable(Conv, sp, interleaveTableSchema, table, columnId, colName, newName, parentSchemaTable, childSchemaTable)

	return interleaveTableSchema

}

// reviewRenameForeignkeyTableSchema review  rename Columnname in Foreignkey Table Schema.
func reviewRenameColumnNameInForeignkeyTableSchema(Conv *internal.Conv, sp ddl.CreateTable, index int, colName string, newName string) {

	relationTable := sp.Fks[index].ReferTable

	relationTableSp := Conv.SpSchema[relationTable]

	_, ok := relationTableSp.ColDefs[colName]

	if ok {
		{
			relationTableSp = renameColumnNameInSpannerColNames(relationTableSp, colName, newName)
			relationTableSp = renameColumnNameInSpannerColDefs(relationTableSp, colName, newName)
			relationTableSp = renameColumnNameInSpannerPK(relationTableSp, colName, newName)
			relationTableSp = renameColumnNameInSpannerSecondaryIndex(relationTableSp, colName, newName)
			relationTableSp = renameColumnNameInSpannerForeignkeyColumns(relationTableSp, colName, newName)
			relationTableSp = renameColumnNameInSpannerForeignkeyReferColumns(relationTableSp, colName, newName)

			renameColumnNameInToSpannerToSource(relationTable, colName, newName, Conv)
			renameColumnNameInSpannerSchemaIssue(relationTable, colName, newName, Conv)

			Conv.SpSchema[relationTable] = relationTableSp

		}
	}
}

// reviewRenameForeignkeyReferTableSchema review  rename Columnname in Foreignkey Refer Table Schema.
func reviewRenameColumnNameInForeignkeyReferTableSchema(Conv *internal.Conv, referTable ddl.CreateTable, table string, colName string, newName string) {

	referTable = renameColumnNameInSpannerColDefs(referTable, colName, newName)

	referTable = renameColumnNameInSpannerPK(referTable, colName, newName)

	referTable = renameColumnNameInSpannerSecondaryIndex(referTable, colName, newName)

	referTable = renameColumnNameInSpannerForeignkeyColumns(referTable, colName, newName)

	referTable = renameColumnNameInSpannerForeignkeyReferColumns(referTable, colName, newName)

	referTable = renameColumnNameInSpannerColNames(referTable, colName, newName)

	renameColumnNameInSpannerSchemaIssue(table, colName, newName, Conv)

	renameColumnNameInToSpannerToSource(table, colName, newName, Conv)

	Conv.SpSchema[table] = referTable

}

// reviewRenameColumnNameInparentTableSchema review  rename Columnname in Parent Table Schema.
func reviewRenameColumnNameInparentTableSchema(Conv *internal.Conv, parentSchemaTable string, interleaveTableSchema []InterleaveTableSchema, colName string, newName string) []InterleaveTableSchema {

	parentSchemaSp := Conv.SpSchema[parentSchemaTable]

	columnId := parentSchemaSp.ColDefs[colName].Id

	_, ok := parentSchemaSp.ColDefs[colName]

	if ok {
		{
			parentSchemaSp = renameColumnNameInSpannerColNames(parentSchemaSp, colName, newName)
			parentSchemaSp = renameColumnNameInSpannerColDefs(parentSchemaSp, colName, newName)
			parentSchemaSp = renameColumnNameInSpannerPK(parentSchemaSp, colName, newName)
			parentSchemaSp = renameColumnNameInSpannerSecondaryIndex(parentSchemaSp, colName, newName)
			parentSchemaSp = renameColumnNameInSpannerForeignkeyColumns(parentSchemaSp, colName, newName)
			parentSchemaSp = renameColumnNameInSpannerForeignkeyReferColumns(parentSchemaSp, colName, newName)

			renameColumnNameInToSpannerToSource(parentSchemaTable, colName, newName, Conv)
			renameColumnNameInSpannerSchemaIssue(parentSchemaTable, colName, newName, Conv)

			Conv.SpSchema[parentSchemaTable] = parentSchemaSp

			interleaveTableSchema = renameinterleaveTableSchema(interleaveTableSchema, parentSchemaTable, columnId, colName, newName)

		}
	}

	return interleaveTableSchema
}

// reviewRenameColumnNameInchildTableSchema review  rename Columnname in Child Table Schema.
func reviewRenameColumnNameInchildTableSchema(Conv *internal.Conv, childSchemaTable string, interleaveTableSchema []InterleaveTableSchema, colName string, newName string) []InterleaveTableSchema {

	childSchemaSp := Conv.SpSchema[childSchemaTable]

	_, ok := childSchemaSp.ColDefs[colName]

	columnId := childSchemaSp.ColDefs[colName].Id

	if ok {
		{

			childSchemaSp = renameColumnNameInSpannerColNames(childSchemaSp, colName, newName)
			childSchemaSp = renameColumnNameInSpannerColDefs(childSchemaSp, colName, newName)
			childSchemaSp = renameColumnNameInSpannerPK(childSchemaSp, colName, newName)
			childSchemaSp = renameColumnNameInSpannerSecondaryIndex(childSchemaSp, colName, newName)
			childSchemaSp = renameColumnNameInSpannerForeignkeyColumns(childSchemaSp, colName, newName)
			childSchemaSp = renameColumnNameInSpannerForeignkeyReferColumns(childSchemaSp, colName, newName)

			renameColumnNameInToSpannerToSource(childSchemaTable, colName, newName, Conv)

			renameColumnNameInSpannerSchemaIssue(childSchemaTable, colName, newName, Conv)

			Conv.SpSchema[childSchemaTable] = childSchemaSp

			{

				interleaveTableSchema = renameinterleaveTableSchema(interleaveTableSchema, childSchemaTable, columnId, colName, newName)
			}

		}
	}
	return interleaveTableSchema
}

// reviewreanmeColumnNameInCurrentTable review  rename Columnname in current Table Schema.
func reviewreanmeColumnNameInCurrentTable(Conv *internal.Conv, sp ddl.CreateTable, interleaveTableSchema []InterleaveTableSchema, table string, columnId string, colName string, newName string, childSchemaTable string, parentSchemaTable string) []InterleaveTableSchema {
	sp = renameColumnNameInSpannerColDefs(sp, colName, newName)

	sp = renameColumnNameInSpannerPK(sp, colName, newName)

	sp = renameColumnNameInSpannerSecondaryIndex(sp, colName, newName)

	sp = renameColumnNameInSpannerForeignkeyColumns(sp, colName, newName)

	sp = renameColumnNameInSpannerForeignkeyReferColumns(sp, colName, newName)

	sp = renameColumnNameInSpannerColNames(sp, colName, newName)

	renameColumnNameInSpannerSchemaIssue(table, colName, newName, Conv)

	renameColumnNameInToSpannerToSource(table, colName, newName, Conv)

	Conv.SpSchema[table] = sp

	if parentSchemaTable != "" || childSchemaTable != "" {
		interleaveTableSchema = renameinterleaveTableSchema(interleaveTableSchema, table, columnId, colName, newName)

	}

	return interleaveTableSchema
}

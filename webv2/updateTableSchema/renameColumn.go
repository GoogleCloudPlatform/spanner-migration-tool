package updateTableSchema

import (
	"fmt"

	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
)

func renameColumn(newName, table, colName string, Conv *internal.Conv) {

	sp := Conv.SpSchema[table]

	renameColumnNameInCurrentTableSchema(Conv, sp, table, colName, newName)

	// update foreignKey relationship Table
	for i, _ := range sp.Fks {

		renameColumnNameInForeignkeyTableSchema(Conv, sp, i, colName, newName)
	}

	for _, sp := range Conv.SpSchema {

		for j := 0; j < len(sp.Fks); j++ {
			if sp.Fks[j].ReferTable == table {
				renameColumnNameInForeignkeyReferTableSchema(Conv, sp, sp.Name, colName, newName)
			}

		}

	}

	// update interleave table relation
	isParent, parentschemaTable := IsParent(table)

	fmt.Println("parentschemaTable :", parentschemaTable)

	if isParent {
		renameColumnNameInparentTableSchema(Conv, parentschemaTable, colName, newName)

	}

	childschemaTable := Conv.SpSchema[table].Parent

	fmt.Println("childschemaTable :", childschemaTable)

	if childschemaTable != "" {

		renameColumnNameInchildTableSchema(Conv, childschemaTable, colName, newName)

	}

}

func renameColumnNameInCurrentTableSchema(Conv *internal.Conv, sp ddl.CreateTable, table string, colName string, newName string) {
	// step I
	sp = renameColumnNameInSpannerColDefs(sp, colName, newName)

	// step II
	sp = renameColumnNameInSpannerPK(sp, colName, newName)

	// step III
	sp = renameColumnNameInSpannerSecondaryIndex(sp, colName, newName)

	// step IV
	sp = renameColumnNameInSpannerForeignkeyColumns(sp, colName, newName)

	// step V
	sp = renameColumnNameInSpannerForeignkeyReferColumns(sp, colName, newName)

	// step VI
	sp = renameColumnNameInSpannerColNames(sp, colName, newName)

	// step VII
	renameColumnNameInSpannerSchemaIssue(table, colName, newName, Conv)

	// step VIII
	renameColumnNameInToSpannerToSource(table, colName, newName, Conv)

	Conv.SpSchema[table] = sp

}

func renameColumnNameInForeignkeyTableSchema(Conv *internal.Conv, sp ddl.CreateTable, index int, colName string, newName string) {

	fmt.Println("update foreignKey Table column names")

	relationTable := sp.Fks[index].ReferTable

	relationTableSp := Conv.SpSchema[relationTable]

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

func renameColumnNameInForeignkeyReferTableSchema(Conv *internal.Conv, sp ddl.CreateTable, table string, colName string, newName string) {
	// step I
	sp = renameColumnNameInSpannerColDefs(sp, colName, newName)

	// step II
	sp = renameColumnNameInSpannerPK(sp, colName, newName)

	// step III
	sp = renameColumnNameInSpannerSecondaryIndex(sp, colName, newName)

	// step IV
	sp = renameColumnNameInSpannerForeignkeyColumns(sp, colName, newName)

	// step V
	sp = renameColumnNameInSpannerForeignkeyReferColumns(sp, colName, newName)

	// step VI
	sp = renameColumnNameInSpannerColNames(sp, colName, newName)

	// step VII
	renameColumnNameInSpannerSchemaIssue(table, colName, newName, Conv)

	// step VIII
	renameColumnNameInToSpannerToSource(table, colName, newName, Conv)

	Conv.SpSchema[table] = sp
}

func renameColumnNameInparentTableSchema(Conv *internal.Conv, parentschemaTable string, colName string, newName string) {

	childSchemaSp := Conv.SpSchema[parentschemaTable]

	childSchemaSp = renameColumnNameInSpannerColNames(childSchemaSp, colName, newName)
	childSchemaSp = renameColumnNameInSpannerColDefs(childSchemaSp, colName, newName)
	childSchemaSp = renameColumnNameInSpannerPK(childSchemaSp, colName, newName)
	childSchemaSp = renameColumnNameInSpannerSecondaryIndex(childSchemaSp, colName, newName)
	childSchemaSp = renameColumnNameInSpannerForeignkeyColumns(childSchemaSp, colName, newName)
	childSchemaSp = renameColumnNameInSpannerForeignkeyReferColumns(childSchemaSp, colName, newName)

	renameColumnNameInToSpannerToSource(parentschemaTable, colName, newName, Conv)
	renameColumnNameInSpannerSchemaIssue(parentschemaTable, colName, newName, Conv)

	Conv.SpSchema[parentschemaTable] = childSchemaSp
}

func renameColumnNameInchildTableSchema(Conv *internal.Conv, childschemaTable string, colName string, newName string) {

	childSchemaSp := Conv.SpSchema[childschemaTable]

	childSchemaSp = renameColumnNameInSpannerColNames(childSchemaSp, colName, newName)
	childSchemaSp = renameColumnNameInSpannerColDefs(childSchemaSp, colName, newName)
	childSchemaSp = renameColumnNameInSpannerPK(childSchemaSp, colName, newName)
	childSchemaSp = renameColumnNameInSpannerSecondaryIndex(childSchemaSp, colName, newName)
	childSchemaSp = renameColumnNameInSpannerForeignkeyColumns(childSchemaSp, colName, newName)
	childSchemaSp = renameColumnNameInSpannerForeignkeyReferColumns(childSchemaSp, colName, newName)

	renameColumnNameInToSpannerToSource(childschemaTable, colName, newName, Conv)
	renameColumnNameInSpannerSchemaIssue(childschemaTable, colName, newName, Conv)

	Conv.SpSchema[childschemaTable] = childSchemaSp
}

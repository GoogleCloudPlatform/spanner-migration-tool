package updateTableSchema

import (
	"fmt"

	"github.com/cloudspannerecosystem/harbourbridge/internal"
)

func renameColumn(newName, table, colName string, Conv *internal.Conv) {

	fmt.Println("renameColumn getting called")

	sp := Conv.SpSchema[table]

	// step I
	sp = renameSpannerColDefs(sp, colName, newName)

	// step II
	sp = renameSpannerPK(sp, colName, newName)

	// step III
	sp = renameSpannerSecondaryIndex(sp, colName, newName)

	// step IV
	sp = renameSpannerForeignkeyColumns(sp, colName, newName)

	// step V
	sp = renameSpannerForeignkeyReferColumns(sp, colName, newName)

	// step VI
	sp = renameSpannerColNames(sp, colName, newName)

	// step VII
	renameSpannerSchemaIssue(table, colName, newName, Conv)

	// step VIII
	renameToSpannerToSource(table, colName, newName, Conv)

	//7
	//sessionState.Conv.SpSchema[table] = sp

	Conv.SpSchema[table] = sp

	// update foreignKey relationship Table column names
	for i, _ := range sp.Fks {

		fmt.Println("update foreignKey Table column names")

		relationTable := sp.Fks[i].ReferTable

		//relationTableSp := sessionState.Conv.SpSchema[relationTable]

		relationTableSp := Conv.SpSchema[relationTable]

		relationTableSp = renameSpannerColNames(relationTableSp, colName, newName)
		relationTableSp = renameSpannerColDefs(relationTableSp, colName, newName)
		relationTableSp = renameSpannerPK(relationTableSp, colName, newName)
		relationTableSp = renameSpannerSecondaryIndex(relationTableSp, colName, newName)
		relationTableSp = renameSpannerForeignkeyColumns(relationTableSp, colName, newName)
		relationTableSp = renameSpannerForeignkeyReferColumns(relationTableSp, colName, newName)

		//todo
		renameToSpannerToSource(relationTable, colName, newName, Conv)
		renameSpannerSchemaIssue(relationTable, colName, newName, Conv)

		//8

		Conv.SpSchema[relationTable] = relationTableSp

	}

	// update interleave table relation
	isParent, childSchema := IsParent(table)

	if isParent {
		fmt.Println("yes", table, "is parent table")

		childSchemaSp := Conv.SpSchema[childSchema]

		childSchemaSp = renameSpannerColNames(childSchemaSp, colName, newName)
		childSchemaSp = renameSpannerColDefs(childSchemaSp, colName, newName)
		childSchemaSp = renameSpannerPK(childSchemaSp, colName, newName)
		childSchemaSp = renameSpannerSecondaryIndex(childSchemaSp, colName, newName)
		childSchemaSp = renameSpannerForeignkeyColumns(childSchemaSp, colName, newName)
		childSchemaSp = renameSpannerForeignkeyReferColumns(childSchemaSp, colName, newName)

		renameToSpannerToSource(childSchema, colName, newName, Conv)
		renameSpannerSchemaIssue(childSchema, colName, newName, Conv)

		Conv.SpSchema[childSchema] = childSchemaSp

	}

	isChild := Conv.SpSchema[table].Parent

	if isChild != "" {

		childSchemaSp := Conv.SpSchema[isChild]

		childSchemaSp = renameSpannerColNames(childSchemaSp, colName, newName)
		childSchemaSp = renameSpannerColDefs(childSchemaSp, colName, newName)
		childSchemaSp = renameSpannerPK(childSchemaSp, colName, newName)
		childSchemaSp = renameSpannerSecondaryIndex(childSchemaSp, colName, newName)
		childSchemaSp = renameSpannerForeignkeyColumns(childSchemaSp, colName, newName)
		childSchemaSp = renameSpannerForeignkeyReferColumns(childSchemaSp, colName, newName)

		renameToSpannerToSource(isChild, colName, newName, Conv)

		renameSpannerSchemaIssue(isChild, colName, newName, Conv)

		Conv.SpSchema[isChild] = childSchemaSp

	}

}

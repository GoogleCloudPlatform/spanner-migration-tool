package updateTableSchema

import (
	"fmt"

	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
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

	Conv.SpSchema[table] = sp

	// update foreignKey relationship Table column names
	for i, _ := range sp.Fks {

		renameForeignkeyTableSchema(Conv, sp, i, colName, newName)
	}

	for _, sp := range Conv.SpSchema {

		for j := 0; j < len(sp.Fks); j++ {
			if sp.Fks[j].ReferTable == table {
				fmt.Println("found")
				fmt.Println("sp.Name :", sp.Name)

				reviewRenameForeignkeyReferTableSchema(Conv, sp, sp.Name, colName, newName)
			}

		}

	}

	// update interleave table relation
	isParent, parentschemaTable := IsParent(table)

	fmt.Println("parentschemaTable :", parentschemaTable)

	if isParent {
		renamechildTableSchema(Conv, parentschemaTable, colName, newName)

	}

	childschemaTable := Conv.SpSchema[table].Parent

	fmt.Println("childschemaTable :", childschemaTable)

	if childschemaTable != "" {

		renamechildTableSchema(Conv, childschemaTable, colName, newName)

	}

}

func renameForeignkeyTableSchema(Conv *internal.Conv, sp ddl.CreateTable, index int, colName string, newName string) {

	fmt.Println("update foreignKey Table column names")

	relationTable := sp.Fks[index].ReferTable

	relationTableSp := Conv.SpSchema[relationTable]

	relationTableSp = renameSpannerColNames(relationTableSp, colName, newName)
	relationTableSp = renameSpannerColDefs(relationTableSp, colName, newName)
	relationTableSp = renameSpannerPK(relationTableSp, colName, newName)
	relationTableSp = renameSpannerSecondaryIndex(relationTableSp, colName, newName)
	relationTableSp = renameSpannerForeignkeyColumns(relationTableSp, colName, newName)
	relationTableSp = renameSpannerForeignkeyReferColumns(relationTableSp, colName, newName)

	renameToSpannerToSource(relationTable, colName, newName, Conv)
	renameSpannerSchemaIssue(relationTable, colName, newName, Conv)

	Conv.SpSchema[relationTable] = relationTableSp

}

func renameparentTableSchema(Conv *internal.Conv, parentschemaTable string, colName string, newName string) {

	childSchemaSp := Conv.SpSchema[parentschemaTable]

	childSchemaSp = renameSpannerColNames(childSchemaSp, colName, newName)
	childSchemaSp = renameSpannerColDefs(childSchemaSp, colName, newName)
	childSchemaSp = renameSpannerPK(childSchemaSp, colName, newName)
	childSchemaSp = renameSpannerSecondaryIndex(childSchemaSp, colName, newName)
	childSchemaSp = renameSpannerForeignkeyColumns(childSchemaSp, colName, newName)
	childSchemaSp = renameSpannerForeignkeyReferColumns(childSchemaSp, colName, newName)

	renameToSpannerToSource(parentschemaTable, colName, newName, Conv)
	renameSpannerSchemaIssue(parentschemaTable, colName, newName, Conv)

	Conv.SpSchema[parentschemaTable] = childSchemaSp
}

func renamechildTableSchema(Conv *internal.Conv, childschemaTable string, colName string, newName string) {

	childSchemaSp := Conv.SpSchema[childschemaTable]

	childSchemaSp = renameSpannerColNames(childSchemaSp, colName, newName)
	childSchemaSp = renameSpannerColDefs(childSchemaSp, colName, newName)
	childSchemaSp = renameSpannerPK(childSchemaSp, colName, newName)
	childSchemaSp = renameSpannerSecondaryIndex(childSchemaSp, colName, newName)
	childSchemaSp = renameSpannerForeignkeyColumns(childSchemaSp, colName, newName)
	childSchemaSp = renameSpannerForeignkeyReferColumns(childSchemaSp, colName, newName)

	renameToSpannerToSource(childschemaTable, colName, newName, Conv)
	renameSpannerSchemaIssue(childschemaTable, colName, newName, Conv)

	Conv.SpSchema[childschemaTable] = childSchemaSp
}

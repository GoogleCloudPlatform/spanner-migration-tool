package updateTableSchema

import (
	"fmt"

	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
	utilities "github.com/cloudspannerecosystem/harbourbridge/webv2/utilities"
)

func removeColumn(table string, colName string, Conv *internal.Conv) {

	sp := Conv.SpSchema[table]

	// step I
	sp = removeSpannerColDefs(sp, colName)

	// step II
	sp = removeSpannerPK(sp, colName)

	// step III
	sp = removeSpannerSecondaryIndex(sp, colName)

	// step IV
	sp = removeSpannerForeignkeyColumns(sp, colName)

	// step V
	sp = removeSpannerForeignkeyReferColumns(sp, colName)

	// step VI
	sp = removeSpannerColNames(sp, colName)

	// step VII
	removeSpannerSchemaIssue(table, colName, Conv)

	// step VIII
	removeToSpannerToSource(table, colName, Conv)

	Conv.SpSchema[table] = sp

	// update foreignKey relationship Table column names
	for i, _ := range sp.Fks {

		removeForeignkeyTableSchema(Conv, sp, i, colName)

	}

	for _, sp := range Conv.SpSchema {

		for j := 0; j < len(sp.Fks); j++ {
			if sp.Fks[j].ReferTable == table {
				fmt.Println("found")
				fmt.Println("sp.Name :", sp.Name)

				removeForeignkeyReferTableSchema(Conv, sp, sp.Name, colName)
			}

		}

	}

	isParent, parentSchemaTable := IsParent(table)

	if isParent {

		removeparentTableSchema(Conv, parentSchemaTable, colName)

	}

	childSchemaTable := Conv.SpSchema[table].Parent

	if childSchemaTable != "" {

		removechildTableSchema(Conv, parentSchemaTable, colName)
	}
}

func removeForeignkeyTableSchema(Conv *internal.Conv, sp ddl.CreateTable, index int, colName string) {

	relationTable := sp.Fks[index].ReferTable
	relationTableSp := Conv.SpSchema[relationTable]

	relationTableSp = removeSpannerColNames(relationTableSp, colName)
	relationTableSp = removeSpannerColDefs(relationTableSp, colName)
	relationTableSp = removeSpannerPK(relationTableSp, colName)
	relationTableSp = removeSpannerSecondaryIndex(relationTableSp, colName)
	relationTableSp = removeSpannerForeignkeyColumns(relationTableSp, colName)
	relationTableSp = removeSpannerForeignkeyReferColumns(relationTableSp, colName)

	removeSpannerSchemaIssue(relationTable, colName, Conv)
	removeToSpannerToSource(relationTable, colName, Conv)

	Conv.SpSchema[relationTable] = relationTableSp
}

func removeForeignkeyReferTableSchema(Conv *internal.Conv, referTable ddl.CreateTable, table string, colName string) {

	referTable = removeSpannerColDefs(referTable, colName)

	// step II
	referTable = removeSpannerPK(referTable, colName)

	// step III
	referTable = removeSpannerSecondaryIndex(referTable, colName)

	// step IV
	referTable = removeSpannerForeignkeyColumns(referTable, colName)

	// step V
	referTable = removeSpannerForeignkeyReferColumns(referTable, colName)

	// step VI
	referTable = removeSpannerColNames(referTable, colName)

	// step VII
	removeSpannerSchemaIssue(table, colName, Conv)

	// step VIII
	removeToSpannerToSource(table, colName, Conv)

	Conv.SpSchema[table] = referTable

}

func removeparentTableSchema(Conv *internal.Conv, parentSchemaTable string, colName string) {

	childSchemaSp := Conv.SpSchema[parentSchemaTable]

	childSchemaSp = removeSpannerColNames(childSchemaSp, colName)
	childSchemaSp = removeSpannerColDefs(childSchemaSp, colName)
	childSchemaSp = removeSpannerPK(childSchemaSp, colName)
	childSchemaSp = removeSpannerSecondaryIndex(childSchemaSp, colName)
	childSchemaSp = removeSpannerForeignkeyColumns(childSchemaSp, colName)
	childSchemaSp = removeSpannerForeignkeyReferColumns(childSchemaSp, colName)

	removeSpannerSchemaIssue(parentSchemaTable, colName, Conv)
	removeToSpannerToSource(parentSchemaTable, colName, Conv)

	Conv.SpSchema[parentSchemaTable] = childSchemaSp

}

func removechildTableSchema(Conv *internal.Conv, childSchemaTable string, colName string) {

	childSchemaSp := Conv.SpSchema[childSchemaTable]

	childSchemaSp = removeSpannerColNames(childSchemaSp, colName)
	childSchemaSp = removeSpannerColDefs(childSchemaSp, colName)
	childSchemaSp = removeSpannerPK(childSchemaSp, colName)
	childSchemaSp = removeSpannerSecondaryIndex(childSchemaSp, colName)
	childSchemaSp = removeSpannerForeignkeyColumns(childSchemaSp, colName)
	childSchemaSp = removeSpannerForeignkeyReferColumns(childSchemaSp, colName)

	removeSpannerSchemaIssue(childSchemaTable, colName, Conv)
	removeToSpannerToSource(childSchemaTable, colName, Conv)

	Conv.SpSchema[childSchemaTable] = childSchemaSp

}

func removeSpannerColNames(sp ddl.CreateTable, colName string) ddl.CreateTable {

	// step VI
	// remove sp.ColNames

	fmt.Println("")
	fmt.Println("step VI")

	for i, col := range sp.ColNames {
		if col == colName {
			sp.ColNames = utilities.Remove(sp.ColNames, i)
			break
		}
	}

	fmt.Println("removing sp.ColNames : ", colName)

	delete(sp.ColDefs, colName)

	fmt.Println("removed sp.ColNames : ", sp.ColNames)

	return sp

}

func removeSpannerPK(sp ddl.CreateTable, colName string) ddl.CreateTable {

	// step II
	// remove sp.Pks

	fmt.Println("")
	fmt.Println("step II")

	for i, pk := range sp.Pks {
		if pk.Col == colName {

			fmt.Println("removing sp.Pks : ", i)

			sp.Pks = utilities.RemovePk(sp.Pks, i)

			fmt.Println("removed sp.Pks : ", sp.Pks)

			break
		}
	}

	return sp
}

func removeSpannerColDefs(sp ddl.CreateTable, colName string) ddl.CreateTable {

	// step I
	// remove sp.ColDefs

	fmt.Println("")
	fmt.Println("step I")

	if _, found := sp.ColDefs[colName]; found {

		fmt.Println("removing sp.ColDefs : ")

		delete(sp.ColDefs, colName)

		fmt.Println("removed sp.ColDefs :", sp.ColDefs)

	}

	return sp
}

func removeSpannerSecondaryIndex(sp ddl.CreateTable, colName string) ddl.CreateTable {

	// step III
	// update sp.Indexes
	fmt.Println("")
	fmt.Println("step III")

	for i, index := range sp.Indexes {
		for j, key := range index.Keys {
			if key.Col == colName {

				fmt.Println("removing sp.Indexes[i].Keys[j].Col : ")

				sp.Indexes[i].Keys = removeSecondaryIndexKey(sp.Indexes[i].Keys, j)

				fmt.Println("removed sp.Indexes[i].Keys[j].Col : ", sp.Indexes[i].Keys[j])

				break
			}
		}
	}

	return sp
}

func removeSecondaryIndexKey(slice []ddl.IndexKey, s int) []ddl.IndexKey {
	return append(slice[:s], slice[s+1:]...)
}

func removeSpannerSchemaIssue(table string, colName string, Conv *internal.Conv) {

	// step VII
	// remove sessionState.Conv.Issues

	fmt.Println("")
	fmt.Println("step VII")

	//sessionState := session.GetSessionState()

	if Conv.Issues != nil {

		if Conv.Issues[table] != nil && Conv.Issues[table][colName] != nil {

			delete(Conv.Issues[table], colName)

		}

	}

}

func removeToSpannerToSource(table string, colName string, Conv *internal.Conv) {

	// step VIII
	// remove ToSpannerToSource

	fmt.Println("")
	fmt.Println("step VII")

	//sessionState := session.GetSessionState()

	srcTableName := Conv.ToSource[table].Name

	srcColName := Conv.ToSource[table].Cols[colName]
	delete(Conv.ToSource[table].Cols, colName)
	delete(Conv.ToSpanner[srcTableName].Cols, srcColName)

}

func removeSpannerForeignkeyColumns(sp ddl.CreateTable, colName string) ddl.CreateTable {

	// step IV
	// update sp.fk.Columns
	fmt.Println("")
	fmt.Println("step IV")

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

func removeFkColumns(slice []string, s int) []string {
	return append(slice[:s], slice[s+1:]...)
}

func removeSpannerForeignkeyReferColumns(sp ddl.CreateTable, colName string) ddl.CreateTable {

	// step IV
	// update sp.fk.ReferColumns
	fmt.Println("")
	fmt.Println("step IV")

	for i, fk := range sp.Fks {
		for j, column := range fk.ReferColumns {

			if column == colName {

				fmt.Println(" removing sp.Fks[i].ReferColumns[j] :", sp.Fks[i].ReferColumns[j])

				sp.Fks[i].ReferColumns = removeFkReferColumns(sp.Fks[i].ReferColumns, j)

				fmt.Println("removed sp.Fks[i].ReferColumns[j] :", sp.Fks[i].ReferColumns)

			}

		}
	}
	return sp
}

func removeFkReferColumns(slice []string, s int) []string {
	return append(slice[:s], slice[s+1:]...)
}

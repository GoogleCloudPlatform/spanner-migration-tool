package updateTableSchema

import (
	"fmt"

	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
	utilities "github.com/cloudspannerecosystem/harbourbridge/webv2/utilities"
)

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
				fmt.Println("found")
				fmt.Println("sp.Name :", sp.Name)

				removeColumnFromForeignkeyReferTableSchema(Conv, sp, sp.Name, colName)
			}

		}

	}

	isParent, parentSchemaTable := IsParent(table)

	if isParent {

		removeColumnFromparentTableSchema(Conv, parentSchemaTable, colName)

	}

	childSchemaTable := Conv.SpSchema[table].Parent

	if childSchemaTable != "" {

		removeColumnFromChildTableSchema(Conv, parentSchemaTable, colName)
	}
}

func removeColumnFromCurrentTableSchema(Conv *internal.Conv, sp ddl.CreateTable, table string, colName string) {
	// step I
	sp = removeColumnFromSpannerColDefs(sp, colName)

	// step II
	sp = removeColumnFromSpannerPK(sp, colName)

	// step III
	sp = removeColumnFromSpannerSecondaryIndex(sp, colName)

	// step IV
	sp = removeColumnFromSpannerForeignkeyColumns(sp, colName)

	// step V
	sp = removeColumnFromSpannerForeignkeyReferColumns(sp, colName)

	// step VI
	sp = removeColumnFromSpannerColNames(sp, colName)

	// step VII
	removeSpannerSchemaIssue(table, colName, Conv)

	// step VIII
	removeColumnFromToSpannerToSource(table, colName, Conv)

	Conv.SpSchema[table] = sp

}

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

func removeColumnFromForeignkeyReferTableSchema(Conv *internal.Conv, referTable ddl.CreateTable, table string, colName string) {

	referTable = removeColumnFromSpannerColDefs(referTable, colName)

	// step II
	referTable = removeColumnFromSpannerPK(referTable, colName)

	// step III
	referTable = removeColumnFromSpannerSecondaryIndex(referTable, colName)

	// step IV
	referTable = removeColumnFromSpannerForeignkeyColumns(referTable, colName)

	// step V
	referTable = removeColumnFromSpannerForeignkeyReferColumns(referTable, colName)

	// step VI
	referTable = removeColumnFromSpannerColNames(referTable, colName)

	// step VII
	removeSpannerSchemaIssue(table, colName, Conv)

	// step VIII
	removeColumnFromToSpannerToSource(table, colName, Conv)

	Conv.SpSchema[table] = referTable

}

func removeColumnFromparentTableSchema(Conv *internal.Conv, parentSchemaTable string, colName string) {

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

func removeColumnFromSpannerColNames(sp ddl.CreateTable, colName string) ddl.CreateTable {

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

func removeColumnFromSpannerPK(sp ddl.CreateTable, colName string) ddl.CreateTable {

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

func removeColumnFromSpannerColDefs(sp ddl.CreateTable, colName string) ddl.CreateTable {

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

func removeColumnFromSpannerSecondaryIndex(sp ddl.CreateTable, colName string) ddl.CreateTable {

	// step III
	// update sp.Indexes
	fmt.Println("")
	fmt.Println("step III")

	for i, index := range sp.Indexes {
		for j, key := range index.Keys {
			if key.Col == colName {

				fmt.Println("removing sp.Indexes[i].Keys[j].Col : ")

				sp.Indexes[i].Keys = removeColumnFromSecondaryIndexKey(sp.Indexes[i].Keys, j)

				fmt.Println("removed sp.Indexes[i].Keys[j].Col : ", sp.Indexes[i].Keys[j])

				break
			}
		}
	}

	return sp
}

func removeColumnFromSecondaryIndexKey(slice []ddl.IndexKey, s int) []ddl.IndexKey {
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

func removeColumnFromToSpannerToSource(table string, colName string, Conv *internal.Conv) {

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

func removeColumnFromSpannerForeignkeyColumns(sp ddl.CreateTable, colName string) ddl.CreateTable {

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

func removeColumnFromSpannerForeignkeyReferColumns(sp ddl.CreateTable, colName string) ddl.CreateTable {

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

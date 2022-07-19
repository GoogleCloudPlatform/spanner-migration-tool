package updateTableSchema

import (
	"fmt"

	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
	"github.com/cloudspannerecosystem/harbourbridge/webv2/session"
	utilities "github.com/cloudspannerecosystem/harbourbridge/webv2/utilities"
)

func removeColumn(table string, colName string, srcTableName string) {

	sessionState := session.GetSessionState()

	sp := sessionState.Conv.SpSchema[table]

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
	removeSpannerSchemaIssue(table, colName)

	// step VIII
	removeToSpannerToSource(table, colName)
	sessionState.Conv.SpSchema[table] = sp

	// update foreignKey relationship Table column names
	for i, _ := range sp.Fks {

		fmt.Println("update foreignKey Table column names")

		relationTable := sp.Fks[i].ReferTable

		relationTableSp := sessionState.Conv.SpSchema[relationTable]

		relationTableSp = removeSpannerColNames(relationTableSp, colName)
		relationTableSp = removeSpannerColDefs(relationTableSp, colName)
		relationTableSp = removeSpannerPK(relationTableSp, colName)
		relationTableSp = removeSpannerSecondaryIndex(relationTableSp, colName)
		relationTableSp = removeSpannerForeignkeyColumns(relationTableSp, colName)
		relationTableSp = removeSpannerForeignkeyReferColumns(relationTableSp, colName)

		//todo
		removeSpannerSchemaIssue(relationTable, colName)

		removeToSpannerToSource(relationTable, colName)

		sessionState.Conv.SpSchema[relationTable] = relationTableSp

	}

	// update interleave table relation
	isParent, childSchema := utilities.IsParent(table)

	if isParent {
		fmt.Println("yes", table, "is parent table")

		childSchemaSp := sessionState.Conv.SpSchema[childSchema]

		childSchemaSp = removeSpannerColNames(childSchemaSp, colName)
		childSchemaSp = removeSpannerColDefs(childSchemaSp, colName)
		childSchemaSp = removeSpannerPK(childSchemaSp, colName)
		childSchemaSp = removeSpannerSecondaryIndex(childSchemaSp, colName)
		childSchemaSp = removeSpannerForeignkeyColumns(childSchemaSp, colName)
		childSchemaSp = removeSpannerForeignkeyReferColumns(childSchemaSp, colName)

		//todo
		removeSpannerSchemaIssue(childSchema, colName)

		removeToSpannerToSource(childSchema, colName)

		sessionState.Conv.SpSchema[childSchema] = childSchemaSp

	}

	isChild := sessionState.Conv.SpSchema[table].Parent

	if isChild != "" {

		childSchemaSp := sessionState.Conv.SpSchema[isChild]

		childSchemaSp = removeSpannerColNames(childSchemaSp, colName)
		childSchemaSp = removeSpannerColDefs(childSchemaSp, colName)
		childSchemaSp = removeSpannerPK(childSchemaSp, colName)
		childSchemaSp = removeSpannerSecondaryIndex(childSchemaSp, colName)
		childSchemaSp = removeSpannerForeignkeyColumns(childSchemaSp, colName)
		childSchemaSp = removeSpannerForeignkeyReferColumns(childSchemaSp, colName)

		//todo
		removeSpannerSchemaIssue(isChild, colName)
		removeToSpannerToSource(isChild, colName)

		sessionState.Conv.SpSchema[isChild] = childSchemaSp
	}
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

	fmt.Println("removed sp.ColNames : ", sp.ColDefs)

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

func removeSpannerSchemaIssue(table string, colName string) {

	// step VII
	// remove sessionState.Conv.Issues

	fmt.Println("")
	fmt.Println("step VII")

	sessionState := session.GetSessionState()

	if sessionState.Conv.Issues != nil {

		if sessionState.Conv.Issues[table] != nil && sessionState.Conv.Issues[table][colName] != nil {

			delete(sessionState.Conv.Issues[table], colName)

		}

	}

}

func removeToSpannerToSource(table string, colName string) {

	// step VIII
	// remove ToSpannerToSource

	fmt.Println("")
	fmt.Println("step VII")

	sessionState := session.GetSessionState()

	srcTableName := sessionState.Conv.ToSource[table].Name

	srcColName := sessionState.Conv.ToSource[table].Cols[colName]
	delete(sessionState.Conv.ToSource[table].Cols, colName)
	delete(sessionState.Conv.ToSpanner[srcTableName].Cols, srcColName)

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

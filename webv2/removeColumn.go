package webv2

import (
	"fmt"

	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
	"github.com/cloudspannerecosystem/harbourbridge/webv2/session"
)

func removeColumn(table string, colName string, srcTableName string) {

	sessionState := session.GetSessionState()

	sp := sessionState.Conv.SpSchema[table]

	sp = removeSpannerColDefs(sp, colName)

	sp = removeSpannerPK(sp, colName)

	sp = removeSpannerColNames(sp, colName)

	removeSpannerSchemaIssue(table, colName)

	removeToSpannerToSource(table, colName)

	srcColName := sessionState.Conv.ToSource[table].Cols[colName]
	delete(sessionState.Conv.ToSource[table].Cols, colName)
	delete(sessionState.Conv.ToSpanner[srcTableName].Cols, srcColName)
	sessionState.Conv.SpSchema[table] = sp
}

func removeSpannerColNames(sp ddl.CreateTable, colName string) ddl.CreateTable {

	// step I
	// remove sp.ColNames

	fmt.Println("")
	fmt.Println("step I")

	for i, col := range sp.ColNames {
		if col == colName {
			sp.ColNames = remove(sp.ColNames, i)
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
	// remove sp.PK

	fmt.Println("")
	fmt.Println("step II")

	for i, pk := range sp.Pks {
		if pk.Col == colName {

			fmt.Println("removing sp.Pks : ", i)

			sp.Pks = removePk(sp.Pks, i)

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

func removeSpannerIndex(sp ddl.CreateTable, colName string) ddl.CreateTable {

	// step IV
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

	sessionState := session.GetSessionState()

	if sessionState.Conv.Issues != nil {

		if sessionState.Conv.Issues[table] != nil && sessionState.Conv.Issues[table][colName] != nil {

			delete(sessionState.Conv.Issues[table], colName)

		}

	}

}

func removeToSpannerToSource(table string, colName string) {

	sessionState := session.GetSessionState()

	srcTableName := sessionState.Conv.ToSource[table].Name

	srcColName := sessionState.Conv.ToSource[table].Cols[colName]
	delete(sessionState.Conv.ToSource[table].Cols, colName)
	delete(sessionState.Conv.ToSpanner[srcTableName].Cols, srcColName)
}

package updateTableSchema

import (
	"fmt"

	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
)

func addColumn(table string, colName string, Conv *internal.Conv) {

	//sessionState := session.GetSessionState()

	fmt.Println("addColumn getting called")

	sp := Conv.SpSchema[table]

	src := Conv.SpSchema[table]

	srcColumnId := src.ColDefs[colName].Id

	fmt.Println("before sp.ColumnDef", sp.ColDefs)

	fmt.Println("")
	fmt.Println("")

	for k, v := range sp.ColDefs {
		fmt.Println("k :", k)
		fmt.Println("v :", v)
	}

	//todo check colName is already present or not

	sp.ColDefs[colName] = ddl.ColumnDef{
		Id:      srcColumnId,
		Name:    colName,
		T:       sp.ColDefs[colName].T,
		NotNull: sp.ColDefs[colName].NotNull,
		Comment: sp.ColDefs[colName].Comment,
	}

	fmt.Println("after Add sp.ColumnDef", sp.ColDefs)

	fmt.Println("")
	fmt.Println("")

	for k, v := range sp.ColDefs {
		fmt.Println("k :", k)
		fmt.Println("v :", v)
	}

	fmt.Println(" before len of sp.ColNames  ", sp.ColNames)

	sp.ColNames = append(sp.ColNames, colName)

	fmt.Println("after len of sp.ColNames  ", sp.ColNames)

	Conv.SpSchema[table] = sp

	srcTableName := Conv.ToSource[table].Name
	srcColName := Conv.ToSource[table].Cols[colName]

	//1
	Conv.ToSpanner[srcTableName].Cols[srcColName] = colName
	Conv.ToSource[table].Cols[colName] = srcColName

}

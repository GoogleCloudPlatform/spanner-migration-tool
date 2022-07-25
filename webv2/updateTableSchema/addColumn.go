package updateTableSchema

import (
	"fmt"
	"net/http"

	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
)

func addColumn(table string, colName string, Conv *internal.Conv, w http.ResponseWriter) error {

	//sessionState := session.GetSessionState()

	fmt.Println("addColumn getting called")

	sp := Conv.SpSchema[table]

	src := Conv.SpSchema[table]

	srcColumnId := src.ColDefs[colName].Id

	//todo check colName is already present or not

	// _, ok := sp.ColDefs[colName]

	// if ok {

	// 	log.Println("colName is already present in table")
	// 	err := fmt.Errorf("colName is already present in table")
	// 	return err
	// }

	fmt.Println("before sp.ColumnDef", sp.ColDefs)

	fmt.Println("")
	fmt.Println("")

	for k, v := range sp.ColDefs {
		fmt.Println("k :", k)
		fmt.Println("v :", v)
	}

	sp.ColDefs[colName] = ddl.ColumnDef{
		Id:      srcColumnId,
		Name:    colName,
		T:       src.ColDefs[colName].T,
		NotNull: src.ColDefs[colName].NotNull,
		Comment: src.ColDefs[colName].Comment,
	}

	fmt.Println("after Add sp.ColumnDef", sp.ColDefs)

	fmt.Println(" before len of sp.ColNames  ", sp.ColNames)

	sp.ColNames = append(sp.ColNames, colName)

	fmt.Println("after len of sp.ColNames  ", sp.ColNames)

	Conv.SpSchema[table] = sp

	srcTableName := Conv.ToSource[table].Name
	srcColName := Conv.ToSource[table].Cols[colName]

	//1
	Conv.ToSpanner[srcTableName].Cols[srcColName] = colName
	Conv.ToSource[table].Cols[colName] = srcColName

	return nil
}

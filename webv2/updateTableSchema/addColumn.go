package updateTableSchema

import (
	"fmt"
	"net/http"

	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
)

func addColumn(table string, colName string, Conv *internal.Conv, w http.ResponseWriter) error {

	fmt.Println("addColumn getting called")
	fmt.Println("")

	sp := Conv.SpSchema[table]

	src := Conv.SrcSchema[table]

	srcColumnId := src.ColDefs[colName].Id

	fmt.Println("before sp.ColumnDef", sp.ColDefs)
	fmt.Println("")

	for k, _ := range sp.ColDefs {
		fmt.Println("Column Name :", k)
	}

	sp.ColDefs[colName] = ddl.ColumnDef{
		Id:   srcColumnId,
		Name: colName,
	}

	fmt.Println("after Add sp.ColumnDef", sp.ColDefs)
	fmt.Println("")

	for k, _ := range sp.ColDefs {
		fmt.Println("Column Name :", k)
	}

	fmt.Println(" before sp.ColNames  ", sp.ColNames)

	if IsColNamesPresent(sp.ColNames, colName) == false {

		sp.ColNames = append(sp.ColNames, colName)

	}

	fmt.Println("after sp.ColNames ", sp.ColNames)

	Conv.SpSchema[table] = sp

	srcTableName := Conv.ToSource[table].Name

	srcColName := src.ColDefs[colName].Name

	Conv.ToSpanner[srcTableName].Cols[srcColName] = colName
	Conv.ToSource[table].Cols[colName] = srcColName

	return nil
}

func IsColNamesPresent(s []string, str string) bool {

	for _, v := range s {
		if v == str {
			return true
		}
	}

	return false
}

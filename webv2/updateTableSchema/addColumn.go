package updateTableSchema

import (
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
)

// addColumn add given column into spannerTable.
func addColumn(table string, colName string, Conv *internal.Conv) {

	sp := Conv.SpSchema[table]
	src := Conv.SrcSchema[table]

	srcColumnId := src.ColDefs[colName].Id

	sp.ColDefs[colName] = ddl.ColumnDef{
		Id:   srcColumnId,
		Name: colName,
	}

	if IsColumnPresentInColNames(sp.ColNames, colName) == false {

		sp.ColNames = append(sp.ColNames, colName)

	}

	Conv.SpSchema[table] = sp

	srcTableName := Conv.ToSource[table].Name
	srcColName := src.ColDefs[colName].Name

	Conv.ToSpanner[srcTableName].Cols[srcColName] = colName
	Conv.ToSource[table].Cols[colName] = srcColName
}

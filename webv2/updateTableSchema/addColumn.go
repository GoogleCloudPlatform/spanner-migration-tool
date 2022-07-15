package webv2

import (
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
	"github.com/cloudspannerecosystem/harbourbridge/webv2/session"
	"github.com/cloudspannerecosystem/harbourbridge/webv2/uniqueid"
)

func addColumn(table string, colName string, srcTableName string) {

	sessionState := session.GetSessionState()

	sp := sessionState.Conv.SpSchema[table]

	sp.ColDefs[colName] = ddl.ColumnDef{
		Id:      uniqueid.GenerateColumnId(),
		Name:    colName,
		T:       sp.ColDefs[colName].T,
		NotNull: sp.ColDefs[colName].NotNull,
		Comment: sp.ColDefs[colName].Comment,
	}

	srcColName := sessionState.Conv.ToSource[table].Cols[colName]
	sessionState.Conv.ToSpanner[srcTableName].Cols[srcColName] = colName
	sessionState.Conv.ToSource[table].Cols[colName] = srcColName

	sessionState.Conv.SpSchema[table] = sp

}

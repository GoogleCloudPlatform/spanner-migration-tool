package updateTableSchema

import (
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
)

func getDDL(spannerTable ddl.CreateTable) string {

	c := ddl.Config{Comments: true, ProtectIds: false}

	ddl := spannerTable.PrintCreateTable(c)

	return ddl
}

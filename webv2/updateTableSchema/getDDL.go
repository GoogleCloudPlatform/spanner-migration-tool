package updateTableSchema

import (
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
)

func getDDL(table string, Conv *internal.Conv) string {

	c := ddl.Config{Comments: true, ProtectIds: false}

	ddl := Conv.SpSchema[table].PrintCreateTable(c)

	return ddl
}

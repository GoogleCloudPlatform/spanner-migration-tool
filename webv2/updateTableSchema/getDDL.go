package updateTableSchema

import (
	"fmt"

	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
)

func getDDL(table string, Conv *internal.Conv) string {

	fmt.Println("inside getDDL table :", table)

	c := ddl.Config{Comments: true, ProtectIds: false, Tables: true, ForeignKeys: true}

	sp := Conv.SpSchema[table]

	fmt.Println("sp.table name", sp.Name)

	ddl := sp.PrintCreateTable(c)

	fmt.Println("")
	fmt.Println("")
	fmt.Println("")

	fmt.Println("ddl :", ddl)

	fmt.Println("")
	fmt.Println("")
	fmt.Println("")
	fmt.Println("")

	return ddl
}

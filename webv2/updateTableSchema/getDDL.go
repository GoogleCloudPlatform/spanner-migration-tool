package updateTableSchema

import (
	"fmt"

	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
)

func getDDL(table string, Conv *internal.Conv) string {

	c := ddl.Config{Comments: true, ProtectIds: false}

	ddl := Conv.SpSchema[table].PrintCreateTable(c)

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

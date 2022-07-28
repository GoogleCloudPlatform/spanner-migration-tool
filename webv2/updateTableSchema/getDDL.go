package updateTableSchema

import (
	"fmt"

	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
)

func getDDL(spannerTable ddl.CreateTable) string {

	fmt.Println("inside getDDL table :", spannerTable.Name)

	c := ddl.Config{Comments: true, ProtectIds: false}

	ddl := spannerTable.PrintCreateTable(c)

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

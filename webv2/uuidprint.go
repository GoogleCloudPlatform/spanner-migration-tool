package webv2

import (
	"fmt"

	"github.com/cloudspannerecosystem/harbourbridge/internal"
)

/*
	PrintAssignUniqueId prints id to console
*/
func PrintAssignUniqueId(conv *internal.Conv) {

	fmt.Println("len of sourcetable :", len(conv.SrcSchema))

	fmt.Println("len of spannertable :", len(conv.SpSchema))

	for sourcetablename, sourcetable := range conv.SrcSchema {

		for spannertablename, spannertable := range conv.SpSchema {

			if sourcetablename == spannertablename {

				fmt.Println("Table")
				fmt.Println("sourcetable id   :", sourcetable.Id, "sourcetable name :", sourcetable.Name)

				fmt.Println("spannertable id :", spannertable.Id, "spannertable name   :", spannertable.Name)

				fmt.Println("spannertable PrimaryKeyId :", spannertable.PrimaryKeyId, "spannertable PrimaryKeyId   :", sourcetable.PrimaryKeyId)

				for _, sourcecolumn := range sourcetable.ColDefs {

					for _, spannercolumn := range spannertable.ColDefs {

						if sourcecolumn.Name == spannercolumn.Name {

							fmt.Println("Column")

							fmt.Println("sourcecolumn id   :", sourcecolumn.Id, "sourcetable name :", sourcecolumn.Name)

							fmt.Println("spannercolumn id :", spannercolumn.Id, "spannercolumn name   :", spannercolumn.Name)

						}
					}

				}

				fmt.Println("")

				fmt.Println("###############################################")

			}
		}
	}

	fmt.Println("print updated")
}

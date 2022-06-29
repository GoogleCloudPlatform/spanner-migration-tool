package uniqueid

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

				for spannercolumnname, spannercolumn := range spannertable.ColDefs {

					if spannercolumn.Name == "synth_id" {

						fmt.Println("synth_id :", spannertable.Name, spannercolumn.Id, spannercolumnname)
					}

				}

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

				for _, sourceforeignkey := range sourcetable.ForeignKeys {

					for _, spannerforeignkey := range spannertable.Fks {

						if sourceforeignkey.Name == spannerforeignkey.Name {

							fmt.Println("ForeignKeys")

							fmt.Println("sourceforeignkey.Id    :", sourceforeignkey.Id, "sourceforeignkey name :", sourceforeignkey.Name)

							fmt.Println("spannerforeignkey id :", spannerforeignkey.Id, "spannerforeignkey name   :", spannerforeignkey.Name)

						}

					}
				}

				fmt.Println("")
				fmt.Println("###############################################")

				for _, sourceindexes := range sourcetable.Indexes {

					for _, spannerindexes := range spannertable.Indexes {

						if sourceindexes.Name == spannerindexes.Name {

							fmt.Println("Indexes")

							fmt.Println("sourceindexes.Id    :", sourceindexes.Id, "sourceindexes name :", sourceindexes.Name)

							fmt.Println("spannerindexes Id   :", spannerindexes.Id, "spannerindexes name   :", spannerindexes.Name)

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

package webv2

import (
	"fmt"

	"github.com/cloudspannerecosystem/harbourbridge/internal"
)

func Updateconv(conv *internal.Conv) {

	tablecounter := 1

	fmt.Println("len of sourcetable :", len(conv.SrcSchema))

	fmt.Println("len of spannertable :", len(conv.SpSchema))

	for sourcetablename, sourcetable := range conv.SrcSchema {

		for spannertablename, spannertable := range conv.SpSchema {

			if sourcetablename == spannertablename {

				sourcetable.Id = tablecounter
				spannertable.Id = tablecounter

				sourcetable.PrimaryKeyId = tablecounter
				spannertable.PrimaryKeyId = tablecounter

				tablecounter = tablecounter + 1

				fmt.Println("Table")
				fmt.Println("sourcetable id   :", sourcetable.Id, "sourcetable name :", sourcetable.Name)

				fmt.Println("spannertable id :", spannertable.Id, "spannertable name   :", spannertable.Name)

				fmt.Println("spannertable PrimaryKeyId :", spannertable.PrimaryKeyId, "spannertable PrimaryKeyId   :", sourcetable.PrimaryKeyId)

				columncounter := 1
				for sourcecolumnname, sourcecolumn := range sourcetable.ColDefs {

					for spannercolumnname, spannercolumn := range spannertable.ColDefs {

						if sourcecolumn.Name == spannercolumn.Name {

							sourcecolumn.Id = columncounter
							spannercolumn.Id = columncounter

							fmt.Println("Column")

							fmt.Println("sourcecolumn id   :", sourcecolumn.Id, "sourcetable name :", sourcecolumn.Name)

							fmt.Println("spannercolumn id :", spannercolumn.Id, "spannercolumn name   :", spannercolumn.Name)

							columncounter = columncounter + 1

							conv.SrcSchema[sourcetablename].ColDefs[sourcecolumnname] = sourcecolumn
							conv.SpSchema[spannertablename].ColDefs[spannercolumnname] = spannercolumn
							break
						}
					}

				}

				fmt.Println("")

				conv.SrcSchema[sourcetablename] = sourcetable
				conv.SpSchema[spannertablename] = spannertable

				fmt.Println("###############################################")
				break
			}
		}
	}

	fmt.Println("id updated")

	//	return conv
}

func Printconv(conv *internal.Conv) {

	tablecounter := 1

	fmt.Println("len of sourcetable :", len(conv.SrcSchema))

	fmt.Println("len of spannertable :", len(conv.SpSchema))

	for sourcetablename, sourcetable := range conv.SrcSchema {

		for spannertablename, spannertable := range conv.SpSchema {

			if sourcetablename == spannertablename {

				tablecounter = tablecounter + 1

				fmt.Println("Table")
				fmt.Println("sourcetable id   :", sourcetable.Id, "sourcetable name :", sourcetable.Name)

				fmt.Println("spannertable id :", spannertable.Id, "spannertable name   :", spannertable.Name)

				fmt.Println("spannertable PrimaryKeyId :", spannertable.PrimaryKeyId, "spannertable PrimaryKeyId   :", sourcetable.PrimaryKeyId)

				columncounter := 1
				for _, sourcecolumn := range sourcetable.ColDefs {

					for _, spannercolumn := range spannertable.ColDefs {

						if sourcecolumn.Name == spannercolumn.Name {

							fmt.Println("Column")

							fmt.Println("sourcecolumn id   :", sourcecolumn.Id, "sourcetable name :", sourcecolumn.Name)

							fmt.Println("spannercolumn id :", spannercolumn.Id, "spannercolumn name   :", spannercolumn.Name)

							columncounter = columncounter + 1

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

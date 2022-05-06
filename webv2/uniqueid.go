package webv2

import (
	"github.com/cloudspannerecosystem/harbourbridge/internal"
)

/*
	AssignUniqueId to handle  cascading effect in UI
*/
func AssignUniqueId(conv *internal.Conv) {

	tableuniqueid := 1

	for sourcetablename, sourcetable := range conv.SrcSchema {

		for spannertablename, spannertable := range conv.SpSchema {

			if sourcetablename == spannertablename {

				sourcetable.Id = tableuniqueid
				spannertable.Id = tableuniqueid

				sourcetable.PrimaryKeyId = tableuniqueid
				spannertable.PrimaryKeyId = tableuniqueid

				tableuniqueid = tableuniqueid + 1

				columnuniqueid := 1

				for sourcecolumnname, sourcecolumn := range sourcetable.ColDefs {

					for spannercolumnname, spannercolumn := range spannertable.ColDefs {

						if sourcecolumn.Name == spannercolumn.Name {

							sourcecolumn.Id = columnuniqueid
							spannercolumn.Id = columnuniqueid

							columnuniqueid = columnuniqueid + 1

							conv.SrcSchema[sourcetablename].ColDefs[sourcecolumnname] = sourcecolumn
							conv.SpSchema[spannertablename].ColDefs[spannercolumnname] = spannercolumn
							break
						}
					}

				}

				conv.SrcSchema[sourcetablename] = sourcetable
				conv.SpSchema[spannertablename] = spannertable
				break
			}
		}
	}
}

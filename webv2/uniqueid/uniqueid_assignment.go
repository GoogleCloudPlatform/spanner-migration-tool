// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package uniqueid

import (
	"strconv"

	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/schema"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
	"github.com/cloudspannerecosystem/harbourbridge/webv2/session"
)

// AssignUniqueId to handles  cascading effect in UI.
// Its iterate over source and spanner schema
// and assign id to table and column.
func AssignUniqueId(conv *internal.Conv) {

	for sourcetablename, sourcetable := range conv.SrcSchema {

		for spannertablename, spannertable := range conv.SpSchema {

			if validSpannerTableName, _ := internal.FixName(sourcetablename); validSpannerTableName == spannertablename {

				tableuniqueid := GenerateTableId()
				sourcetable.Id = tableuniqueid
				spannertable.Id = tableuniqueid

				for spannercolumnname, spannercolumn := range spannertable.ColDefs {

					if spannercolumn.Name == "synth_id" {

						columnuniqueid := GenerateColumnId()
						spannercolumn.Id = columnuniqueid
						conv.SpSchema[spannertablename].ColDefs[spannercolumnname] = spannercolumn
					}

				}

				for sourcecolumnname, sourcecolumn := range sourcetable.ColDefs {

					for spannercolumnname, spannercolumn := range spannertable.ColDefs {

						if validSpannerColumnName, _ := internal.FixName(sourcecolumn.Name); validSpannerColumnName == spannercolumn.Name {

							columnuniqueid := GenerateColumnId()
							sourcecolumn.Id = columnuniqueid
							spannercolumn.Id = columnuniqueid

							conv.SrcSchema[sourcetablename].ColDefs[sourcecolumnname] = sourcecolumn
							conv.SpSchema[spannertablename].ColDefs[spannercolumnname] = spannercolumn

							break
						}
					}

				}

				for sourceforeignkeyindex, sourceforeignkey := range sourcetable.ForeignKeys {

					for spannerforeignkeyindex, spannerforeignkey := range spannertable.ForeignKeys {

						if validSpannerFkName, _ := internal.FixName(sourceforeignkey.Name); validSpannerFkName == spannerforeignkey.Name {

							foreignkeyid := GenerateForeignkeyId()

							sourceforeignkey.Id = foreignkeyid
							spannerforeignkey.Id = foreignkeyid

							conv.SrcSchema[sourcetable.Name].ForeignKeys[sourceforeignkeyindex] = sourceforeignkey
							conv.SpSchema[spannertable.Name].ForeignKeys[spannerforeignkeyindex] = spannerforeignkey
						}

					}
				}

				for sourcei, sourceindexes := range sourcetable.Indexes {

					for ind, spannerindexes := range spannertable.Indexes {

						if validSpannerIndexName, _ := internal.FixName(sourceindexes.Name); validSpannerIndexName == spannerindexes.Name {

							indexesid := GenerateIndexesId()

							sourceindexes.Id = indexesid
							spannerindexes.Id = indexesid

							conv.SrcSchema[sourcetable.Name].Indexes[sourcei] = sourceindexes
							conv.SpSchema[spannertable.Name].Indexes[ind] = spannerindexes

						}

					}
				}

				updateSpannerTableIndexKeyOrder(spannertable)
				updateSourceTableIndexKeyOrder(sourcetable)

				updateSpannerTableSecondaryIndexKeyOrder(spannertable)
				updateSourceTableSecondaryIndexKeyOrder(sourcetable)

				conv.SrcSchema[sourcetablename] = sourcetable
				conv.SpSchema[spannertablename] = spannertable
				break
			}
		}
	}

}

// updateSpannerTableIndexKeyOrder Update Primary Key Order as columnId.
func updateSpannerTableIndexKeyOrder(spannertable ddl.CreateTable) {

	for i := 0; i < len(spannertable.PrimaryKeys); i++ {
		for spannercolumnname := range spannertable.ColDefs {
			if spannertable.PrimaryKeys[i].ColId == spannercolumnname {

				o := getSpannerColumnIndex(spannertable, spannercolumnname)
				spannertable.PrimaryKeys[i].Order = o

			}
		}
	}
}

// updateSourceTableIndexKeyOrder Update Primary Key Order as columnId.
func updateSourceTableIndexKeyOrder(sourcetable schema.Table) {

	for i := 0; i < len(sourcetable.PrimaryKeys); i++ {
		for sourcecolumnname := range sourcetable.ColDefs {
			if sourcetable.PrimaryKeys[i].ColId == sourcecolumnname {

				o := getSourceColumnIndex(sourcetable, sourcecolumnname)
				sourcetable.PrimaryKeys[i].Order = o
			}
		}
	}
}

// updateSpannerTableSecondaryIndexKeyOrder Update Secondary Index Key s Order as Inserted Order.
func updateSpannerTableSecondaryIndexKeyOrder(spannertable ddl.CreateTable) {

	for i := 0; i < len(spannertable.Indexes); i++ {

		for j := 0; j < len(spannertable.Indexes[i].Keys); j++ {

			for spannercolumnname := range spannertable.ColDefs {
				if spannertable.Indexes[i].Keys[j].ColId == spannercolumnname {

					o := getSpannerColumnIndex(spannertable, spannercolumnname)
					spannertable.Indexes[i].Keys[j].Order = o

				}
			}
		}
	}
}

// updateSourceTableSecondaryIndexKeyOrder Update Secondary Index Keys Order as Inserted Order.
func updateSourceTableSecondaryIndexKeyOrder(sourcetable schema.Table) {

	for i := 0; i < len(sourcetable.Indexes); i++ {

		for j := 0; j < len(sourcetable.Indexes[i].Keys); j++ {

			for spannercolumnname := range sourcetable.ColDefs {
				if sourcetable.Indexes[i].Keys[j].ColId == spannercolumnname {

					o := getSourceColumnIndex(sourcetable, spannercolumnname)
					sourcetable.Indexes[i].Keys[j].Order = o

				}
			}
		}
	}
}

// getSpannerColumnIndex return columnn index as Inserted Order.
func getSpannerColumnIndex(spannertable ddl.CreateTable, columnName string) int {

	for i := 0; i < len(spannertable.ColIds); i++ {
		if spannertable.ColIds[i] == columnName {
			return i + 1
		}
	}
	return 0
}

// getColumnIndex return columnn index as Inserted Order.
func getSourceColumnIndex(sourcetable schema.Table, columnName string) int {

	for i := 0; i < len(sourcetable.ColIds); i++ {
		if sourcetable.ColIds[i] == columnName {
			return i + 1
		}
	}
	return 0
}

func GenerateId() string {

	sessionState := session.GetSessionState()

	counter, _ := strconv.Atoi(sessionState.Counter.ObjectId)

	counter = counter + 1

	sessionState.Counter.ObjectId = strconv.Itoa(counter)
	return sessionState.Counter.ObjectId
}

func GenerateTableId() string {
	tablePrefix := "t"
	id := GenerateId()
	tableId := tablePrefix + id
	return tableId
}

func GenerateColumnId() string {

	columnPrefix := "c"
	id := GenerateId()
	columnId := columnPrefix + id
	return columnId
}

func GenerateForeignkeyId() string {

	foreignKeyPrefix := "f"
	id := GenerateId()
	foreignKeyId := foreignKeyPrefix + id
	return foreignKeyId
}

func GenerateIndexesId() string {

	indexesPrefix := "i"
	id := GenerateId()

	indexesId := indexesPrefix + id
	return indexesId
}

func InitObjectId() {

	sessionState := session.GetSessionState()
	sessionState.Counter.ObjectId = "0"
}

// CopyUniqueIdToSpannerTable copy ids from source table to spanner table content
func CopyUniqueIdToSpannerTable(conv *internal.Conv, spannertablename string) {
	sourcetablename := conv.ToSource[spannertablename].Name
	spannertable := conv.SpSchema[spannertablename]
	sourcetable := conv.SrcSchema[sourcetablename]
	spannertable.Id = sourcetable.Id

	for spannercolumnname, spannercolumn := range spannertable.ColDefs {
		if spannercolumn.Name == "synth_id" {
			columnuniqueid := GenerateColumnId()
			spannercolumn.Id = columnuniqueid
			spannertable.ColDefs[spannercolumnname] = spannercolumn
		}
	}

	for _, sourcecolumn := range sourcetable.ColDefs {
		for spannercolumnname, spannercolumn := range spannertable.ColDefs {
			if validSpannerColumnName, _ := internal.FixName(sourcecolumn.Name); validSpannerColumnName == spannercolumn.Name {
				spannercolumn.Id = sourcecolumn.Id
				spannertable.ColDefs[spannercolumnname] = spannercolumn
				break
			}
		}
	}

	for _, sourceforeignkey := range sourcetable.ForeignKeys {
		for spannerforeignkeyindex, spannerforeignkey := range spannertable.ForeignKeys {
			if validSpannerFkName, _ := internal.FixName(sourceforeignkey.Name); validSpannerFkName == spannerforeignkey.Name {
				spannerforeignkey.Id = sourceforeignkey.Id
				spannertable.ForeignKeys[spannerforeignkeyindex] = spannerforeignkey
			}
		}
	}

	for _, sourceindexes := range sourcetable.Indexes {
		for ind, spannerindexes := range spannertable.Indexes {
			if validSpannerIndexName, _ := internal.FixName(sourceindexes.Name); validSpannerIndexName == spannerindexes.Name {
				spannerindexes.Id = sourceindexes.Id
				spannertable.Indexes[ind] = spannerindexes
			}
		}
	}

	updateSpannerTableIndexKeyOrder(spannertable)
	conv.SpSchema[spannertablename] = spannertable
}

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

// Package ddl provides a go representation of Spanner DDL
// as well as helpers for building and manipulating Spanner DDL.
// We only implement enough DDL types to meet the needs of HarbourBridge.
//
// Definitions are from
// https://cloud.google.com/spanner/docs/data-definition-language.

package webv2

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

			if sourcetablename == spannertablename {

				tableuniqueid := generateTableId()
				sourcetable.Id = tableuniqueid
				spannertable.Id = tableuniqueid

				for sourcecolumnname, sourcecolumn := range sourcetable.ColDefs {

					for spannercolumnname, spannercolumn := range spannertable.ColDefs {

						if sourcecolumn.Name == spannercolumn.Name {

							columnuniqueid := generateColumnId()
							sourcecolumn.Id = columnuniqueid
							spannercolumn.Id = columnuniqueid

							conv.SrcSchema[sourcetablename].ColDefs[sourcecolumnname] = sourcecolumn
							conv.SpSchema[spannertablename].ColDefs[spannercolumnname] = spannercolumn

							break
						}
					}

				}

				for sourceforeignkeyindex, sourceforeignkey := range sourcetable.ForeignKeys {

					for spannerforeignkeyindex, spannerforeignkey := range spannertable.Fks {

						if sourceforeignkey.Name == spannerforeignkey.Name {

							foreignkeyid := generateForeignkeyId()

							sourceforeignkey.Id = foreignkeyid
							spannerforeignkey.Id = foreignkeyid

							conv.SrcSchema[sourcetable.Name].ForeignKeys[sourceforeignkeyindex] = sourceforeignkey
							conv.SpSchema[spannertable.Name].Fks[spannerforeignkeyindex] = spannerforeignkey
						}

					}
				}

				for sourcei, sourceindexes := range sourcetable.Indexes {

					for spanneri, spannerindexes := range spannertable.Indexes {

						if sourceindexes.Name == spannerindexes.Name {

							indexesid := generateIndexesId()

							sourceindexes.Id = indexesid
							spannerindexes.Id = indexesid

							conv.SrcSchema[sourcetable.Name].Indexes[sourcei] = sourceindexes
							conv.SpSchema[spannertable.Name].Indexes[spanneri] = spannerindexes

						}

					}
				}

				updateSpannerTableIndexKeyOrder(spannertable)
				updateSourceTableIndexKeyOrder(sourcetable)

				conv.SrcSchema[sourcetablename] = sourcetable
				conv.SpSchema[spannertablename] = spannertable
				break
			}
		}
	}

}

// updateSpannerTableIndexKeyOrder Update Primary Key Order as columnId.
func updateSpannerTableIndexKeyOrder(spannertable ddl.CreateTable) {

	for i := 0; i < len(spannertable.Pks); i++ {
		for spannercolumnname, _ := range spannertable.ColDefs {
			if spannertable.Pks[i].Col == spannercolumnname {

				o := getSpannerColumnIndex(spannertable, spannercolumnname)
				spannertable.Pks[i].Order = o

			}
		}
	}
}

// updateSourceTableIndexKeyOrder Update Primary Key Order as columnId.
func updateSourceTableIndexKeyOrder(sourcetable schema.Table) {

	for i := 0; i < len(sourcetable.PrimaryKeys); i++ {
		for sourcecolumnname, _ := range sourcetable.ColDefs {
			if sourcetable.PrimaryKeys[i].Column == sourcecolumnname {

				o := getSourceColumnIndex(sourcetable, sourcecolumnname)
				sourcetable.PrimaryKeys[i].Order = o
			}
		}
	}
}

// getSpannerColumnIndex return columnn index as Inserted Order.
func getSpannerColumnIndex(spannertable ddl.CreateTable, columnName string) int {

	for i := 0; i < len(spannertable.ColNames); i++ {
		if spannertable.ColNames[i] == columnName {
			return i + 1
		}
	}
	return 0
}

// getColumnIndex return columnn index as Inserted Order.
func getSourceColumnIndex(sourcetable schema.Table, columnName string) int {

	for i := 0; i < len(sourcetable.ColNames); i++ {
		if sourcetable.ColNames[i] == columnName {
			return i + 1
		}
	}
	return 0
}

func generateId() string {

	sessionState := session.GetSessionState()

	counter, _ := strconv.Atoi(sessionState.Counter.ObjectId)

	counter = counter + 1

	sessionState.Counter.ObjectId = strconv.Itoa(counter)
	return sessionState.Counter.ObjectId
}

func generateTableId() string {
	tablePrefix := "t"
	id := generateId()
	tableId := tablePrefix + id
	return tableId
}

func generateColumnId() string {

	columnPrefix := "c"
	id := generateId()
	columnId := columnPrefix + id
	return columnId
}

func generateForeignkeyId() string {

	foreignKeyPrefix := "f"
	id := generateId()
	foreignKeyId := foreignKeyPrefix + id
	return foreignKeyId
}

func generateIndexesId() string {

	indexesPrefix := "i"
	id := generateId()

	indexesId := indexesPrefix + id
	return indexesId
}

func InitObjectId() {

	sessionState := session.GetSessionState()
	sessionState.Counter.ObjectId = "0"
}

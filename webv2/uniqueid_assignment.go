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
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/schema"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
)

// AssignUniqueId to handles  cascading effect in UI.
// Its iterate over source and spanner schema
// and assign id to table and column.
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

				for sourcecolumnname, sourcecolumn := range sourcetable.ColDefs {

					for spannercolumnname, spannercolumn := range spannertable.ColDefs {

						if sourcecolumn.Name == spannercolumn.Name {

							index := getColumnIndex(spannertable, spannercolumn.Name)

							sourcecolumn.Id = index
							spannercolumn.Id = index

							conv.SrcSchema[sourcetablename].ColDefs[sourcecolumnname] = sourcecolumn
							conv.SpSchema[spannertablename].ColDefs[spannercolumnname] = spannercolumn

							updateSpannerTableIndexKeyOrder(spannertable)
							updateSourceTableIndexKeyOrder(sourcetable)
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

// updateSpannerTableIndexKeyOrder Update Primary Key Order as columnId.
func updateSpannerTableIndexKeyOrder(spannertable ddl.CreateTable) {

	for i := 0; i < len(spannertable.Pks); i++ {
		for spannercolumnname, spannercolumn := range spannertable.ColDefs {
			if spannertable.Pks[i].Col == spannercolumnname {
				spannertable.Pks[i].Order = spannercolumn.Id
			}
		}
	}
}

// updateSourceTableIndexKeyOrder Update Primary Key Order as columnId.
func updateSourceTableIndexKeyOrder(sourcetable schema.Table) {

	for i := 0; i < len(sourcetable.PrimaryKeys); i++ {
		for sourcecolumnname, spannercolumn := range sourcetable.ColDefs {
			if sourcetable.PrimaryKeys[i].Column == sourcecolumnname {
				sourcetable.PrimaryKeys[i].Order = spannercolumn.Id
			}
		}
	}
}

// getColumnIndex return columnn index as Inserted Order.
func getColumnIndex(spannertable ddl.CreateTable, columnName string) int {

	for i := 0; i < len(spannertable.ColNames); i++ {
		if spannertable.ColNames[i] == columnName {
			return i + 1
		}
	}
	return 0
}

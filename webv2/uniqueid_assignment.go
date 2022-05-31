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
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
)

// AssignUniqueId to handle  cascading effect in UI.

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
							UpdateIndexKeyOrder(spannertable)
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

func UpdateIndexKeyOrder(spannertable ddl.CreateTable) {

	for i := 0; i < len(spannertable.Pks); i++ {
		for spannercolumnname, spannercolumn := range spannertable.ColDefs {
			if spannertable.Pks[i].Col == spannercolumnname {
				spannertable.Pks[i].Order = spannercolumn.Id
			}
		}
	}
}

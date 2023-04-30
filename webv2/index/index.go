// Copyright 2022 Google LLC
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

package index

import (
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
	"github.com/cloudspannerecosystem/harbourbridge/webv2/session"
	utilities "github.com/cloudspannerecosystem/harbourbridge/webv2/utilities"
)

// IndexSuggestion adds redundant index issue and interleved index suggestion in issues and suggestions tab.
func IndexSuggestion() {

	sessionState := session.GetSessionState()

	for _, spannerTable := range sessionState.Conv.SpSchema {
		CheckIndexSuggestion(spannerTable.Indexes, spannerTable)
	}
}

func AssignInitialOrders() {
	sessionState := session.GetSessionState()
	conv := sessionState.Conv

	for _, spannerTable := range conv.SpSchema {
		for _, index := range spannerTable.Indexes {
			order := 1
			for i, key := range index.Keys {
				key.Order = order
				index.Keys[i] = key
				order = order + 1

			}
		}
	}
	sessionState.Conv = conv
}

// Helper method for checking Index Suggestion.
func CheckIndexSuggestion(index []ddl.CreateIndex, spannerTable ddl.CreateTable) {

	checkRedundantIndex(index, spannerTable)
	checkInterleaveIndex(index, spannerTable)
}

// redundantIndex check for redundant Index.
// If present adds Redundant as an issue in Issues.
func checkRedundantIndex(index []ddl.CreateIndex, spannerTable ddl.CreateTable) {

	var primaryKeyFirstColumnId string
	pks := spannerTable.PrimaryKeys

	for i := 0; i < len(index); i++ {

		for i := range pks {
			if pks[i].Order == 1 {
				primaryKeyFirstColumnId = pks[i].ColId
				break
			}
		}
		if len(index[i].Keys) > 0 {
			indexFirstColumnId := index[i].Keys[0].ColId

			if primaryKeyFirstColumnId == indexFirstColumnId {
				columnId := indexFirstColumnId
				sessionState := session.GetSessionState()
				schemaissue := sessionState.Conv.SchemaIssues[spannerTable.Id].ColumnLevelIssues[columnId]
				schemaissue = append(schemaissue, internal.RedundantIndex)
				sessionState.Conv.SchemaIssues[spannerTable.Id].ColumnLevelIssues[columnId] = schemaissue
			}
		}
	}
}

// interleaveIndex suggests if an index can be converted to interleave.
// If possible it gets added as a suggestion.
func checkInterleaveIndex(index []ddl.CreateIndex, spannerTable ddl.CreateTable) {

	// Suggestion gets added only if the table can be interleaved.
	isInterleavable := spannerTable.ParentId != ""

	if isInterleavable {

		var primaryKeyFirstColumnId string
		pks := spannerTable.PrimaryKeys

		for i := 0; i < len(index); i++ {

			for i := range pks {
				if pks[i].Order == 1 {
					primaryKeyFirstColumnId = pks[i].ColId
					break
				}
			}
			if len(index[i].Keys) > 0 {
				indexFirstColumnId := index[i].Keys[0].ColId

				sessionState := session.GetSessionState()

				// Ensuring it is not a redundant index.
				if primaryKeyFirstColumnId != indexFirstColumnId {

					schemaissue := sessionState.Conv.SchemaIssues[spannerTable.Id].ColumnLevelIssues[indexFirstColumnId]
					fks := spannerTable.ForeignKeys

					for i := range fks {
						if fks[i].ColIds[0] == indexFirstColumnId {
							schemaissue = append(schemaissue, internal.InterleaveIndex)
							sessionState.Conv.SchemaIssues[spannerTable.Id].ColumnLevelIssues[indexFirstColumnId] = schemaissue

						}
					}

					// Interleave suggestion if the column is of type auto increment.
					if utilities.IsSchemaIssuePresent(schemaissue, internal.AutoIncrement) {
						schemaissue = append(schemaissue, internal.AutoIncrementIndex)
						sessionState.Conv.SchemaIssues[spannerTable.Id].ColumnLevelIssues[indexFirstColumnId] = schemaissue
					}

					for _, c := range spannerTable.ColDefs {

						if indexFirstColumnId == c.Id {

							if c.T.Name == ddl.Timestamp {

								columnId := c.Id
								sessionState := session.GetSessionState()
								schemaissue := sessionState.Conv.SchemaIssues[spannerTable.Id].ColumnLevelIssues[columnId]

								schemaissue = append(schemaissue, internal.AutoIncrementIndex)
								sessionState.Conv.SchemaIssues[spannerTable.Id].ColumnLevelIssues[columnId] = schemaissue
							}
						}
					}
				}
			}

		}

	}
}

// RemoveIndexIssues removes the issues in a column which is part of the passed Index.
// This is called when we drop an index or make changes in the primarykey of the current table.
// Editing the primary key can affect the issues in an index (eg. Changing pk order affects Redundant index issue).
func RemoveIndexIssues(tableId string, Index ddl.CreateIndex) {

	for i := 0; i < len(Index.Keys); i++ {

		columnId := Index.Keys[i].ColId

		{
			schemaissue := []internal.SchemaIssue{}
			sessionState := session.GetSessionState()
			if sessionState.Conv.SchemaIssues != nil {
				schemaissue = sessionState.Conv.SchemaIssues[tableId].ColumnLevelIssues[columnId]
			}

			if len(schemaissue) > 0 {

				schemaissue = removeColumnIssue(schemaissue)

				if sessionState.Conv.SchemaIssues[tableId].ColumnLevelIssues[columnId] == nil {

					s := map[string][]internal.SchemaIssue{
						columnId: schemaissue,
					}
					sessionState.Conv.SchemaIssues = map[string]internal.TableIssues{}

					sessionState.Conv.SchemaIssues[tableId] = internal.TableIssues{
						ColumnLevelIssues: s,
					}

				} else {

					sessionState.Conv.SchemaIssues[tableId].ColumnLevelIssues[columnId] = schemaissue

				}
			}
		}
	}
}

func removeColumnIssue(schemaissue []internal.SchemaIssue) []internal.SchemaIssue {

	if utilities.IsSchemaIssuePresent(schemaissue, internal.RedundantIndex) {
		schemaissue = utilities.RemoveSchemaIssue(schemaissue, internal.RedundantIndex)
	}

	if utilities.IsSchemaIssuePresent(schemaissue, internal.InterleaveIndex) {
		schemaissue = utilities.RemoveSchemaIssue(schemaissue, internal.InterleaveIndex)
	}

	if utilities.IsSchemaIssuePresent(schemaissue, internal.AutoIncrementIndex) {
		schemaissue = utilities.RemoveSchemaIssue(schemaissue, internal.AutoIncrementIndex)
	}

	return schemaissue
}

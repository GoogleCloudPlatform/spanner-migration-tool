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

// Helper method for checking Index Suggestion.
func CheckIndexSuggestion(index []ddl.CreateIndex, spannerTable ddl.CreateTable) {

	checkRedundantIndex(index, spannerTable)
	checkInterleaveIndex(index, spannerTable)
}

// redundantIndex check for redundant Index.
// If present adds Redundant as an issue in Issues.
func checkRedundantIndex(index []ddl.CreateIndex, spannerTable ddl.CreateTable) {

	var primaryKeyFirstColumn string
	pks := spannerTable.Pks

	for i := 0; i < len(index); i++ {

		keys := index[i].Keys

		for i := range pks {
			if pks[i].Order == 1 {
				primaryKeyFirstColumn = pks[i].Col
				break
			}
		}

		indexFirstColumn := index[i].Keys[0].Col

		if primaryKeyFirstColumn == indexFirstColumn {
			columnname := keys[i].Col
			sessionState := session.GetSessionState()
			schemaissue := sessionState.Conv.Issues[spannerTable.Name][columnname]
			schemaissue = append(schemaissue, internal.RedundantIndex)
			sessionState.Conv.Issues[spannerTable.Name][columnname] = schemaissue
		}

	}
}

// interleaveIndex suggests if an index can be converted to interleave.
// If possible it gets added as a suggestion.
func checkInterleaveIndex(index []ddl.CreateIndex, spannerTable ddl.CreateTable) {

	// Suggestion gets added only if the table can be interleaved.
	isInterleavable := spannerTable.Parent != ""

	if isInterleavable {

		var primaryKeyFirstColumn string
		pks := spannerTable.Pks

		for i := 0; i < len(index); i++ {

			for i := range pks {
				if pks[i].Order == 1 {
					primaryKeyFirstColumn = pks[i].Col
					break
				}
			}

			indexFirstColumn := index[i].Keys[0].Col

			sessionState := session.GetSessionState()

			// Ensuring it is not a redundant index.
			if primaryKeyFirstColumn != indexFirstColumn {

				schemaissue := sessionState.Conv.Issues[spannerTable.Name][indexFirstColumn]
				fks := spannerTable.Fks

				for i := range fks {
					if fks[i].Columns[0] == indexFirstColumn {
						schemaissue = append(schemaissue, internal.InterleaveIndex)
						sessionState.Conv.Issues[spannerTable.Name][indexFirstColumn] = schemaissue

					}
				}

				// Interleave suggestion if the column is of type auto increment.
				if utilities.IsSchemaIssuePresent(schemaissue, internal.AutoIncrement) {
					schemaissue = append(schemaissue, internal.AutoIncrementIndex)
					sessionState.Conv.Issues[spannerTable.Name][indexFirstColumn] = schemaissue
				}

				for _, c := range spannerTable.ColDefs {

					if indexFirstColumn == c.Name {

						if c.T.Name == ddl.Timestamp {

							columnname := c.Name
							sessionState := session.GetSessionState()
							schemaissue := sessionState.Conv.Issues[spannerTable.Name][columnname]

							schemaissue = append(schemaissue, internal.AutoIncrementIndex)
							sessionState.Conv.Issues[spannerTable.Name][columnname] = schemaissue
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
func RemoveIndexIssues(table string, Index ddl.CreateIndex) {

	for i := 0; i < len(Index.Keys); i++ {

		column := Index.Keys[i].Col

		{
			schemaissue := []internal.SchemaIssue{}
			sessionState := session.GetSessionState()
			if sessionState.Conv.Issues != nil {
				schemaissue = sessionState.Conv.Issues[table][column]
			}

			if len(schemaissue) > 0 {

				schemaissue = removeColumnIssue(schemaissue)

				if sessionState.Conv.Issues[table][column] == nil {

					s := map[string][]internal.SchemaIssue{
						column: schemaissue,
					}
					sessionState.Conv.Issues = map[string]map[string][]internal.SchemaIssue{}

					sessionState.Conv.Issues[table] = s

				} else {

					sessionState.Conv.Issues[table][column] = schemaissue

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

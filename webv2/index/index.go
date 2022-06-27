package index

import (
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
	helpers "github.com/cloudspannerecosystem/harbourbridge/webv2/helpers"
	"github.com/cloudspannerecosystem/harbourbridge/webv2/session"
)

// IndexSuggestion adds redundant index and interleved index  suggestion in schema conversion process for database.
func IndexSuggestion() {

	sessionState := session.GetSessionState()

	for _, spannerTable := range sessionState.Conv.SpSchema {

		CheckIndexSuggestion(spannerTable.Indexes, spannerTable)
	}
}

//Helpers method for checking Index Suggestion.
func CheckIndexSuggestion(index []ddl.CreateIndex, spannerTable ddl.CreateTable) {

	redundantIndex(index, spannerTable)
}

// redundantIndex check for redundant Index.
// If present adds IndexRedandant as an issue in Issues.
func redundantIndex(index []ddl.CreateIndex, spannerTable ddl.CreateTable) {

	for i := 0; i < len(index); i++ {

		keys := index[i].Keys

		for i := 0; i < len(keys); i++ {

			for _, c := range spannerTable.Pks {

				if keys[i].Col == c.Col {

					columnname := keys[i].Col
					sessionState := session.GetSessionState()
					schemaissue := sessionState.Conv.Issues[spannerTable.Name][columnname]

					schemaissue = append(schemaissue, internal.IndexRedandant)

					sessionState.Conv.Issues[spannerTable.Name][columnname] = schemaissue

				}
			}

		}

	}
}

// RemoveIndexIssues remove all  index suggestion from given list.
// RemoveSchemaIssues is used when we are  removing index.
func RemoveIndexIssues(table string, Index ddl.CreateIndex) {

	for i := 0; i < len(Index.Keys); i++ {

		column := Index.Keys[i].Col

		{
			schemaissue := []internal.SchemaIssue{}
			sessionState := session.GetSessionState()
			schemaissue = sessionState.Conv.Issues[table][column]

			if len(schemaissue) > 0 {

				schemaissue = RemoveIndexIssue(schemaissue)

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

// RemoveSchemaIssue removes issue from the schemaissue list.
func RemoveIndexIssue(schemaissue []internal.SchemaIssue) []internal.SchemaIssue {

	switch {

	case helpers.IsSchemaIssuePrsent(schemaissue, internal.IndexRedandant):
		schemaissue = helpers.RemoveSchemaIssue(schemaissue, internal.IndexRedandant)
	}

	return schemaissue
}

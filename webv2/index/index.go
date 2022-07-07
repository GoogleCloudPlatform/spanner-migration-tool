package index

import (
	"fmt"

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
	interleaveIndex(index, spannerTable)
}

// redundantIndex check for redundant Index.
// If present adds IndexRedandant as an issue in Issues.
func redundantIndex(index []ddl.CreateIndex, spannerTable ddl.CreateTable) {

	for i := 0; i < len(index); i++ {

		keys := index[i].Keys
		var primaryKeyFirstColumn string
		pks := spannerTable.Pks

		for i := range pks {
			if pks[i].Order == 1 {
				primaryKeyFirstColumn = pks[i].Col
			}
		}

		indexFirstColumn := index[i].Keys[0].Col

		if primaryKeyFirstColumn == indexFirstColumn {
			columnname := keys[i].Col
			sessionState := session.GetSessionState()
			schemaissue := sessionState.Conv.Issues[spannerTable.Name][columnname]
			schemaissue = append(schemaissue, internal.IndexRedandant)
			sessionState.Conv.Issues[spannerTable.Name][columnname] = schemaissue
		}

	}
}

// interleaveIndex suggests if an index can be converted to interleave.
// If possible it gets added as a suggestion.
func interleaveIndex(index []ddl.CreateIndex, spannerTable ddl.CreateTable) {

	//Suggestion gets added only if the table can be interleaved.
	isInterleavable := spannerTable.Parent != ""

	if isInterleavable {

		for i := 0; i < len(index); i++ {
			var primaryKeyFirstColumn string
			pks := spannerTable.Pks
			for i := range pks {
				if pks[i].Order == 1 {
					primaryKeyFirstColumn = pks[i].Col
				}
			}

			indexFirstColumn := index[i].Keys[0].Col

			sessionState := session.GetSessionState()

			//Ensuring it is not a redundant index.
			if primaryKeyFirstColumn != indexFirstColumn {

				schemaissue := sessionState.Conv.Issues[spannerTable.Name][indexFirstColumn]
				fks := spannerTable.Fks

				for i := range fks {
					if fks[i].Columns[0] == indexFirstColumn {
						schemaissue = append(schemaissue, internal.InterleaveIndex)
						sessionState.Conv.Issues[spannerTable.Name][indexFirstColumn] = schemaissue

					}
				}

				//Interleave suggestion if the column is of type auto increment.
				if helpers.IsSchemaIssuePresent(schemaissue, internal.AutoIncrement) {
					schemaissue = append(schemaissue, internal.HotspotIndex)
					sessionState.Conv.Issues[spannerTable.Name][indexFirstColumn] = schemaissue
				}

				for _, c := range spannerTable.ColDefs {

					if indexFirstColumn == c.Name {

						if c.T.Name == ddl.Timestamp {

							columnname := c.Name
							sessionState := session.GetSessionState()
							schemaissue := sessionState.Conv.Issues[spannerTable.Name][columnname]

							schemaissue = append(schemaissue, internal.HotspotIndex)
							sessionState.Conv.Issues[spannerTable.Name][columnname] = schemaissue
						}

					}
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

	if helpers.IsSchemaIssuePresent(schemaissue, internal.IndexRedandant) {
		schemaissue = helpers.RemoveSchemaIssue(schemaissue, internal.IndexRedandant)
	}

	if helpers.IsSchemaIssuePresent(schemaissue, internal.InterleaveIndex) {
		schemaissue = helpers.RemoveSchemaIssue(schemaissue, internal.InterleaveIndex)
	}

	if helpers.IsSchemaIssuePresent(schemaissue, internal.HotspotIndex) {
		schemaissue = helpers.RemoveSchemaIssue(schemaissue, internal.HotspotIndex)
	}

	return schemaissue
}

// to detect timestamp index
// todo : add or remove suggestion based on timestamp index added or remove.
func TimestampIndex(index []ddl.CreateIndex, spannerTable ddl.CreateTable) {

	fmt.Println("TimestampIndex for ", spannerTable.Name)
	for i := 0; i < len(index); i++ {

		keys := index[i].Keys

		fmt.Println("Keys :", keys)

		for i := 0; i < len(keys); i++ {

			for _, c := range spannerTable.ColDefs {

				if keys[i].Col == c.Name {

					if c.T.Name == ddl.String {

						fmt.Println("TimestampIndex :", spannerTable.Name, c.Name)

						columnname := keys[i].Col
						sessionState := session.GetSessionState()
						schemaissue := sessionState.Conv.Issues[spannerTable.Name][columnname]

						schemaissue = append(schemaissue, internal.INDEX_TIMESTAMP)
						sessionState.Conv.Issues[spannerTable.Name][columnname] = schemaissue
					}

				}
			}

		}

	}
}

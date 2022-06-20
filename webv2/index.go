package webv2

import (
	"fmt"

	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
	helpers "github.com/cloudspannerecosystem/harbourbridge/webv2/helpers"
	"github.com/cloudspannerecosystem/harbourbridge/webv2/session"
)

func SuggestIndex() {

	sessionState := session.GetSessionState()

	for _, spannerTable := range sessionState.Conv.SpSchema {

		RedenundantIndex(spannerTable.Indexes, spannerTable)
	}
}

func DetectredandantIndex(index []ddl.CreateIndex, spannerTable ddl.CreateTable) {

	RedenundantIndex(index, spannerTable)
}

func RedenundantIndex(index []ddl.CreateIndex, spannerTable ddl.CreateTable) {

	for i := 0; i < len(index); i++ {

		keys := index[i].Keys

		for i := 0; i < len(keys); i++ {

			for _, c := range spannerTable.Pks {

				if keys[i].Col == c.Col {

					fmt.Println("RedenundantIndex :", spannerTable.Name, c.Col)

					columnname := keys[i].Col
					sessionState := session.GetSessionState()
					schemaissue := sessionState.Conv.Issues[spannerTable.Name][columnname]

					schemaissue = append(schemaissue, internal.IndexRedandant)
					fmt.Println("I am adding index IndexRedandant ", internal.IndexRedandant)
					sessionState.Conv.Issues[spannerTable.Name][columnname] = schemaissue

				}
			}

		}

	}
}

func RemoveIndexIssue(table string, Index ddl.CreateIndex) {

	for i := 0; i < len(Index.Keys); i++ {

		column := Index.Keys[i].Col

		schemaissue := []internal.SchemaIssue{}
		sessionState := session.GetSessionState()
		schemaissue = sessionState.Conv.Issues[table][column]

		if len(schemaissue) > 0 {

			schemaissue = RemoveIndexIssues(schemaissue)

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

//RemoveIndexIssues
func RemoveIndexIssues(schemaissue []internal.SchemaIssue) []internal.SchemaIssue {

	switch {

	case helpers.IsSchemaIssuePrsent(schemaissue, internal.IndexRedandant):
		fmt.Println("I am removing : IndexRedandant", internal.IndexRedandant)
		schemaissue = helpers.RemoveSchemaIssue(schemaissue, internal.IndexRedandant)
	}

	return schemaissue
}

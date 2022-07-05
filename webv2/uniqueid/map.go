package uniqueid

// AssignUniqueId to handles  cascading effect in UI.
// Its iterate over source and spanner schema
// and assign id to table and column.

import (
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/webv2/session"
)

func UpdateConvViewModel() {

	cvm := internal.MakeConvViewModel()

	sessionState := session.GetSessionState()

	cvm.Conv = sessionState.Conv

	for _, spannertable := range cvm.Conv.SpSchema {

		cvm.SpSchemaMap[spannertable.Id] = spannertable

		for _, spannercolumn := range spannertable.ColDefs {

			cvm.SpSchemaMap[spannertable.Id].ColDefs[spannercolumn.Id] = spannercolumn
		}
	}

	for _, sourcetable := range cvm.Conv.SrcSchema {

		cvm.SrcSchemaMap[sourcetable.Id] = sourcetable

		for _, sourcecolumn := range sourcetable.ColDefs {

			cvm.SrcSchemaMap[sourcetable.Id].ColDefs[sourcecolumn.Id] = sourcecolumn
		}
	}

	sessionState.ConvViewModel = cvm

}

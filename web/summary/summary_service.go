package summary

import (
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/web/session"
)

// getSummary returns table wise summary of conversion.
func getSummary() map[string]ConversionSummary {
	sessionState := session.GetSessionState()
	reports := internal.AnalyzeTables(sessionState.Conv, nil)

	summary := make(map[string]ConversionSummary)
	for _, t := range reports {
		cs := ConversionSummary{
			SrcTable: t.SrcTable,
			SpTable:  t.SpTable,
		}
		for _, x := range t.Body {
			switch x.Heading {
			case "Note", "Notes":
				{
					cs.Notes = x.Lines
					cs.NotesCount = len(x.Lines)
				}
			case "Warning", "Warnings":
				{
					cs.Warnings = x.Lines
					cs.WarningsCount = len(x.Lines)
				}
			}
			summary[t.SrcTable] = cs
		}
	}
	return summary
}

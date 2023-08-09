package summary

import (
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal/reports"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/session"
)

// getSummary returns table wise summary of conversion.
func getSummary() map[string]ConversionSummary {
	sessionState := session.GetSessionState()
	sessionState.Conv.ConvLock.Lock()
	defer sessionState.Conv.ConvLock.Unlock()
	reports := reports.AnalyzeTables(sessionState.Conv, nil)

	summary := make(map[string]ConversionSummary)
	for _, t := range reports {
		cs := ConversionSummary{
			SrcTable:    t.SrcTable,
			SpTable:     t.SpTable,
			Notes:       []string{},
			Warnings:    []string{},
			Errors:      []string{},
			Suggestions: []string{},
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
			case "Error", "Errors":
				{
					cs.Errors = x.Lines
					cs.ErrorsCount = len(x.Lines)
				}
			case "Suggestion", "Suggestions":
				{
					cs.Suggestions = x.Lines
					cs.SuggestionsCount = len(x.Lines)
				}
			}
			summary[t.SpTable] = cs
		}
	}
	return summary
}

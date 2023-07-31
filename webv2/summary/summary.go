package summary

import (
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal/reports"
)

type ConversionSummary struct {
	SrcTable         string
	SpTable          string
	Errors           []reports.Issue
	Warnings         []reports.Issue
	Suggestions      []reports.Issue
	Notes            []reports.Issue
	ErrorsCount      int
	WarningsCount    int
	SuggestionsCount int
	NotesCount       int
}

package summary

import (
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal/reports"
)

type ConversionSummary struct {
	SrcTable         string
	SpTable          string
	Errors           []reports.IssueClassified
	Warnings         []reports.IssueClassified
	Suggestions      []reports.IssueClassified
	Notes            []reports.IssueClassified
	ErrorsCount      int
	WarningsCount    int
	SuggestionsCount int
	NotesCount       int
}

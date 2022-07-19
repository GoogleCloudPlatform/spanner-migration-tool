package summary

type ConversionSummary struct {
	SrcTable         string
	SpTable          string
	Errors           []string
	Warnings         []string
	Suggestions      []string
	Notes            []string
	ErrorsCount      int
	WarningsCount    int
	SuggestionsCount int
	NotesCount       int
}

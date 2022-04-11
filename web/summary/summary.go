package summary

type ConversionSummary struct {
	SrcTable      string
	SpTable       string
	Warnings      []string
	Notes         []string
	WarningsCount int
	NotesCount    int
}

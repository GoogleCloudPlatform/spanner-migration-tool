package reports

import (
	"bufio"
	"time"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
)

//report_helpers.go contains helpers methods to calculate the various elements of a report.
//Calculation of new elements should go here.

type tableReport struct {
	SrcTable      string
	SpTable       string
	rows          int64
	badRows       int64
	Cols          int64
	Warnings      int64
	Errors        int64
	SyntheticPKey string // Empty string means no synthetic primary key was needed.
	Body          []tableReportBody
}

type tableReportBody struct {
	Heading   string
	IssueBody []Issue
}

type Summary struct {
	Text   string `json:"text"`
	Rating string `json:"rating"`
	DbName string `json:"dbName"`
}

type IgnoredStatement struct {
	StatementType string `json:"statementType"`
	Statement     string `json:"statement"`
}

type ConversionMetadata struct {
	ConversionType string        `json:"conversionType"`
	Duration       time.Duration `json:"duration"`
}

type StatementStat struct {
	Statement  string `json:"statement"`
	Schema     int64  `json:"schema"`
	Data       int64  `json:"data"`
	Skip       int64  `json:"skip"`
	Error      int64  `json:"error"`
	TotalCount int64  `json:"totalCount"`
}

type StatementStats struct {
	DriverName     string          `json:"driverName"`
	StatementStats []StatementStat `json:"statementStats"`
}

type NameChange struct {
	NameChangeType string `json:"nameChangeType"`
	SourceTable    string `json:"sourceTable"`
	OldName        string `json:"oldName"`
	NewName        string `json:"newName"`
}

type Issues struct {
	IssueType string  `json:"issueType"`
	IssueList []Issue `json:"issueList"`
}

type Issue struct {
	Category    string `json:"category"`
	Description string `json:"description"`
}

type SchemaReport struct {
	Rating       string `json:"rating"`
	PkMissing    bool   `json:"pkMissing"`
	Issues       int64  `json:"issues"`
	Warnings     int64  `json:"warnings"`
	TotalColumns int64  `json:"totalColumns"`
}

type DataReport struct {
	Rating    string `json:"rating"`
	BadRows   int64  `json:"badRows"`
	TotalRows int64  `json:"totalRows"`
	DryRun    bool   `json:"dryRun"`
}

type TableReport struct {
	SrcTableName string       `json:"srcTableName"`
	SpTableName  string       `json:"spTableName"`
	SchemaReport SchemaReport `json:"schemaReport"`
	DataReport   DataReport   `json:"dataReport"`
	Issues       []Issues     `json:"issues"`
}

type UnexpectedCondition struct {
	Count     int64  `json:"count"`
	Condition string `json:"condition"`
}

type UnexpectedConditions struct {
	Reparsed             int64
	UnexpectedConditions []UnexpectedCondition `json:"unexpectedConditions"`
}

type StructuredReport struct {
	Summary              Summary              `json:"summary"`
	IsSharded            bool                 `json:"isSharded"`
	IgnoredStatements    []IgnoredStatement   `json:"ignoredStatements"`
	ConversionMetadata   []ConversionMetadata `json:"conversionMetadata"`
	MigrationType        string               `json:"migrationType"`
	StatementStats       StatementStats       `json:"statementStats"`
	NameChanges          []NameChange         `json:"nameChanges"`
	TableReports         []TableReport        `json:"tableReports"`
	UnexpectedConditions UnexpectedConditions `json:"unexpectedConditions"`
	SchemaOnly           bool                 `json:"-"`
}

type ReportInterface interface {
	GenerateStructuredReport(driverName string, dbName string, conv *internal.Conv, badWrites map[string]int64, printTableReports bool, printUnexpecteds bool) StructuredReport
	GenerateTextReport(structuredReport StructuredReport, w *bufio.Writer)
}

type ReportImpl struct {}
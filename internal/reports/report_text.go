package reports

import (
	"bufio"
	"fmt"
	"sort"
	"strings"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
)

//report_text.go contains the logic to convert a structured spanner migration tool 
//report to a human readable text report.
// The structure of the report created is present in (internal/reports/REPORT.md)
// A sample report can be found in (test_data/mysql_text_report.txt)
func GenerateTextReport(structuredReport StructuredReport, w *bufio.Writer) {
	writeHeading(w, "Summary of Conversion")
	w.WriteString(structuredReport.Summary.Text)
	w.WriteString("\n")
	w.WriteString(writeConversionMetadata(structuredReport.ConversionMetadata, w))
	if len(structuredReport.IgnoredStatements) > 0 {
		justifyLines(w, fmt.Sprintf("Note that the following source DB statements "+
			"were detected but ignored: %s.",
			strings.Join(getStatementsFromIgnoredStatements(structuredReport.IgnoredStatements), ", ")), 80, 0)
		w.WriteString("\n\n")
	}
	statementsMsg := ""
	var isDump bool
	if strings.Contains(structuredReport.StatementStats.DriverName, "dump") {
		isDump = true
	}
	if isDump {
		statementsMsg = "stats on the " + structuredReport.StatementStats.DriverName + " statements processed, followed by "
	}
	justifyLines(w, "The remainder of this report provides "+statementsMsg+
		"a table-by-table listing of "+structuredReport.MigrationType+" conversion details. "+
		"For background on the "+structuredReport.MigrationType+" conversion process used, "+
		"and explanations of the terms and notes used in this "+
		"report, see Spanner migration tool's README.", 80, 0)
	w.WriteString("\n\n")
	if isDump {
		writeStatementStats(structuredReport, w)
	}
	writeNameChanges(structuredReport, w)
	writeTableReports(structuredReport, w)
	writeUnexpectedConditionsv2(structuredReport, w)

}

func writeUnexpectedConditionsv2(structuredReport StructuredReport, w *bufio.Writer) {
	reparseInfo := func() {
		if structuredReport.UnexpectedConditions.Reparsed > 0 {
			fmt.Fprintf(w, "Note: there were %d %s reparse events while looking for statement boundaries.\n\n", structuredReport.UnexpectedConditions.Reparsed, structuredReport.StatementStats.DriverName)
		}
	}
	writeHeading(w, "Unexpected Conditions")
	if len(structuredReport.UnexpectedConditions.UnexpectedConditions) == 0 {
		w.WriteString("There were no unexpected conditions encountered during processing.\n\n")
		reparseInfo()
		return
	}
	switch structuredReport.StatementStats.DriverName {
	case constants.MYSQLDUMP:
		w.WriteString("For debugging only. This section provides details of unexpected conditions\n")
		w.WriteString("encountered as we processed the mysqldump data. In particular, the AST node\n")
		w.WriteString("representation used by the pingcap/tidb/parser library used for parsing\n")
		w.WriteString("mysqldump output is highly permissive: almost any construct can appear at\n")
		w.WriteString("any node in the AST tree. The list details all unexpected nodes and\n")
		w.WriteString("conditions.\n")
	case constants.PGDUMP:
		w.WriteString("For debugging only. This section provides details of unexpected conditions\n")
		w.WriteString("encountered as we processed the pg_dump data. In particular, the AST node\n")
		w.WriteString("representation used by the pganalyze/pg_query_go library used for parsing\n")
		w.WriteString("pg_dump output is highly permissive: almost any construct can appear at\n")
		w.WriteString("any node in the AST tree. The list details all unexpected nodes and\n")
		w.WriteString("conditions.\n")
	default:
		w.WriteString("For debugging only. This section provides details of unexpected conditions\n")
		w.WriteString("encountered as we processed the " + structuredReport.StatementStats.DriverName + " data. The list details\n")
		w.WriteString("all unexpected conditions\n")
	}
	w.WriteString("  --------------------------------------\n")
	fmt.Fprintf(w, "  %6s  %s\n", "count", "condition")
	w.WriteString("  --------------------------------------\n")
	for _, cond := range structuredReport.UnexpectedConditions.UnexpectedConditions {
		fmt.Fprintf(w, "  %6d  %s\n", cond.Count, cond.Condition)
	}
	w.WriteString("\n")
	reparseInfo()
}

// Generates table by table report from the structured report in a text based format.
// This looks like the following -
// ----------------------------
// Table no_pk
// ----------------------------
// Schema conversion: POOR (67% of 3 columns mapped cleanly) + missing primary key.
// Data conversion: POOR (60% of 5000 rows written to Spanner).

// Warnings
// 1) Column 'synth_id' was added because this table didn't have a primary key.
//    Spanner requires a primary key for every table.
// 2) Some columns will consume more storage in Spanner e.g. for column 'b', source
//    DB type int(11) is mapped to Spanner data type int64.
func writeTableReports(structuredReport StructuredReport, w *bufio.Writer) {
	for _, tableReport := range structuredReport.TableReports {
		h := fmt.Sprintf("Table %s", tableReport.SrcTableName)
		if tableReport.SrcTableName != tableReport.SpTableName {
			h = h + fmt.Sprintf(" (mapped to Spanner table %s)", tableReport.SpTableName)
		}
		writeHeading(w, h)
		rate := ""
		if structuredReport.MigrationType == "SCHEMA" || structuredReport.MigrationType == "SCHEMA_AND_DATA" {
			schemaRatingText := ""
			pkMsg := " missing primary key"
			s := fmt.Sprintf(" (%s%% of %d columns mapped cleanly)", pct(tableReport.SchemaReport.TotalColumns, tableReport.SchemaReport.Warnings), tableReport.SchemaReport.TotalColumns)
			schemaRatingText = schemaRatingText + tableReport.SchemaReport.Rating + s
			if tableReport.SchemaReport.PkMissing {
				schemaRatingText = schemaRatingText + fmt.Sprintf(" +%s", pkMsg)
			}
			rate = rate + fmt.Sprintf("Schema conversion: %s.\n", schemaRatingText)
		}
		if !structuredReport.SchemaOnly {
			dataRatingText := ""
			if tableReport.DataReport.DryRun {
				dataRatingText = "successfully converted"
			} else {
				dataRatingText = "written"
			}
			s := fmt.Sprintf(" (%s%% of %d rows %s to Spanner)", pct(tableReport.DataReport.TotalRows, tableReport.DataReport.BadRows), tableReport.DataReport.TotalRows, dataRatingText)
			dataRatingText = tableReport.DataReport.Rating + s
			rate = rate + fmt.Sprintf("Data conversion: %s.\n", dataRatingText)
		}
		w.WriteString(rate)
		w.WriteString("\n")
		for _, issue := range tableReport.Issues {
			fmt.Fprintf(w, "%s\n", issue.IssueType)
			for i, l := range issue.IssueList {
				justifyLines(w, fmt.Sprintf("%d) %s.\n", i+1, l.Description), 80, 3)
			}
			w.WriteString("\n")
		}
	}
}

func writeNameChanges(structuredReport StructuredReport, w *bufio.Writer) {
	if structuredReport.NameChanges != nil {
		w.WriteString("-----------------------------------------------------------------------------------------------------\n")
		w.WriteString("Name Changes in Migration\n")
		w.WriteString("-----------------------------------------------------------------------------------------------------\n")
		fmt.Fprintf(w, "%25s %15s %25s %25s\n", "Source Table", "Change", "Old Name", "New Name")
		w.WriteString("-----------------------------------------------------------------------------------------------------\n")
		for _, nameChange := range structuredReport.NameChanges {
			fmt.Fprintf(w, "%25s %15s %25s %25s\n", nameChange.SourceTable, nameChange.NameChangeType, nameChange.OldName, nameChange.NewName)
		}
		w.WriteString("-----------------------------------------------------------------------------------------------------\n\n\n")
	} else {
		w.WriteString("No Name Changes in Migration\n")
	}
}

func writeStatementStats(structuredReport StructuredReport, w *bufio.Writer) {
	type stat struct {
		statement string
		count     int64
	}
	var l []stat
	for _, x := range structuredReport.StatementStats.StatementStats {
		l = append(l, stat{x.Statement, x.Schema + x.Data + x.Skip + x.Error})
	}
	// Sort by alphabetical order of statements.
	sort.Slice(l, func(i, j int) bool {
		return l[i].statement < l[j].statement
	})
	writeHeading(w, "Statements Processed")
	w.WriteString("Analysis of statements in " + structuredReport.StatementStats.DriverName + " output, broken down by statement type.\n")
	w.WriteString("  schema: statements successfully processed for Spanner schema information.\n")
	w.WriteString("    data: statements successfully processed for data.\n")
	w.WriteString("    skip: statements not relevant for Spanner schema or data.\n")
	w.WriteString("   error: statements that could not be processed.\n")
	w.WriteString("  --------------------------------------\n")
	fmt.Fprintf(w, "  %6s %6s %6s %6s  %s\n", "schema", "data", "skip", "error", "statement")
	w.WriteString("  --------------------------------------\n")
	for _, x := range l {
		s := findStatementStat(structuredReport.StatementStats.StatementStats, x.statement)
		fmt.Fprintf(w, "  %6d %6d %6d %6d  %s\n", s.Schema, s.Data, s.Skip, s.Error, x.statement)
	}
	if structuredReport.StatementStats.DriverName == constants.PGDUMP {
		w.WriteString("See github.com/pganalyze/pg_query_go for definitions of statement types\n")
		w.WriteString("(pganalyze/pg_query_go is the library we use for parsing pg_dump output).\n")
		w.WriteString("\n")
	} else if structuredReport.StatementStats.DriverName == constants.MYSQLDUMP {
		w.WriteString("See https://github.com/pingcap/parser for definitions of statement types\n")
		w.WriteString("(pingcap/tidb/parser is the library we use for parsing mysqldump output).\n")
		w.WriteString("\n")
	}
}

func findStatementStat(statementStats []StatementStat, statement string) StatementStat {
	for _, statementStat := range statementStats {
		if statementStat.Statement == statement {
			return statementStat
		}
	}
	return StatementStat{}
}

func writeConversionMetadata(ConversionMetadataList []ConversionMetadata, w *bufio.Writer) string {
	var schemaConvMetadata, dataConvMetadata ConversionMetadata
	for _, convMetadata := range ConversionMetadataList {
		if convMetadata.ConversionType == "Schema" && convMetadata.Duration.Microseconds() != 0 {
			schemaConvMetadata = convMetadata
		}
		if convMetadata.ConversionType == "Data" && convMetadata.Duration.Microseconds() != 0 {
			dataConvMetadata = convMetadata
		}
	}
	res := ""
	if schemaConvMetadata.Duration.Microseconds() != 0 || dataConvMetadata.Duration.Microseconds() != 0 {
		writeHeading(w, "Time duration of Conversion")
		if schemaConvMetadata.Duration.Microseconds() != 0 {
			res += fmt.Sprintf("Schema conversion duration : %s \n", schemaConvMetadata.Duration)
		}
		if dataConvMetadata.Duration.Microseconds() != 0 {
			res += fmt.Sprintf("Data conversion duration : %s \n", dataConvMetadata.Duration)
		}
		res += "\n"
	}
	return res
}

func getStatementsFromIgnoredStatements(ignoredStatements []IgnoredStatement) []string {
	var statements []string
	for _, ignoredStatment := range ignoredStatements {
		statements = append(statements, ignoredStatment.Statement)
	}
	return statements
}

func writeHeading(w *bufio.Writer, s string) {
	w.WriteString(strings.Join([]string{
		"----------------------------\n",
		s, "\n",
		"----------------------------\n"}, ""))
}

// justifyLines writes s out to w, adding newlines between words
// to keep line length under 'limit'. Newlines are indented
// 'indent' spaces.
func justifyLines(w *bufio.Writer, s string, limit int, indent int) {
	n := 0
	startOfLine := true
	words := strings.Split(s, " ") // This only handles spaces (newlines, tabs ignored).
	for _, x := range words {
		if n+len(x) > limit && !startOfLine {
			w.WriteString("\n")
			w.WriteString(strings.Repeat(" ", indent))
			n = indent
			startOfLine = true
		}
		if startOfLine {
			w.WriteString(x)
			n += len(x)
		} else {
			w.WriteString(" " + x)
			n += len(x) + 1
		}
		startOfLine = false
	}
}

// pct prints a percentage representation of (total-bad)/total
func pct(total, bad int64) string {
	if bad == 0 || total == 0 {
		return "100"
	}
	pct := 100.0 * float64(total-bad) / float64(total)
	if pct > 99.9 {
		return fmt.Sprintf("%2.5f", pct)
	}
	if pct > 95.0 {
		return fmt.Sprintf("%2.3f", pct)
	}
	return fmt.Sprintf("%2.0f", pct)
}

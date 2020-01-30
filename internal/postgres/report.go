// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package postgres

import (
	"bufio"
	"fmt"
	"sort"
	"strings"

	"harbourbridge/spanner/ddl"
)

// GenerateReport analyzes schema and data conversion stats and writes a
// detailed report to w and returns a brief summary (as a string).
func GenerateReport(conv *Conv, w *bufio.Writer, badWrites map[string]int64) string {
	reports := analyzeTables(conv, badWrites)
	summary := generateSummary(conv, reports, badWrites)
	writeHeading(w, "Summary of Conversion")
	w.WriteString(summary)
	ignored := ignoredStatements(conv)
	w.WriteString("\n")
	if len(ignored) > 0 {
		justifyLines(w, fmt.Sprintf("Note that the following PostgreSQL statements "+
			"were detected but ignored: %s.",
			strings.Join(ignored, ", ")), 80, 0)
		w.WriteString("\n\n")
	}
	justifyLines(w, "The remainder of this report provides stats on "+
		"the pg_dump statements processed, followed by a table-by-table "+
		"listing of schema and data conversion details. "+
		"For background on the schema and data conversion process used, "+
		"and explanations of the terms and notes used in this "+
		"report, see HarbourBridge's README.", 80, 0)
	w.WriteString("\n\n")
	writeStmtStats(conv, w)
	for _, t := range reports {
		h := fmt.Sprintf("Table %s", t.pgTable)
		if t.pgTable != t.spTable {
			h = h + fmt.Sprintf(" (mapped to Spanner table %s)", t.spTable)
		}
		writeHeading(w, h)
		w.WriteString(rateConversion(t.rows, t.badRows, t.cols, t.warnings, t.syntheticPKey != "", false))
		w.WriteString("\n")
		for _, x := range t.body {
			fmt.Fprintf(w, "%s\n", x.heading)
			for i, l := range x.lines {
				justifyLines(w, fmt.Sprintf("%d) %s.\n", i+1, l), 80, 3)
			}
			w.WriteString("\n")
		}
	}
	writeUnexpectedConditions(conv, w)
	return summary
}

type tableReport struct {
	pgTable       string
	spTable       string
	rows          int64
	badRows       int64
	cols          int64
	warnings      int64
	syntheticPKey string // Empty string means no synthetic primary key was needed.
	body          []tableReportBody
}

type tableReportBody struct {
	heading string
	lines   []string
}

func analyzeTables(conv *Conv, badWrites map[string]int64) (r []tableReport) {
	// Process tables in alphabetical order. This ensures that tables
	// appear in alphabetical order in report.txt.
	var tables []string
	for t := range conv.pgSchema {
		tables = append(tables, t)
	}
	sort.Strings(tables)
	for _, pgTable := range tables {
		r = append(r, buildTableReport(conv, pgTable, badWrites))
	}
	return r
}

func buildTableReport(conv *Conv, pgTable string, badWrites map[string]int64) tableReport {
	spTable, err := GetSpannerTable(conv, pgTable)
	pgSchema, ok1 := conv.pgSchema[pgTable]
	spSchema, ok2 := conv.spSchema[spTable]
	tr := tableReport{pgTable: pgTable, spTable: spTable}
	if err != nil || !ok1 || !ok2 {
		m := "bad PostgreSQL-to-Spanner table mapping or Spanner schema"
		conv.unexpected("report: " + m)
		tr.body = []tableReportBody{tableReportBody{heading: "Internal error: " + m}}
		return tr
	}
	issues, cols, warnings := analyzeCols(conv, pgTable, spTable)
	tr.cols = cols
	tr.warnings = warnings
	if pk, ok := conv.syntheticPKeys[spTable]; ok {
		tr.syntheticPKey = pk.col
		tr.body = buildTableReportBody(conv, pgTable, issues, spSchema, pgSchema, &pk.col)
	} else {
		tr.body = buildTableReportBody(conv, pgTable, issues, spSchema, pgSchema, nil)
	}
	fillRowStats(conv, spTable, badWrites, &tr)
	return tr
}

func buildTableReportBody(conv *Conv, pgTable string, issues map[string][]schemaIssue, spSchema ddl.CreateTable, pgSchema pgTableDef, syntheticPK *string) []tableReportBody {
	var body []tableReportBody
	for _, p := range []struct {
		heading  string
		severity severity
	}{
		{"Warning", warning},
		{"Note", note},
	} {
		// Print out issues is alphabetical column order.
		var cols []string
		for t := range issues {
			cols = append(cols, t)
		}
		sort.Strings(cols)
		var l []string
		if syntheticPK != nil {
			// Warnings about synthetic primary keys must be handled as a special case
			// because we have a Spanner column with no matching PostgreSQL col.
			// Much of the generic code for processing issues assumes we have both.
			if p.severity == warning {
				l = append(l, fmt.Sprintf("Column '%s' was added because this table didn't have a primary key. Spanner requires a primary key for every table", *syntheticPK))
			}
		}
		issueBatcher := make(map[schemaIssue]bool)
		for _, pgCol := range cols {
			for _, i := range issues[pgCol] {
				if issueDB[i].severity != p.severity {
					continue
				}
				if issueDB[i].batch {
					if issueBatcher[i] {
						// Have already reported a previous instance of this
						// (batched) issue, so skip this one.
						continue
					}
					issueBatcher[i] = true
				}
				spCol, err := GetSpannerCol(conv, pgTable, pgCol, true)
				if err != nil {
					conv.unexpected(err.Error())
				}
				pgType := printType(pgSchema.cols[pgCol])
				sType := spSchema.Cds[spCol].PrintColumnDefType()
				// A note on case: Both PostgreSQL types and Spanner types are case
				// insensitive. PostgreSQL often uses lower case (e.g. pg_dump uses
				// lower case) and pg_query uses lower case, so pgType is lower-case.
				// However Spanner defaults to upper case, and the Spanner AST uses
				// upper case, so sType is upper case. When printing PostgreSQL and
				// Spanner types for comparison purposes, this can be distracting.
				// Hence we switch to lower-case for Spanner types here.
				sType = strings.ToLower(sType)
				switch i {
				case defaultValue:
					l = append(l, fmt.Sprintf("%s e.g. column '%s'", issueDB[i].brief, pgCol))
				case foreignKey:
					l = append(l, fmt.Sprintf("Column '%s' uses foreign keys which Spanner does not support", pgCol))
				case timestamp:
					// Avoid the confusing "timestamp is mapped to timestamp" message.
					l = append(l, fmt.Sprintf("Some columns have PostgreSQL type 'timestamp without timezone' which is mapped to Spanner type timestamp e.g. column '%s'. %s", pgCol, issueDB[i].brief))
				case widened:
					l = append(l, fmt.Sprintf("%s e.g. for column '%s', PostgreSQL type %s is mapped to Spanner type %s", issueDB[i].brief, pgCol, pgType, sType))
				default:
					l = append(l, fmt.Sprintf("Column '%s': type %s is mapped to %s. %s", pgCol, pgType, sType, issueDB[i].brief))
				}
			}
		}
		if len(l) == 0 {
			continue
		}
		heading := p.heading
		if len(l) > 1 {
			heading = heading + "s"
		}
		body = append(body, tableReportBody{heading: heading, lines: l})
	}
	return body
}

func fillRowStats(conv *Conv, spTable string, badWrites map[string]int64, tr *tableReport) {
	rows := conv.stats.rows[spTable]
	goodConvRows := conv.stats.goodRows[spTable]
	badConvRows := conv.stats.badRows[spTable]
	badRowWrites := badWrites[spTable]
	// Note on rows:
	// rows: all rows we encountered during processing.
	// goodConvRows: rows we successfully converted.
	// badConvRows: rows we failed to convert.
	// badRowWrites: rows we converted, but could not write to Spanner.
	if rows != goodConvRows+badConvRows || badRowWrites > goodConvRows {
		conv.unexpected(fmt.Sprintf("Inconsistent row counts for table %s: %d %d %d %d\n", spTable, rows, goodConvRows, badConvRows, badRowWrites))
	}
	tr.rows = rows
	tr.badRows = badConvRows + badRowWrites
}

// Provides a description and severity for each schema issue.
// Note on batch: for some issues, we'd like to report just the first instance
// in a table and suppress other instances i.e. adding more instances
// of the issue in the same table has little value and could be very noisy.
// This is controlled via 'batch': if true, we count only the first instance
// for assessing warnings, and we give only the first instance in the report.
// TODO: add links in these descriptions to further documentation
// e.g. for timestamp description.
var issueDB = map[schemaIssue]struct {
	brief    string // Short description of issue.
	severity severity
	batch    bool // Whether multiple instances of this issue are combined.
}{
	defaultValue:          {brief: "Some columns have default values which Spanner does not support", severity: warning, batch: true},
	foreignKey:            {brief: "Spanner does not support foreign keys", severity: warning},
	multiDimensionalArray: {brief: "Spanner doesn't support multi-dimensional arrays", severity: warning},
	noGoodType:            {brief: "No appropriate Spanner type", severity: warning},
	numeric:               {brief: "Spanner does not support numeric. This type mapping could lose precision and is not recommended for production use", severity: warning},
	numericThatFits:       {brief: "Spanner does not support numeric, but this type mapping preserves the numeric's specified precision", severity: note},
	serial:                {brief: "Spanner does not support autoincrementing types", severity: warning},
	timestamp:             {brief: "Spanner timestamp is closer to PostgreSQL timestamptz", severity: note, batch: true},
	widened:               {brief: "Some columns will consume more storage in Spanner", severity: note, batch: true},
}

type severity int

const (
	warning severity = iota
	note
)

// analyzeCols returns information about the quality of schema mappings
// for table 'pgTable'. It assumes 'pgTable' is in the conv.pgSchema map.
func analyzeCols(conv *Conv, pgTable, spTable string) (map[string][]schemaIssue, int64, int64) {
	pgSchema := conv.pgSchema[pgTable]
	m := make(map[string][]schemaIssue)
	warnings := int64(0)
	warningBatcher := make(map[schemaIssue]bool)
	// Note on how we count warnings when there are multiple warnings
	// per column and/or multiple warnings per table.
	// non-batched warnings: count at most one warning per column.
	// batched warnings: count at most one warning per table.
	for c, pc := range pgSchema.cols {
		colWarning := false
		m[c] = pc.issues
		for _, i := range pc.issues {
			switch {
			case issueDB[i].severity == warning && issueDB[i].batch:
				warningBatcher[i] = true
			case issueDB[i].severity == warning && !issueDB[i].batch:
				colWarning = true
			}
		}
		if colWarning {
			warnings++
		}
	}
	warnings += int64(len(warningBatcher))
	return m, int64(len(pgSchema.cols)), warnings
}

// rateSchema returns an string summarizing the quality of PostgreSQL
// to Spanner schema conversion. 'cols' and 'warnings' are respectively
// the number of columns converted and the warnings encountered
// (both weighted by number of data rows).
// 'missingPKey' indicates whether the PostgreSQL schema had a primary key.
// 'summary' indicates whether this is a per-table rating or an overall
// summary rating.
func rateSchema(cols, warnings int64, missingPKey, summary bool) string {
	pkMsg := "missing primary key"
	if summary {
		pkMsg = "some missing primary keys"
	}
	switch {
	case cols == 0:
		return "NONE (no schema found)"
	case warnings == 0 && !missingPKey:
		return "EXCELLENT (all columns mapped cleanly)"
	case warnings == 0 && missingPKey:
		return fmt.Sprintf("GOOD (all columns mapped cleanly, but %s)", pkMsg)
	case good(cols, warnings) && !missingPKey:
		return "GOOD (most columns mapped cleanly)"
	case good(cols, warnings) && missingPKey:
		return fmt.Sprintf("GOOD (most columns mapped cleanly, but %s)", pkMsg)
	case ok(cols, warnings) && !missingPKey:
		return "OK (some columns did not map cleanly)"
	case ok(cols, warnings) && missingPKey:
		return fmt.Sprintf("OK (some columns did not map cleanly + %s)", pkMsg)
	case !missingPKey:
		return "POOR (many columns did not map cleanly)"
	default:
		return fmt.Sprintf("POOR (many columns did not map cleanly + %s)", pkMsg)
	}
}

func rateData(rows int64, badRows int64) string {
	s := fmt.Sprintf(" (%s%% of %d rows written to Spanner)", pct(rows, badRows), rows)
	switch {
	case rows == 0:
		return "NONE (no data rows found)"
	case badRows == 0:
		return fmt.Sprintf("EXCELLENT (all %d rows written to Spanner)", rows)
	case good(rows, badRows):
		return "GOOD" + s
	case ok(rows, badRows):
		return "OK" + s
	default:
		return "POOR" + s
	}
}

func good(total, badCount int64) bool {
	return badCount < total/20
}

func ok(total, badCount int64) bool {
	return badCount < total/3
}

func rateConversion(rows, badRows, cols, warnings int64, missingPKey, summary bool) string {
	return fmt.Sprintf("Schema conversion: %s.\n", rateSchema(cols, warnings, missingPKey, summary)) +
		fmt.Sprintf("Data conversion: %s.\n", rateData(rows, badRows))
}

func generateSummary(conv *Conv, r []tableReport, badWrites map[string]int64) string {
	cols := int64(0)
	warnings := int64(0)
	missingPKey := false
	for _, t := range r {
		weight := t.rows // Weight col data by how many rows in table.
		if weight == 0 { // Tables without data count as if they had one row.
			weight = 1
		}
		cols += t.cols * weight
		warnings += t.warnings * weight
		if t.syntheticPKey != "" {
			missingPKey = true
		}
	}
	// Don't use tableReport for rows/badRows stats because tableReport
	// provides per-table stats for each table in the schema i.e. it omits
	// rows for tables not in the schema. To handle this corner-case, use
	// the source of truth for row stats: conv.stats.
	rows := conv.Rows()
	badRows := conv.BadRows() // Bad rows encountered during data conversion.
	// Add in bad rows while writing to Spanner.
	for _, n := range badWrites {
		badRows += n
	}
	return rateConversion(rows, badRows, cols, warnings, missingPKey, true)
}

func ignoredStatements(conv *Conv) (l []string) {
	for s := range conv.stats.statement {
		switch s {
		case "CreateFunctionStmt":
			l = append(l, "functions")
		case "CreateSeqStmt":
			l = append(l, "sequences")
		case "CreatePLangStmt":
			l = append(l, "procedures")
		case "CreateTrigStmt":
			l = append(l, "triggers")
		case "IndexStmt":
			l = append(l, "(non-primary) indexes")
		case "ViewStmt":
			l = append(l, "views")
		}
	}
	sort.Strings(l)
	return l
}

func writeStmtStats(conv *Conv, w *bufio.Writer) {
	type stat struct {
		statement string
		count     int64
	}
	var l []stat
	for s, x := range conv.stats.statement {
		l = append(l, stat{s, x.schema + x.data + x.skip + x.error})
	}
	// Sort by alphabetical order of statements.
	sort.Slice(l, func(i, j int) bool {
		return l[i].statement < l[j].statement
	})
	writeHeading(w, "Statements Processed")
	w.WriteString("Analysis of statements in pg_dump output, broken down by statement type.\n")
	w.WriteString("  schema: statements successfully processed for Spanner schema information.\n")
	w.WriteString("    data: statements successfully processed for data.\n")
	w.WriteString("    skip: statements not relevant for Spanner schema or data.\n")
	w.WriteString("   error: statements that could not be processed.\n")
	w.WriteString("  --------------------------------------\n")
	fmt.Fprintf(w, "  %6s %6s %6s %6s  %s\n", "schema", "data", "skip", "error", "statement")
	w.WriteString("  --------------------------------------\n")
	for _, x := range l {
		s := conv.stats.statement[x.statement]
		fmt.Fprintf(w, "  %6d %6d %6d %6d  %s\n", s.schema, s.data, s.skip, s.error, x.statement)
	}
	w.WriteString("See github.com/lfittl/pg_query_go/nodes for definitions of statement types\n")
	w.WriteString("(lfittl/pg_query_go is the library we use for parsing pg_dump output).\n")
	w.WriteString("\n")
}

func writeUnexpectedConditions(conv *Conv, w *bufio.Writer) {
	reparseInfo := func() {
		if conv.stats.reparsed > 0 {
			fmt.Fprintf(w, "Note: there were %d pg_dump reparse events while looking for statement boundaries.\n\n", conv.stats.reparsed)
		}
	}
	writeHeading(w, "Unexpected Conditions")
	if len(conv.stats.unexpected) == 0 {
		w.WriteString("There were no unexpected conditions encountered during processing.\n\n")
		reparseInfo()
		return
	}
	w.WriteString("For debugging only. This section provides details of unexpected conditions\n")
	w.WriteString("encountered as we processed the pg_dump data. In particular, the AST node\n")
	w.WriteString("representation used by the lfittl/pg_query_go library used for parsing\n")
	w.WriteString("pg_dump output is highly permissive: almost any construct can appear at\n")
	w.WriteString("any node in the AST tree. The list details all unexpected nodes and\n")
	w.WriteString("conditions.\n")
	w.WriteString("  --------------------------------------\n")
	fmt.Fprintf(w, "  %6s  %s\n", "count", "condition")
	w.WriteString("  --------------------------------------\n")
	for s, n := range conv.stats.unexpected {
		fmt.Fprintf(w, "  %6d  %s\n", n, s)
	}
	w.WriteString("\n")
	reparseInfo()
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

func writeHeading(w *bufio.Writer, s string) {
	w.WriteString(strings.Join([]string{
		"----------------------------\n",
		s, "\n",
		"----------------------------\n"}, ""))
}

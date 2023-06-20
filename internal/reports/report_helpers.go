// Copyright 2023 Google LLC
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

package reports

import (
	"fmt"
	"sort"
	"strings"

	"github.com/cloudspannerecosystem/harbourbridge/common/constants"
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/proto/migration"
	"github.com/cloudspannerecosystem/harbourbridge/schema"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
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
	Heading string
	Lines   []string
}

// AnalyzeTables generates table reports for all processed tables.
func AnalyzeTables(conv *internal.Conv, badWrites map[string]int64) (r []tableReport) {
	// Process tables in alphabetical order. This ensures that tables
	// appear in alphabetical order in report.txt.
	var tableNames []string
	for _, srcTable := range conv.SrcSchema {
		tableNames = append(tableNames, srcTable.Name)
	}
	sort.Strings(tableNames)
	for _, tableName := range tableNames {
		tableId, err := internal.GetTableIdFromSrcName(conv.SrcSchema, tableName)
		if err != nil {
			continue
		}
		if _, isPresent := conv.SpSchema[tableId]; isPresent {
			r = append(r, buildTableReport(conv, tableId, badWrites))
		}
	}
	return r
}

func buildTableReport(conv *internal.Conv, tableId string, badWrites map[string]int64) tableReport {
	srcSchema, ok1 := conv.SrcSchema[tableId]
	spSchema, ok2 := conv.SpSchema[tableId]
	tr := tableReport{SrcTable: tableId, SpTable: tableId}
	if !ok1 || !ok2 {
		m := "bad source-DB-to-Spanner table mapping or Spanner schema"
		conv.Unexpected("report: " + m)
		tr.Body = []tableReportBody{{Heading: "Internal error: " + m}}
		return tr
	}
	if *conv.Audit.MigrationType != migration.MigrationData_DATA_ONLY {
		issues, cols, warnings := AnalyzeCols(conv, tableId)
		tr.Cols = cols
		tr.Warnings = warnings
		tr.Errors = int64(len(conv.SchemaIssues[tableId].TableLevelIssues))
		if pk, ok := conv.SyntheticPKeys[tableId]; ok {
			tr.SyntheticPKey = pk.ColId
			synthColName := conv.SpSchema[tableId].ColDefs[pk.ColId].Name
			tr.Body = buildTableReportBody(conv, tableId, issues, spSchema, srcSchema, &synthColName, nil, conv.SchemaIssues[tableId].TableLevelIssues)
		} else if pk, ok := conv.UniquePKey[tableId]; ok {
			tr.Body = buildTableReportBody(conv, tableId, issues, spSchema, srcSchema, nil, pk, conv.SchemaIssues[tableId].TableLevelIssues)
		} else {
			tr.Body = buildTableReportBody(conv, tableId, issues, spSchema, srcSchema, nil, nil, conv.SchemaIssues[tableId].TableLevelIssues)
		}

	}
	if !conv.SchemaMode() {
		fillRowStats(conv, tableId, badWrites, &tr)
	}
	return tr
}

func buildTableReportBody(conv *internal.Conv, tableId string, issues map[string][]internal.SchemaIssue, spSchema ddl.CreateTable, srcSchema schema.Table, syntheticPK *string, uniquePK []string, tableLevelIssues []internal.SchemaIssue) []tableReportBody {
	var body []tableReportBody
	for _, p := range []struct {
		heading  string
		severity severity
	}{
		{"Warning", warning},
		{"Note", note},
		{"Suggestion", suggestion},
		{"Error", errors},
	} {
		// Print out issues is alphabetical column order.
		var colNames []string
		for colId := range issues {
			colNames = append(colNames, conv.SpSchema[tableId].ColDefs[colId].Name)
		}
		sort.Strings(colNames)
		var l []string
		if p.severity == errors && len(tableLevelIssues) != 0 {
			for _, issue := range tableLevelIssues {
				if issue == internal.RowLimitExceeded {
					l = append(l, IssueDB[internal.RowLimitExceeded].Brief)
				}
			}

		}
		if syntheticPK != nil {
			// Warnings about synthetic primary keys must be handled as a special case
			// because we have a Spanner column with no matching source DB col.
			// Much of the generic code for processing issues assumes we have both.
			if p.severity == warning {
				l = append(l, fmt.Sprintf("Column '%s' was added because this table didn't have a primary key. Spanner requires a primary key for every table", *syntheticPK))
			}
		}
		if uniquePK != nil {
			// Warning about using a column with unique constraint as primary key
			// in case primary key is absent.
			if p.severity == warning {
				l = append(l, fmt.Sprintf("UNIQUE constraint on column(s) '%s' replaced with primary key since this table didn't have one. Spanner requires a primary key for every table", strings.Join(uniquePK, ", ")))
			}
		}

		if p.severity == warning {
			for _, spFk := range conv.SpSchema[tableId].ForeignKeys {
				srcFk, err := internal.GetSrcFkFromId(conv.SrcSchema[tableId].ForeignKeys, spFk.Id)
				if err != nil {
					continue
				}
				_, isChanged := internal.FixName(srcFk.Name)
				if isChanged && srcFk.Name != spFk.Name {
					l = append(l, fmt.Sprintf("%s, Foreign Key '%s' is mapped to '%s'", IssueDB[internal.IllegalName].Brief, srcFk.Name, spFk.Name))
				}
			}
			for _, spIdx := range conv.SpSchema[tableId].Indexes {
				srcIdx, err := internal.GetSrcIndexFromId(conv.SrcSchema[tableId].Indexes, spIdx.Id)
				if err != nil {
					continue
				}
				_, isChanged := internal.FixName(srcIdx.Name)
				if isChanged && srcIdx.Name != spIdx.Name {
					l = append(l, fmt.Sprintf("%s, Index '%s' is mapped to '%s'", IssueDB[internal.IllegalName].Brief, srcIdx.Name, spIdx.Name))
				}
			}

			_, isChanged := internal.FixName(srcSchema.Name)
			if isChanged && (spSchema.Name != srcSchema.Name) {
				l = append(l, fmt.Sprintf("%s, Table '%s' is mapped to '%s'", IssueDB[internal.IllegalName].Brief, srcSchema.Name, spSchema.Name))
			}
		}

		issueBatcher := make(map[internal.SchemaIssue]bool)
		for _, colName := range colNames {
			colId, _ := internal.GetColIdFromSpName(conv.SpSchema[tableId].ColDefs, colName)
			for _, i := range issues[colId] {
				if IssueDB[i].severity != p.severity {
					continue
				}
				if IssueDB[i].batch {
					if issueBatcher[i] {
						// Have already reported a previous instance of this
						// (batched) issue, so skip this one.
						continue
					}
					issueBatcher[i] = true
				}
				srcColType := srcSchema.ColDefs[colId].Type.Print()
				spColType := spSchema.ColDefs[colId].T.PrintColumnDefType()
				if conv.SpDialect == constants.DIALECT_POSTGRESQL {
					spColType = spSchema.ColDefs[colId].T.PGPrintColumnDefType()
				}
				srcColName := srcSchema.ColDefs[colId].Name
				spColName := spSchema.ColDefs[colId].Name

				// A note on case: Spanner types are case insensitive, but
				// default to upper case. In particular, the Spanner AST uses
				// upper case, so spType is upper case. Many source DBs
				// default to lower case. When printing source DB and
				// Spanner types for comparison purposes, this can be distracting.
				// Hence we switch to lower-case for Spanner types here.
				// TODO: add logic to choose case for Spanner types based
				// on case of srcType.
				spColType = strings.ToLower(spColType)
				switch i {
				case internal.DefaultValue:
					l = append(l, fmt.Sprintf("%s e.g. column '%s'", IssueDB[i].Brief, spColName))
				case internal.ForeignKey:
					l = append(l, fmt.Sprintf("Column '%s' uses foreign keys which HarbourBridge does not support yet", spColName))
				case internal.AutoIncrement:
					l = append(l, fmt.Sprintf("Column '%s' is an autoincrement column. %s", spColName, IssueDB[i].Brief))
				case internal.Timestamp:
					// Avoid the confusing "timestamp is mapped to timestamp" message.
					l = append(l, fmt.Sprintf("Some columns have source DB type 'timestamp without timezone' which is mapped to Spanner type timestamp e.g. column '%s'. %s", spColName, IssueDB[i].Brief))
				case internal.Datetime:
					l = append(l, fmt.Sprintf("Some columns have source DB type 'datetime' which is mapped to Spanner type timestamp e.g. column '%s'. %s", spColName, IssueDB[i].Brief))
				case internal.Widened:
					l = append(l, fmt.Sprintf("%s e.g. for column '%s', source DB type %s is mapped to Spanner data type %s", IssueDB[i].Brief, spColName, srcColType, spColType))
				case internal.HotspotTimestamp:
					str := fmt.Sprintf(" %s for Table %s and Column  %s", IssueDB[i].Brief, spSchema.Name, spColName)

					if !internal.Contains(l, str) {
						l = append(l, str)
					}
				case internal.HotspotAutoIncrement:
					str := fmt.Sprintf(" %s for Table %s and Column  %s", IssueDB[i].Brief, spSchema.Name, spColName)

					if !internal.Contains(l, str) {
						l = append(l, str)
					}
				case internal.InterleavedNotInOrder:
					parent, _, _ := getInterleaveDetail(conv, tableId, colId, i)
					str := fmt.Sprintf(" Table %s can be interleaved with table %s %s  %s and Column %s", spSchema.Name, parent, IssueDB[i].Brief, spSchema.Name, spColName)

					if !internal.Contains(l, str) {
						l = append(l, str)
					}
				case internal.InterleavedOrder:
					parent, _, _ := getInterleaveDetail(conv, tableId, colId, i)
					str := fmt.Sprintf("Table %s %s %s go to Interleave Table Tab", spSchema.Name, IssueDB[i].Brief, parent)

					if !internal.Contains(l, str) {
						l = append(l, str)
					}
				case internal.InterleavedAddColumn:
					parent, _, _ := getInterleaveDetail(conv, tableId, colId, i)
					str := fmt.Sprintf(" %s %s add %s as a primary key in table %s", IssueDB[i].Brief, parent, spColName, spSchema.Name)

					if !internal.Contains(l, str) {
						l = append(l, str)
					}
				case internal.InterleavedRenameColumn:
					parent, fkName, referColName := getInterleaveDetail(conv, tableId, colId, i)
					str := fmt.Sprintf(" %s %s rename %s primary key in table %s to match the foreign key %s refer column \"%s\"", IssueDB[i].Brief, parent, spColName, spSchema.Name, fkName, referColName)

					if !internal.Contains(l, str) {
						l = append(l, str)
					}
				case internal.InterleavedChangeColumnSize:
					parent, fkName, referColName := getInterleaveDetail(conv, tableId, colId, i)
					str := fmt.Sprintf(" %s %s change column size of column %s primary key in table %s to match the foreign key %s refer column \"%s\"", IssueDB[i].Brief, parent, spColName, spSchema.Name, fkName, referColName)

					if !internal.Contains(l, str) {
						l = append(l, str)
					}
				case internal.RedundantIndex:
					str := fmt.Sprintf(" %s for Table %s and Column  %s", IssueDB[i].Brief, spSchema.Name, spColName)

					if !internal.Contains(l, str) {
						l = append(l, str)
					}

				case internal.AutoIncrementIndex:
					str := fmt.Sprintf(" %s for Table %s and Column  %s", IssueDB[i].Brief, spSchema.Name, spColName)

					if !internal.Contains(l, str) {
						l = append(l, str)
					}

				case internal.InterleaveIndex:
					str := fmt.Sprintf("Column %s of Table %s %s", spColName, spSchema.Name, IssueDB[i].Brief)

					if !internal.Contains(l, str) {
						l = append(l, str)
					}

				case internal.IllegalName:
					l = append(l, fmt.Sprintf("%s, Column '%s' is mapped to '%s'", IssueDB[i].Brief, srcColName, spColName))
				default:
					l = append(l, fmt.Sprintf("Column '%s': type %s is mapped to %s. %s", spColName, srcColType, spColType, IssueDB[i].Brief))
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
		body = append(body, tableReportBody{Heading: heading, Lines: l})
	}
	return body
}

func getInterleaveDetail(conv *internal.Conv, tableId string, colId string, issueType internal.SchemaIssue) (parent, fkName, referColName string) {
	table := conv.SpSchema[tableId]
	for _, fk := range table.ForeignKeys {
		for i, columnId := range fk.ColIds {
			if columnId != colId {
				continue
			}
			colPkOrder, err1 := getPkOrderForReport(table.PrimaryKeys, columnId)
			refColPkOrder, err2 := getPkOrderForReport(conv.SpSchema[fk.ReferTableId].PrimaryKeys, fk.ReferColumnIds[i])

			if err2 != nil || refColPkOrder != 1 {
				continue
			}

			switch issueType {
			case internal.InterleavedOrder:
				if colPkOrder == 1 && err1 == nil {
					return conv.SpSchema[fk.ReferTableId].Name, "", ""
				}
			case internal.InterleavedNotInOrder:
				if err1 == nil && colPkOrder != 1 {
					return conv.SpSchema[fk.ReferTableId].Name, "", ""
				}
			case internal.InterleavedRenameColumn:
			case internal.InterleavedChangeColumnSize:
				if err1 == nil {
					parentTable := conv.SpSchema[fk.ReferTableId]
					return conv.SpSchema[fk.ReferTableId].Name, fk.Name, parentTable.ColDefs[fk.ReferColumnIds[i]].Name
				}
			case internal.InterleavedAddColumn:
				if err1 != nil {
					return conv.SpSchema[fk.ReferTableId].Name, "", ""
				}
			}
		}
	}
	return "", "", ""
}

func getPkOrderForReport(pks []ddl.IndexKey, colId string) (int, error) {
	for _, pk := range pks {
		if pk.ColId == colId {
			return pk.Order, nil
		}
	}
	return 0, fmt.Errorf("column is not a part of primary key")
}

func fillRowStats(conv *internal.Conv, srcTable string, badWrites map[string]int64, tr *tableReport) {
	rows := conv.Stats.Rows[srcTable]
	goodConvRows := conv.Stats.GoodRows[srcTable]
	badConvRows := conv.Stats.BadRows[srcTable]
	badRowWrites := badWrites[srcTable]
	// Note on rows:
	// rows: all rows we encountered during processing.
	// goodConvRows: rows we successfully converted.
	// badConvRows: rows we failed to convert.
	// badRowWrites: rows we converted, but could not write to Spanner.
	if rows != goodConvRows+badConvRows || badRowWrites > goodConvRows {
		conv.Unexpected(fmt.Sprintf("Inconsistent row counts for table %s: %d %d %d %d\n", srcTable, rows, goodConvRows, badConvRows, badRowWrites))
	}
	tr.rows = rows
	tr.badRows = badConvRows + badRowWrites
}

// IssueDB provides a description and severity for each schema issue.
// Note on batch: for some issues, we'd like to report just the first instance
// in a table and suppress other instances i.e. adding more instances
// of the issue in the same table has little value and could be very noisy.
// This is controlled via 'batch': if true, we count only the first instance
// for assessing warnings, and we give only the first instance in the report.
// TODO: add links in these descriptions to further documentation
// e.g. for timestamp description.
var IssueDB = map[internal.SchemaIssue]struct {
	Brief    string // Short description of issue.
	severity severity
	batch    bool // Whether multiple instances of this issue are combined.
}{
	internal.DefaultValue:                {Brief: "Some columns have default values which Spanner does not support", severity: warning, batch: true},
	internal.ForeignKey:                  {Brief: "Spanner does not support foreign keys", severity: warning},
	internal.MultiDimensionalArray:       {Brief: "Spanner doesn't support multi-dimensional arrays", severity: warning},
	internal.NoGoodType:                  {Brief: "No appropriate Spanner type", severity: warning},
	internal.Numeric:                     {Brief: "Spanner does not support numeric. This type mapping could lose precision and is not recommended for production use", severity: warning},
	internal.NumericThatFits:             {Brief: "Spanner does not support numeric, but this type mapping preserves the numeric's specified precision", severity: suggestion},
	internal.Decimal:                     {Brief: "Spanner does not support decimal. This type mapping could lose precision and is not recommended for production use", severity: warning},
	internal.DecimalThatFits:             {Brief: "Spanner does not support decimal, but this type mapping preserves the decimal's specified precision", severity: suggestion},
	internal.Serial:                      {Brief: "Spanner does not support autoincrementing types", severity: warning},
	internal.AutoIncrement:               {Brief: "Spanner does not support auto_increment attribute", severity: warning},
	internal.Timestamp:                   {Brief: "Spanner timestamp is closer to PostgreSQL timestamptz", severity: suggestion, batch: true},
	internal.Datetime:                    {Brief: "Spanner timestamp is closer to MySQL timestamp", severity: warning, batch: true},
	internal.Time:                        {Brief: "Spanner does not support time/year types", severity: warning, batch: true},
	internal.Widened:                     {Brief: "Some columns will consume more storage in Spanner", severity: warning, batch: true},
	internal.StringOverflow:              {Brief: "String overflow issue might occur as maximum supported length in Spanner is 2621440", severity: warning},
	internal.HotspotTimestamp:            {Brief: "Timestamp Hotspot Occured", severity: warning},
	internal.HotspotAutoIncrement:        {Brief: "Autoincrement Hotspot Occured", severity: warning},
	internal.InterleavedOrder:            {Brief: "can be converted as Interleaved with Table", severity: suggestion},
	internal.RedundantIndex:              {Brief: "Redundant Index", severity: warning},
	internal.AutoIncrementIndex:          {Brief: "Auto increment column in Index can create a Hotspot", severity: warning},
	internal.InterleaveIndex:             {Brief: "can be converted to an Interleave Index", severity: suggestion},
	internal.InterleavedNotInOrder:       {Brief: "if primary key order parameter is changed to 1 for the table", severity: suggestion},
	internal.InterleavedAddColumn:        {Brief: "Candidate for Interleaved Table", severity: suggestion},
	internal.IllegalName:                 {Brief: "Names must adhere to the spanner regular expression {a-z|A-Z}[{a-z|A-Z|0-9|_}+]", severity: warning},
	internal.InterleavedRenameColumn:     {Brief: "Candidate for Interleaved Table", severity: suggestion},
	internal.InterleavedChangeColumnSize: {Brief: "Candidate for Interleaved Table", severity: suggestion},
	internal.RowLimitExceeded:            {Brief: "Non key columns exceed the spanner limit of 1600 MB. Please modify the column sizes", severity: errors},
}

type severity int

const (
	warning severity = iota
	note
	suggestion
	errors
)

// AnalyzeCols returns information about the quality of schema mappings
// for table 'srcTable'. It assumes 'srcTable' is in the conv.SrcSchema map.
func AnalyzeCols(conv *internal.Conv, tableId string) (map[string][]internal.SchemaIssue, int64, int64) {
	srcSchema := conv.SrcSchema[tableId]
	m := make(map[string][]internal.SchemaIssue)
	warnings := int64(0)
	warningBatcher := make(map[internal.SchemaIssue]bool)
	// Note on how we count warnings when there are multiple warnings
	// per column and/or multiple warnings per table.
	// non-batched warnings: count at most one warning per column.
	// batched warnings: count at most one warning per table.
	for c, l := range conv.SchemaIssues[tableId].ColumnLevelIssues {
		colWarning := false
		m[c] = l
		for _, i := range l {
			switch {
			case IssueDB[i].severity == warning && IssueDB[i].batch:
				warningBatcher[i] = true
			case IssueDB[i].severity == warning && !IssueDB[i].batch:
				colWarning = true
			}
		}
		if colWarning {
			warnings++
		}
	}
	warnings += int64(len(warningBatcher))
	return m, int64(len(srcSchema.ColDefs)), warnings
}

// rateSchema returns an string summarizing the quality of source DB
// to Spanner schema conversion. 'cols' and 'warnings' are respectively
// the number of columns converted and the warnings encountered
// (both weighted by number of data rows).
// 'missingPKey' indicates whether the source DB schema had a primary key.
// 'summary' indicates whether this is a per-table rating or an overall
// summary rating.

func RateSchema(cols, warnings, errors int64, missingPKey, summary bool) (string, string) {
	pkMsg := "missing primary key"
	s := fmt.Sprintf(" (%s%% of %d columns mapped cleanly)", pct(cols, warnings), cols)
	if summary {
		pkMsg = "some missing primary keys"
	}
	switch {
	case cols == 0:
		return "NONE", "NONE (no schema found)"
	case errors != 0:
		return "POOR", "POOR" + s
	case warnings == 0 && !missingPKey:
		return "EXCELLENT", fmt.Sprintf("EXCELLENT (all %d columns mapped cleanly)", cols)
	case warnings == 0 && missingPKey:
		return "GOOD", fmt.Sprintf("GOOD (all columns mapped cleanly, but %s)", pkMsg)
	case good(cols, warnings) && !missingPKey:
		return "GOOD", "GOOD" + s
	case good(cols, warnings) && missingPKey:
		return "GOOD", "GOOD" + s + fmt.Sprintf(" + %s", pkMsg)
	case ok(cols, warnings) && !missingPKey:
		return "OK", "OK" + s
	case ok(cols, warnings) && missingPKey:
		return "OK", "OK" + s + fmt.Sprintf(" + %s", pkMsg)
	case !missingPKey:
		return "POOR", "POOR" + s
	default:
		return "POOR", "POOR" + s + fmt.Sprintf(" + %s", pkMsg)
	}
}

func rateData(rows int64, badRows int64, dryRun bool) (string, string) {
	reportText := ""
	if dryRun {
		reportText = "successfully converted"
	} else {
		reportText = "written"
	}
	s := fmt.Sprintf(" (%s%% of %d rows %s to Spanner)", pct(rows, badRows), rows, reportText)
	switch {
	case rows == 0:
		return "NONE", "NONE (no data rows found)"
	case badRows == 0:
		return "EXCELLENT", fmt.Sprintf("EXCELLENT (all %d rows %s to Spanner)", rows, reportText)
	case good(rows, badRows):
		return "GOOD", "GOOD" + s
	case ok(rows, badRows):
		return "OK", "OK" + s
	default:
		return "POOR", "POOR" + s
	}
}

func good(total, badCount int64) bool {
	return float64(badCount) < float64(total)/20
}

func ok(total, badCount int64) bool {
	return float64(badCount) < (float64(total))/3
}

func rateConversion(rows, badRows, cols, warnings, errors int64, missingPKey, summary bool, schemaOnly bool, migrationType migration.MigrationData_MigrationType, dryRun bool) (string, string) {
	rate := ""
	var rating string
	if migrationType != migration.MigrationData_DATA_ONLY {
		var rateSchemaReport string
		rating, rateSchemaReport = RateSchema(cols, warnings, errors, missingPKey, summary)
		rate = rate + fmt.Sprintf("Schema conversion: %s.\n", rateSchemaReport)
	}
	if !schemaOnly {
		var rateDataReport string
		rating, rateDataReport = rateData(rows, badRows, dryRun)
		rate = rate + fmt.Sprintf("Data conversion: %s.\n", rateDataReport)
	}
	return rating, rate
}

// GenerateSummary creates a summarized version of a tableReport.
func GenerateSummary(conv *internal.Conv, r []tableReport, badWrites map[string]int64) (string, string) {
	cols := int64(0)
	warnings := int64(0)
	errors := int64(0)
	missingPKey := false
	for _, t := range r {
		weight := t.rows // Weight col data by how many rows in table.
		if weight == 0 { // Tables without data count as if they had one row.
			weight = 1
		}
		cols += t.Cols * weight
		warnings += t.Warnings * weight
		if t.SyntheticPKey != "" {
			missingPKey = true
		}
	}
	// Don't use tableReport for rows/badRows stats because tableReport
	// provides per-table stats for each table in the schema i.e. it omits
	// rows for tables not in the schema. To handle this corner-case, use
	// the source of truth for row stats: conv.Stats.
	rows := conv.Rows()
	badRows := conv.BadRows() // Bad rows encountered during data conversion.
	// Add in bad rows while writing to Spanner.
	for _, n := range badWrites {
		badRows += n
	}
	return rateConversion(rows, badRows, cols, warnings, errors, missingPKey, true, conv.SchemaMode(), *conv.Audit.MigrationType, conv.Audit.DryRun)
}

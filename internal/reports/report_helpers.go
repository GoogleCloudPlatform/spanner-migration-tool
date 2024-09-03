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

	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/proto/migration"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/schema"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
)

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
		schemaIssues := conv.SchemaIssues[tableId].TableLevelIssues
		tr.Errors = int64(len(schemaIssues))
		if pk, ok := conv.SyntheticPKeys[tableId]; ok {
			tr.SyntheticPKey = pk.ColId
			synthColName := conv.SpSchema[tableId].ColDefs[pk.ColId].Name
			tr.Body = buildTableReportBody(conv, tableId, issues, spSchema, srcSchema, &synthColName, nil, schemaIssues)
		} else if pk, ok := conv.UniquePKey[tableId]; ok {
			tr.Body = buildTableReportBody(conv, tableId, issues, spSchema, srcSchema, nil, pk, schemaIssues)
		} else {
			tr.Body = buildTableReportBody(conv, tableId, issues, spSchema, srcSchema, nil, nil, schemaIssues)
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
		severity Severity
	}{
		{"Warning", warning},
		{"Note", note},
		{"Suggestion", suggestion},
		{"Error", Errors},
	} {
		// Print out issues is alphabetical column order.
		var colNames []string
		for colId := range issues {
			colNames = append(colNames, conv.SpSchema[tableId].ColDefs[colId].Name)
		}
		sort.Strings(colNames)
		l := []Issue{}
		if p.severity == Errors && len(tableLevelIssues) != 0 {
			for _, issue := range tableLevelIssues {
				if issue == internal.RowLimitExceeded {
					toAppend := Issue{
						Category:    IssueDB[internal.RowLimitExceeded].Category,
						Description: IssueDB[internal.RowLimitExceeded].Brief,
					}
					l = append(l, toAppend)
				}
			}

		}

		if p.severity == warning {
			flag := false
			for _, spFk := range conv.SpSchema[tableId].ForeignKeys {
				srcFk, err := internal.GetSrcFkFromId(conv.SrcSchema[tableId].ForeignKeys, spFk.Id)
				if err != nil {
					continue
				}
				if srcFk.OnDelete == "" && srcFk.OnUpdate == "" && flag == false {
					flag = true
					issue := internal.ForeignKeyActionNotSupported
					toAppend := Issue{
						Category:    IssueDB[issue].Category,
						Description: fmt.Sprintf("Table '%s': %s", conv.SpSchema[tableId].Name, IssueDB[issue].Brief),
					}
					l = append(l, toAppend)
				}

				if srcFk.OnDelete != spFk.OnDelete {
					issue := internal.ForeignKeyOnDelete
					toAppend := Issue{
						Category:    IssueDB[issue].Category,
						Description: fmt.Sprintf("Table '%s': ON DELETE action of Foreign Key '%s' mapped from %s to %s - %s", conv.SpSchema[tableId].Name, srcFk.Name, srcFk.OnDelete, spFk.OnDelete, IssueDB[issue].Brief),
					}
					l = append(l, toAppend)
				}

				if srcFk.OnUpdate != spFk.OnUpdate {
					issue := internal.ForeignKeyOnUpdate
					toAppend := Issue{
						Category:    IssueDB[issue].Category,
						Description: fmt.Sprintf("Table '%s': ON UPDATE action of Foreign Key '%s' mapped from %s to %s - %s", conv.SpSchema[tableId].Name, srcFk.Name, srcFk.OnUpdate, spFk.OnUpdate, IssueDB[issue].Brief),
					}
					l = append(l, toAppend)
				}

				_, isChanged := internal.FixName(srcFk.Name)
				if isChanged && srcFk.Name != spFk.Name {
					toAppend := Issue{
						Category:    IssueDB[internal.IllegalName].Category,
						Description: fmt.Sprintf("%s, Foreign Key '%s' is mapped to '%s' for table '%s'", IssueDB[internal.IllegalName].Brief, srcFk.Name, spFk.Name, conv.SpSchema[tableId].Name),
					}
					l = append(l, toAppend)
				}
			}
			for _, spIdx := range conv.SpSchema[tableId].Indexes {
				srcIdx, err := internal.GetSrcIndexFromId(conv.SrcSchema[tableId].Indexes, spIdx.Id)
				if err != nil {
					continue
				}
				_, isChanged := internal.FixName(srcIdx.Name)
				if isChanged && srcIdx.Name != spIdx.Name {
					toAppend := Issue{
						Category:    IssueDB[internal.IllegalName].Category,
						Description: fmt.Sprintf("%s, Index '%s' is mapped to '%s' for table '%s'", IssueDB[internal.IllegalName].Brief, srcIdx.Name, spIdx.Name, conv.SpSchema[tableId].Name),
					}
					l = append(l, toAppend)
				}
			}

			_, isChanged := internal.FixName(srcSchema.Name)
			if isChanged && (spSchema.Name != srcSchema.Name) {
				toAppend := Issue{
					Category:    IssueDB[internal.IllegalName].Category,
					Description: fmt.Sprintf("%s, Table '%s' is mapped to '%s'", IssueDB[internal.IllegalName].Brief, srcSchema.Name, spSchema.Name),
				}
				l = append(l, toAppend)
			}
		}

		issueBatcher := make(map[internal.SchemaIssue]bool)
		for _, colName := range colNames {
			colId, _ := internal.GetColIdFromSpName(conv.SpSchema[tableId].ColDefs, colName)
			for _, i := range issues[colId] {
				if IssueDB[i].Severity != p.severity {
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
					toAppend := Issue{
						Category:    IssueDB[i].Category,
						Description: fmt.Sprintf("%s for table '%s' e.g. column '%s'", IssueDB[i].Brief, conv.SpSchema[tableId].Name, spColName),
					}
					l = append(l, toAppend)
				case internal.ForeignKey:
					toAppend := Issue{
						Category:    IssueDB[i].Category,
						Description: fmt.Sprintf("Column '%s' in table '%s' uses foreign keys which Spanner migration tool does not support yet", conv.SpSchema[tableId].Name, spColName),
					}
					l = append(l, toAppend)
				case internal.AutoIncrement:
					toAppend := Issue{
						Category:    IssueDB[i].Category,
						Description: fmt.Sprintf("Column '%s' is an autoincrement column in table '%s'. %s", spColName, conv.SpSchema[tableId].Name, IssueDB[i].Brief),
					}
					l = append(l, toAppend)
				case internal.SequenceCreated:
					toAppend := Issue{
						Category:    IssueDB[i].Category,
						Description: fmt.Sprintf("Auto-Increment has been converted to Sequence '%s' for column '%s' in table '%s'. Set Skipped Range or Start with Counter to avoid duplicate value errors.", conv.SpSchema[tableId].ColDefs[colId].AutoGen.Name, spColName, conv.SpSchema[tableId].Name),
					}
					l = append(l, toAppend)
				case internal.Timestamp:
					// Avoid the confusing "timestamp is mapped to timestamp" message.
					toAppend := Issue{
						Category:    IssueDB[i].Category,
						Description: fmt.Sprintf("Some columns have source DB type 'timestamp without timezone' which is mapped to Spanner type timestamp in table '%s' e.g. column '%s'. %s", conv.SpSchema[tableId].Name, spColName, IssueDB[i].Brief),
					}
					l = append(l, toAppend)
				case internal.Datetime:
					toAppend := Issue{
						Category:    IssueDB[i].Category,
						Description: fmt.Sprintf("Some columns have source DB type 'datetime' which is mapped to Spanner type timestamp in table '%s' e.g. column '%s'. %s", conv.SpSchema[tableId].Name, spColName, IssueDB[i].Brief),
					}
					l = append(l, toAppend)
				case internal.Widened:
					toAppend := Issue{
						Category:    IssueDB[i].Category,
						Description: fmt.Sprintf("Table '%s': %s e.g. for column '%s', source DB type %s is mapped to Spanner data type %s", conv.SpSchema[tableId].Name, IssueDB[i].Brief, spColName, srcColType, spColType),
					}
					l = append(l, toAppend)
				case internal.HotspotTimestamp:
					str := fmt.Sprintf(" %s for Table '%s' and Column  '%s'", IssueDB[i].Brief, spSchema.Name, spColName)

					if !Contains(l, str) {
						toAppend := Issue{
							Category:    IssueDB[i].Category,
							Description: str,
						}
						l = append(l, toAppend)
					}
				case internal.HotspotAutoIncrement:
					str := fmt.Sprintf(" %s for Table '%s' and Column '%s'", IssueDB[i].Brief, spSchema.Name, spColName)

					if !Contains(l, str) {
						toAppend := Issue{
							Category:    IssueDB[i].Category,
							Description: str,
						}
						l = append(l, toAppend)
					}
				case internal.InterleavedNotInOrder:
					parent, _, _ := getInterleaveDetail(conv, tableId, colId, i)
					str := fmt.Sprintf(" Table '%s' can be interleaved with table '%s' %s '%s' and Column '%s'", spSchema.Name, parent, IssueDB[i].Brief, spSchema.Name, spColName)

					if !Contains(l, str) {
						toAppend := Issue{
							Category:    IssueDB[i].Category,
							Description: str,
						}
						l = append(l, toAppend)
					}
				case internal.InterleavedOrder:
					parent, _, _ := getInterleaveDetail(conv, tableId, colId, i)
					str := fmt.Sprintf("Table '%s' %s '%s' go to Interleave Table Tab", spSchema.Name, IssueDB[i].Brief, parent)

					if !Contains(l, str) {
						toAppend := Issue{
							Category:    IssueDB[i].Category,
							Description: str,
						}
						l = append(l, toAppend)
					}
				case internal.InterleavedAddColumn:
					parent, _, _ := getInterleaveDetail(conv, tableId, colId, i)
					str := fmt.Sprintf("Table '%s' is %s '%s' add '%s' as a primary key in table '%s'", conv.SpSchema[tableId].Name, IssueDB[i].Brief, parent, spColName, spSchema.Name)

					if !Contains(l, str) {
						toAppend := Issue{
							Category:    IssueDB[i].Category,
							Description: str,
						}
						l = append(l, toAppend)
					}
				case internal.InterleavedRenameColumn:
					parent, fkName, referColName := getInterleaveDetail(conv, tableId, colId, i)
					str := fmt.Sprintf(" %s '%s' rename '%s' primary key in table '%s' to match the foreign key '%s' refer column '%s'", IssueDB[i].Brief, parent, spColName, spSchema.Name, fkName, referColName)

					if !Contains(l, str) {
						toAppend := Issue{
							Category:    IssueDB[i].Category,
							Description: str,
						}
						l = append(l, toAppend)
					}
				case internal.InterleavedChangeColumnSize:
					parent, fkName, referColName := getInterleaveDetail(conv, tableId, colId, i)
					str := fmt.Sprintf(" %s '%s' change column size of column '%s' primary key in table '%s' to match the foreign key '%s' refer column '%s'", IssueDB[i].Brief, parent, spColName, spSchema.Name, fkName, referColName)

					if !Contains(l, str) {
						toAppend := Issue{
							Category:    IssueDB[i].Category,
							Description: str,
						}
						l = append(l, toAppend)
					}
				case internal.RedundantIndex:
					str := fmt.Sprintf(" %s for Table '%s' and Column  '%s'", IssueDB[i].Brief, spSchema.Name, spColName)

					if !Contains(l, str) {
						toAppend := Issue{
							Category:    IssueDB[i].Category,
							Description: str,
						}
						l = append(l, toAppend)
					}

				case internal.AutoIncrementIndex:
					str := fmt.Sprintf(" %s for Table '%s' and Column '%s'", IssueDB[i].Brief, spSchema.Name, spColName)

					if !Contains(l, str) {
						toAppend := Issue{
							Category:    IssueDB[i].Category,
							Description: str,
						}
						l = append(l, toAppend)
					}

				case internal.InterleaveIndex:
					str := fmt.Sprintf("Column '%s' of Table '%s' %s", spColName, spSchema.Name, IssueDB[i].Brief)

					if !Contains(l, str) {
						toAppend := Issue{
							Category:    IssueDB[i].Category,
							Description: str,
						}
						l = append(l, toAppend)
					}
				case internal.ShardIdColumnAdded:
					str := fmt.Sprintf("Table '%s': '%s' %s", conv.SpSchema[tableId].Name, conv.SpSchema[tableId].ColDefs[conv.SpSchema[tableId].ShardIdColumn].Name, IssueDB[i].Brief)
					toAppend := Issue{
						Category:    IssueDB[i].Category,
						Description: str,
					}
					l = append(l, toAppend)

				case internal.ShardIdColumnPrimaryKey:
					str := fmt.Sprintf("Table '%s': '%s' %s", conv.SpSchema[tableId].Name, conv.SpSchema[tableId].ColDefs[conv.SpSchema[tableId].ShardIdColumn].Name, IssueDB[i].Brief)
					toAppend := Issue{
						Category:    IssueDB[i].Category,
						Description: str,
					}
					l = append(l, toAppend)

				case internal.IllegalName:
					toAppend := Issue{
						Category:    IssueDB[i].Category,
						Description: fmt.Sprintf("%s, Column '%s' is mapped to '%s' for table '%s'", IssueDB[i].Brief, srcColName, spColName, conv.SpSchema[tableId].Name),
					}
					l = append(l, toAppend)
				case internal.ArrayTypeNotSupported:
					toAppend := Issue{
						Category:    IssueDB[i].Category,
						Description: fmt.Sprintf("Table '%s': Column '%s', %s", conv.SpSchema[tableId].Name, spColName, IssueDB[i].Brief),
					}
					l = append(l, toAppend)
				case internal.MissingPrimaryKey:
					toAppend := Issue{
						Category:    IssueDB[i].Category,
						Description: fmt.Sprintf("Column '%s' was added because table '%s' didn't have a primary key. Spanner requires a primary key for every table", *syntheticPK, conv.SpSchema[tableId].Name),
					}
					l = append(l, toAppend)
				case internal.UniqueIndexPrimaryKey:
					toAppend := Issue{
						Category:    IssueDB[i].Category,
						Description: fmt.Sprintf("UNIQUE constraint on column(s) '%s' replaced with primary key since table '%s' didn't have one. Spanner requires a primary key for every table", strings.Join(uniquePK, ", "), conv.SpSchema[tableId].Name),
					}
					l = append(l, toAppend)
				default:
					toAppend := Issue{
						Category:    IssueDB[i].Category,
						Description: fmt.Sprintf("Table '%s': Column '%s', type %s is mapped to %s. %s", conv.SpSchema[tableId].Name, spColName, srcColType, spColType, IssueDB[i].Brief),
					}
					l = append(l, toAppend)
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
		body = append(body, tableReportBody{Heading: heading, IssueBody: l})
	}
	return body
}

// Contains check string present in list.
func Contains(l []Issue, str string) bool {
	for _, s := range l {
		if s.Description == str {
			return true
		}
	}
	return false
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
			case internal.InterleavedRenameColumn, internal.InterleavedChangeColumnSize:
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
	Brief               string // Short description of issue.
	Severity            Severity
	batch               bool   // Whether multiple instances of this issue are combined.
	Category            string // Standarized issue type
	CategoryDescription string
}{
	internal.DefaultValue:          {Brief: "Some columns have default values which Spanner migration tool does not migrate. Please add the default constraints manually after the migration is complete", Severity: note, batch: true, Category: "MISSING_DEFAULT_VALUE_CONSTRAINTS"},
	internal.ForeignKey:            {Brief: "Spanner does not support foreign keys", Severity: warning, Category: "FOREIGN_KEY_USES"},
	internal.MultiDimensionalArray: {Brief: "Spanner doesn't support multi-dimensional arrays", Severity: warning, Category: "MULTI_DIMENSIONAL_ARRAY_USES"},
	internal.NoGoodType: {Brief: "No appropriate Spanner type. The column will be made nullable in Spanner", Severity: warning, Category: "INAPPROPRIATE_TYPE",
		CategoryDescription: "No appropriate Spanner type"},
	internal.Numeric:              {Brief: "Spanner does not support numeric. This type mapping could lose precision and is not recommended for production use", Severity: warning, Category: "NUMERIC_USES"},
	internal.NumericThatFits:      {Brief: "Spanner does not support numeric, but this type mapping preserves the numeric's specified precision", Severity: suggestion, Category: "NUMERIC_THAT_FITS"},
	internal.Decimal:              {Brief: "Spanner does not support decimal. This type mapping could lose precision and is not recommended for production use", Severity: warning, Category: "DECIMAL_USES"},
	internal.DecimalThatFits:      {Brief: "Spanner does not support decimal, but this type mapping preserves the decimal's specified precision", Severity: suggestion, Category: "DECIMAL_THAT_FITS"},
	internal.Serial:               {Brief: "Spanner does not support autoincrementing types", Severity: warning, Category: "AUTOINCREMENTING_TYPE_USES"},
	internal.AutoIncrement:        {Brief: "Spanner does not support auto_increment attribute", Severity: warning, Category: "AUTO_INCREMENT_ATTRIBUTE_USES"},
	internal.Timestamp:            {Brief: "Spanner timestamp is closer to PostgreSQL timestamptz", Severity: suggestion, batch: true, Category: "TIMESTAMP_SUGGESTION"},
	internal.Datetime:             {Brief: "Spanner timestamp is closer to MySQL timestamp", Severity: warning, batch: true, Category: "TIMESTAMP_WARNING"},
	internal.Time:                 {Brief: "Spanner does not support time/year types", Severity: warning, batch: true, Category: "TIME_YEAR_TYPE_USES"},
	internal.Widened:              {Brief: "Some columns will consume more storage in Spanner", Severity: warning, batch: true, Category: "STORAGE_WARNING"},
	internal.StringOverflow:       {Brief: "String overflow issue might occur as maximum supported length in Spanner is 2621440", Severity: warning, Category: "STRING_OVERFLOW_WARNING"},
	internal.HotspotTimestamp:     {Brief: "Timestamp Hotspot Occured", Severity: warning, Category: "TIMESTAMP_HOTSPOT"},
	internal.HotspotAutoIncrement: {Brief: "Autoincrement Hotspot Occured", Severity: warning, Category: "AUTOINCREMENT_HOTSPOT"},
	internal.InterleavedOrder: {Brief: "can be converted as Interleaved with Table", Severity: suggestion, Category: "INTERLEAVE_TABLE_SUGGESTION",
		CategoryDescription: "Some tables can be interleaved"},
	internal.RedundantIndex:     {Brief: "Redundant Index", Severity: warning, Category: "REDUNDANT_INDEX"},
	internal.AutoIncrementIndex: {Brief: "Auto increment column in Index can create a Hotspot", Severity: warning, Category: "AUTO-INCREMENT_INDEX"},
	internal.InterleaveIndex: {Brief: "can be converted to an Interleave Index", Severity: suggestion, Category: "INTERLEAVE_INDEX_SUGGESTION",
		CategoryDescription: "Some columns can be converted to interleave index"},
	internal.InterleavedNotInOrder: {Brief: "if primary key order parameter is changed for the table", Severity: suggestion, Category: "INTERLEAVED_NOT_IN_ORDER",
		CategoryDescription: "Some tables can be interleaved with parent table if primary key order parameter is changed to 1"},
	internal.InterleavedAddColumn: {Brief: "Candidate for Interleaved Table", Severity: suggestion, Category: "ADD_INTERLEAVED_COLUMN",
		CategoryDescription: "If there is some primary key added in table, it can be interleaved"},
	internal.IllegalName: {Brief: "Names must adhere to the spanner regular expression {a-z|A-Z}[{a-z|A-Z|0-9|_}+]", Severity: warning, Category: "ILLEGAL_NAME"},
	internal.InterleavedRenameColumn: {Brief: "Candidate for Interleaved Table", Severity: suggestion, Category: "RENAME_INTERLEAVED_COLUMN_PRIMARY_KEY",
		CategoryDescription: "If primary key is renamed in table to match the foreign key, the table can be interleaved"},
	internal.InterleavedChangeColumnSize: {Brief: "Candidate for Interleaved Table", Severity: suggestion, Category: "CHANGE_INTERLEAVED_COLUMN_SIZE",
		CategoryDescription: "If column size of this table's primary key is changed to match the foreign key, the table can be interleaved"},
	internal.RowLimitExceeded: {Brief: "Non key columns exceed the spanner limit of 1600 MB. Please modify the column sizes", Severity: Errors, Category: "ROW_LIMIT_EXCEEDED"},
	internal.ShardIdColumnAdded: {Brief: "column was added because this is a sharded migration and this column cannot be dropped", Severity: note, Category: "SHARD_ID_COLUMN_ADDED",
		CategoryDescription: "Shard id column was added because this is a sharded migration and that column couldn't be dropped"},
	internal.ShardIdColumnPrimaryKey: {Brief: "column is not a part of primary key. You may go to the Primary Key tab and add this column as a part of Primary Key", Severity: suggestion, Category: "SHARD_ID_ADD_COLUMN_PRIMARY_KEY",
		CategoryDescription: "Shard id column is not a part of primary key. Please add it to primary key"},
	internal.MissingPrimaryKey: {Category: "MISSING_PRIMARY_KEY",
		CategoryDescription: "Primary Key is missing, synthetic column created as a primary key"},
	internal.UniqueIndexPrimaryKey: {Category: "UNIQUE_INDEX_PRIMARY_KEY",
		CategoryDescription: "Primary Key is missing, unique column(s) used as primary key"},
	internal.ArrayTypeNotSupported:        {Brief: "Array datatype migration is not fully supported. Please validate data after data migration", Severity: warning, Category: "ARRAY_TYPE_NOT_SUPPORTED"},
	internal.SequenceCreated:              {Brief: "Auto Increment has been converted to Sequence, set Skipped Range or Start with Counter to avoid duplicate value errors", Severity: warning, Category: "SEQUENCE_CREATED"},
	internal.ForeignKeyOnDelete:           {Brief: "Spanner supports only ON DELETE CASCADE/NO ACTION", Severity: warning, Category: "FOREIGN_KEY_ACTIONS"},
	internal.ForeignKeyOnUpdate:           {Brief: "Spanner supports only ON UPDATE NO ACTION", Severity: warning, Category: "FOREIGN_KEY_ACTIONS"},
	internal.ForeignKeyActionNotSupported: {Brief: "Spanner supports foreign key action migration only for MySQL and PostgreSQL", Severity: warning, Category: "FOREIGN_KEY_ACTIONS"},
	internal.NumericPKNotSupported:        {Brief: "Spanner PostgreSQL does not support numeric primary keys / unique indices", Severity: warning, Category: "NUMERIC_PK_NOT_SUPPORTED"},
}

type Severity int

const (
	warning Severity = iota
	note
	suggestion
	Errors
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
			case IssueDB[i].Severity == warning && IssueDB[i].batch:
				warningBatcher[i] = true
			case IssueDB[i].Severity == warning && !IssueDB[i].batch:
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

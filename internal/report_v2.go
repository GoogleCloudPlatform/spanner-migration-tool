// Copyright 2023 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Implements structured report generation for Harbourbridge.
package internal

import (
	"strings"
	"time"

	"github.com/cloudspannerecosystem/harbourbridge/proto/migration"
)

type Summary struct {
	Text string `json:"text"`
	Rating string `json:"rating"`
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

type Warnings struct {
	WarningType string   `json:"warningType"`
	WarningList []string `json:"warningList"`
}

type SchemaReport struct {
	Rating       string `json:"rating"`
	PkMissing    bool   `json:"pkMissing"`
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
	SrcTableName  string       `json:"srcTableName"`
	SpTableName   string       `json:"spTableName"`
	SchemaReport  SchemaReport `json:"schemaReport"`
	DataReport    DataReport   `json:"dataReport"`
	Warnings      []Warnings   `json:"warnings"`
	SyntheticPKey string       `json:"syntheticPKey"`
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
	IgnoredStatements    []IgnoredStatement   `json:"ignoredStatements"`
	ConversionMetadata   []ConversionMetadata `json:"conversionMetadata"`
	MigrationType        string               `json:"migrationType"`
	StatementStats       StatementStats       `json:"statementStats"`
	NameChanges          []NameChange         `json:"nameChanges"`
	TableReports         []TableReport        `json:"tableReports"`
	UnexpectedConditions UnexpectedConditions `json:"unexpectedConditions"`
	SchemaOnly           bool                 `json:schemaOnly`
}

// A report consists of the following parts:
// 1. Summary (overall quality of conversion)
// 2. Ignored statements
// 3. Conversion duration
// 4. Migration Type
// 5. Statement stats (in case of dumps)
// 6. Name changes
// 7. Individual table reports (Detailed + Quality of conversion for each)
// 8. Unexpected conditions
//
// This method the RAW structured report in JSON format. Several utilities can be built on top of
// this raw, nested JSON data to output the reports in different user and machine friendly formats
// such as CSV, TXT etc.
// Note that creating this functionality separately outside of report.go is done for backward
// compatibility with the existing text based report. Some logic has also been duplicated.
// This duplication will be removed once the text based report relies on using the structured data
// as the source of truth.
func GenerateStructuredReport(driverName string, conv *Conv, badWrites map[string]int64, printTableReports bool, printUnexpecteds bool) StructuredReport {
	//Create report object
	var hbReport = StructuredReport{}
	hbReport.SchemaOnly = conv.SchemaMode()
	tableReports := AnalyzeTables(conv, badWrites)
	//1. Generate summary
	rating, summary := GenerateSummary(conv, tableReports, badWrites)
	hbReport.Summary = Summary{Text: summary, Rating: rating}

	//2. Ignored Statements
	hbReport.IgnoredStatements = fetchIgnoredStatements(conv)

	//3. Conversion Metadata
	hbReport.ConversionMetadata = append(hbReport.ConversionMetadata, ConversionMetadata{ConversionType: "Schema", Duration: conv.Audit.SchemaConversionDuration})
	hbReport.ConversionMetadata = append(hbReport.ConversionMetadata, ConversionMetadata{ConversionType: "Data", Duration: conv.Audit.DataConversionDuration})

	//4. Migration Type
	hbReport.MigrationType = mapMigrationType(*conv.Audit.MigrationType)

	//5. Statement statistics
	var isDump bool
	if strings.Contains(driverName, "dump") {
		isDump = true
	}
	if isDump {
		hbReport.StatementStats.DriverName = driverName
		hbReport.StatementStats.StatementStats = fetchStatementStats(driverName, conv)
	}

	//6. Name changes
	hbReport.NameChanges = fetchNameChanges(conv)

	//7. Table Reports
	if printTableReports {
		hbReport.TableReports = fetchTableReports(tableReports, *conv.Audit.MigrationType, conv.SchemaMode(), conv.Audit.DryRun)
	}

	//8. Unexpected Conditions
	if printUnexpecteds {
		hbReport.UnexpectedConditions = fetchUnexceptedConditions(driverName, conv)
	}

	return hbReport
}

func mapMigrationType(migrationType migration.MigrationData_MigrationType) string {
	if migrationType == migration.MigrationData_DATA_ONLY {
		return "DATA"
	}
	if migrationType == migration.MigrationData_SCHEMA_AND_DATA {
		return "SCHEMA_AND_DATA"
	}
	if migrationType == migration.MigrationData_SCHEMA_ONLY {
		return "SCHEMA"
	}
	return "UNSPECIFIED"
}

func fetchIgnoredStatements(conv *Conv) (ignoredStatements []IgnoredStatement) {
	for s := range conv.Stats.Statement {
		switch s {
		case "CreateFunctionStmt":
			ignoredStatements = append(ignoredStatements, IgnoredStatement{StatementType: "function", Statement: s})
		case "CreateSeqStmt", "CreateSequenceStmt":
			ignoredStatements = append(ignoredStatements, IgnoredStatement{StatementType: "sequence", Statement: s})
		case "CreatePLangStmt", "CreateProcedureStmt":
			ignoredStatements = append(ignoredStatements, IgnoredStatement{StatementType: "procedure", Statement: s})
		case "CreateTrigStmt":
			ignoredStatements = append(ignoredStatements, IgnoredStatement{StatementType: "trigger", Statement: s})
		case "IndexStmt", "CreateIndexStmt":
			ignoredStatements = append(ignoredStatements, IgnoredStatement{StatementType: "(non-primary) index", Statement: s})
		case "ViewStmt", "CreateViewStmt":
			ignoredStatements = append(ignoredStatements, IgnoredStatement{StatementType: "view", Statement: s})
		}
	}
	return ignoredStatements
}

func fetchStatementStats(driverName string, conv *Conv) (statementStats []StatementStat) {
	for s, x := range conv.Stats.Statement {
		statementStats = append(statementStats, StatementStat{Statement: s, Schema: x.Schema, Data: x.Data, Skip: x.Skip, Error: x.Error})
	}
	return statementStats
}

func fetchNameChanges(conv *Conv) (nameChanges []NameChange) {
	for tableId, spTable := range conv.SpSchema {
		srcTable := conv.SrcSchema[tableId]
		if srcTable.Name != spTable.Name {
			nameChanges = append(nameChanges, NameChange{NameChangeType: "TableName", SourceTable: srcTable.Name, OldName: srcTable.Name, NewName: spTable.Name})
		}
		for colId, spCol := range spTable.ColDefs {
			srcCol, ok := srcTable.ColDefs[colId]
			if !ok {
				continue
			}
			if srcCol.Name != spCol.Name {
				nameChanges = append(nameChanges, NameChange{NameChangeType: "ColumnName", SourceTable: srcTable.Name, OldName: srcCol.Name, NewName: spCol.Name})
			}
		}
		for _, spFk := range conv.SpSchema[tableId].ForeignKeys {
			srcFk, err := GetSrcFkFromId(conv.SrcSchema[tableId].ForeignKeys, spFk.Id)
			if err != nil {
				continue
			}
			if srcFk.Name != spFk.Name {
				nameChanges = append(nameChanges, NameChange{NameChangeType: "ForeignKey", SourceTable: srcTable.Name, OldName: srcFk.Name, NewName: spFk.Name})
			}
		}
		for _, spIdx := range conv.SpSchema[tableId].Indexes {
			srcIdx, err := GetSrcIndexFromId(conv.SrcSchema[tableId].Indexes, spIdx.Id)
			if err != nil {
				continue
			}
			if srcIdx.Name != spIdx.Name {
				nameChanges = append(nameChanges, NameChange{NameChangeType: "Index", SourceTable: srcTable.Name, OldName: srcIdx.Name, NewName: spIdx.Name})
			}
		}
	}
	return nameChanges
}

func fetchTableReports(inputTableReports []tableReport, migrationType migration.MigrationData_MigrationType, schemaOnly bool, dryRun bool) (tableReports []TableReport) {
	for _, t := range inputTableReports {
		//1. src and Sp Table Names
		tableReport := TableReport{SrcTableName: t.SrcTable}
		tableReport.SpTableName = t.SpTable

		//2. Schema Report
		if migrationType != migration.MigrationData_DATA_ONLY {
			tableReport.SchemaReport = getSchemaReport(t.Cols, t.Warnings, t.SyntheticPKey != "")
		}
		//3. Data Report
		if !schemaOnly {
			tableReport.DataReport = getDataReport(t.rows, t.badRows, dryRun)
		}
		//4. Warnings
		for _, x := range t.Body {
			var warnings = Warnings{WarningType: x.Heading}
			for _, l := range x.Lines {
				warnings.WarningList = append(warnings.WarningList, l)
			}
			tableReport.Warnings = append(tableReport.Warnings, warnings)
		}
		//5. Sythetic PKey
		tableReport.SyntheticPKey = t.SyntheticPKey
		tableReports = append(tableReports, tableReport)
	}
	return tableReports
}

func getSchemaReport(cols, warnings int64, missingPKey bool) (schemaReport SchemaReport) {
	schemaReport.TotalColumns = cols
	schemaReport.Warnings = warnings
	schemaReport.PkMissing = missingPKey
	switch {
	case cols == 0:
		schemaReport.Rating = "NONE"
	case warnings == 0 && !missingPKey:
		schemaReport.Rating = "EXCELLENT"
	case warnings == 0 && missingPKey:
		schemaReport.Rating = "EXCELLENT_WITH_MISSING_PRIMARY_KEYS"
	case good(cols, warnings) && !missingPKey:
		schemaReport.Rating = "GOOD"
	case good(cols, warnings) && missingPKey:
		schemaReport.Rating = "GOOD_WITH_MISSING_PRIMARY_KEYS"
	case ok(cols, warnings) && !missingPKey:
		schemaReport.Rating = "OK"
	case ok(cols, warnings) && missingPKey:
		schemaReport.Rating = "OK_WITH_MISSING_PRIMARY_KEYS"
	case !missingPKey:
		schemaReport.Rating = "POOR"
	default:
		schemaReport.Rating = "POOR_WITH_MISSING_PRIMARY_KEYS"
	}
	return schemaReport
}

func getDataReport(rows int64, badRows int64, dryRun bool) (dataReport DataReport) {
	dataReport.DryRun = dryRun
	dataReport.TotalRows = rows
	dataReport.BadRows = badRows
	switch {
	case rows == 0:
		dataReport.Rating = "NONE"
	case badRows == 0:
		dataReport.Rating = "EXCELLENT"
	case good(rows, badRows):
		dataReport.Rating = "GOOD"
	case ok(rows, badRows):
		dataReport.Rating = "OK"
	default:
		dataReport.Rating = "POOR"
	}
	return dataReport
}

func fetchUnexceptedConditions(driverName string, conv *Conv) (unexpectedConditions UnexpectedConditions) {
	unexpectedConditions.Reparsed = conv.Stats.Reparsed
	for s, n := range conv.Stats.Unexpected {
		unexpectedConditions.UnexpectedConditions = append(unexpectedConditions.UnexpectedConditions, UnexpectedCondition{Count: n, Condition: s})
	}
	return unexpectedConditions
}

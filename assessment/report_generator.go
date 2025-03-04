/* Copyright 2025 Google LLC
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
// limitations under the License.*/

package assessment

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/assessment/utils"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
)

type SchemaReportRow struct {
	element          string
	elementType      string // consider enum ?
	sourceDefinition string
	targetName       string
	targetDefinition string
	//DB
	dbChangeType   string
	dbChangeEffort string
	dbChanges      string
	dbImpact       string
	//Code
	codeChangeType    string // consider enum ?
	codeChangeEffort  string
	codeImpactedFiles string
	codeSnippets      string
}

func GenerateReport(dbName string, assessmentOutput utils.AssessmentOutput) {
	//pull data from assessment output
	//Write to report in require format
	//publish report locally/on GCS
	logger.Log.Info(fmt.Sprintf("%+v", assessmentOutput))

	f, err := os.Create(dbName + "_schema.txt")
	if err != nil {
		logger.Log.Error(fmt.Sprintf("Can't create schema file %s: %v", dbName, err))
		return
	}
	defer f.Close()

	w := csv.NewWriter(f)
	w.Comma = '|'
	w.UseCRLF = true

	w.WriteAll(generateSchemaReport(assessmentOutput))

	logger.Log.Info("completed publishing sample report")
}

func generateSchemaReport(assessmentOutput utils.AssessmentOutput) [][]string {
	var records [][]string

	headers := getHeaders()

	records = append(records, headers)

	schemaReportRows := convertToSchemaReportRows(assessmentOutput)
	for _, schemaRow := range schemaReportRows {
		var row []string
		row = append(row, schemaRow.element)
		row = append(row, schemaRow.elementType)
		row = append(row, schemaRow.sourceDefinition)
		row = append(row, schemaRow.targetName)
		row = append(row, schemaRow.targetDefinition)
		row = append(row, schemaRow.dbChangeEffort)
		row = append(row, schemaRow.dbChanges)
		row = append(row, schemaRow.dbImpact)
		row = append(row, schemaRow.codeChangeEffort)
		row = append(row, schemaRow.codeChangeType)
		row = append(row, schemaRow.codeImpactedFiles)
		row = append(row, schemaRow.codeSnippets)

		records = append(records, row)
	}

	return records
}

func getHeaders() []string {
	headers := []string{
		"Element",
		"Element Type",
		"Source Definition",
		"Target Name",
		"Target Definition",
		//DB
		"DB Change Effort",
		"DB Changes",
		"DB Impact",
		//CODE
		"Code Change Type",
		"Code Change Effort",
		"Impacted Files",
		"Related Code Snippets",
	}
	return headers
}

func convertToSchemaReportRows(assessmentOutput utils.AssessmentOutput) []SchemaReportRow {
	rows := []SchemaReportRow{}

	//Populate table info
	for id, table := range assessmentOutput.SchemaAssessment.SourceTableDefs {
		spTable := assessmentOutput.SchemaAssessment.SpannerTableDefs[id]
		row := SchemaReportRow{}
		row.element = table.Name
		row.elementType = "Table"
		row.sourceDefinition = tableDefinitionToString(table)

		row.targetName = spTable.Name
		row.targetDefinition = tableDefinitionToString(spTable)

		row.dbChangeEffort = "Automatic"
		row.dbChanges, row.dbImpact = calculateTableDbChangesAndImpact(table, spTable)

		//Populate code info
		rows = append(rows, row)
	}

	//Populate column info
	for id, column := range assessmentOutput.SchemaAssessment.SourceColDefs {
		spColumn := assessmentOutput.SchemaAssessment.SpannerColDefs[id]
		row := SchemaReportRow{}
		row.element = column.TableName + "." + column.Name
		row.elementType = "Column"
		row.sourceDefinition = sourceColumnDefinitionToString(column)
		row.targetName = spColumn.TableName + "." + spColumn.Name
		row.targetDefinition = spannerColumnDefinitionToString(spColumn)

		row.dbChangeEffort = "Automatic"
		row.dbChanges, row.dbImpact = calculateColumnDbChangesAndImpact(column, spColumn)

		rows = append(rows, row)

		//Populate code info
	}

	return rows
}

func tableDefinitionToString(srcTable utils.TableDetails) string {
	sourceDefinition := ""
	if strings.Contains(srcTable.Charset, "utf") {
		sourceDefinition += "CHARSET=" + srcTable.Charset + " "
	}
	for k, v := range srcTable.Properties {
		sourceDefinition += k + "=" + v + " "
	}
	return sourceDefinition
}

func spannerColumnDefinitionToString(columnDefinition utils.SpColumnDetails) string {
	s := columnDefinition.Datatype

	if columnDefinition.Len > 0 {
		s += "(" + fmt.Sprint(columnDefinition.Len) + ")"
	}

	if !columnDefinition.IsNull {
		s += " NOT NULL"
	}
	return s
}

func sourceColumnDefinitionToString(columnDefinition utils.SrcColumnDetails) string {
	s := columnDefinition.Datatype

	if len(columnDefinition.Mods) > 0 {
		var l []string
		for _, x := range columnDefinition.Mods {
			l = append(l, strconv.FormatInt(x, 10))
		}
		s = fmt.Sprintf("%s(%s)", s, strings.Join(l, ","))
	}

	if !columnDefinition.IsNull {
		s += " NOT NULL"
	}
	return s
}

// TODO move calculation logic to assessment engine
func calculateTableDbChangesAndImpact(srcTable utils.TableDetails, spTable utils.TableDetails) (string, string) {
	changes := []string{}
	impact := []string{}
	if !strings.Contains(srcTable.Charset, "utf8") { // TODO add charset level comparisons - per source
		changes = append(changes, "charset")
		impact = append(impact, "storage increase")
	}
	if len(changes) == 0 {
		changes = append(changes, "None")
	}
	return strings.Join(changes, ","), strings.Join(impact, ",")
}

// TODO move calculation logic to assessment engine
func calculateColumnDbChangesAndImpact(srcCol utils.SrcColumnDetails, spCol utils.SpColumnDetails) (string, string) {
	changes := []string{}
	impact := []string{}
	if srcCol.Datatype != spCol.Datatype { // TODO type specific checks on size
		changes = append(changes, "type")

		if getSrcColSizeBytesMySQL(srcCol) < getSpColSizeBytes(spCol) {
			impact = append(impact, "storage increase")
		}
	}

	//TODO Add check for unsigned
	//TODO add check for not null to null scenarios
	//TODO add check for size overflow
	//TODO add diffs in modifiers and features - generated cols, auto inc, default etc

	if len(changes) == 0 {
		changes = append(changes, "None")
	}
	if len(impact) == 0 {
		impact = append(impact, "None")
	}
	return strings.Join(changes, ","), strings.Join(impact, ",")
}

// TODO - move to source specific interfaces - also account for charsets
func getSrcColSizeBytesMySQL(srcCol utils.SrcColumnDetails) int64 {
	switch strings.ToLower(srcCol.Datatype) {
	case "date":
		return 4
	case "timestamp":
		return 4
	case "bit":
		return srcCol.Mods[0] / 8
	case "int":
		return 4
	case "integer":
		return 4
	case "float":
		return 4 // Add precision pspecific handling
	case "text":
		return 2 ^ 16
	case "mediumtext":
		return 2 ^ 24
	case "longtext":
		return 2 ^ 32
	default:
		//TODO - add all types
		return 4
	}
}

// TODO - move to source specific interfaces - also account for charsets
func getSpColSizeBytes(spCol utils.SpColumnDetails) int64 {
	var size int64
	switch strings.ToUpper(spCol.Datatype) {
	case "ARRAY":
		size = spCol.Len
	case "BOOL":
		size = 1
	case "BYTES":
		size = spCol.Len
	case "DATE":
		size = 4
	case "FLOAT32":
		size = 4
	case "FLOAT64":
		size = 8
	case "INT64":
		size = 8
	case "JSON":
		size = spCol.Len
	case "NUMERIC":
		size = 22 //TODO - calculate based on precision
	case "STRING":
		size = spCol.Len

	case "STRUCT":
		return 8 // TODO - get sum of parts
	case "TIMESTAMP":
		return 12
	default:
		//TODO - add all types
		return 8
	}
	return 8 + size //Overhead per col plus size
}

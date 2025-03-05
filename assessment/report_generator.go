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
		row = append(row, schemaRow.dbChangeType)
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
		"DB Change Type",
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
		row := SchemaReportRow{}
		row.element = table.Name
		row.elementType = "Table"
		row.sourceDefinition = "N/A" //Todo - get table definition

		row.targetName = assessmentOutput.SchemaAssessment.SpannerTableDefs[id].Name
		row.targetDefinition = "N/A" // Get from spanner table def

		row.dbChangeEffort = "Automatic"
		row.dbChangeType = "None"
		row.dbChanges = "N/A"
		row.dbImpact = "N/A"

		//Populate code info
		rows = append(rows, row)
	}

	//Populate column info
	for id, column := range assessmentOutput.SchemaAssessment.SourceColDefs {
		row := SchemaReportRow{}
		row.element = column.TableName + "." + column.Name
		row.elementType = "Column"
		row.sourceDefinition = sourceColumnDefinitionToString(column)
		row.targetName = assessmentOutput.SchemaAssessment.SpannerColDefs[id].TableName + "." + assessmentOutput.SchemaAssessment.SpannerColDefs[id].Name
		row.targetDefinition = spannerColumnDefinitionToString(assessmentOutput.SchemaAssessment.SpannerColDefs[id])

		row.dbChangeEffort = "Automatic"
		row.dbChangeType = "None"
		row.dbChanges = "N/A"
		row.dbImpact = "N/A"

		rows = append(rows, row)

		//Populate code info
	}

	return rows
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

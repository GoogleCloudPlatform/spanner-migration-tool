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
	for _, tableName := range assessmentOutput.SchemaAssessment.TableNames {
		row := SchemaReportRow{}
		row.element = tableName
		row.elementType = "Table"
		row.sourceDefinition = "N/A" //Todo - get table definition

		row.targetName = "N/A"       // Get from spanner table def
		row.targetDefinition = "N/A" // Get from spanner table def

		row.dbChangeEffort = "Automatic"
		row.dbChangeType = "None"

		//Populate code info
		rows = append(rows, row)
	}

	//Populate column info
	for tableName, columnNames := range assessmentOutput.SchemaAssessment.ColumnNames {
		for _, columnName := range columnNames {
			row := SchemaReportRow{}
			columnDefinition := assessmentOutput.SchemaAssessment.ColumnAssessmentOutput[columnName]
			row.element = tableName + "." + columnName
			row.elementType = "Column"
			row.sourceDefinition = columnDefinitionToString(columnDefinition)
			row.targetDefinition = "N/A"

			row.dbChangeEffort = "Automatic"
			row.dbChangeType = "None"

			rows = append(rows, row)
		}

		//Populate code info
	}

	return rows
}

func columnDefinitionToString(columnDefinition utils.ColumnDetails) string {
	s := columnDefinition.Datatype

	if columnDefinition.Size > 0 {
		s += " (" + fmt.Sprint(columnDefinition.Size) + ")"
	}

	if !columnDefinition.IsNull {
		s += " NOT NULL"
	}
	return s
}

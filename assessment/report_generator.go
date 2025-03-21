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
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/assessment/utils"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
)

type SchemaReportRow struct {
	element          string
	elementType      string // consider enum ?
	sourceName       string
	sourceDefinition string
	sourceTableName  string //populate table name where applicable
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

func dumpCsvReport(fileName string, records [][]string) {
	f, err := os.Create(fileName)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("Can't create csv file %s: %v", fileName, err))
		return
	}
	defer f.Close()

	w := csv.NewWriter(f)
	w.Comma = '\t'
	w.UseCRLF = true

	w.WriteAll(records)
}

func writeRawSnippets(dbName string, snippets []utils.Snippet) {
	f, err := os.Create(dbName + "_raw_snippets.txt")
	if err != nil {
		logger.Log.Error(fmt.Sprintf("Can't create raw snippets file %s: %v", dbName, err))
		return
	}
	defer f.Close()

	jsonWriter := json.NewEncoder(f)
	jsonWriter.Encode(snippets)
	logger.Log.Info("completed publishing raw snippets")
}

func generateCodeReport(dbName string, assessmentOutput utils.AssessmentOutput) {
	//pull data from assessment output
	//Write to report in require format
	//publish report locally/on GCS

	if assessmentOutput.SchemaAssessment.CodeSnippets != nil {
		dumpCsvReport(dbName+"_non_schema_changes.txt", fetchNonSchemaChanges(assessmentOutput.SchemaAssessment.CodeSnippets))
		logger.Log.Info("completed publishing non schema changes report")
		writeRawSnippets(dbName, *assessmentOutput.SchemaAssessment.CodeSnippets)
	}
}

func fetchNonSchemaChanges(snippets *[]utils.Snippet) [][]string {
	var nonSchemaChanges [][]string

	nonSchemaChanges = append(nonSchemaChanges, getNonSchemaChangeHeaders())
	for _, snippet := range *snippets {
		if strings.Compare(snippet.SourceMethodSignature, snippet.SuggestedMethodSignature) != 0 {
			nonSchemaChanges = append(nonSchemaChanges, convertNonSchemaSnippetsToRow(&snippet))
		}
	}
	return nonSchemaChanges
}

func convertNonSchemaSnippetsToRow(snippet *utils.Snippet) []string {

	var row []string
	row = append(row, sanitizeCsvRow(&snippet.Id))
	row = append(row, sanitizeCsvRow(&snippet.RelativeFilePath))
	row = append(row, sanitizeCsvRow(&snippet.SourceMethodSignature))
	row = append(row, sanitizeCsvRow(&snippet.SuggestedMethodSignature))
	row = append(row, sanitizeCsvRow(&snippet.NumberOfAffectedLines))
	row = append(row, sanitizeCsvRow(&snippet.Explanation))
	return row
}

func getNonSchemaChangeHeaders() []string {
	headers := []string{
		"Snippet Id",
		"File",
		"Source Method Definition",
		"Suggested Method Definition",
		"Number of Lines Affected",
		"Explanation",
	}
	return headers
}

func GenerateReport(dbName string, assessmentOutput utils.AssessmentOutput) {

	dumpCsvReport(dbName+"_schema.txt", generateSchemaReport(assessmentOutput))
	logger.Log.Info("completed publishing schema report")

	generateCodeReport(dbName, assessmentOutput)
}

func generateSchemaReport(assessmentOutput utils.AssessmentOutput) [][]string {
	var records [][]string

	headers := getSchemaHeaders()

	records = append(records, headers)

	schemaReportRows := convertToSchemaReportRows(assessmentOutput)
	for _, schemaRow := range schemaReportRows {
		var row []string
		//row = append(row, schemaRow.element)
		row = append(row, sanitizeCsvRow(&schemaRow.elementType))
		row = append(row, sanitizeCsvRow(&schemaRow.sourceTableName))
		row = append(row, sanitizeCsvRow(&schemaRow.sourceName))
		row = append(row, sanitizeCsvRow(&schemaRow.sourceDefinition))
		row = append(row, sanitizeCsvRow(&schemaRow.targetName))
		row = append(row, sanitizeCsvRow(&schemaRow.targetDefinition))
		// DB
		row = append(row, sanitizeCsvRow(&schemaRow.dbChangeEffort))
		row = append(row, sanitizeCsvRow(&schemaRow.dbChanges))
		row = append(row, sanitizeCsvRow(&schemaRow.dbImpact))
		// CODE
		//row = append(row, sanitizeCsvRow(schemaRow.codeChangeEffort)
		row = append(row, sanitizeCsvRow(&schemaRow.codeChangeType))
		row = append(row, sanitizeCsvRow(&schemaRow.codeImpactedFiles))
		row = append(row, sanitizeCsvRow(&schemaRow.codeSnippets))

		records = append(records, row)
	}

	return records
}

func sanitizeCsvRow(s *string) string {
	if s == nil {
		return ""
	}
	*s = strings.ReplaceAll(*s, "\t", " ")
	*s = strings.ReplaceAll(*s, "\n", " ")
	return *s
}

func getSchemaHeaders() []string {
	headers := []string{
		//"Element",
		"Element Type",
		"Source Table Name",
		"Source Name",
		"Source Definition",
		"Target Name",
		"Target Definition",
		//DB
		"DB Change Effort",
		"DB Changes",
		"DB Impact",
		//CODE
		"Code Change Type",
		//"Code Change Effort",
		"Impacted Files",
		"Related Code Snippets",
	}
	return headers
}

func convertToSchemaReportRows(assessmentOutput utils.AssessmentOutput) []SchemaReportRow {
	rows := []SchemaReportRow{}

	//Populate table info
	for _, tableAssessment := range assessmentOutput.SchemaAssessment.TableAssessmentOutput {
		spTable := tableAssessment.SpannerTableDef
		row := SchemaReportRow{}
		row.element = tableAssessment.SourceTableDef.Name
		row.elementType = "Table"

		row.sourceTableName = tableAssessment.SourceTableDef.Name
		row.sourceName = tableAssessment.SourceTableDef.Name
		row.sourceDefinition = tableDefinitionToString(*tableAssessment.SourceTableDef)

		row.targetName = spTable.Name
		row.targetDefinition = "N/A"

		row.dbChangeEffort = "Automatic"
		row.dbChanges, row.dbImpact = calculateTableDbChangesAndImpact(tableAssessment)

		//Populate code info
		populateTableCodeImpact(*tableAssessment.SourceTableDef, *tableAssessment.SpannerTableDef, assessmentOutput.SchemaAssessment.CodeSnippets, &row)
		rows = append(rows, row)

		//Populate column info
		for _, columnAssessment := range tableAssessment.Columns {
			spColumn := columnAssessment.SpannerColDef
			column := columnAssessment.SourceColDef
			row := SchemaReportRow{}
			row.element = column.TableName + "." + column.Name
			row.elementType = getElementTypeForColumn(*column)

			row.sourceTableName = column.TableName
			row.sourceName = column.Name
			row.sourceDefinition = sourceColumnDefinitionToString(*column)
			row.targetName = spColumn.TableName + "." + spColumn.Name
			row.targetDefinition = spannerColumnDefinitionToString(*spColumn)

			row.dbChanges, row.dbImpact, row.dbChangeEffort = calculateColumnDbChangesAndImpact(columnAssessment)

			//Populate code info
			//logger.Log.Info(fmt.Sprintf("%s.%s", column.TableName, column.Name))
			populateColumnCodeImpact(*column, *spColumn, assessmentOutput.SchemaAssessment.CodeSnippets, &row, columnAssessment)

			rows = append(rows, row)
		}
		populateCheckConstraints(tableAssessment, spTable.Name, &rows)
		populateForeignKeys(tableAssessment, spTable.Name, &rows)
		populateIndexes(tableAssessment, spTable.Name, &rows)

	}

	populateStoredProcedureInfo(assessmentOutput.SchemaAssessment.StoredProcedureAssessmentOutput, &rows)
	populateTriggerInfo(assessmentOutput.SchemaAssessment.TriggerAssessmentOutput, &rows)
	populateFunctionInfo(assessmentOutput.SchemaAssessment.FunctionAssessmentOutput, &rows)
	populateViewInfo(assessmentOutput.SchemaAssessment.ViewAssessmentOutput, &rows)
	populateSequenceInfo(assessmentOutput.SchemaAssessment.SpSequences, assessmentOutput.SchemaAssessment.TableAssessmentOutput, assessmentOutput.SchemaAssessment.CodeSnippets, &rows)

	return rows
}

func populateIndexes(tableAssessment utils.TableAssessment, spTableName string, rows *[]SchemaReportRow) {
	for id := range tableAssessment.SourceIndexDef {
		srcIndex := tableAssessment.SourceIndexDef[id]
		row := SchemaReportRow{}
		row.element = tableAssessment.SourceTableDef.Name + "." + srcIndex.Name
		row.elementType = "Index"
		// TODO : Right now we migrate all mysql indexes to spanner, we need to do it based on index type and then modify the fields here for unsupported index types
		row.sourceTableName = tableAssessment.SourceTableDef.Name
		row.sourceName = srcIndex.Name
		row.sourceDefinition = srcIndex.Ddl
		row.targetName = spTableName + "." + tableAssessment.SpannerIndexDef[id].Name
		row.targetDefinition = tableAssessment.SpannerIndexDef[id].Ddl

		row.dbChangeEffort = "Automatic"
		row.dbChanges = "None"
		row.dbImpact = "None"

		row.codeChangeEffort = "None"
		row.codeChangeType = "None"
		row.codeImpactedFiles = "None"
		row.codeSnippets = "None"
		*rows = append(*rows, row)
	}
}

func populateCheckConstraints(tableAssessment utils.TableAssessment, spTableName string, rows *[]SchemaReportRow) {
	for id, srcConstraint := range tableAssessment.SourceTableDef.CheckConstraints {
		row := SchemaReportRow{}
		row.element = tableAssessment.SourceTableDef.Name + "." + srcConstraint.Name
		row.elementType = "Check Constraint"

		row.sourceTableName = tableAssessment.SourceTableDef.Name
		row.sourceName = srcConstraint.Name
		row.sourceDefinition = srcConstraint.Expr
		if _, found := tableAssessment.SpannerTableDef.CheckConstraints[id]; !found {
			row.targetName = "N/A"
			row.targetDefinition = "N/A"

			row.dbChangeEffort = "Manual"
			row.dbChanges = "Unknown"
			row.dbImpact = ""

		} else {
			row.targetName = spTableName + "." + tableAssessment.SpannerTableDef.CheckConstraints[id].Name
			row.targetDefinition = tableAssessment.SpannerTableDef.CheckConstraints[id].Expr

			row.dbChangeEffort = "Automatic"
			row.dbChanges = "None"
			row.dbImpact = "None"
		}
		row.codeChangeEffort = "None"
		row.codeChangeType = "None"
		row.codeImpactedFiles = "None"
		row.codeSnippets = "None"
		*rows = append(*rows, row)
	}
}

func populateForeignKeys(tableAssessment utils.TableAssessment, spTableName string, rows *[]SchemaReportRow) {
	for id, fk := range tableAssessment.SourceTableDef.SourceForeignKey {
		spFk := tableAssessment.SpannerTableDef.SpannerForeignKey[id]
		row := SchemaReportRow{}
		row.element = tableAssessment.SourceTableDef.Name + "." + fk.Definition.Name
		row.elementType = "Foreign Key"

		row.sourceTableName = tableAssessment.SourceTableDef.Name
		row.sourceName = fk.Definition.Name
		row.sourceDefinition = fk.Ddl[strings.Index(fk.Ddl, "CONSTRAINT"):]
		row.targetName = spTableName + "." + spFk.Definition.Name
		row.targetDefinition = spFk.Ddl[strings.Index(spFk.Ddl, "CONSTRAINT"):]

		if fk.Definition.OnDelete != spFk.Definition.OnDelete || fk.Definition.OnUpdate != spFk.Definition.OnUpdate {
			row.dbChangeEffort = "Automatic"
			row.dbChanges = "reference_option"
			row.dbImpact = "None"

			row.codeChangeEffort = "Modify"
			row.codeChangeType = "Manual"
			row.codeImpactedFiles = "Unknown"
			row.codeSnippets = "None"
		} else {
			row.dbChangeEffort = "Automatic"
			row.dbChanges = "None"
			row.dbImpact = "None"

			row.codeChangeEffort = "None"
			row.codeChangeType = "None"
			row.codeImpactedFiles = "None"
			row.codeSnippets = "None"
		}

		*rows = append(*rows, row)
	}
}

func tableDefinitionToString(srcTable utils.SrcTableDetails) string {
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
	columnDef := ddl.ColumnDef{
		Name:         columnDefinition.Name,
		DefaultValue: columnDefinition.DefaultValue,
		AutoGen:      columnDefinition.AutoGen,
		T: ddl.Type{
			Name:    columnDefinition.Datatype,
			Len:     columnDefinition.Len,
			IsArray: columnDefinition.IsArray,
		},
		NotNull: !columnDefinition.IsNull,
	}
	s, _ := columnDef.PrintColumnDef(ddl.Config{})
	return s
}

func getElementTypeForColumn(columnDefinition utils.SrcColumnDetails) string {
	if columnDefinition.GeneratedColumn.IsPresent {
		return "Generated Column"
	}
	return "Column"
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

	if columnDefinition.IsUnsigned {
		s += " UNSIGNED"
	}
	if columnDefinition.GeneratedColumn.IsPresent {
		s += " GENERATED ALWAYS AS " + columnDefinition.GeneratedColumn.Statement
		if columnDefinition.GeneratedColumn.IsVirtual {
			s += " VIRTUAL"
		} else {
			s += " STORED"
		}
	}
	if columnDefinition.DefaultValue.IsPresent {
		s += " DEFAULT " + columnDefinition.DefaultValue.Value.Statement
	}
	if !columnDefinition.IsNull {
		s += " NOT NULL"
	}

	if columnDefinition.IsOnUpdateTimestampSet {
		s += " ON UPDATE CURRENT_TIMESTAMP"
	}

	if columnDefinition.AutoGen.Name != "" && columnDefinition.AutoGen.GenerationType == constants.AUTO_INCREMENT {
		s += " AUTO_INCREMENT"
	}

	return s
}

// TODO move calculation logic to assessment engine
func calculateTableDbChangesAndImpact(tableAssessment utils.TableAssessment) (string, string) {
	changes := []string{}
	impact := []string{}
	if !tableAssessment.CompatibleCharset {
		changes = append(changes, "charset")
	}

	if tableAssessment.SizeIncreaseInBytes > 0 {
		impact = append(impact, "storage increase")
	} else if tableAssessment.SizeIncreaseInBytes < 0 {
		impact = append(impact, "storage decrease")
	}

	if len(changes) == 0 {
		changes = append(changes, "None")
	}
	if len(impact) == 0 {
		impact = append(impact, "None")
	}
	return strings.Join(changes, ","), strings.Join(impact, ",")
}

func calculateColumnDbChangesAndImpact(columnAssessment utils.ColumnAssessment) (string, string, string) {
	changes := []string{}
	impact := []string{}
	changeEffort := "Automatic"
	if !columnAssessment.CompatibleDataType { // TODO type specific checks on size
		changes = append(changes, "type")
	}
	if columnAssessment.SourceColDef.IsOnUpdateTimestampSet || (columnAssessment.SourceColDef.DefaultValue.IsPresent && !columnAssessment.SpannerColDef.DefaultValue.IsPresent) {
		changes = append(changes, "feature")
		changeEffort = "Partial"
	}

	if columnAssessment.SizeIncreaseInBytes > 0 {
		impact = append(impact, "storage increase")
	} else if columnAssessment.SizeIncreaseInBytes < 0 {
		impact = append(impact, "storage decrease")
	}

	// TODO: fetch it from maxValue field in column definition
	if columnAssessment.SourceColDef.Datatype == "bigint" && columnAssessment.SourceColDef.IsUnsigned {
		impact = append(impact, "potential overflow")
	}

	if columnAssessment.SourceColDef.AutoGen.Name != "" && columnAssessment.SourceColDef.AutoGen.GenerationType == constants.AUTO_INCREMENT {
		changes = append(changes, "feature")
	}
	if columnAssessment.SourceColDef.GeneratedColumn.IsPresent {
		changeEffort = "Partial"
	}

	//TODO add check for not null to null scenarios

	if len(changes) == 0 {
		changes = append(changes, "None")
	}
	if len(impact) == 0 {
		impact = append(impact, "None")
	}
	return strings.Join(changes, ","), strings.Join(impact, ","), changeEffort
}

func populateTableCodeImpact(srcTableDef utils.SrcTableDetails, spTableDef utils.SpTableDetails, codeSnippets *[]utils.Snippet, row *SchemaReportRow) {
	if srcTableDef.Name == spTableDef.Name {
		row.codeChangeType = "None"
		row.codeChangeEffort = "None"
		row.codeImpactedFiles = "None"
		row.codeSnippets = "None"
		return
	}

	if codeSnippets == nil {
		row.codeChangeType = "Unavailable"
		row.codeChangeEffort = "Unavailable"
		row.codeImpactedFiles = "Unavailable"
		row.codeSnippets = "Unavailable"
		return
	}

	impactedFiles := []string{}
	relatedSnippets := []string{}
	for _, snippet := range *codeSnippets {
		if srcTableDef.Name == snippet.TableName { //TODO add check that column is empty here
			if !slices.Contains(impactedFiles, snippet.RelativeFilePath) {
				impactedFiles = append(impactedFiles, snippet.RelativeFilePath)
			}
			relatedSnippets = append(relatedSnippets, snippet.Id)
		}
	}
	if len(impactedFiles) == 0 {
		row.codeImpactedFiles = "None"
		row.codeChangeType = "None"
		row.codeChangeEffort = "None"
		row.codeSnippets = ""
	} else {
		row.codeImpactedFiles = strings.Join(impactedFiles, ",")
		row.codeChangeType = "Suggested"
		row.codeChangeEffort = "TBD" //not implemented yet
		row.codeSnippets = strings.Join(relatedSnippets, ",")
	}

}

func populateColumnCodeImpact(srcColumnDef utils.SrcColumnDetails, spColumnDef utils.SpColumnDetails, codeSnippets *[]utils.Snippet, row *SchemaReportRow, columnAssessment utils.ColumnAssessment) {
	if columnAssessment.CompatibleDataType {
		row.codeChangeType = "None"
		row.codeChangeEffort = "None"
		row.codeImpactedFiles = "None"
		row.codeSnippets = "None"
		return
	}

	if codeSnippets == nil {
		row.codeChangeType = "Unavailable"
		row.codeChangeEffort = "Unavailable"
		row.codeImpactedFiles = "Unavailable"
		row.codeSnippets = "Unavailable"
		return
	}

	if srcColumnDef.IsOnUpdateTimestampSet {
		row.codeChangeEffort = "Rewrite"
		row.codeChangeType = "Manual"
		row.codeImpactedFiles = "TBD" //not implemented yet
		row.codeSnippets = ""
		return
	}

	impactedFiles := []string{}
	relatedSnippets := []string{}
	for _, snippet := range *codeSnippets {
		if srcColumnDef.TableName == snippet.TableName && srcColumnDef.Name == snippet.ColumnName {
			if !slices.Contains(impactedFiles, snippet.RelativeFilePath) {
				impactedFiles = append(impactedFiles, snippet.RelativeFilePath)
			}
			relatedSnippets = append(relatedSnippets, snippet.Id)
		}
	}
	if len(impactedFiles) == 0 {
		row.codeImpactedFiles = "None"
		row.codeChangeType = "None"
		row.codeChangeEffort = "None"
		row.codeSnippets = ""
	} else {
		row.codeImpactedFiles = strings.Join(impactedFiles, ",")
		row.codeChangeType = "Suggested"
		row.codeChangeEffort = "Small"
		row.codeSnippets = strings.Join(relatedSnippets, ",")
	}
}

func populateStoredProcedureInfo(storedProcedureAssessmentOutput map[string]utils.StoredProcedureAssessment, rows *[]SchemaReportRow) {
	for _, sproc := range storedProcedureAssessmentOutput {
		row := SchemaReportRow{}
		row.element = sproc.Name
		row.elementType = "Stored Procedure"

		row.sourceTableName = "N/A"
		row.sourceName = sproc.Name
		row.sourceDefinition = sproc.Definition

		populateChangesForUnsupportedElements(&row)

		*rows = append(*rows, row)
	}
}

func populateTriggerInfo(triggerAssessmentOutput map[string]utils.TriggerAssessment, rows *[]SchemaReportRow) {
	for _, trigger := range triggerAssessmentOutput {
		row := SchemaReportRow{}
		row.element = trigger.Name
		row.elementType = "Trigger"

		row.sourceTableName = "N/A"
		row.sourceName = trigger.Name
		row.sourceDefinition = trigger.Operation

		populateChangesForUnsupportedElements(&row)

		*rows = append(*rows, row)
	}
}

func populateFunctionInfo(functionAssessmentOutput map[string]utils.FunctionAssessment, rows *[]SchemaReportRow) {
	for _, function := range functionAssessmentOutput {
		row := SchemaReportRow{}
		row.element = function.Name
		row.elementType = "Function"

		row.sourceTableName = "N/A"
		row.sourceName = function.Name
		row.sourceDefinition = function.Definition

		populateChangesForUnsupportedElements(&row)

		*rows = append(*rows, row)
	}
}

func populateViewInfo(viewAssessmentOutput map[string]utils.ViewAssessment, rows *[]SchemaReportRow) {
	for _, view := range viewAssessmentOutput {
		row := SchemaReportRow{}
		row.element = view.SrcName
		row.elementType = "View"

		row.sourceTableName = "N/A"
		row.sourceName = view.SrcName
		row.sourceDefinition = view.SrcViewType
		row.targetName = view.SpName
		row.targetDefinition = "Unknown"

		row.dbChangeEffort = "Manual"
		row.dbChanges = "Unknown"
		row.dbImpact = "None"

		row.codeChangeEffort = "Unknown" //Change based on availability of code
		row.codeChangeType = "Manual"
		row.codeImpactedFiles = "Unknown"
		row.codeSnippets = ""

		*rows = append(*rows, row)
	}
}

func populateChangesForUnsupportedElements(row *SchemaReportRow) {
	row.targetName = "Not supported"
	row.targetDefinition = "N/A"

	row.dbChangeEffort = "Not Supported"
	row.dbChanges = "Drop"
	row.dbImpact = "Less Compute"

	row.codeChangeEffort = "Rewrite"
	row.codeChangeType = "Manual"
	row.codeImpactedFiles = "Unknown"
	row.codeSnippets = ""
}

func populateSequenceInfo(sequenceAssessmentOutput map[string]ddl.Sequence, tableAssessments []utils.TableAssessment, codeSnippets *[]utils.Snippet, rows *[]SchemaReportRow) {

	srcTableIdToName := make(map[string]string)
	for _, table := range tableAssessments {
		srcTableIdToName[table.SourceTableDef.Id] = table.SourceTableDef.Name
	}

	for _, sequence := range sequenceAssessmentOutput {
		row := SchemaReportRow{}
		row.element = "N/A"
		row.elementType = "Sequence"

		row.sourceTableName = "N/A" // TO be corrected
		if len(sequence.ColumnsUsingSeq) == 1 {
			tableId := ""
			for tableId, _ = range sequence.ColumnsUsingSeq {
				//nothing to do
			}
			sourceTableName, found := srcTableIdToName[tableId]
			if found {
				row.sourceTableName = sourceTableName
			}
		}

		row.sourceName = sequence.Name
		row.sourceDefinition = "N/A"
		row.targetName = sequence.Name
		row.targetDefinition = sequence.PrintSequence(ddl.Config{})

		row.dbChangeEffort = "Automatic"
		row.dbChanges = "None"
		row.dbImpact = "N/A"

		row.codeChangeEffort = "Modify"
		row.codeChangeType = "Manual"
		if codeSnippets == nil {
			row.codeImpactedFiles = "Unavailable"
			row.codeSnippets = "Unavailable"
		} else {
			row.codeImpactedFiles = "Unknown"
			row.codeSnippets = "Unkown"
		}

		*rows = append(*rows, row)
	}
}

func getRelativePath(projectPath string, filePath string) string {
	if strings.HasPrefix(filePath, projectPath) {
		return strings.Replace(filePath, projectPath, "", 1)
	}
	return filePath
}

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
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
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
	"go.uber.org/zap"
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

	//Action Item
	actionItems *[]string
}

type CodeReportRow struct {
	snippetId           string
	relativeFilePath    string
	sourceDefinition    string
	suggestedDefinition string
	loc                 int
	schemaRelated       string
	explanation         string
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

func writeRawSnippets(assessmentsFolder string, snippets []utils.Snippet) {
	f, err := os.Create(assessmentsFolder + "raw_snippets.txt")
	if err != nil {
		logger.Log.Error(fmt.Sprintf("Can't create raw snippets file %s: %v", assessmentsFolder, err))
		return
	}
	defer f.Close()

	jsonWriter := json.NewEncoder(f)
	jsonWriter.Encode(snippets)
	logger.Log.Info("completed publishing raw snippets")
}

func generateCodeSummary(appAssessment *utils.AppCodeAssessmentOutput) [][]string {

	//Add codebase details

	var rows [][]string

	rows = append(rows, []string{"Language", appAssessment.Language})

	rows = append(rows, []string{"Framework", appAssessment.Framework})

	rows = append(rows, []string{"App Code Files", fmt.Sprint(appAssessment.TotalFiles)})

	rows = append(rows, []string{"Lines of code", fmt.Sprint(appAssessment.TotalLoc)})

	rows = append(rows, getNonSchemaChangeHeaders())

	codeReportRows := convertToCodeReportRows(appAssessment.CodeSnippets)
	for _, codeReportRow := range codeReportRows {
		var row []string
		row = append(row, utils.SanitizeCsvRow(&codeReportRow.snippetId))
		row = append(row, utils.SanitizeCsvRow(&codeReportRow.relativeFilePath))
		row = append(row, utils.SanitizeCsvRow(&codeReportRow.sourceDefinition))
		row = append(row, utils.SanitizeCsvRow(&codeReportRow.suggestedDefinition))
		row = append(row, fmt.Sprint(codeReportRow.loc))
		row = append(row, utils.SanitizeCsvRow(&codeReportRow.schemaRelated))
		row = append(row, utils.SanitizeCsvRow(&codeReportRow.explanation))

		rows = append(rows, row)
	}

	return rows
}

func convertToCodeReportRows(snippets *[]utils.Snippet) []CodeReportRow {

	rows := []CodeReportRow{}

	for _, snippet := range *snippets {
		row := CodeReportRow{}

		row.snippetId = snippet.Id
		row.relativeFilePath = snippet.RelativeFilePath

		if strings.TrimSpace(snippet.SourceMethodSignature) == "" {
			row.sourceDefinition = strings.Join(snippet.SourceCodeSnippet, "\n")
			row.suggestedDefinition = strings.Join(snippet.SuggestedCodeSnippet, "\n")
		} else {
			row.sourceDefinition = snippet.SourceMethodSignature
			row.suggestedDefinition = snippet.SuggestedMethodSignature
		}

		if snippet.NumberOfAffectedLines > 0 {
			row.loc = snippet.NumberOfAffectedLines
		} else {
			row.loc = len(snippet.SourceCodeSnippet)
		}

		if strings.TrimSpace(snippet.SchemaChange) == "" {
			row.schemaRelated = "No"
		} else {
			row.schemaRelated = "Yes"
		}

		if strings.TrimSpace(snippet.Explanation) == "" {
			if strings.TrimSpace(snippet.TableName) == "" {
				row.explanation = ""
			} else {
				row.explanation = "changes to " + snippet.TableName
			}
		} else {
			row.explanation = snippet.Explanation
		}

		if row.loc > 0 {

			rows = append(rows, row)
		}
	}
	return rows
}

func getNonSchemaChangeHeaders() []string {
	headers := []string{
		"Snippet Id",
		"File",
		"Source Definition",
		"Suggested Definition",
		"Number of Lines Affected",
		"Related to schema change",
		"Explanation",
	}
	return headers
}

func GenerateReport(dbName string, assessmentOutput utils.AssessmentOutput) {

	folderPath := "assessment_" + dbName + "/"
	err := os.Mkdir(folderPath, 0755)
	if err != nil {
		logger.Log.Warn("unable to create directory to dump assessment report")
		return
	}

	logger.Log.Info("assessment reports will be saved in folder: " + folderPath)
	schemaFile := folderPath + "schema.csv"
	dumpCsvReport(schemaFile, generateSchemaReport(assessmentOutput))
	logger.Log.Info("completed publishing schema report at: " + schemaFile)

	if assessmentOutput.AppCodeAssessment != nil && assessmentOutput.AppCodeAssessment.TotalFiles > 0 {
		codeChangesFile := folderPath + "code_changes.csv"
		dumpCsvReport(codeChangesFile, generateCodeSummary(assessmentOutput.AppCodeAssessment))
		logger.Log.Info("completed publishing code changes report: " + codeChangesFile)
		writeRawSnippets(folderPath, *assessmentOutput.AppCodeAssessment.CodeSnippets)
		logger.Log.Info("completed publishing code changes report")
	} else {
		logger.Log.Info("not performing application assessment as code is not detected")
	}

	// Generate query assessment report
	if assessmentOutput.QueryAssessment.QueryTranslationResult != nil {
		queryFile := folderPath + "query_assessment_report.csv"
		err := GenerateQueryAssessmentReport(*assessmentOutput.QueryAssessment.QueryTranslationResult, queryFile)
		if err != nil {
			logger.Log.Error("failed to generate query assessment report", zap.Error(err))
		} else {
			logger.Log.Info("completed publishing query assessment report: " + queryFile)
		}
	}
	logger.Log.Info("assessment complete!")
}

func generateSchemaReport(assessmentOutput utils.AssessmentOutput) [][]string {
	var records [][]string

	headers := getSchemaHeaders()

	records = append(records, headers)

	schemaReportRows := convertToSchemaReportRows(assessmentOutput)
	for _, schemaRow := range schemaReportRows {
		var row []string
		//row = append(row, schemaRow.element)
		row = append(row, utils.SanitizeCsvRow(&schemaRow.elementType))
		row = append(row, utils.SanitizeCsvRow(&schemaRow.sourceTableName))
		row = append(row, utils.SanitizeCsvRow(&schemaRow.sourceName))
		row = append(row, utils.SanitizeCsvRow(&schemaRow.sourceDefinition))
		row = append(row, utils.SanitizeCsvRow(&schemaRow.targetName))
		row = append(row, utils.SanitizeCsvRow(&schemaRow.targetDefinition))
		// DB
		row = append(row, utils.SanitizeCsvRow(&schemaRow.dbChangeEffort))
		row = append(row, utils.SanitizeCsvRow(&schemaRow.dbChanges))
		row = append(row, utils.SanitizeCsvRow(&schemaRow.dbImpact))
		// CODE
		//row = append(row, utils.SanitizeCsvRow(schemaRow.codeChangeEffort)
		row = append(row, utils.SanitizeCsvRow(&schemaRow.codeChangeType))
		row = append(row, utils.SanitizeCsvRow(&schemaRow.codeImpactedFiles))
		row = append(row, utils.SanitizeCsvRow(&schemaRow.codeSnippets))

		actionItemsStr := utils.JoinString(schemaRow.actionItems, "None")
		row = append(row, utils.SanitizeCsvRow(&actionItemsStr))
		records = append(records, row)
	}

	return records
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
		"Code Snippet References",
		"Action Items",
	}
	return headers
}

func convertToSchemaReportRows(assessmentOutput utils.AssessmentOutput) []SchemaReportRow {

	rows := []SchemaReportRow{}
	var codeSnippets *[]utils.Snippet
	codeSnippets = nil
	if assessmentOutput.AppCodeAssessment != nil {
		codeSnippets = assessmentOutput.AppCodeAssessment.CodeSnippets
	}

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
		populateTableCodeImpact(*tableAssessment.SourceTableDef, *tableAssessment.SpannerTableDef, codeSnippets, &row)

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

			row.dbChanges, row.dbImpact, row.dbChangeEffort, row.actionItems = calculateColumnDbChangesAndImpact(columnAssessment)

			//Populate code info
			//logger.Log.Info(fmt.Sprintf("%s.%s", column.TableName, column.Name))
			populateColumnCodeImpact(*column, *spColumn, codeSnippets, &row, columnAssessment)

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
	populateSequenceInfo(assessmentOutput.SchemaAssessment.SpSequences, assessmentOutput.SchemaAssessment.TableAssessmentOutput, codeSnippets, &rows)

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

			row.dbChangeEffort = "Small"
			row.dbChanges = "Unknown"
			row.dbImpact = ""
			row.actionItems = &[]string{"Alter column to apply check constraint"}

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

			row.codeChangeEffort = "Modify" //TODO Check number of references in queries and modify
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
		NotNull: columnDefinition.NotNull,
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
	if columnDefinition.NotNull {
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

func calculateColumnDbChangesAndImpact(columnAssessment utils.ColumnAssessment) (string, string, string, *[]string) {
	changes := []string{}
	impact := []string{}
	changeEffort := "Automatic"
	actionItems := []string{}
	if !columnAssessment.CompatibleDataType { // TODO type specific checks on size
		changes = append(changes, "type")
	}
	if columnAssessment.SourceColDef.IsOnUpdateTimestampSet { //TODO Add Code change effort for this
		changes = append(changes, "feature")
		changeEffort = "None"
		actionItems = append(actionItems, "Update queries to include PENDING_COMMIT_TIMESTAMP")
	}

	if columnAssessment.SourceColDef.DefaultValue.IsPresent && !columnAssessment.SpannerColDef.DefaultValue.IsPresent {
		switch columnAssessment.SourceColDef.DefaultValue.Value.Statement {
		case "NULL":
			//Nothing to do - equivalent
		default:
			changes = append(changes, "feature")
			changeEffort = "Small"
			actionItems = append(actionItems, "Alter column to apply default value")
		}
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
		changeEffort = "Small"
		actionItems = append(actionItems, "Update schema to add generated column")
	}

	//TODO add check for not null to null scenarios

	if len(changes) == 0 {
		changes = append(changes, "None")
	}
	if len(impact) == 0 {
		impact = append(impact, "None")
	}

	return strings.Join(changes, ","), strings.Join(impact, ","), changeEffort, &actionItems
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
	if codeSnippets != nil {
		for _, snippet := range *codeSnippets {
			if srcTableDef.Name == snippet.TableName { //TODO add check that column is empty here
				if !slices.Contains(impactedFiles, snippet.RelativeFilePath) {
					impactedFiles = append(impactedFiles, snippet.RelativeFilePath)
				}
				relatedSnippets = append(relatedSnippets, snippet.Id)
			}
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
		row.codeChangeEffort = "Large"
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

		row.sourceTableName = trigger.TargetTable
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

		row.dbChangeEffort = "Small"
		row.dbChanges = "Unknown"
		row.dbImpact = "None"

		row.codeChangeEffort = "Unknown" //Change based on availability of code
		row.codeChangeType = "Manual"
		row.codeImpactedFiles = "Unknown"
		row.codeSnippets = ""

		row.actionItems = &[]string{"Create view manually"}

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

	row.actionItems = &[]string{"Rewrite in application code"}
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

func hashNormalizedQuery(normalized string) string {
	h := sha256.New()
	h.Write([]byte(normalized))
	return "q" + hex.EncodeToString(h.Sum(nil))[:8]
}

func codeChangeEffort(complexity string) string {
	switch strings.ToLower(complexity) {
	case "simple":
		return "Low"
	case "moderate", "medium":
		return "Medium"
	case "complex":
		return "High"
	default:
		return ""
	}
}

func GenerateQueryAssessmentReport(queries []utils.QueryTranslationResult, outputPath string) error {
	f, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	w.Comma = '\t'
	defer w.Flush()

	// Write header
	w.Write([]string{
		"Query ID", "Query Type", "Normalized Query Text", "Original Query Example",
		"Associated Source Table(s)", "Associated Spanner Table(s)", "Incompatibility Type(s)", "Suggested Spanner Query",
		"Reason for Change", "Estimated Code Change Effort", "Code Change Details", "Number of Executions",
		"Databases Referenced", "Source of Information",
	})

	for _, q := range queries {
		queryID := hashNormalizedQuery(q.NormalizedQuery)
		queryType := q.QueryType
		srcTables := ""
		if q.SourceTablesAffected != nil {
			srcTables = strings.Join(q.SourceTablesAffected, ", ")
		}
		spTables := ""
		if q.SpannerTablesAffected != nil {
			spTables = strings.Join(q.SpannerTablesAffected, ", ")
		}

		// Collect Incompatibility Types
		var incompatibilityTypes []string
		if q.CrossDBJoins {
			incompatibilityTypes = append(incompatibilityTypes, "Cross-DB Join")
		}

		for _, functionUsed := range q.FunctionsUsed {
			if _, ok := utils.SupportedFunctions[functionUsed]; !ok {
				incompatibilityTypes = append(incompatibilityTypes, "Unsupported Function: "+functionUsed)
			}
		}
		for _, operatorUsed := range q.OperatorsUsed {
			if _, ok := utils.SupportedOperators[operatorUsed]; !ok {
				incompatibilityTypes = append(incompatibilityTypes, "Unsupported Operator: "+operatorUsed)
			}
		}
		// Add details from ComparisonAnalysis
		if q.ComparisonAnalysis.LiteralComparisons != nil && len(q.ComparisonAnalysis.LiteralComparisons.PrecisionIssues) > 0 {
			incompatibilityTypes = append(incompatibilityTypes, "Literal Precision Issues: "+strings.Join(q.ComparisonAnalysis.LiteralComparisons.PrecisionIssues, ", "))
		}
		if q.ComparisonAnalysis.DataTypeComparisons != nil && len(q.ComparisonAnalysis.DataTypeComparisons.IncompatibleTypes) > 0 {
			incompatibilityTypes = append(incompatibilityTypes, "Incompatible Data Types: "+strings.Join(q.ComparisonAnalysis.DataTypeComparisons.IncompatibleTypes, ", "))
		}
		if q.ComparisonAnalysis.TimestampComparisons != nil && len(q.ComparisonAnalysis.TimestampComparisons.TimezoneIssues) > 0 {
			incompatibilityTypes = append(incompatibilityTypes, "Timestamp Timezone Issue: "+strings.Join(q.ComparisonAnalysis.TimestampComparisons.TimezoneIssues, ", "))
		}
		if q.ComparisonAnalysis.DateComparisons != nil && len(q.ComparisonAnalysis.DateComparisons.FormatIssues) > 0 {
			incompatibilityTypes = append(incompatibilityTypes, "Date Format Issue: "+strings.Join(q.ComparisonAnalysis.DateComparisons.FormatIssues, ", "))
		}
		numExec := ""
		if q.ExecutionCount > 0 {
			numExec = fmt.Sprintf("%d", q.ExecutionCount)
		}

		databasesReferenced := ""
		if len(q.DatabasesReferenced) > 0 {
			databasesReferenced = strings.Join(q.DatabasesReferenced, ", ")
		}

		// Find code snippet id if possible
		codeChangeDetails := "None/Unavailable"
		if q.SnippetId != "" {
			codeChangeDetails = q.SnippetId
		}

		explaination := q.Explanation
		spannerQuery := q.SpannerQuery
		if q.TranslationError != "" {
			explaination = q.TranslationError
			spannerQuery = ""
		}

		w.Write([]string{
			queryID,
			queryType,
			q.NormalizedQuery,
			q.OriginalQuery,
			srcTables,
			spTables,
			strings.Join(incompatibilityTypes, ", "),
			spannerQuery,
			explaination,
			codeChangeEffort(q.Complexity),
			codeChangeDetails,
			numExec,
			databasesReferenced,
			q.AssessmentSource,
		})
	}
	return nil
}

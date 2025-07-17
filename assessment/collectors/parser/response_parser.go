/*
	Copyright 2025 Google LLC

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
*/
package assessment

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	. "github.com/GoogleCloudPlatform/spanner-migration-tool/assessment/utils"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"go.uber.org/zap"
)

// ParseStringArrayInterface Parse input into []string. Validate the type of input:
// If input is of type string, then a string array with 1 element is returned.
// If input is of string array, then the parsed string array is returned.
func ParseStringArrayInterface(input any) []string {
	if input == nil {
		return []string{}
	}
	switch input := input.(type) {
	case []string:
		return input
	case string:
		return []string{input}
	case []any:
		parsedStringArray := make([]string, 0, len(input))
		for _, parsedInputLine := range input {
			if parsedInputLine == nil {
				logger.Log.Error("Error in parsing string array:", zap.Any("any", input))
				continue
			}
			switch parsedInputLine := parsedInputLine.(type) {
			case string:
				parsedStringArray = append(parsedStringArray, parsedInputLine)
			default:
				logger.Log.Error("Error in parsing string array:", zap.Any("any", input))
				continue
			}
		}
		return parsedStringArray
	default:
		logger.Log.Error("Error in parsing string array:", zap.Any("any", input))
		return []string{}
	}
}

func ParseAnyToString(anyType any) string {
	return fmt.Sprintf("%v", anyType)
}

func ParseAnyToInteger(anyType any) int {
	str := ParseAnyToString(anyType)
	i, err := strconv.Atoi(str)
	if err != nil {
		logger.Log.Debug("could not parse string to int" + str)
		return 0
	}
	return i
}

func ParseSchemaImpact(schemaImpactResponse map[string]any, projectPath, filePath string) (*Snippet, error) {
	logger.Log.Debug("schemaImpactResponse:", zap.Any("sec: ", schemaImpactResponse))
	return &Snippet{
		SchemaChange:          ParseAnyToString(schemaImpactResponse["schema_change"]),
		TableName:             ParseAnyToString(schemaImpactResponse["table"]),
		ColumnName:            ParseAnyToString(schemaImpactResponse["column"]),
		NumberOfAffectedLines: ParseAnyToInteger(schemaImpactResponse["number_of_affected_lines"]),
		SourceCodeSnippet:     ParseStringArrayInterface(schemaImpactResponse["existing_code_lines"]),
		SuggestedCodeSnippet:  ParseStringArrayInterface(schemaImpactResponse["new_code_lines"]),
		RelativeFilePath:      GetRelativeFilePath(projectPath, filePath),
		FilePath:              filePath,
		IsDao:                 true,
	}, nil
}

func ParseCodeImpact(codeImpactResponse map[string]any, projectPath, filePath string) (*Snippet, error) {
	//To check if it is mandatory for the response to contain these methods
	return &Snippet{
		SourceMethodSignature:    ParseAnyToString(codeImpactResponse["original_method_signature"]),
		SuggestedMethodSignature: ParseAnyToString(codeImpactResponse["new_method_signature"]),
		SourceCodeSnippet:        ParseStringArrayInterface(codeImpactResponse["code_sample"]),
		SuggestedCodeSnippet:     ParseStringArrayInterface(codeImpactResponse["suggested_change"]),
		NumberOfAffectedLines:    ParseAnyToInteger(codeImpactResponse["number_of_affected_lines"]),
		Complexity:               ParseAnyToString(codeImpactResponse["complexity"]),
		Explanation:              ParseAnyToString(codeImpactResponse["description"]),
		RelativeFilePath:         GetRelativeFilePath(projectPath, filePath),
		FilePath:                 filePath,
		IsDao:                    false,
	}, nil
}

func GetRelativeFilePath(projectPath, filePath string) string {
	relativeFilePath := filePath
	if strings.HasPrefix(filePath, projectPath) {
		relativeFilePath = strings.Replace(filePath, projectPath, "", 1)
	}
	return relativeFilePath
}

func ParseNonDaoFileChanges(fileAnalyzerResponse string, projectPath, filePath string, fileIndex int) ([]Snippet, []string, error) {

	var result map[string]any
	err := json.Unmarshal([]byte(fileAnalyzerResponse), &result)
	if err != nil {
		return nil, nil, err
	}
	snippets := []Snippet{}
	codeSnippetIndex := 0
	for _, codeImpactResponse := range result["file_modifications"].([]any) {
		codeImpact, err := ParseCodeImpact(codeImpactResponse.(map[string]any), projectPath, filePath)
		if err != nil {
			return nil, nil, err
		}
		codeImpact.Id = fmt.Sprintf("snippet_%d_%d", fileIndex, codeSnippetIndex)
		snippets = append(snippets, *codeImpact)
		codeSnippetIndex++
	}
	generalWarnings := []string{}
	if result["general_warnings"] != nil {
		generalWarnings = ParseStringArrayInterface(result["general_warnings"].([]any))
	}
	return snippets, generalWarnings, nil
}

func ParseDaoFileChanges(fileAnalyzerResponse string, projectPath, filePath string, fileIndex int) ([]Snippet, []string, []QueryTranslationResult, error) {
	var result map[string]any
	err := json.Unmarshal([]byte(fileAnalyzerResponse), &result)
	if err != nil {
		return nil, nil, nil, err
	}
	snippets := []Snippet{}
	queryResults := []QueryTranslationResult{}
	codeSnippetIndex := 0
	codeChanges, ok := result["code_changes"].([]any)
	if !ok {
		return nil, nil, nil, fmt.Errorf("missing code_changes array in response")
	}
	for _, codeChangeRaw := range codeChanges {
		cc := codeChangeRaw.(map[string]any)

		snippet := Snippet{
			SchemaChange:          ParseAnyToString(cc["schema_change"]),
			TableName:             ParseAnyToString(cc["table"]),
			ColumnName:            ParseAnyToString(cc["column"]),
			NumberOfAffectedLines: ParseAnyToInteger(cc["number_of_affected_lines"]),
			SourceCodeSnippet:     ParseStringArrayInterface(cc["existing_code_lines"]),
			SuggestedCodeSnippet:  ParseStringArrayInterface(cc["new_code_lines"]),
			RelativeFilePath:      GetRelativeFilePath(projectPath, filePath),
			FilePath:              filePath,
			IsDao:                 true,
		}
		snippet.Id = fmt.Sprintf("snippet_%d_%d", fileIndex, codeSnippetIndex)
		if !IsCodeEqual(&snippet.SourceCodeSnippet, &snippet.SuggestedCodeSnippet) {
			snippets = append(snippets, snippet)
			codeSnippetIndex++
		} else {
			logger.Log.Debug("not emmitting as code snippets are equal")
		}
		// If there is a query_change, extract QueryTranslationResult and link to snippet ID
		if cc["query_change"] != nil {
			qc := cc["query_change"].(map[string]any)
			var migrationAnalysis *MigrationAnalysis
			if qc["migration_analysis"] != nil {
				b, _ := json.Marshal(qc["migration_analysis"])
				_ = json.Unmarshal(b, &migrationAnalysis)
			}
			queryResult := QueryTranslationResult{
				OriginalQuery:        ParseAnyToString(qc["old_query"]),
				NormalizedQuery:      ParseAnyToString(qc["normalized_query"]),
				SpannerQuery:         ParseAnyToString(qc["new_query"]),
				Explanation:          ParseAnyToString(qc["explanation"]),
				Complexity:           ParseAnyToString(qc["complexity"]),
				SourceCodeSnippet:    ParseStringArrayInterface(cc["existing_code_lines"]),
				SuggestedCodeSnippet: ParseStringArrayInterface(cc["new_code_lines"]),
				FilePath:             filePath,
				IsDao:                true,
				MigrationAnalysis:    migrationAnalysis,
				Source:               "app_code",
				SnippetId:            snippet.Id,
			}
			queryResults = append(queryResults, queryResult)
		}
	}
	generalWarnings := []string{}
	if result["general_warnings"] != nil {
		generalWarnings = ParseStringArrayInterface(result["general_warnings"].([]any))
	}
	return snippets, generalWarnings, queryResults, nil
}

func IsCodeEqual(sourceCode *[]string, suggestedCode *[]string) bool {
	if sourceCode == nil && suggestedCode == nil {
		return true
	} else if sourceCode == nil || suggestedCode == nil {
		return false
	}

	srcCode := ""
	for _, codeLine := range *sourceCode {
		srcCode += strings.TrimSpace(codeLine)
	}

	sugCode := ""
	for _, codeLine := range *suggestedCode {
		sugCode += strings.TrimSpace(codeLine)
	}

	return srcCode == sugCode
}

func ParseFileAnalyzerResponse(projectPath, filePath, fileAnalyzerResponse string, isDao bool, fileIndex int) (*CodeAssessment, []QueryTranslationResult, error) {
	var snippets []Snippet
	var err error
	var generalWarnings []string
	var queryResults []QueryTranslationResult
	if isDao {
		//This logic is incorrect - the dependent files need to show up as schema impact
		snippets, generalWarnings, queryResults, err = ParseDaoFileChanges(fileAnalyzerResponse, projectPath, filePath, fileIndex)
	} else {
		snippets, generalWarnings, err = ParseNonDaoFileChanges(fileAnalyzerResponse, projectPath, filePath, fileIndex)
	}
	if err != nil {
		return nil, nil, err
	}
	return &CodeAssessment{
		Snippets:        &snippets,
		GeneralWarnings: generalWarnings,
	}, queryResults, nil
}

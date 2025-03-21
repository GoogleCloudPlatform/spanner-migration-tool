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
	"strings"

	. "github.com/GoogleCloudPlatform/spanner-migration-tool/assessment/utils"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"go.uber.org/zap"
)

// ParseStringArrayInterface Parse input into []string. Validate the type of input:
// If input is of type string, then a string array with 1 element is returned.
// If input is of string array, then the parsed string array is returned.
func ParseStringArrayInterface(input any) []string {
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

func parseAnyToString(anyType any) string {
	return fmt.Sprintf("%v", anyType)
}

func ParseSchemaImpact(schemaImpactResponse map[string]any, projectPath, filePath string) (*Snippet, error) {
	logger.Log.Debug("schemaImpactResponse:", zap.Any("sec: ", schemaImpactResponse))
	return &Snippet{
		SchemaChange:          parseAnyToString(schemaImpactResponse["schema_change"]),
		TableName:             parseAnyToString(schemaImpactResponse["table"]),
		ColumnName:            parseAnyToString(schemaImpactResponse["column"]),
		NumberOfAffectedLines: parseAnyToString(schemaImpactResponse["number_of_affected_lines"]),
		SourceCodeSnippet:     ParseStringArrayInterface(schemaImpactResponse["existing_code_lines"]),
		SuggestedCodeSnippet:  ParseStringArrayInterface(schemaImpactResponse["new_code_lines"]),
		RelativeFilePath:      getRelativeFilePath(projectPath, filePath),
		FilePath:              filePath,
		IsDao:                 true,
	}, nil
}

func ParseCodeImpact(codeImpactResponse map[string]any, projectPath, filePath string) (*Snippet, error) {
	return &Snippet{
		SourceMethodSignature:    parseAnyToString(codeImpactResponse["original_method_signature"]),
		SuggestedMethodSignature: parseAnyToString(codeImpactResponse["new_method_signature"]),
		SourceCodeSnippet:        ParseStringArrayInterface(codeImpactResponse["code_sample"]),
		SuggestedCodeSnippet:     ParseStringArrayInterface(codeImpactResponse["suggested_change"]),
		NumberOfAffectedLines:    parseAnyToString(codeImpactResponse["number_of_affected_lines"]),
		Complexity:               parseAnyToString(codeImpactResponse["complexity"]),
		Explanation:              parseAnyToString(codeImpactResponse["description"]),
		RelativeFilePath:         getRelativeFilePath(projectPath, filePath),
		FilePath:                 filePath,
		IsDao:                    false,
	}, nil
}

func getRelativeFilePath(projectPath, filePath string) string {
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
		codeImpact.Id = fmt.Sprintf("spippet_%d_%d", fileIndex, codeSnippetIndex)
		snippets = append(snippets, *codeImpact)
		codeSnippetIndex++
	}
	generalWarnings := []string{}
	if result["general_warnings"] != nil {
		generalWarnings = ParseStringArrayInterface(result["general_warnings"].([]any))
	}
	return snippets, generalWarnings, nil
}

func ParseDaoFileChanges(fileAnalyzerResponse string, projectPath, filePath string, fileIndex int) ([]Snippet, []string, error) {

	var result map[string]any
	err := json.Unmarshal([]byte(fileAnalyzerResponse), &result)
	if err != nil {
		return nil, nil, err
	}
	snippets := []Snippet{}
	codeSnippetIndex := 0
	for _, schemaImpactResponse := range result["schema_impact"].([]any) {
		codeSchemaImpact, err := ParseSchemaImpact(schemaImpactResponse.(map[string]any), projectPath, filePath)
		if err != nil {
			return nil, nil, err
		}
		codeSchemaImpact.Id = fmt.Sprintf("spippet_%d_%d", fileIndex, codeSnippetIndex)
		snippets = append(snippets, *codeSchemaImpact)
		codeSnippetIndex++
	}
	generalWarnings := []string{}
	if result["general_warnings"] != nil {
		generalWarnings = ParseStringArrayInterface(result["general_warnings"].([]any))
	}
	return snippets, generalWarnings, nil
}

func ParseFileAnalyzerResponse(projectPath, filePath, fileAnalyzerResponse string, isDao bool, fileIndex int) (*CodeAssessment, error) {
	var snippets []Snippet
	var err error
	var generalWarnings []string
	if isDao {
		//This logic is incorrect - the dependent files need to show up as schema impact
		snippets, generalWarnings, err = ParseDaoFileChanges(fileAnalyzerResponse, projectPath, filePath, fileIndex)
	} else {
		snippets, generalWarnings, err = ParseNonDaoFileChanges(fileAnalyzerResponse, projectPath, filePath, fileIndex)
		if err != nil {
			return nil, err
		}
	}
	if err != nil {
		return nil, err
	}
	return &CodeAssessment{
		Snippets:        &snippets,
		GeneralWarnings: generalWarnings,
	}, nil
}

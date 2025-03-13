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

	. "github.com/GoogleCloudPlatform/spanner-migration-tool/assessment/utils"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"go.uber.org/zap"
)

func ParseStringArrayInterface(input []any) []string {

	parsedStringArray := make([]string, len(input))
	for _, codeLines := range input {
		parsedStringArray = append(parsedStringArray, codeLines.(string))
	}
	return parsedStringArray
}

func parseAnyToString(anyType any) string {
	return fmt.Sprintf("%v", anyType)
}

func ParseSchemaImpact(schemaImpactResponse map[string]any, filePath string) (*Snippet, error) {
	logger.Log.Debug("schemaImpactResponse:", zap.Any("sec: ", schemaImpactResponse))
	return &Snippet{
		SchemaChange:          parseAnyToString(schemaImpactResponse["schema_change"]),
		TableName:             parseAnyToString(schemaImpactResponse["table"]),
		ColumnName:            parseAnyToString(schemaImpactResponse["column"]),
		NumberOfAffectedLines: parseAnyToString(schemaImpactResponse["number_of_affected_lines"]),
		SourceCodeSnippet:     ParseStringArrayInterface(schemaImpactResponse["existing_code_lines"].([]any)),
		SuggestedCodeSnippet:  ParseStringArrayInterface(schemaImpactResponse["new_code_lines"].([]any)),
		FileName:              filePath,
		IsDao:                 true,
	}, nil
}

func ParseCodeImpact(codeImpactResponse map[string]any, filePath string) (*Snippet, error) {

	return &Snippet{
		SourceMethodSignature:    parseAnyToString(codeImpactResponse["original_method_signature"]),
		SuggestedMethodSignature: parseAnyToString(codeImpactResponse["new_method_signature"]),
		SourceCodeSnippet:        ParseStringArrayInterface(codeImpactResponse["code_sample"].([]any)),
		SuggestedCodeSnippet:     ParseStringArrayInterface(codeImpactResponse["suggested_change"].([]any)),
		NumberOfAffectedLines:    parseAnyToString(codeImpactResponse["number_of_affected_lines"]),
		Complexity:               parseAnyToString(codeImpactResponse["complexity"]),
		Explanation:              parseAnyToString(codeImpactResponse["description"]),
		FileName:                 filePath,
		IsDao:                    false,
	}, nil
}

func ParseNonDaoFileChanges(fileAnalyzerResponse string, filePath string, codeSnippetIndex *int) ([]Snippet, []string, error) {

	var result map[string]any
	err := json.Unmarshal([]byte(fileAnalyzerResponse), &result)
	if err != nil {
		return nil, nil, err
	}
	snippets := []Snippet{}
	for _, codeImpactResponse := range result["file_modifications"].([]any) {
		codeImpact, err := ParseCodeImpact(codeImpactResponse.(map[string]any), filePath)
		if err != nil {
			return nil, nil, err
		}
		codeImpact.Id = fmt.Sprintf("spippet_%d", *codeSnippetIndex)
		snippets = append(snippets, *codeImpact)
		*codeSnippetIndex++
	}
	generalWarnings := []string{}
	if result["general_warnings"] != nil {
		generalWarnings = ParseStringArrayInterface(result["general_warnings"].([]any))
	}
	return snippets, generalWarnings, nil
}

func ParseDaoFileChanges(fileAnalyzerResponse string, filePath string, codeSnippetIndex *int) ([]Snippet, []string, error) {

	var result map[string]any
	err := json.Unmarshal([]byte(fileAnalyzerResponse), &result)
	if err != nil {
		return nil, nil, err
	}
	snippets := []Snippet{}
	for _, schemaImpactResponse := range result["schema_impact"].([]any) {
		codeSchemaImpact, err := ParseSchemaImpact(schemaImpactResponse.(map[string]any), filePath)
		if err != nil {
			return nil, nil, err
		}
		codeSchemaImpact.Id = fmt.Sprintf("spippet_%d", *codeSnippetIndex)
		snippets = append(snippets, *codeSchemaImpact)
		*codeSnippetIndex++
	}
	generalWarnings := []string{}
	if result["general_warnings"] != nil {
		generalWarnings = ParseStringArrayInterface(result["general_warnings"].([]any))
	}
	return snippets, generalWarnings, nil
}

func ParseFileAnalyzerResponse(filePath, fileAnalyzerResponse string, isDao bool) (*CodeAssessment, error) {
	var snippets []Snippet
	var err error
	var generalWarnings []string
	codeSnippetIndex := 0
	if isDao {
		snippets, generalWarnings, err = ParseDaoFileChanges(fileAnalyzerResponse, filePath, &codeSnippetIndex)
	} else {
		snippets, generalWarnings, err = ParseNonDaoFileChanges(fileAnalyzerResponse, filePath, &codeSnippetIndex)
		if err != nil {
			return nil, err
		}
	}
	if err != nil {
		return nil, err
	}
	return &CodeAssessment{
		Snippets:        snippets,
		GeneralWarnings: generalWarnings,
	}, nil
}

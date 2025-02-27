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
)

func ParseStringArrayInterface(input []any) []string {

	parsedStringArray := make([]string, len(input))
	for _, codeLines := range input {
		parsedStringArray = append(parsedStringArray, codeLines.(string))
	}
	return parsedStringArray
}

func ParseSchemaImpact(schemaImpactResponse map[string]any, filePath string) (*Snippet, error) {
	fmt.Printf("%+v\n", schemaImpactResponse)

	return &Snippet{
		schemaChange:          schemaImpactResponse["schema_change"].(string),
		tableName:             schemaImpactResponse["table"].(string),
		columnName:            schemaImpactResponse["column"].(string),
		numberOfAffectedLines: schemaImpactResponse["number_of_affected_lines"].(string),
		sourceCodeSnippet:     ParseStringArrayInterface(schemaImpactResponse["existing_code_lines"].([]any)),
		suggestedCodeSnippet:  ParseStringArrayInterface(schemaImpactResponse["new_code_lines"].([]any)),
		fileName:              filePath,
		isDao:                 true,
	}, nil
}

func ParseCodeImpact(codeImpactResponse map[string]any, filePath string) (*Snippet, error) {

	return &Snippet{
		sourceMethodSignature:    codeImpactResponse["original_method_signature"].(string),
		suggestedMethodSignature: codeImpactResponse["new_method_signature"].(string),
		sourceCodeSnippet:        ParseStringArrayInterface(codeImpactResponse["code_sample"].([]any)),
		suggestedCodeSnippet:     ParseStringArrayInterface(codeImpactResponse["suggested_change"].([]any)),
		numberOfAffectedLines:    codeImpactResponse["number_of_affected_lines"].(string),
		complexity:               codeImpactResponse["complexity"].(string),
		explanation:              codeImpactResponse["description"].(string),
		fileName:                 filePath,
		isDao:                    false,
	}, nil
}

func ParseNonDaoFileChanges(fileAnalyzerResponse string, filePath string) ([]Snippet, []string, error) {

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
		snippets = append(snippets, *codeImpact)
	}
	generalWarnings := []string{}
	if result["general_warnings"] != nil {
		generalWarnings = ParseStringArrayInterface(result["general_warnings"].([]any))
	}
	return snippets, generalWarnings, nil
}

func ParseDaoFileChanges(fileAnalyzerResponse string, filePath string) ([]Snippet, []string, error) {

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
		fmt.Printf("%+v\n", *codeSchemaImpact)
		snippets = append(snippets, *codeSchemaImpact)
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
	if isDao {
		snippets, generalWarnings, err = ParseDaoFileChanges(fileAnalyzerResponse, filePath)
	} else {
		snippets, generalWarnings, err = ParseNonDaoFileChanges(fileAnalyzerResponse, filePath)
		if err != nil {
			return nil, err
		}
	}
	if err != nil {
		return nil, err
	}
	fmt.Println(len(snippets))
	return &CodeAssessment{
		snippets:        snippets,
		generalWarnings: generalWarnings,
	}, nil
}

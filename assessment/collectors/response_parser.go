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

import "encoding/json"

func ParseStringArrayInterface(input []any) []string {

	parsedStringArray := make([]string, len(input))
	for _, codeLines := range input {
		parsedStringArray = append(parsedStringArray, codeLines.(string))
	}
	return parsedStringArray
}

func ParseSchemaImpact(schemaImpactResponse map[string]any) (*CodeSchemaImpact, error) {

	return &CodeSchemaImpact{
		schemaChange:          schemaImpactResponse["schema_change"].(string),
		tableName:             schemaImpactResponse["table"].(string),
		columnName:            schemaImpactResponse["column"].(string),
		numberOfAffectedLines: schemaImpactResponse["number_of_affected_lines"].(string),
		sourceCodeSnippet:     ParseStringArrayInterface(schemaImpactResponse["existing_code_lines"].([]any)),
		suggestedCodeSnippet:  ParseStringArrayInterface(schemaImpactResponse["new_code_lines"].([]any)),
	}, nil
}

func ParseCodeImpact(codeImpactResponse map[string]any) (*CodeImpact, error) {

	return &CodeImpact{
		sourceMethodSignature:    codeImpactResponse["original_method_signature"].(string),
		suggestedMethodSignature: codeImpactResponse["new_method_signature"].(string),
		sourceCodeSnippet:        ParseStringArrayInterface(codeImpactResponse["code_sample"].([]any)),
		suggestedCodeSnippet:     ParseStringArrayInterface(codeImpactResponse["suggested_change"].([]any)),
		numberOfAffectedLines:    codeImpactResponse["number_of_affected_lines"].(string),
		complexity:               codeImpactResponse["complexity"].(string),
		explanation:              codeImpactResponse["description"].(string),
	}, nil
}

func ParseNonDaoFileChanges(fileAnalyzerResponse string) ([]CodeImpact, error) {

	var result map[string]any
	err := json.Unmarshal([]byte(fileAnalyzerResponse), &result)
	if err != nil {
		return nil, err
	}
	codeImpacts := []CodeImpact{}
	for _, codeImpactResponse := range result["file_modifications"].([]any) {
		codeImpact, err := ParseCodeImpact(codeImpactResponse.(map[string]any))
		if err != nil {
			return nil, err
		}
		codeImpacts = append(codeImpacts, *codeImpact)
	}
	return codeImpacts, nil
}

func ParseDaoFileChanges(fileAnalyzerResponse string) ([]CodeSchemaImpact, error) {

	var result map[string]any
	err := json.Unmarshal([]byte(fileAnalyzerResponse), &result)
	if err != nil {
		return nil, err
	}
	codeSchemaImpacts := []CodeSchemaImpact{}
	for _, schemaImpactResponse := range result["schema_impact"].([]any) {
		codeSchemaImpact, err := ParseSchemaImpact(schemaImpactResponse.(map[string]any))
		if err != nil {
			return nil, err
		}
		codeSchemaImpacts = append(codeSchemaImpacts, *codeSchemaImpact)
	}
	return codeSchemaImpacts, nil
}

func ParseFileAnalyzerResponse(filePath, fileAnalyzerResponse string, isDao bool) (*CodeAssessment, error) {
	if isDao {
		codeSchemaImpacts, err := ParseDaoFileChanges(fileAnalyzerResponse)
		if err != nil {
			return nil, err
		}
		return &CodeAssessment{
			fileName:         filePath,
			isDao:            isDao,
			codeSchemaImpact: codeSchemaImpacts,
		}, nil
	} else {
		codeImpacts, err := ParseNonDaoFileChanges(fileAnalyzerResponse)
		if err != nil {
			return nil, err
		}
		return &CodeAssessment{
			fileName:   filePath,
			isDao:      isDao,
			codeImpact: codeImpacts,
		}, nil
	}
}

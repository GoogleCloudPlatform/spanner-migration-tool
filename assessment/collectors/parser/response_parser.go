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
	if anyType == nil {
		return ""
	}
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

func ParseAnyToBool(anyType any) bool {
	str := ParseAnyToString(anyType)
	i, err := strconv.ParseBool(str)
	if err != nil {
		logger.Log.Debug("could not parse string to int" + str)
		return false
	}
	return i
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
		if !IsCodeEqual(&codeImpact.SourceCodeSnippet, &codeImpact.SuggestedCodeSnippet) {
			codeImpact.Id = fmt.Sprintf("snippet_%d_%d", fileIndex, codeSnippetIndex)
			snippets = append(snippets, *codeImpact)
			codeSnippetIndex++
		} else {
			logger.Log.Debug("not emmitting as code snippets are equal")
		}
	}
	generalWarnings := []string{}
	if result["general_warnings"] != nil {
		generalWarnings = ParseStringArrayInterface(result["general_warnings"].([]any))
	}
	return snippets, generalWarnings, nil
}

func ParseDaoFileChanges(fileAnalyzerResponse string, projectPath, filePath string, fileIndex int) ([]Snippet, []QueryTranslationResult, error) {
	var result map[string]any
	err := json.Unmarshal([]byte(fileAnalyzerResponse), &result)
	if err != nil {
		return nil, nil, err
	}
	snippets := []Snippet{}
	queryResults := []QueryTranslationResult{}
	codeSnippetIndex := 0
	codeChanges, ok := result["code_changes"].([]any)
	if !ok {
		return nil, nil, fmt.Errorf("missing code_changes array in response")
	}
	for _, codeChangeRaw := range codeChanges {
		cc := codeChangeRaw.(map[string]any)
		snippet := Snippet{
			NumberOfAffectedLines: ParseAnyToInteger(cc["number_of_affected_lines"]),
			SourceCodeSnippet:     ParseStringArrayInterface(cc["existing_code_lines"]),
			SuggestedCodeSnippet:  ParseStringArrayInterface(cc["new_code_lines"]),
			RelativeFilePath:      GetRelativeFilePath(projectPath, filePath),
			FilePath:              filePath,
			IsDao:                 true,
		}
		if cc["schema_change"] != nil {
			sc := cc["schema_change"].(map[string]any)
			snippet.TableName = ParseAnyToString(sc["table"])
			snippet.ColumnName = ParseAnyToString(sc["column"])
			snippet.SchemaChange = ParseAnyToString(cc["explanation"])
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
			queryResult := QueryTranslationResult{
				OriginalQuery:           ParseAnyToString(qc["old_query"]),
				NormalizedQuery:         ParseAnyToString(qc["normalized_query"]),
				SpannerQuery:            ParseAnyToString(qc["new_query"]),
				Explanation:             ParseAnyToString(qc["explanation"]),
				Complexity:              ParseAnyToString(qc["complexity"]),
				Source:                  "app_code",
				SnippetId:               snippet.Id,
				NumberOfQueryOccurances: ParseAnyToInteger(qc["number_of_query_occurances"]),
				CrossDBJoins:            ParseAnyToBool(qc["cross_db_joins"]),
				SourceTablesAffected:    ParseStringArrayInterface(qc["tables_affected"]),
				FunctionsUsed:           ParseStringArrayInterface(qc["functions_used"]),
				OperatorsUsed:           ParseStringArrayInterface(qc["operators_used"]),
				DatabasesReferenced:     ParseStringArrayInterface(qc["databases_referenced"]),
				SelectForUpdate:         ParseAnyToBool(qc["select_for_update"]),
			}
			queryResults = append(queryResults, queryResult)
		}
	}
	return snippets, queryResults, nil
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
		snippets, queryResults, err = ParseDaoFileChanges(fileAnalyzerResponse, projectPath, filePath, fileIndex)
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

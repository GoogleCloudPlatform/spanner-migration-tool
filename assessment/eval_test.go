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
	"context"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"

	assessment "github.com/GoogleCloudPlatform/spanner-migration-tool/assessment/collectors"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"go.uber.org/zap"
)

func init() {
	logger.Log = zap.NewNop()
}

type TestCase struct {
	FilePath     string   `json:"filePath"`
	CodeContent  string   `json:"code_content"`
	SourceSchema string   `json:"source_Schema"`
	TargetSchema string   `json:"target_schema"`
	GroundTruth  []string `json:"ground_truth"`
}

func calculateMetrics(predicted, groundTruth []string) (precision, recall, f1Score float64) {
	if len(predicted) == 0 && len(groundTruth) == 0 {
		return 1.0, 1.0, 1.0
	}

	predSet, trueSet := make(map[string]bool), make(map[string]bool)
	for _, p := range predicted {
		predSet[p] = true
	}
	for _, t := range groundTruth {
		trueSet[t] = true
	}

	truePositives, falsePositives, falseNegatives := 0, 0, 0
	for p := range predSet {
		if trueSet[p] {
			truePositives++
		} else {
			falsePositives++
		}
	}
	for t := range trueSet {
		if !predSet[t] {
			falseNegatives++
		}
	}

	if truePositives == 0 {
		return 0, 0, 0
	}

	precision = float64(truePositives) / float64(truePositives+falsePositives)
	recall = float64(truePositives) / float64(truePositives+falseNegatives)
	f1Score = 2 * (precision * recall) / (precision + recall)

	return precision, recall, f1Score
}

func mapLinesToNumbers(content string) map[string]string {
	lineMap := make(map[string]string)
	for i, line := range strings.Split(content, "\n") {
		trimmedLine := strings.TrimSpace(line)
		lineMap[trimmedLine] = strconv.Itoa(i + 1)
	}
	return lineMap
}

func TestAccuracy(t *testing.T) {
	// Skipping the test for now, as it's used to assess the accuracy of Application Code Assessment
	t.Skip("Test skipped for now; currently evaluating accuracy of Application Code Assessment")
	ctx := context.Background()

	projectID := os.Getenv("SPANNER_MIGRATION_TOOL_TESTS_GCLOUD_PROJECT_ID")
	location := os.Getenv("SPANNER_MIGRATION_TOOL_TESTS_GCLOUD_LOCATION_ID")

	jsonData, err := os.ReadFile("eval_data.json")
	if err != nil {
		t.Fatal("Failed to read JSON data: ", err)
	}

	var testCases []TestCase
	if err := json.Unmarshal(jsonData, &testCases); err != nil {
		t.Fatal("Failed to parse JSON: ", err)
	}

	var totalTruePositives, totalFalsePositives, totalFalseNegatives int

	for i, tc := range testCases {
		language := "go"
		if strings.HasSuffix(tc.FilePath, "java") {
			language = "java"
		}
		summarizer, err := assessment.NewMigrationSummarizer(ctx, nil, projectID, location, tc.SourceSchema, tc.TargetSchema, tc.FilePath, language)

		if err != nil {
			t.Fatal("Failed to initialize migration summarizer: ", err)
		}

		response := summarizer.AnalyzeFile(ctx, tc.FilePath, tc.FilePath, "", tc.CodeContent, i)
		lineMap := mapLinesToNumbers(tc.CodeContent)

		// Extract predicted lines
		uniquePredicted := make(map[string]bool)
		for _, snippet := range *response.CodeAssessment.Snippets {
			for _, line := range snippet.SourceCodeSnippet {
				if lineNum, ok := lineMap[strings.TrimSpace(line)]; ok {
					uniquePredicted[lineNum] = true
				}
			}
		}

		var predictedLines []string
		for line := range uniquePredicted {
			predictedLines = append(predictedLines, line)
		}

		fmt.Println("GroundTruth: ", tc.GroundTruth, "Predicted Lines: ", uniquePredicted)

		precision, recall, f1Score := calculateMetrics(predictedLines, tc.GroundTruth)
		fmt.Printf("Test Case %d - Precision: %.2f, Recall: %.2f, F1 Score: %.2f\n", i+1, precision, recall, f1Score)

		// Track cumulative metrics
		for _, p := range predictedLines {
			if contains(tc.GroundTruth, p) {
				totalTruePositives++
			} else {
				totalFalsePositives++
			}
		}
		for _, t := range tc.GroundTruth {
			if !uniquePredicted[t] {
				totalFalseNegatives++
			}
		}

	}

	// Calculate overall metrics
	totalPrecision := float64(totalTruePositives) / float64(totalTruePositives+totalFalsePositives)
	totalRecall := float64(totalTruePositives) / float64(totalTruePositives+totalFalseNegatives)
	totalF1Score := 2 * (totalPrecision * totalRecall) / (totalPrecision + totalRecall)

	fmt.Printf("\nOverall Accuracy - Precision: %.2f, Recall: %.2f, F1 Score: %.2f\n", totalPrecision, totalRecall, totalF1Score)
}

func contains(slice []string, item string) bool {
	return slices.Contains(slice, item)
}

package assessment

import (
	"encoding/csv"
	"os"
	"testing"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/assessment/utils"
	"github.com/stretchr/testify/assert"
)

func TestGenerateQueryAssessmentReport_MoreCoverage(t *testing.T) {
	queries := []utils.QueryTranslationResult{
		{
			NormalizedQuery:      "SELECT * FROM users WHERE id = ?",
			OriginalQuery:        "SELECT * FROM users WHERE id = 1",
			SpannerQuery:         "SELECT * FROM `users` WHERE `id` = 1",
			SourceTablesAffected: nil,
			Complexity:           "complex",
			ExecutionCount:       0,
			AssessmentSource:     "performance_schema",
			CrossDBJoins:         true,
			SelectForUpdate:      true,
			TranslationError:     "some error",
			FunctionsUsed:        []string{"unsupported_function"},
			OperatorsUsed:        []string{"unsupported_operator"},
			ComparisonAnalysis: utils.ComparisonAnalysis{
				LiteralComparisons: &utils.LiteralComparisonAnalysis{
					PrecisionIssues: []string{"col1"},
				},
				DataTypeComparisons: &utils.DataTypeComparisonAnalysis{
					IncompatibleTypes: []string{"col2"},
				},
				TimestampComparisons: &utils.TimestampComparisonAnalysis{
					TimezoneIssues: []string{"col3"},
				},
				DateComparisons: &utils.DateComparisonAnalysis{
					FormatIssues: []string{"col4"},
				},
			},
			DatabasesReferenced: []string{"db1", "db2"},
			SnippetId:           "snippet123",
		},
	}

	// Create a temporary file for the report
	tmpfile, err := os.CreateTemp("", "query_assessment_report_*.csv")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name()) // clean up

	// Generate the report
	err = GenerateQueryAssessmentReport(queries, tmpfile.Name())
	assert.NoError(t, err)

	// Read the generated report
	f, err := os.Open(tmpfile.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	reader := csv.NewReader(f)
	reader.Comma = '\t'
	records, err := reader.ReadAll()
	if err != nil {
		t.Fatal(err)
	}

	// Verify the report content
	assert.Len(t, records, 2, "Expected 2 records (1 header + 1 data row)")

	// Verify header
	expectedHeader := []string{
		"Query ID", "Query Type", "Normalized Query Text", "Original Query Example",
		"Associated Source Table(s)", "Associated Spanner Table(s)", "Incompatibility Type(s)", "Suggested Spanner Query",
		"Reason for Change", "Estimated Code Change Effort", "Code Change Details", "Number of Executions",
		"Databases Referenced", "Source of Information",
	}
	assert.Equal(t, expectedHeader, records[0])

	// Verify data row
	assert.Equal(t, "q6f540be5", records[1][0])
	assert.Equal(t, "", records[1][4])
	assert.Contains(t, records[1][6], "Cross-DB Join")
	assert.Contains(t, records[1][6], "Unsupported Function: unsupported_function")
	assert.Contains(t, records[1][6], "Unsupported Operator: unsupported_operator")
	assert.Contains(t, records[1][6], "Literal Precision Issues: col1")
	assert.Contains(t, records[1][6], "Incompatible Data Types: col2")
	assert.Contains(t, records[1][6], "Timestamp Timezone Issue: col3")
	assert.Contains(t, records[1][6], "Date Format Issue: col4")
	assert.Equal(t, "High", records[1][9])
	assert.Equal(t, "snippet123", records[1][10])
	assert.Equal(t, "", records[1][11])
	assert.Equal(t, "db1, db2", records[1][12])
}

func TestGenerateQueryAssessmentReport_FileCreationError(t *testing.T) {
	queries := []utils.QueryTranslationResult{}
	// Attempt to create a report in a non-existent directory to trigger an error.
	outputPath := "/non_existent_dir/report.csv"
	err := GenerateQueryAssessmentReport(queries, outputPath)
	assert.Error(t, err)
}

func TestCodeChangeEffort(t *testing.T) {
	assert.Equal(t, "Low", codeChangeEffort("simple"))
	assert.Equal(t, "Medium", codeChangeEffort("moderate"))
	assert.Equal(t, "Medium", codeChangeEffort("medium"))
	assert.Equal(t, "High", codeChangeEffort("complex"))
	assert.Equal(t, "", codeChangeEffort("unknown"))
}

func TestGenerateQueryAssessmentReport(t *testing.T) {
	queries := []utils.QueryTranslationResult{
		{
			NormalizedQuery:       "SELECT * FROM users WHERE id = ?",
			OriginalQuery:         "SELECT * FROM users WHERE id = 1",
			SpannerQuery:          "SELECT * FROM `users` WHERE `id` = 1",
			SourceTablesAffected:  []string{"users"},
			SpannerTablesAffected: []string{"users"},
			Complexity:            "simple",
			ExecutionCount:        100,
			AssessmentSource:      "app_code",
		},
		{
			NormalizedQuery:       "SELECT * FROM products WHERE price > ?",
			OriginalQuery:         "SELECT * FROM products WHERE price > 100.0",
			SpannerQuery:          "",
			SourceTablesAffected:  []string{"products"},
			SpannerTablesAffected: []string{"products"},
			Complexity:            "moderate",
			ExecutionCount:        50,
			AssessmentSource:      "app_code",
			TranslationError:      "Error while translating query",
		},
	}

	// Create a temporary file for the report
	tmpfile, err := os.CreateTemp("", "query_assessment_report_*.csv")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name()) // clean up

	// Generate the report
	err = GenerateQueryAssessmentReport(queries, tmpfile.Name())
	assert.NoError(t, err)

	// Read the generated report
	f, err := os.Open(tmpfile.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	reader := csv.NewReader(f)
	reader.Comma = '\t'
	records, err := reader.ReadAll()
	if err != nil {
		t.Fatal(err)
	}

	// Verify the report content
	assert.Len(t, records, 3, "Expected 3 records (1 header + 2 data rows)")

	// Verify header
	expectedHeader := []string{
		"Query ID", "Query Type", "Normalized Query Text", "Original Query Example",
		"Associated Source Table(s)", "Associated Spanner Table(s)", "Incompatibility Type(s)", "Suggested Spanner Query",
		"Reason for Change", "Estimated Code Change Effort", "Code Change Details", "Number of Executions",
		"Databases Referenced", "Source of Information",
	}
	assert.Equal(t, expectedHeader, records[0])

	// Verify first data row
	assert.Equal(t, "q6f540be5", records[1][0])
	assert.Equal(t, "SELECT", records[1][1])
	assert.Equal(t, "SELECT * FROM users WHERE id = ?", records[1][2])
	assert.Equal(t, "SELECT * FROM users WHERE id = 1", records[1][3])
	assert.Equal(t, "users", records[1][4])
	assert.Equal(t, "users", records[1][5])
	assert.Equal(t, "", records[1][6])
	assert.Equal(t, "SELECT * FROM `users` WHERE `id` = 1", records[1][7])
	assert.Equal(t, "", records[1][8])
	assert.Equal(t, "Low", records[1][9])
	assert.Equal(t, "None/Unavailable", records[1][10])
	assert.Equal(t, "100", records[1][11])
	assert.Equal(t, "", records[1][12])
	assert.Equal(t, "app_code", records[1][13])

	// Verify second data row
	assert.Equal(t, "q41cc943b", records[2][0])
	assert.Equal(t, "SELECT", records[2][1])
	assert.Equal(t, "SELECT * FROM products WHERE price > ?", records[2][2])
	assert.Equal(t, "SELECT * FROM products WHERE price > 100.0", records[2][3])
	assert.Equal(t, "products", records[2][4])
	assert.Equal(t, "products", records[2][5])
	assert.Equal(t, "", records[2][6])
	assert.Equal(t, "", records[2][7])
	assert.Equal(t, "Error while translating query", records[2][8])
	assert.Equal(t, "Medium", records[2][9])
	assert.Equal(t, "None/Unavailable", records[2][10])
	assert.Equal(t, "50", records[2][11])
	assert.Equal(t, "", records[2][12])
	assert.Equal(t, "app_code", records[2][13])
}

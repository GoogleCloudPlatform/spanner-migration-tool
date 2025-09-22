package assessment

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"

	"cloud.google.com/go/vertexai/genai"
	assessment "github.com/GoogleCloudPlatform/spanner-migration-tool/assessment/collectors"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/assessment/utils"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/schema"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
	"github.com/stretchr/testify/assert"
	"google.golang.org/api/option"
)

func TestCombineAndDeduplicateQueries(t *testing.T) {
	// Helper function to find a specific result in a slice.
	findResult := func(results []utils.QueryTranslationResult, normalizedQuery string) (utils.QueryTranslationResult, bool) {
		for _, res := range results {
			if res.NormalizedQuery == normalizedQuery {
				return res, true
			}
		}
		return utils.QueryTranslationResult{}, false
	}

	t.Run("empty inputs should return empty slice", func(t *testing.T) {
		result := combineAndDeduplicateQueries(
			[]utils.QueryAssessmentInfo{},
			&utils.AppCodeAssessmentOutput{QueryTranslationResult: &[]utils.QueryTranslationResult{}},
		)
		assert.Empty(t, result)
	})

	t.Run("only performance schema queries", func(t *testing.T) {
		perfQueries := []utils.QueryAssessmentInfo{
			{Query: "SELECT * FROM users WHERE id = ?", Count: 100},
			{Query: "SELECT * FROM products WHERE id = ?", Count: 50},
		}
		result := combineAndDeduplicateQueries(perfQueries, nil)
		assert.Len(t, result, 2)

		q1, ok1 := findResult(result, "SELECT * FROM users WHERE id = ?")
		assert.True(t, ok1)
		assert.Equal(t, "performance_schema", q1.AssessmentSource)
		assert.Equal(t, 100, q1.ExecutionCount)

		q2, ok2 := findResult(result, "SELECT * FROM products WHERE id = ?")
		assert.True(t, ok2)
		assert.Equal(t, "performance_schema", q2.AssessmentSource)
		assert.Equal(t, 50, q2.ExecutionCount)
	})

	t.Run("only app code queries", func(t *testing.T) {
		appQueries := []utils.QueryTranslationResult{
			{NormalizedQuery: "SELECT * FROM users WHERE id = ?", OriginalQuery: "SELECT * FROM users WHERE id = 1", AssessmentSource: "app_code"},
			{NormalizedQuery: "INSERT INTO orders VALUES (?)", OriginalQuery: "INSERT INTO orders VALUES (1)", AssessmentSource: "app_code"},
		}
		result := combineAndDeduplicateQueries([]utils.QueryAssessmentInfo{}, &utils.AppCodeAssessmentOutput{QueryTranslationResult: &appQueries})
		assert.Len(t, result, 2)

		q1, ok1 := findResult(result, "SELECT * FROM users WHERE id = ?")
		assert.True(t, ok1)
		assert.Equal(t, "app_code", q1.AssessmentSource)
		assert.Equal(t, "SELECT * FROM users WHERE id = 1", q1.OriginalQuery)

		q2, ok2 := findResult(result, "INSERT INTO orders VALUES (?)")
		assert.True(t, ok2)
		assert.Equal(t, "app_code", q2.AssessmentSource)
	})

	t.Run("no common queries", func(t *testing.T) {
		perfQueries := []utils.QueryAssessmentInfo{
			{Query: "SELECT * FROM users WHERE id = ?", Count: 100},
		}
		appQueries := []utils.QueryTranslationResult{
			{NormalizedQuery: "SELECT * FROM products WHERE id = ?", OriginalQuery: "SELECT * FROM products", AssessmentSource: "app_code"},
		}
		result := combineAndDeduplicateQueries(perfQueries, &utils.AppCodeAssessmentOutput{QueryTranslationResult: &appQueries})
		assert.Len(t, result, 2)

		q1, ok1 := findResult(result, "SELECT * FROM users WHERE id = ?")
		assert.True(t, ok1)
		assert.Equal(t, "performance_schema", q1.AssessmentSource)

		q2, ok2 := findResult(result, "SELECT * FROM products WHERE id = ?")
		assert.True(t, ok2)
		assert.Equal(t, "app_code", q2.AssessmentSource)
	})

	t.Run("some common queries should be deduplicated", func(t *testing.T) {
		perfQueries := []utils.QueryAssessmentInfo{
			{Query: "SELECT * FROM users WHERE id = ?", Count: 100}, // Common query
			{Query: "SELECT * FROM orders WHERE id = ?", Count: 25}, // Unique to perf
		}
		appQueries := []utils.QueryTranslationResult{
			{NormalizedQuery: "SELECT * FROM users WHERE id = ?", OriginalQuery: "SELECT * FROM users WHERE id = 1", AssessmentSource: "app_code"}, // Common query
			{NormalizedQuery: "INSERT INTO products values(?)", OriginalQuery: "INSERT INTO products values(1)", AssessmentSource: "app_code"},     // Unique to app code
		}
		result := combineAndDeduplicateQueries(perfQueries, &utils.AppCodeAssessmentOutput{QueryTranslationResult: &appQueries})
		assert.Len(t, result, 3)

		// Check the deduplicated query
		dedupQuery, ok1 := findResult(result, "SELECT * FROM users WHERE id = ?")
		assert.True(t, ok1)
		assert.Equal(t, "SELECT * FROM users WHERE id = 1", dedupQuery.OriginalQuery)
		assert.Equal(t, "app_code, performance_schema", dedupQuery.AssessmentSource)
		assert.Equal(t, 100, dedupQuery.ExecutionCount) // Should be from performance schema

		// Check the unique perf query
		perfQuery, ok2 := findResult(result, "SELECT * FROM orders WHERE id = ?")
		assert.True(t, ok2)
		assert.Equal(t, "SELECT * FROM orders WHERE id = ?", perfQuery.OriginalQuery)
		assert.Equal(t, "performance_schema", perfQuery.AssessmentSource)
		assert.Equal(t, 25, perfQuery.ExecutionCount)

		// Check the unique app code query
		appQuery, ok3 := findResult(result, "INSERT INTO products values(?)")
		assert.True(t, ok3)
		assert.Equal(t, "INSERT INTO products values(1)", appQuery.OriginalQuery)
		assert.Equal(t, "app_code", appQuery.AssessmentSource)
	})
}

func TestPerformQueryAssessment(t *testing.T) {

	ctx := context.Background()
	projectId := "test-project"
	assessmentConfig := map[string]string{"location": "us-central1"}
	conv := &internal.Conv{
		SpDialect: constants.DIALECT_GOOGLESQL,
		SrcSchema: make(map[string]schema.Table),
		SpSchema:  make(map[string]ddl.CreateTable),
	}
	collectors := assessmentCollectors{}

	t.Run("success with a mix of queries", func(t *testing.T) {
		aiClientService.NewClientFunc = func(ctx context.Context, projectID, location string, opts ...option.ClientOption) (*genai.Client, error) {
			return &genai.Client{}, nil
		}

		aiClientService.TranslateQueriesFunc = func(ctx context.Context, queries []utils.QueryTranslationInput, aiClient *genai.Client, mysqlSchema, spannerSchema string) ([]utils.QueryTranslationResult, error) {
			assert.Len(t, queries, 1) // Only one performance schema query should be passed.
			assert.Equal(t, "SELECT * FROM users WHERE id = ?", queries[0].Query)
			return []utils.QueryTranslationResult{
				{
					OriginalQuery:    queries[0].Query,
					SpannerQuery:     "SELECT * FROM `users` WHERE id = ?",
					AssessmentSource: "performance_schema",
				},
			}, nil
		}

		// Input queries: one from perf, one from app code.
		queries := []utils.QueryTranslationResult{
			{OriginalQuery: "SELECT * FROM users", NormalizedQuery: "SELECT * FROM users WHERE id = ?", AssessmentSource: "performance_schema", ExecutionCount: 100},
			{OriginalQuery: "INSERT INTO products", NormalizedQuery: "INSERT INTO products", AssessmentSource: "app_code", ExecutionCount: 50},
		}

		result, err := performQueryAssessment(ctx, collectors, queries, projectId, assessmentConfig, conv)

		assert.NoError(t, err)
		assert.Len(t, result, 2)
		assert.Equal(t, "INSERT INTO products", result[0].OriginalQuery)
		assert.Equal(t, "SELECT * FROM users WHERE id = ?", result[1].OriginalQuery)
		assert.Equal(t, "SELECT * FROM `users` WHERE id = ?", result[1].SpannerQuery)
	})

	t.Run("genai.NewClient returns an error", func(t *testing.T) {
		aiClientService.NewClientFunc = func(ctx context.Context, projectID, location string, opts ...option.ClientOption) (*genai.Client, error) {
			return nil, errors.New("client creation error")
		}

		queries := []utils.QueryTranslationResult{}
		result, err := performQueryAssessment(ctx, collectors, queries, projectId, assessmentConfig, conv)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Error creating ai client")
		assert.Nil(t, result)
	})

	t.Run("TranslateQueriesToSpanner returns an error", func(t *testing.T) {
		aiClientService.NewClientFunc = func(ctx context.Context, projectID, location string, opts ...option.ClientOption) (*genai.Client, error) {
			return &genai.Client{}, nil
		}

		aiClientService.TranslateQueriesFunc = func(ctx context.Context, queries []utils.QueryTranslationInput, aiClient *genai.Client, mysqlSchema, spannerSchema string) ([]utils.QueryTranslationResult, error) {
			return nil, errors.New("translation failed")
		}

		queries := []utils.QueryTranslationResult{
			{OriginalQuery: "SELECT * FROM users", NormalizedQuery: "SELECT * FROM users WHERE id = ?", AssessmentSource: "performance_schema"},
		}

		result, err := performQueryAssessment(ctx, collectors, queries, projectId, assessmentConfig, conv)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Error translating queries")
		assert.Nil(t, result)
	})

	t.Run("input queries is empty", func(t *testing.T) {
		aiClientService.NewClientFunc = func(ctx context.Context, projectID, location string, opts ...option.ClientOption) (*genai.Client, error) {
			return &genai.Client{}, nil
		}

		aiClientService.TranslateQueriesFunc = func(ctx context.Context, queries []utils.QueryTranslationInput, aiClient *genai.Client, mysqlSchema, spannerSchema string) ([]utils.QueryTranslationResult, error) {
			assert.Empty(t, queries)
			return nil, fmt.Errorf("no performance schema queries to translate")
		}

		result, err := performQueryAssessment(ctx, collectors, []utils.QueryTranslationResult{}, projectId, assessmentConfig, conv)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no performance schema queries to translate")
		assert.Nil(t, result)
	})

	t.Run("only non-performance_schema queries", func(t *testing.T) {
		aiClientService.NewClientFunc = func(ctx context.Context, projectID, location string, opts ...option.ClientOption) (*genai.Client, error) {
			return &genai.Client{}, nil
		}

		aiClientService.TranslateQueriesFunc = func(ctx context.Context, queries []utils.QueryTranslationInput, aiClient *genai.Client, mysqlSchema, spannerSchema string) ([]utils.QueryTranslationResult, error) {
			assert.Empty(t, queries)
			return nil, fmt.Errorf("no performance schema queries to translate")
		}

		queries := []utils.QueryTranslationResult{
			{OriginalQuery: "INSERT INTO products", AssessmentSource: "app_code"},
		}

		result, err := performQueryAssessment(ctx, collectors, queries, projectId, assessmentConfig, conv)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no performance schema queries to translate")
		assert.Len(t, result, 1)
		assert.Equal(t, "INSERT INTO products", result[0].OriginalQuery)
	})
}

func TestFetchSpannerTableNames(t *testing.T) {
	baseConv := &internal.Conv{
		SpDialect: constants.DIALECT_GOOGLESQL,
		SrcSchema: map[string]schema.Table{
			"t1": {Name: "table1", Id: "t1"},
			"t2": {Name: "table2", Id: "t2"},
		},
		SpSchema: map[string]ddl.CreateTable{
			"t1": {Name: "sp_table1", Id: "t1"},
		},
	}

	tests := []struct {
		name          string
		tableNames    []string
		setup         func(conv *internal.Conv) // Optional setup for the test case
		expectedNames []string
		expectedErr   string
	}{
		{
			name:          "Success - single table",
			tableNames:    []string{"table1"},
			expectedNames: []string{"sp_table1"},
			expectedErr:   "",
		},
		{
			name:       "Success - multiple tables",
			tableNames: []string{"table1", "table2"},
			setup: func(conv *internal.Conv) {
				conv.SpSchema["t2"] = ddl.CreateTable{Name: "sp_table2", Id: "t2"}
			},
			expectedNames: []string{"sp_table1", "sp_table2"},
			expectedErr:   "",
		},
		{
			name:          "Error - table not in source schema",
			tableNames:    []string{"table3"},
			expectedNames: nil,
			expectedErr:   "error getting table id from source name",
		},
		{
			name:          "Error - spanner table not found",
			tableNames:    []string{"table2"},
			expectedNames: nil,
			expectedErr:   "spanner table not found for source table: table2",
		},
		{
			name:          "Success - empty input",
			tableNames:    []string{},
			expectedNames: []string{},
			expectedErr:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a deep copy of the baseConv for each test run to ensure isolation.
			conv := &internal.Conv{
				SpDialect: baseConv.SpDialect,
				SrcSchema: make(map[string]schema.Table),
				SpSchema:  make(map[string]ddl.CreateTable),
			}
			for k, v := range baseConv.SrcSchema {
				conv.SrcSchema[k] = v
			}
			for k, v := range baseConv.SpSchema {
				conv.SpSchema[k] = v
			}

			if tt.setup != nil {
				tt.setup(conv)
			}

			actualNames, actualErr := fetchSpannerTableNames(conv, tt.tableNames)

			if tt.expectedErr != "" {
				assert.Contains(t, actualErr, tt.expectedErr)
				assert.Nil(t, actualNames)
			} else {
				assert.Empty(t, actualErr)
				assert.Equal(t, tt.expectedNames, actualNames)
			}
		}
	}
}

func TestPerformSchemaAssessment(t *testing.T) {
	ctx := context.Background()

	t.Run("panics if infoSchemaCollector is nil", func(t *testing.T) {
		collectors := assessmentCollectors{
			infoSchemaCollector: nil, // Intentionally nil
		}
		assert.Panics(t, func() {
			performSchemaAssessment(ctx, collectors)
		}, "The code should panic on a nil-pointer dereference")
	})

	t.Run("success with a sample infoSchemaCollector", func(t *testing.T) {
		// 1. Create a sample internal.Conv object.
		conv := &internal.Conv{
			SrcSchema: map[string]schema.Table{
				"t1": {
					Id:   "t1",
					Name: "table1",
					ColDefs: map[string]schema.Column{
						"c1": {Id: "c1", Name: "col1", Type: schema.Type{Name: "int"}, NotNull: true},
						"c2": {Id: "c2", Name: "col2", Type: schema.Type{Name: "varchar", Mods: []int64{20}}},
					},
					PrimaryKeys: []schema.Key{{ColId: "c1", Order: 1}},
					Indexes: []schema.Index{
						{Id: "idx1", Name: "index1", Unique: false, Keys: []schema.Key{{ColId: "c2", Order: 1}}},
					},
				},
			},
			SpSchema: map[string]ddl.CreateTable{
				"t1": {
					Id:   "t1",
					Name: "table1",
					ColDefs: map[string]ddl.ColumnDef{
						"c1": {Id: "c1", Name: "col1", T: ddl.Type{Name: "INT64"}, NotNull: true},
						"c2": {Id: "c2", Name: "col2", T: ddl.Type{Name: "STRING", Len: 20}},
					},
					PrimaryKeys: []ddl.IndexKey{{ColId: "c1", Order: 1}},
					Indexes: []ddl.CreateIndex{
						{Id: "idx1", Name: "index1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Order: 1}}},
					},
				},
			},
			SpSequences: map[string]ddl.Sequence{
				"seq1": {Name: "my_sequence"},
			},
			UsedNames: make(map[string]bool),
		}

		// 2. Create a sample InfoSchemaCollector.
		// In a real test, this would be populated by a mock or a test database.

		tables := map[string]utils.TableAssessmentInfo{
			"t1": {
				Name:      "table1",
				Charset:   "utf8",
				Collation: "utf8_general_ci",
				ColumnAssessmentInfos: map[string]utils.ColumnAssessmentInfo[any]{
					"c1": {MaxColumnSize: 8},
					"c2": {MaxColumnSize: 20},
				},
			},
		}
		indexes := []utils.IndexAssessmentInfo{
			{IndexDef: schema.Index{Id: "idx1", Name: "index1"}, TableId: "t1", Ty: "BTREE"},
		}
		triggers := []utils.TriggerAssessmentInfo{{Name: "my_trigger", TargetTable: "table1", Operation: "INSERT"}}
		storedProcedures := []utils.StoredProcedureAssessmentInfo{{Name: "my_sp", Definition: "BEGIN ... END;"}}
		functions := []utils.FunctionAssessmentInfo{{Name: "my_func", Definition: "RETURN 1;"}}
		views := []utils.ViewAssessmentInfo{{Name: "my_view", Definition: "SELECT * FROM table1"}}

		infoSchemaCollector, err := assessment.BuildInforSchemaCollector(tables, indexes, triggers, storedProcedures, functions, views, conv)
		assert.NoError(t, err)

		collectors := assessmentCollectors{
			infoSchemaCollector: &infoSchemaCollector,
		}

		// 4. Call performSchemaAssessment.
		result, err := performSchemaAssessment(ctx, collectors)

		// 5. Assert results.
		assert.NoError(t, err)
		assert.NotNil(t, result)

		// Table assessments
		assert.Len(t, result.TableAssessmentOutput, 1)
		tableAssessment := result.TableAssessmentOutput[0]
		assert.Equal(t, "table1", tableAssessment.SourceTableDef.Name)
		assert.Equal(t, "table1", tableAssessment.SpannerTableDef.Name)
		assert.False(t, tableAssessment.CompatibleCharset)      // utf8 is not compatible
		assert.Equal(t, 1, tableAssessment.SizeIncreaseInBytes) // dummy implementation

		// Column assessments
		assert.Len(t, tableAssessment.Columns, 2)
		// Sort columns for consistent test results
		sort.Slice(tableAssessment.Columns, func(i, j int) bool {
			return tableAssessment.Columns[i].SourceColDef.Name < tableAssessment.Columns[j].SourceColDef.Name
		})
		col1Assessment := tableAssessment.Columns[0]
		assert.Equal(t, "col1", col1Assessment.SourceColDef.Name)
		assert.Equal(t, "col1", col1Assessment.SpannerColDef.Name)
		assert.True(t, col1Assessment.CompatibleDataType)
		assert.Equal(t, 8, col1Assessment.SizeIncreaseInBytes)

		// Other schema objects
		assert.Len(t, result.TriggerAssessmentOutput, 1)
		assert.Len(t, result.StoredProcedureAssessmentOutput, 1)
		assert.Len(t, result.FunctionAssessmentOutput, 1)
		assert.Len(t, result.ViewAssessmentOutput, 1)
		assert.Len(t, result.SpSequences, 1)
	})
}

func TestPerformAppAssessment(t *testing.T) {
	ctx := context.Background()

	t.Run("returns nil when app collector is nil", func(t *testing.T) {
		collectors := assessmentCollectors{
			appAssessmentCollector: nil,
		}
		output, err := performAppAssessment(ctx, collectors)
		assert.NoError(t, err)
		assert.Nil(t, output)
	})

	// Note: Testing the success and error paths of performAppAssessment is difficult
	// as it requires mocking `AnalyzeProject` on the concrete `assessment.MigrationCodeSummarizer` type.
	// A full test would require either refactoring the production code to use an interface
	// for the collector, or setting up a complex integration test.
}

func TestIsCharsetCompatible(t *testing.T) {
	testCases := []struct {
		name    string
		charset string
		want    bool
	}{
		{
			name:    "compatible charset (non-utf8)",
			charset: "latin1",
			want:    true,
		},
		{
			name:    "incompatible charset (utf8)",
			charset: "utf8",
			want:    false,
		},
		{
			name:    "incompatible charset (utf8mb4)",
			charset: "utf8mb4",
			want:    false,
		},
		{
			name:    "empty charset string",
			charset: "",
			want:    true,
		},
		{
			name:    "case sensitivity check (UTF8)",
			charset: "UTF8",
			want:    true, // strings.Contains is case-sensitive
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := isCharsetCompatible(tc.charset)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestTableSizeDiffBytes(t *testing.T) {
	// Note: This test reflects the current dummy implementation.
	// It should be updated when the function logic is implemented.
	t.Run("dummy implementation returns 1", func(t *testing.T) {
		got := tableSizeDiffBytes(&utils.SrcTableDetails{}, &utils.SpTableDetails{})
		assert.Equal(t, 1, got, "Expected dummy implementation to return 1")
	})
}

func TestGetSpColSizeBytes(t *testing.T) {
	const mb = 10 * 1024 * 1024 // 10MB
	testCases := []struct {
		name  string
		input utils.SpColumnDetails
		want  int64
	}{
		{
			name:  "ARRAY type",
			input: utils.SpColumnDetails{Datatype: "ARRAY"},
			want:  mb,
		},
		{
			name:  "BOOL type",
			input: utils.SpColumnDetails{Datatype: "BOOL"},
			want:  8 + 1,
		},
		{
			name:  "BYTES type with length",
			input: utils.SpColumnDetails{Datatype: "BYTES", Len: 256},
			want:  8 + 256,
		},
		{
			name:  "DATE type",
			input: utils.SpColumnDetails{Datatype: "DATE"},
			want:  8 + 4,
		},
		{
			name:  "FLOAT32 type",
			input: utils.SpColumnDetails{Datatype: "FLOAT32"},
			want:  8 + 4,
		},
		{
			name:  "FLOAT64 type",
			input: utils.SpColumnDetails{Datatype: "FLOAT64"},
			want:  8 + 8,
		},
		{
			name:  "INT64 type",
			input: utils.SpColumnDetails{Datatype: "INT64"},
			want:  8 + 8,
		},
		{
			name:  "JSON type",
			input: utils.SpColumnDetails{Datatype: "JSON"},
			want:  mb,
		},
		{
			name:  "NUMERIC type",
			input: utils.SpColumnDetails{Datatype: "NUMERIC"},
			want:  8 + 22,
		},
		{
			name:  "PROTO type with length",
			input: utils.SpColumnDetails{Datatype: "PROTO", Len: 1024},
			want:  8 + 1024,
		},
		{
			name:  "STRING type with length",
			input: utils.SpColumnDetails{Datatype: "STRING", Len: 512},
			want:  8 + 512,
		},
		{
			name:  "STRUCT type",
			input: utils.SpColumnDetails{Datatype: "STRUCT"},
			want:  mb,
		},
		{
			name:  "TIMESTAMP type",
			input: utils.SpColumnDetails{Datatype: "TIMESTAMP"},
			want:  12, // This case returns directly
		},
		{
			name:  "Default case for unknown type",
			input: utils.SpColumnDetails{Datatype: "UNKNOWN_TYPE"},
			want:  8, // This case returns directly
		},
		{
			name:  "Case-insensitivity check (string)",
			input: utils.SpColumnDetails{Datatype: "string", Len: 100},
			want:  8 + 100,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := getSpColSizeBytes(tc.input)
			assert.Equal(t, tc.want, got)
		})
	}
}

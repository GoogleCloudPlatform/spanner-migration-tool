package utils

import (
	"context"
	"errors"
	"sync"
	"testing"

	"cloud.google.com/go/vertexai/genai"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/task"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"
)

func init() {
	logger.Log = zap.NewNop()
}

// mockGenerativeModel is a mock implementation of the genai.GenerativeModel for testing.
type mockGenerativeModel struct {
	GenerateContentFunc func(ctx context.Context, parts ...genai.Part) (*genai.GenerateContentResponse, error)
}

func (m *mockGenerativeModel) GenerateContent(ctx context.Context, parts ...genai.Part) (*genai.GenerateContentResponse, error) {
	if m.GenerateContentFunc != nil {
		return m.GenerateContentFunc(ctx, parts...)
	}
	return nil, errors.New("GenerateContentFunc not implemented")
}

type MockLLMRetryClient struct {
	mock.Mock
}

func (m *MockLLMRetryClient) GenerateContentWithRetry(ctx context.Context, model *genai.GenerativeModel, parts genai.Part, i int, log *zap.Logger) (*genai.GenerateContentResponse, error) {
	args := m.Called(ctx, model, parts, i, log)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*genai.GenerateContentResponse), args.Error(1)
}

// Mock data for embedded files
var (
	mockQueryTranslationPromptTemplate = "Translate this query: {{MYSQL_QUERY}}\nWith schemas:\nMySQL: {{MYSQL_SCHEMA}}\nSpanner: {{SPANNER_SCHEMA}}\n\nExamples: {{QUERY_EXAMPLES}}"
	mockQueryTranslationExamples       = `[{"mysql_query": "SELECT * FROM users", "spanner_query": "SELECT * FROM users"}]`
)

func TestTranslateQueriesToSpanner(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name           string
		queries        []QueryTranslationInput
		mockTaskResult task.TaskResult[*QueryTranslationResult]
		mockTaskError  error
		expectedResult []QueryTranslationResult
		expectedError  bool
	}{
		{
			name: "Successful Translation",
			queries: []QueryTranslationInput{
				{Query: "SELECT * FROM users", Count: 10},
			},
			mockTaskResult: task.TaskResult[*QueryTranslationResult]{
				Result: &QueryTranslationResult{
					OriginalQuery:  "SELECT * FROM users",
					SpannerQuery:   "SELECT * FROM users",
					Source:         "performance_schema",
					ExecutionCount: 10,
				},
			},
			mockTaskError: nil,
			expectedResult: []QueryTranslationResult{
				{
					OriginalQuery:  "SELECT * FROM users",
					SpannerQuery:   "SELECT * FROM users",
					Source:         "performance_schema",
					ExecutionCount: 10,
				},
			},
			expectedError: false,
		},
		{
			name:           "No queries to translate",
			queries:        []QueryTranslationInput{},
			mockTaskResult: task.TaskResult[*QueryTranslationResult]{},
			mockTaskError:  nil,
			expectedResult: nil,
			expectedError:  true,
		},
		{
			name: "Translation failure",
			queries: []QueryTranslationInput{
				{Query: "SELECT * FROM users", Count: 10},
			},
			mockTaskResult: task.TaskResult[*QueryTranslationResult]{
				Result: &QueryTranslationResult{
					OriginalQuery:    "SELECT * FROM users",
					TranslationError: "mock translation error",
				},
			},
			mockTaskError: errors.New("mock translation error"),
			expectedResult: []QueryTranslationResult{
				{
					OriginalQuery:    "SELECT * FROM users",
					TranslationError: "mock translation error",
				},
			},
			expectedError: false, // The function should not return an error, but the result will contain one.
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			originalTranslateQueryTask := TranslateQueryTask
			TranslateQueryTask = func(input *LLMQueryTranslationInput, mutex *sync.Mutex) task.TaskResult[*QueryTranslationResult] {
				assert.Equal(t, tt.queries[0].Query, input.MySQLQuery)
				return tt.mockTaskResult
			}
			defer func() { TranslateQueryTask = originalTranslateQueryTask }()

			var result []QueryTranslationResult
			var err error

			if len(tt.queries) == 0 {
				result, err = TranslateQueriesToSpanner(ctx, tt.queries, nil, "", "")
			} else {
				result, err = TranslateQueriesToSpanner(ctx, tt.queries, nil, "", "")
			}

			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			// Clean up the expected result's error message for comparison
			if len(tt.expectedResult) > 0 && tt.expectedResult[0].TranslationError != "" {
				if result[0].TranslationError != "" {
					assert.Contains(t, result[0].TranslationError, tt.expectedResult[0].TranslationError)
				} else {
					assert.Fail(t, "expected translation error, but got none")
				}
			} else {
				assert.Equal(t, tt.expectedResult, result)
			}
		})
	}
}

func TestTranslateQueryTask(t *testing.T) {
	ctx := context.Background()

	queryTranslationPromptTemplate = mockQueryTranslationPromptTemplate
	QueryTranslationExamples = []byte(mockQueryTranslationExamples)

	tests := []struct {
		name           string
		input          *LLMQueryTranslationInput
		mockResponse   *genai.GenerateContentResponse
		mockError      error
		expectedResult *QueryTranslationResult
		expectedError  bool
	}{
		{
			name: "Successful Translation",
			input: &LLMQueryTranslationInput{
				Context:       ctx,
				MySQLQuery:    "SELECT * FROM users",
				MySQLSchema:   "mysql_schema",
				SpannerSchema: "spanner_schema",
				Count:         10,
				AIClient:      &genai.Client{},
			},
			mockResponse: &genai.GenerateContentResponse{
				Candidates: []*genai.Candidate{
					{
						Content: &genai.Content{
							Parts: []genai.Part{genai.Text(`{"new_query": "SELECT * FROM users"}`)}, // Corrected JSON
						},
					},
				},
			},
			mockError: nil,
			expectedResult: &QueryTranslationResult{
				OriginalQuery:  "SELECT * FROM users",
				SpannerQuery:   "SELECT * FROM users",
				Source:         "performance_schema",
				ExecutionCount: 10,
			},
			expectedError: false,
		},
		{
			name: "AI Client not provided",
			input: &LLMQueryTranslationInput{
				Context:    ctx,
				MySQLQuery: "SELECT * FROM products",
				Count:      5,
				AIClient:   nil,
			},
			expectedResult: &QueryTranslationResult{
				OriginalQuery:    "SELECT * FROM products",
				TranslationError: "AI client not provided",
			},
			expectedError: false,
		},
		{
			name: "LLM returns an error",
			input: &LLMQueryTranslationInput{
				Context:       ctx,
				MySQLQuery:    "SELECT * FROM products",
				MySQLSchema:   "mysql_schema",
				SpannerSchema: "spanner_schema",
				Count:         5,
				AIClient:      &genai.Client{},
			},
			mockError: errors.New("LLM API error"),
			expectedResult: &QueryTranslationResult{
				OriginalQuery:    "SELECT * FROM products",
				TranslationError: "LLM error: LLM API error",
			},
			expectedError: true,
		},
		{
			name: "LLM returns invalid JSON",
			input: &LLMQueryTranslationInput{
				Context:       ctx,
				MySQLQuery:    "SELECT * FROM orders",
				MySQLSchema:   "mysql_schema",
				SpannerSchema: "spanner_schema",
				Count:         15,
				AIClient:      &genai.Client{},
			},
			mockResponse: &genai.GenerateContentResponse{
				Candidates: []*genai.Candidate{
					{
						Content: &genai.Content{
							Parts: []genai.Part{genai.Text(`invalid json`)},
						},
					},
				},
			},
			mockError: nil,
			expectedResult: &QueryTranslationResult{
				OriginalQuery:    "SELECT * FROM orders",
				TranslationError: "JSON parsing error: invalid character 'i' looking for beginning of value",
			},
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockRetryClient := new(MockLLMRetryClient)

			if tt.mockResponse != nil || tt.mockError != nil {
				mockRetryClient.On("GenerateContentWithRetry", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(tt.mockResponse, tt.mockError)
			} else {
				tt.input.RetryClient = nil
			}

			if tt.input.AIClient != nil {
				tt.input.RetryClient = mockRetryClient
			}

			result := defaultTranslateQueryTask(tt.input, &sync.Mutex{})

			if tt.expectedError {
				assert.Error(t, result.Err)
			} else {
				assert.NoError(t, result.Err)
			}
			if result.Result != nil {
				assert.EqualExportedValues(t, tt.expectedResult, result.Result)
			} else {
				assert.Nil(t, tt.expectedResult)
			}

			mockRetryClient.AssertExpectations(t)
		})
	}
}

func TestBuildTranslationPrompt(t *testing.T) {
	mysqlQuery := "SELECT * FROM users"
	mysqlSchema := "CREATE TABLE users (id INT, name VARCHAR(255))"
	spannerSchema := "CREATE TABLE users (id INT64, name STRING(255))"

	prompt := buildTranslationPrompt(mysqlQuery, mysqlSchema, spannerSchema)

	assert.Contains(t, prompt, mysqlQuery)
	assert.Contains(t, prompt, mysqlSchema)
	assert.Contains(t, prompt, spannerSchema)
	assert.Contains(t, prompt, string(QueryTranslationExamples))
}

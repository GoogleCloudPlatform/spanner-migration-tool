/*
	Copyright 2025 Google LLC

//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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
	"database/sql"
	"fmt"
	"testing"

	"cloud.google.com/go/vertexai/genai"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/assessment/collectors/common"
	sourcesCommon "github.com/GoogleCloudPlatform/spanner-migration-tool/assessment/sources"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/assessment/utils"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/profiles"
	"github.com/stretchr/testify/assert"
)

type mockPerformanceSchema struct {
	queries []utils.QueryAssessmentInfo
	err     error
}

func (m *mockPerformanceSchema) GetAllQueries() ([]utils.QueryAssessmentInfo, error) {
	return m.queries, m.err
}

func (m *mockPerformanceSchema) GetTopQueries(topN int) ([]utils.QueryAssessmentInfo, error) {
	if m.err != nil {
		return nil, m.err
	}
	if topN > len(m.queries) {
		topN = len(m.queries)
	}
	return m.queries[:topN], nil
}

type mockDBConnector struct {
	db  *sql.DB
	err error
}

func (m *mockDBConnector) Connect(driver string, config interface{}) (*sql.DB, error) {
	return m.db, m.err
}

type mockConnectionConfigProvider struct {
	config interface{}
	err    error
}

func (m *mockConnectionConfigProvider) GetConnectionConfig(sourceProfile profiles.SourceProfile) (interface{}, error) {
	return m.config, m.err
}

func mockPerformanceSchemaProvider(db *sql.DB, sourceProfile profiles.SourceProfile) (sourcesCommon.PerformanceSchema, error) {
	if db == nil {
		return nil, fmt.Errorf("database connection is nil")
	}
	return &mockPerformanceSchema{
		queries: []utils.QueryAssessmentInfo{
			{Query: "SELECT * FROM users", Count: 10},
			{Query: "SELECT * FROM products", Count: 20},
		},
		err: nil,
	}, nil
}

func mockPerformanceSchemaProviderWithError(db *sql.DB, sourceProfile profiles.SourceProfile) (sourcesCommon.PerformanceSchema, error) {
	return nil, fmt.Errorf("error getting performance schema")
}

func TestGetPerformanceSchemaCollector(t *testing.T) {
	db := new(sql.DB)
	tests := []struct {
		name                 string
		sourceProfile        profiles.SourceProfile
		dbConnector          common.DBConnector
		configProvider       common.ConnectionConfigProvider
		perfSchemaProvider   func(*sql.DB, profiles.SourceProfile) (sourcesCommon.PerformanceSchema, error)
		expectError          bool
		expectedQueryCount   int
		expectedErrorMessage string
	}{
		{
			name: "Successful creation",
			sourceProfile: profiles.SourceProfile{
				Driver: "mysql",
			},
			dbConnector: &mockDBConnector{
				db:  db,
				err: nil,
			},
			configProvider: &mockConnectionConfigProvider{
				config: "test-config",
				err:    nil,
			},
			perfSchemaProvider:   mockPerformanceSchemaProvider,
			expectError:          false,
			expectedQueryCount:   2,
			expectedErrorMessage: "",
		},
		{
			name: "Connection config error",
			sourceProfile: profiles.SourceProfile{
				Driver: "mysql",
			},
			dbConnector: &mockDBConnector{},
			configProvider: &mockConnectionConfigProvider{
				err: fmt.Errorf("connection config error"),
			},
			perfSchemaProvider:   nil,
			expectError:          true,
			expectedQueryCount:   0,
			expectedErrorMessage: "failed to get connection config: connection config error",
		},
		{
			name: "DB connection error",
			sourceProfile: profiles.SourceProfile{
				Driver: "mysql",
			},
			dbConnector: &mockDBConnector{
				err: fmt.Errorf("db connection error"),
			},
			configProvider: &mockConnectionConfigProvider{
				config: "test-config",
				err:    nil,
			},
			perfSchemaProvider:   nil,
			expectError:          true,
			expectedQueryCount:   0,
			expectedErrorMessage: "failed to connect to database: db connection error",
		},
		{
			name: "Performance schema provider error",
			sourceProfile: profiles.SourceProfile{
				Driver: "mysql",
			},
			dbConnector: &mockDBConnector{
				db:  db,
				err: nil,
			},
			configProvider: &mockConnectionConfigProvider{
				config: "test-config",
				err:    nil,
			},
			perfSchemaProvider:   mockPerformanceSchemaProviderWithError,
			expectError:          true,
			expectedQueryCount:   0,
			expectedErrorMessage: "failed to get performance schema: error getting performance schema",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			collector, err := GetPerformanceSchemaCollector(tt.sourceProfile, tt.dbConnector, tt.configProvider, tt.perfSchemaProvider)

			if tt.expectError {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedErrorMessage, err.Error())
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedQueryCount, len(collector.queries))
				assert.NotNil(t, collector.generateContentFn)
			}
		})
	}
}

func TestTranslateQueryTask(t *testing.T) {
	collector := PerformanceSchemaCollector{
		queries: []utils.QueryAssessmentInfo{
			{Query: "SELECT * FROM test", Count: 5},
		},
	}
	input := &QueryTranslationInput{
		Context:       context.Background(),
		MySQLQuery:    "SELECT * FROM test",
		MySQLSchema:   "mysql-schema",
		SpannerSchema: "spanner-schema",
		QueryIndex:    0,
		AIClient:      &genai.Client{},
	}

	t.Run("Successful translation", func(t *testing.T) {
		collector.generateContentFn = func(ctx context.Context, model *genai.GenerativeModel, parts ...genai.Part) (*genai.GenerateContentResponse, error) {
			return &genai.GenerateContentResponse{
				Candidates: []*genai.Candidate{
					{
						Content: &genai.Content{
							Parts: []genai.Part{
								genai.Text(`{"new_query": "SELECT * FROM test_spanner"}`),
							},
						},
					},
				},
			}, nil
		}
		taskResult := collector.TranslateQueryTask(input, nil)
		assert.Nil(t, taskResult.Err)
		assert.NotNil(t, taskResult.Result)
		assert.Equal(t, "SELECT * FROM test_spanner", taskResult.Result.Result.SpannerQuery)
	})

	t.Run("LLM error", func(t *testing.T) {
		collector.generateContentFn = func(ctx context.Context, model *genai.GenerativeModel, parts ...genai.Part) (*genai.GenerateContentResponse, error) {
			return nil, fmt.Errorf("llm error")
		}
		taskResult := collector.TranslateQueryTask(input, nil)
		assert.NotNil(t, taskResult.Err)
		assert.Contains(t, taskResult.Result.Result.TranslationError, "LLM error")
	})

	t.Run("JSON parsing error", func(t *testing.T) {
		collector.generateContentFn = func(ctx context.Context, model *genai.GenerativeModel, parts ...genai.Part) (*genai.GenerateContentResponse, error) {
			return &genai.GenerateContentResponse{
				Candidates: []*genai.Candidate{
					{
						Content: &genai.Content{
							Parts: []genai.Part{
								genai.Text(`invalid json`),
							},
						},
					},
				},
			}, nil
		}
		taskResult := collector.TranslateQueryTask(input, nil)
		assert.NotNil(t, taskResult.Err)
		assert.Contains(t, taskResult.Result.Result.TranslationError, "JSON parsing error")
	})
}

func TestTranslateQueriesToSpanner_NoQueries(t *testing.T) {
	collector := PerformanceSchemaCollector{
		queries: []utils.QueryAssessmentInfo{},
	}

	_, err := collector.TranslateQueriesToSpanner(context.Background(), nil, "mysql-schema", "spanner-schema")
	assert.Error(t, err)
	assert.Equal(t, "no queries to translate", err.Error())
}

func TestTranslateQueriesToSpanner_NilAIClient(t *testing.T) {
	collector := PerformanceSchemaCollector{
		queries: []utils.QueryAssessmentInfo{
			{Query: "SELECT * FROM test", Count: 5},
		},
	}

	results, err := collector.TranslateQueriesToSpanner(context.Background(), nil, "mysql-schema", "spanner-schema")
	assert.NoError(t, err)
	assert.NotNil(t, results)
	assert.Equal(t, 1, len(results))
	assert.Equal(t, "AI client not provided", results[0].TranslationError)
}

func TestBuildTranslationPrompt(t *testing.T) {
	collector := PerformanceSchemaCollector{}
	mysqlQuery := "SELECT * FROM test"
	mysqlSchema := "CREATE TABLE test (id INT)"
	spannerSchema := "CREATE TABLE test (id INT64)"

	prompt := collector.buildTranslationPrompt(mysqlQuery, mysqlSchema, spannerSchema)

	assert.Contains(t, prompt, "**MySQL Schema**: "+mysqlSchema)
	assert.Contains(t, prompt, "**Spanner Schema**: "+spannerSchema)
	assert.Contains(t, prompt, "**MySQL Query:**\n```sql\n"+mysqlQuery)
}

func TestListQueries(t *testing.T) {
	queries := []utils.QueryAssessmentInfo{{Query: "SELECT 1"}, {Query: "SELECT 2"}}
	c := PerformanceSchemaCollector{queries: queries}
	assert.Equal(t, queries, c.ListQueries())
}

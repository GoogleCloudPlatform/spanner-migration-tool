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
	"errors"
	"testing"

	"cloud.google.com/go/aiplatform/apiv1/aiplatformpb"
	"github.com/googleapis/gax-go/v2"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/structpb"
)

// fakeClient satisfies PredictionClientInterface
type fakeClient struct {
	predictResp *aiplatformpb.PredictResponse
	predictErr  error
	closeCalled bool
}

func (f *fakeClient) Predict(ctx context.Context, req *aiplatformpb.PredictRequest, _ ...gax.CallOption) (*aiplatformpb.PredictResponse, error) {
	if f.predictErr != nil {
		return nil, f.predictErr
	}
	if f.predictResp != nil {
		return f.predictResp, nil
	}

	// Default response with embedding [0.1, 0.2, 0.3]
	listVal := &structpb.ListValue{
		Values: []*structpb.Value{
			structpb.NewNumberValue(0.1),
			structpb.NewNumberValue(0.2),
			structpb.NewNumberValue(0.3),
		},
	}
	structVal := &structpb.Struct{
		Fields: map[string]*structpb.Value{
			"embeddings": structpb.NewStructValue(&structpb.Struct{
				Fields: map[string]*structpb.Value{
					"values": structpb.NewListValue(listVal),
				},
			}),
		},
	}

	return &aiplatformpb.PredictResponse{
		Predictions: []*structpb.Value{
			structpb.NewStructValue(structVal),
		},
	}, nil
}

func (f *fakeClient) Close() error {
	f.closeCalled = true
	return nil
}

func TestCreateEmbededTextsWithClient(t *testing.T) {
	ctx := context.Background()

	goMysqlMigrationConcept = []byte(`[
		{
			"id": "1",
			"example": "SELECT * FROM users",
			"rewrite": {
				"theory": "simple select",
				"options": [{"mysql_code": "SELECT * FROM users", "spanner_code": "SELECT * FROM users"}]
			}
		}
	]`)

	client := &fakeClient{}
	concepts, err := createEmbededTextsWithClient(ctx, client, "test-proj", "us-central1", "mock-model", "go-sql-driver/mysql_go-sql-spanner")

	assert.NoError(t, err)
	assert.Len(t, concepts, 1)
	assert.InDeltaSlice(t, []float32{0.1, 0.2, 0.3}, concepts[0].Embedding, 0.001)
}

func TestCreateEmbededTextsWithClientJava(t *testing.T) {
	ctx := context.Background()

	javaMysqlMigrationConcept = []byte(`[
		{
			"id": "1",
			"example": "SELECT * FROM users",
			"rewrite": {
				"theory": "simple select",
				"options": [{"mysql_code": "SELECT * FROM users", "spanner_code": "SELECT * FROM users"}]
			}
		}
	]`)

	client := &fakeClient{}
	concepts, err := createEmbededTextsWithClient(ctx, client, "test-proj", "us-central1", "mock-model", "jdbc_jdbc")

	assert.NoError(t, err)
	assert.Len(t, concepts, 1)
	assert.InDeltaSlice(t, []float32{0.1, 0.2, 0.3}, concepts[0].Embedding, 0.001)
}

func TestCreateEmbededTextsWithClient_UnsupportedLanguage(t *testing.T) {
	ctx := context.Background()
	client := &fakeClient{}

	concepts, err := createEmbededTextsWithClient(ctx, client, "test-proj", "us-central1", "mock-model", "python")

	assert.Nil(t, concepts)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported sourceTargetFramework")
}
func TestCreateEmbededTextsWithClient_PredictError(t *testing.T) {
	ctx := context.Background()
	client := &fakeClient{predictErr: errors.New("predict failure")}

	_, err := createEmbededTextsWithClient(ctx, client, "test-proj", "us-central1", "mock-model", "go-sql-driver/mysql_go-sql-spanner")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "predict failure")
}

func TestCreateEmbededTextsWithClient_InvalidJSON(t *testing.T) {
	ctx := context.Background()
	// Temporarily assign invalid JSON
	oldGoConcept := goMysqlMigrationConcept
	goMysqlMigrationConcept = []byte("invalid json")
	defer func() { goMysqlMigrationConcept = oldGoConcept }()

	client := &fakeClient{}
	_, err := createEmbededTextsWithClient(ctx, client, "test-proj", "us-central1", "mock-model", "go-sql-driver/mysql_go-sql-spanner")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid character")
}

func TestFakeClient_CloseCalled(t *testing.T) {
	client := &fakeClient{}
	err := client.Close()
	assert.NoError(t, err)
	assert.True(t, client.closeCalled)
}

func TestCreateQueryExampleEmbeddingsFromFile(t *testing.T) {
	// Save and restore the original mysqlQueryExamples
	oldMysqlQueryExamples := mysqlQueryExamples
	defer func() { mysqlQueryExamples = oldMysqlQueryExamples }()

	// Mock mysqlQueryExamples with a single example
	mysqlQueryExamples = []byte(`[
		{
			"id": "1",
			"example": "SELECT * FROM employees",
			"rewrite": {
				"theory": "simple select",
				"options": [{"mysql_code": "SELECT * FROM employees", "spanner_code": "SELECT * FROM employees"}]
			}
		}
	]`)

	// Patch embeddingsForConceptsFunc to use a fake embedding function
	fakeEmbedFunc := func(project, location string, concepts []MySqlMigrationConcept) ([]MySqlMigrationConcept, error) {
		for i := range concepts {
			concepts[i].Embedding = []float32{0.1, 0.2, 0.3}
		}
		return concepts, nil
	}
	oldEmbeddingsForConceptsFunc := embeddingsForConceptsFunc
	embeddingsForConceptsFunc = fakeEmbedFunc
	defer func() { embeddingsForConceptsFunc = oldEmbeddingsForConceptsFunc }()

	concepts, err := createQueryExampleEmbeddingsFromFile("test-proj", "us-central1")
	assert.NoError(t, err)
	assert.Len(t, concepts, 1)
	assert.Equal(t, "1", concepts[0].ID)
	assert.InDeltaSlice(t, []float32{0.1, 0.2, 0.3}, concepts[0].Embedding, 0.001)
}

func TestCreateQueryExampleEmbeddingsFromFileWithClient(t *testing.T) {
	oldMysqlQueryExamples := mysqlQueryExamples
	defer func() { mysqlQueryExamples = oldMysqlQueryExamples }()
	mysqlQueryExamples = []byte(`[
		{
			"id": "1",
			"example": "SELECT * FROM employees",
			"rewrite": {
				"theory": "simple select",
				"options": [{"mysql_code": "SELECT * FROM employees", "spanner_code": "SELECT * FROM employees"}]
			}
		}
	]`)
	client := &fakeClient{}
	concepts, err := createQueryExampleEmbeddingsFromFileWithClient("test-proj", "us-central1", client)
	assert.NoError(t, err)
	assert.Len(t, concepts, 1)
	assert.Equal(t, "1", concepts[0].ID)
	assert.InDeltaSlice(t, []float32{0.1, 0.2, 0.3}, concepts[0].Embedding, 0.001)
}

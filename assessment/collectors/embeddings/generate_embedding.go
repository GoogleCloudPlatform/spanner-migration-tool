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
	_ "embed"
	"encoding/json"
	"fmt"
	"os"

	aiplatform "cloud.google.com/go/aiplatform/apiv1"
	"cloud.google.com/go/aiplatform/apiv1/aiplatformpb"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"github.com/googleapis/gax-go/v2"
	"go.uber.org/zap"
	"google.golang.org/api/option"
	"google.golang.org/protobuf/types/known/structpb"
)

//go:embed go_concept_examples.json
var goMysqlMigrationConcept []byte

//go:embed java_concept_examples.json
var javaMysqlMigrationConcept []byte

//go:embed vertx_concept_examples.json
var vertxMysqlMigrationConcept []byte

type MySqlMigrationConcept struct {
	ID      string `json:"id"`
	Example string `json:"example"`
	Rewrite struct {
		Theory  string `json:"theory"`
		Options []struct {
			MySQLCode   string `json:"mysql_code"`
			SpannerCode string `json:"spanner_code"`
		} `json:"options"`
	} `json:"rewrite"`
	Embedding []float32 `json:"embedding,omitempty"`
}

// PredictionClientInterface allows mocking
type PredictionClientInterface interface {
	Predict(context.Context, *aiplatformpb.PredictRequest, ...gax.CallOption) (*aiplatformpb.PredictResponse, error)
	Close() error
}

func createEmbededTextsFromFile(project, location, sourceTargetFramework string) ([]MySqlMigrationConcept, error) {
	ctx := context.Background()
	apiEndpoint := fmt.Sprintf("%s-aiplatform.googleapis.com:443", location)
	model := "text-embedding-preview-0815"

	client, err := aiplatform.NewPredictionClient(ctx, option.WithEndpoint(apiEndpoint))
	if err != nil {
		return nil, err
	}
	defer client.Close()

	return createEmbededTextsWithClient(ctx, client, project, location, model, sourceTargetFramework)
}

func createEmbededTextsWithClient(ctx context.Context, client PredictionClientInterface, project, location, model, sourceTargetFramework string) ([]MySqlMigrationConcept, error) {
	var data []byte
	switch sourceTargetFramework {
	case "go-sql-driver/mysql_go-sql-spanner":
		data = goMysqlMigrationConcept
	case "jdbc_jdbc":
		data = javaMysqlMigrationConcept
	case "vertx-mysql-client_vertx-jdbc-client":
		data = vertxMysqlMigrationConcept
	default:
		return nil, fmt.Errorf("unsupported sourceTargetFramework: %s", sourceTargetFramework)
	}

	var concepts []MySqlMigrationConcept
	if err := json.Unmarshal(data, &concepts); err != nil {
		return nil, err
	}

	var instances []*structpb.Value
	for _, c := range concepts {
		instances = append(instances, structpb.NewStructValue(&structpb.Struct{
			Fields: map[string]*structpb.Value{
				"content":   structpb.NewStringValue(c.Example),
				"task_type": structpb.NewStringValue("SEMANTIC_SIMILARITY"),
			},
		}))
	}

	req := &aiplatformpb.PredictRequest{
		Endpoint:  fmt.Sprintf("projects/%s/locations/%s/publishers/google/models/%s", project, location, model),
		Instances: instances,
	}

	resp, err := client.Predict(ctx, req)
	if err != nil {
		return nil, err
	}

	for i, prediction := range resp.Predictions {
		values := prediction.GetStructValue().GetFields()["embeddings"].GetStructValue().GetFields()["values"].GetListValue().GetValues()
		embedding := make([]float32, len(values))
		for j, v := range values {
			embedding[j] = float32(v.GetNumberValue())
		}
		concepts[i].Embedding = embedding
	}
	return concepts, nil
}

func embedTextsFromFile(project, location, inputPath, outputPath string) error {
	concepts, err := createEmbededTextsFromFile(project, location, "java")
	if err != nil {
		return err
	}

	output, err := json.MarshalIndent(concepts, "", "  ")
	if err != nil {
		return err
	}

	if err := os.WriteFile(outputPath, output, 0644); err != nil {
		return err
	}

	logger.Log.Debug("Embeddings saved to", zap.String("path", outputPath))
	return nil
}

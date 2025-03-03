//go:build ignore

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
	"log"
	"math"
	"os"
	"sort"

	aiplatform "cloud.google.com/go/aiplatform/apiv1"
	"cloud.google.com/go/aiplatform/apiv1/aiplatformpb"
	"google.golang.org/api/option"
	"google.golang.org/protobuf/types/known/structpb"
)

const EmbeddingModel = "text-embedding-preview-0815"

type MysqlConceptDb struct {
	data map[string]MySqlMigrationConcept
}

func NewMysqlConceptDb(projectId, location string) (*MysqlConceptDb, error) {
	mysqlMigrationConcepts, err := createEmbededTextsFromFile(projectId, location)
	if err != nil {
		return nil, err
	}

	db := &MysqlConceptDb{data: make(map[string]MySqlMigrationConcept)}
	for _, concept := range mysqlMigrationConcepts {
		db.data[concept.ID] = concept
	}
	return db, nil
}

func NewExampleDb(filePath string) (*MysqlConceptDb, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var records []MySqlMigrationConcept
	if err := json.NewDecoder(file).Decode(&records); err != nil {
		return nil, err
	}
	db := &MysqlConceptDb{data: make(map[string]MySqlMigrationConcept)}
	for _, record := range records {
		db.data[record.ID] = record
	}
	return db, nil
}

func cosineSimilarity(a, b []float32) float32 {
	var dotProduct, normA, normB float32
	for i := range a {
		dotProduct += a[i] * b[i]
		normA += a[i] * a[i]
		normB += b[i] * b[i]
	}
	if normA == 0 || normB == 0 {
		return 0
	}
	return dotProduct / (float32(math.Sqrt(float64(normA))) * float32(math.Sqrt(float64(normB))))
}

func embedTexts(project, location string, texts []string) ([][]float32, error) {
	ctx := context.Background()
	client, err := aiplatform.NewPredictionClient(ctx, option.WithEndpoint(location+"-aiplatform.googleapis.com:443"))
	if err != nil {
		return nil, err
	}
	defer client.Close()

	endpoint := fmt.Sprintf("projects/%s/locations/%s/publishers/google/models/%s", project, location, EmbeddingModel)

	instances := make([]*structpb.Value, len(texts))
	for i, text := range texts {
		instances[i] = structpb.NewStructValue(&structpb.Struct{
			Fields: map[string]*structpb.Value{
				"content":   structpb.NewStringValue(text),
				"task_type": structpb.NewStringValue("SEMANTIC_SIMILARITY"),
			},
		})
	}

	req := &aiplatformpb.PredictRequest{
		Endpoint:  endpoint,
		Instances: instances,
	}

	resp, err := client.Predict(ctx, req)
	if err != nil {
		return nil, err
	}

	var embeddings [][]float32
	for _, prediction := range resp.Predictions {
		values := prediction.GetStructValue().Fields["embeddings"].GetStructValue().Fields["values"].GetListValue().Values
		vector := make([]float32, len(values))
		for j, value := range values {
			vector[j] = float32(value.GetNumberValue())
		}

		embeddings = append(embeddings, vector)
	}
	return embeddings, nil
}

func (db *MysqlConceptDb) Search(searchTerms []string, project, location string, distance float32, topK int) map[string]map[string]interface{} {
	if len(searchTerms) == 0 {
		return nil
	}
	searchEmbeddings, err := embedTexts(project, location, searchTerms)

	if err != nil {
		log.Fatalf("Failed to get embeddings: %v", err)
	}

	targetSimilarity := 1 - distance
	var results []struct {
		Similarity float32
		ID         string
	}

	for _, record := range db.data {
		for _, searchEmbedding := range searchEmbeddings {
			similarity := cosineSimilarity(searchEmbedding, record.Embedding)
			if similarity >= targetSimilarity {
				results = append(results, struct {
					Similarity float32
					ID         string
				}{similarity, record.ID})
			}
		}
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].Similarity > results[j].Similarity
	})

	output := make(map[string]map[string]interface{})
	for i := 0; i < topK && i < len(results); i++ {
		record := db.data[results[i].ID]
		b, _ := json.MarshalIndent(record.Rewrite, "", "")
		output[record.ID] = map[string]interface{}{
			"distance": 1 - results[i].Similarity,
			"example":  record.Example,
			"rewrite":  string(b),
		}
	}
	return output
}

// Sample Usage
// func main() {
// 	db, err := NewExampleDb("output.json")
// 	if err != nil {
// 		log.Fatalf("Failed to load database: %v", err)
// 	}

// 	searchResults := db.Search([]string{
// 		"How to migrate from `AUTO_INCREMENT` in PG to Spanner?",
// 	}, "", "", 0.25, 3)

// 	resultJSON, _ := json.MarshalIndent(searchResults, "", "  ")
// 	logger.Log.Debug(string(resultJSON))
// }

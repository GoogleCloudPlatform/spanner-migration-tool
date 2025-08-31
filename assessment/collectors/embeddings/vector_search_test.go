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
	"testing"

	"github.com/stretchr/testify/assert"
)

// fakeEmbedder returns dummy embeddings with fixed values.
func fakeEmbedder(project, location string, texts []string) ([][]float32, error) {
	// Each embedding is a vector of length 3 with arbitrary fixed values.
	embeddings := make([][]float32, len(texts))
	for i := range texts {
		embeddings[i] = []float32{1.0, 0.0, 0.0}
	}
	return embeddings, nil
}

func TestCosineSimilarity(t *testing.T) {
	a := []float32{1, 0, 0}
	b := []float32{0, 1, 0}
	c := []float32{1, 0, 0}
	assert.Equal(t, float32(0), cosineSimilarity(a, b), "Orthogonal vectors should have 0 similarity")
	assert.Equal(t, float32(1), cosineSimilarity(a, c), "Same vectors should have similarity 1")
}
func TestSearch(t *testing.T) {
	// Override the embedTextsFunc to avoid real API calls
	original := embedTextsFunc
	embedTextsFunc = fakeEmbedder
	defer func() { embedTextsFunc = original }()

	// Setup a db with one concept with embedding vector {1,0,0}
	db := &MysqlConceptDb{
		data: map[string]MySqlMigrationConcept{
			"1": {
				ID:      "1",
				Example: "Example SQL",
				Rewrite: struct {
					Theory  string `json:"theory"`
					Options []struct {
						MySQLCode   string `json:"mysql_code"`
						SpannerCode string `json:"spanner_code"`
					} `json:"options"`
				}{
					Theory: "Sample theory",
					Options: []struct {
						MySQLCode   string `json:"mysql_code"`
						SpannerCode string `json:"spanner_code"`
					}{
						{
							MySQLCode:   "SELECT * FROM test;",
							SpannerCode: "SELECT * FROM test_spanner;",
						},
					},
				},
				Embedding: []float32{1.0, 0.0, 0.0},
			},
		},
	}

	results := db.Search([]string{"test"}, "test-project", "us-central1", 0.1, 5)
	assert.NotNil(t, results)
	assert.Contains(t, results, "1")

	result := results["1"]
	assert.InDelta(t, 0.0, result["distance"].(float32), 0.0001)
	assert.Equal(t, "Example SQL", result["example"])
	assert.JSONEq(t, `{
		"theory": "Sample theory",
		"options": [
			{
				"mysql_code": "SELECT * FROM test;",
				"spanner_code": "SELECT * FROM test_spanner;"
			}
		]
	}`, result["rewrite"].(string))
}

func TestSearch_NoTerms(t *testing.T) {
	db := &MysqlConceptDb{data: make(map[string]MySqlMigrationConcept)}
	results := db.Search([]string{}, "test-project", "us-central1", 0.1, 5)
	assert.Nil(t, results)
}

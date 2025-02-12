package assessment

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"

	aiplatform "cloud.google.com/go/aiplatform/apiv1"
	"cloud.google.com/go/aiplatform/apiv1/aiplatformpb"
	"google.golang.org/api/option"
	"google.golang.org/protobuf/types/known/structpb"
)

type ExampleData struct {
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

func embedTextsFromFile(project, location, filePath, outputPath string) error {
	ctx := context.Background()
	apiEndpoint := fmt.Sprintf("%s-aiplatform.googleapis.com:443", location)
	model := "text-embedding-preview-0815"

	client, err := aiplatform.NewPredictionClient(ctx, option.WithEndpoint(apiEndpoint))
	if err != nil {
		return err
	}
	defer client.Close()

	// Read the JSON file
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		return err
	}

	var examples []ExampleData
	if err := json.Unmarshal(data, &examples); err != nil {
		return err
	}

	instances := make([]*structpb.Value, len(examples))
	for i, example := range examples {
		instances[i] = structpb.NewStructValue(&structpb.Struct{
			Fields: map[string]*structpb.Value{
				"content":   structpb.NewStringValue(example.Example),
				"task_type": structpb.NewStringValue("SEMANTIC_SIMILARITY"),
			},
		})
	}

	req := &aiplatformpb.PredictRequest{
		Endpoint:  fmt.Sprintf("projects/%s/locations/%s/publishers/google/models/%s", project, location, model),
		Instances: instances,
	}

	resp, err := client.Predict(ctx, req)
	if err != nil {
		return err
	}

	for i, prediction := range resp.Predictions {
		values := prediction.GetStructValue().Fields["embeddings"].GetStructValue().Fields["values"].GetListValue().Values
		embeddings := make([]float32, len(values))
		for j, value := range values {
			embeddings[j] = float32(value.GetNumberValue())
		}
		examples[i].Embedding = embeddings
	}

	// Save updated data to a new JSON file
	outputData, err := json.MarshalIndent(examples, "", "  ")
	if err != nil {
		return err
	}

	if err := ioutil.WriteFile(outputPath, outputData, 0644); err != nil {
		return err
	}

	fmt.Println("Embeddings saved to", outputPath)
	return nil
}

// Sample Usage
func main() {
	if err := embedTextsFromFile("span-cloud-testing", "us-central1", "/usr/local/google/home/gauravpurohit/ai/app-migration-poc/concept_examples.json", "output.json"); err != nil {
		fmt.Println("Error:", err)
	}
}

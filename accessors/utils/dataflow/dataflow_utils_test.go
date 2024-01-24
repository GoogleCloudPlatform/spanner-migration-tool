// Copyright 2023 Google LLC
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
package dataflowutils

import (
	"context"
	"fmt"
	"os"
	"testing"

	dataflowaccessor "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/dataflow"
	storageaccessor "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/storage"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func init() {
	logger.Log = zap.NewNop()
}

func TestMain(m *testing.M) {
	res := m.Run()
	os.Exit(res)
}

type StorageAccessorMock struct {
	storageaccessor.StorageAccessorImpl
}

var readAnyFileMock func(ctx context.Context, filePath string) (string, error)

func (sam StorageAccessorMock) ReadAnyFile(ctx context.Context, filePath string) (string, error) {
	return readAnyFileMock(ctx, filePath)
}

func TestUnmarshalDataflowTuningConfig(t *testing.T) {
	testCases := []struct {
		name            string
		readAnyFileMock func(ctx context.Context, filePath string) (string, error)
		expectError     bool
		want            dataflowaccessor.DataflowTuningConfig
	}{
		{
			name: "Basic",
			readAnyFileMock: func(ctx context.Context, filePath string) (string, error) {
				return `{
					"projectId": "test-project",
					"jobName": "test-job-name",
					"location": "us-central1",
					"network": "test-network",
					"subnetwork": "test-subnetwork",
					"hostProjectId": "test-host-project",
					"maxWorkers": 3,
					"numWorkers": 2,
					"serviceAccountEmail": "abc@xyz.com",
					"machineType": "n1-standard-8",
					"additionalUserLabels": {"my": "label"},
					"kmsKeyName": "test-key",
					"gcsTemplatePath": "gs://path",
					"additionalExperiments": ["xyz","123"],
					"enableStreamingEngine": true
				}`, nil
			},
			expectError: false,
			want: dataflowaccessor.DataflowTuningConfig{
				ProjectId:             "test-project",
				JobName:               "test-job-name",
				Location:              "us-central1",
				Network:               "test-network",
				Subnetwork:            "test-subnetwork",
				VpcHostProjectId:      "test-host-project",
				MaxWorkers:            3,
				NumWorkers:            2,
				ServiceAccountEmail:   "abc@xyz.com",
				MachineType:           "n1-standard-8",
				AdditionalUserLabels:  map[string]string{"my": "label"},
				KmsKeyName:            "test-key",
				GcsTemplatePath:       "gs://path",
				AdditionalExperiments: []string{"xyz", "123"},
				EnableStreamingEngine: true,
			},
		},
		{
			name: "Defaults",
			readAnyFileMock: func(ctx context.Context, filePath string) (string, error) {
				return `{}`, nil
			},
			expectError: false,
			want: dataflowaccessor.DataflowTuningConfig{
				ProjectId:             "",
				JobName:               "",
				Location:              "",
				Network:               "",
				Subnetwork:            "",
				VpcHostProjectId:      "",
				MaxWorkers:            0,
				NumWorkers:            0,
				ServiceAccountEmail:   "",
				MachineType:           "",
				AdditionalUserLabels:  nil,
				KmsKeyName:            "",
				GcsTemplatePath:       "",
				AdditionalExperiments: nil,
				EnableStreamingEngine: false,
			},
		},
		{
			name: "ReadAnyFile throws error",
			readAnyFileMock: func(ctx context.Context, filePath string) (string, error) {
				return "", fmt.Errorf("test error")
			},
			expectError: true,
			want:        dataflowaccessor.DataflowTuningConfig{},
		},
		{
			name: "Json unmarshall throws error",
			readAnyFileMock: func(ctx context.Context, filePath string) (string, error) {
				return "{\"abc\"", nil
			},
			expectError: true,
			want:        dataflowaccessor.DataflowTuningConfig{},
		},
	}
	ctx := context.Background()
	saMock := StorageAccessorMock{}
	for _, tc := range testCases {
		readAnyFileMock = tc.readAnyFileMock
		got, err := UnmarshalDataflowTuningConfig(ctx, &saMock, "unused/path/due/to/mock")
		assert.Equal(t, tc.expectError, err != nil)
		assert.Equal(t, tc.want, got)
	}
}

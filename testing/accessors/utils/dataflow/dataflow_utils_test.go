// Copyright 2023 Google LLC
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

// TODO: Refactor this file and other integration tests by moving all common code
// to remove redundancy.

package dataflowutils_test

import (
	"os"
	"testing"

	"cloud.google.com/go/dataflow/apiv1beta3/dataflowpb"
	dataflowutils "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/utils/dataflow"
	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	res := m.Run()
	os.Exit(res)
}

func getTemplateDfRequest() *dataflowpb.LaunchFlexTemplateRequest {
	launchParameters := &dataflowpb.LaunchFlexTemplateParameter{
		JobName:  "test-job",
		Template: &dataflowpb.LaunchFlexTemplateParameter_ContainerSpecGcsPath{ContainerSpecGcsPath: "gs://template/Cloud_Datastream_to_Spanner"},
		Parameters: map[string]string{
			"inputFilePattern":                "gs://inputFilePattern",
			"streamName":                      "my-stream",
			"instanceId":                      "my-instance",
			"databaseId":                      "my-dbName",
			"sessionFilePath":                 "gs://session.json",
			"deadLetterQueueDirectory":        "gs://dlq",
			"transformationContextFilePath":   "gs://transformationContext.json",
			"directoryWatchDurationInMinutes": "480", // Setting directory watch timeout to 8 hours
		},
		Environment: &dataflowpb.FlexTemplateRuntimeEnvironment{
			MaxWorkers:            50,
			NumWorkers:            10,
			ServiceAccountEmail:   "svc-account@google.com",
			TempLocation:          "gs://temp-location",
			MachineType:           "n2-standard-16",
			AdditionalExperiments: []string{"use_runner_V2", "test-experiment"},
			Network:               "my-network",
			Subnetwork:            "my-subnetwork",
			AdditionalUserLabels:  map[string]string{"name": "wrench"},
			KmsKeyName:            "sample-kms-key",
			IpConfiguration:       dataflowpb.WorkerIPAddressConfiguration_WORKER_IP_PRIVATE,
			WorkerRegion:          "test-worker-region",
			WorkerZone:            "test-worker-zone",
			EnableStreamingEngine: true,
			FlexrsGoal:            1,
			StagingLocation:       "gs://staging-location",
		},
	}
	req := &dataflowpb.LaunchFlexTemplateRequest{
		ProjectId:       "test-project",
		LaunchParameter: launchParameters,
		Location:        "us-central1",
	}
	return req
}

func TestGcloudCmdWithAllParams(t *testing.T) {

	req := getTemplateDfRequest()
	expectedCmd := "gcloud dataflow flex-template run test-job " +
		"--project=test-project --region=us-central1 " +
		"--template-file-gcs-location=gs://template/Cloud_Datastream_to_Spanner " +
		"--num-workers 10 --max-workers 50 --service-account-email svc-account@google.com " +
		"--temp-location gs://temp-location --worker-machine-type n2-standard-16 " +
		"--additional-experiments use_runner_V2,test-experiment --network my-network " +
		"--subnetwork my-subnetwork --additional-user-labels name=wrench " +
		"--dataflow-kms-key sample-kms-key --disable-public-ips --worker-region test-worker-region " +
		"--worker-zone test-worker-zone --enable-streaming-engine " +
		"--flexrs-goal FLEXRS_SPEED_OPTIMIZED --staging-location gs://staging-location " +
		"--parameters databaseId=my-dbName,deadLetterQueueDirectory=gs://dlq," +
		"directoryWatchDurationInMinutes=480,inputFilePattern=gs://inputFilePattern," +
		"instanceId=my-instance,sessionFilePath=gs://session.json,streamName=my-stream," +
		"transformationContextFilePath=gs://transformationContext.json"
	assert.Equal(t, expectedCmd, dataflowutils.GetGcloudDataflowCommand(req))
}

func TestGcloudCmdWithPartialParams(t *testing.T) {

	req := getTemplateDfRequest()
	req.LaunchParameter.Parameters = make(map[string]string)
	req.LaunchParameter.Environment.FlexrsGoal = 0
	req.LaunchParameter.Environment.IpConfiguration = 0
	req.LaunchParameter.Environment.EnableStreamingEngine = false
	req.LaunchParameter.Environment.AdditionalExperiments = []string{}
	req.LaunchParameter.Environment.AdditionalUserLabels = make(map[string]string)
	req.LaunchParameter.Environment.WorkerRegion = ""
	req.LaunchParameter.Environment.NumWorkers = 0

	expectedCmd := "gcloud dataflow flex-template run test-job " +
		"--project=test-project --region=us-central1 " +
		"--template-file-gcs-location=gs://template/Cloud_Datastream_to_Spanner " +
		"--max-workers 50 --service-account-email svc-account@google.com " +
		"--temp-location gs://temp-location --worker-machine-type n2-standard-16 " +
		"--network my-network --subnetwork my-subnetwork " +
		"--dataflow-kms-key sample-kms-key " +
		"--worker-zone test-worker-zone " +
		"--staging-location gs://staging-location"
	assert.Equal(t, expectedCmd, dataflowutils.GetGcloudDataflowCommand(req))
}

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
	dataflowaccessor "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/dataflow"
	dataflowutils "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/utils/dataflow"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"github.com/google/go-cmp/cmp"
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

func getParameters() map[string]string {
	return map[string]string{
		"inputFilePattern":                "gs://inputFilePattern",
		"streamName":                      "my-stream",
		"instanceId":                      "my-instance",
		"databaseId":                      "my-dbName",
		"sessionFilePath":                 "gs://session.json",
		"deadLetterQueueDirectory":        "gs://dlq",
		"transformationContextFilePath":   "gs://transformationContext.json",
		"directoryWatchDurationInMinutes": "480", // Setting directory watch timeout to 8 hours
	}
}

func getTuningConfig() dataflowaccessor.DataflowTuningConfig {
	return dataflowaccessor.DataflowTuningConfig{
		ProjectId:             "test-project",
		JobName:               "test-job",
		Location:              "us-central1",
		VpcHostProjectId:      "host-project",
		Network:               "my-network",
		Subnetwork:            "my-subnetwork",
		MaxWorkers:            50,
		NumWorkers:            10,
		ServiceAccountEmail:   "svc-account@google.com",
		MachineType:           "n2-standard-64",
		AdditionalUserLabels:  map[string]string{"name": "wrench"},
		KmsKeyName:            "sample-kms-key",
		GcsTemplatePath:       "gs://template/Cloud_Datastream_to_Spanner",
		AdditionalExperiments: []string{"use_runner_V2", "test-experiment"},
		EnableStreamingEngine: true,
	}
}

func getTemplateDfRequest1() *dataflowpb.LaunchFlexTemplateRequest {
	return &dataflowpb.LaunchFlexTemplateRequest{
		ProjectId: "test-project",
		Location:  "us-central1",
		LaunchParameter: &dataflowpb.LaunchFlexTemplateParameter{
			JobName:    "test-job",
			Template:   &dataflowpb.LaunchFlexTemplateParameter_ContainerSpecGcsPath{ContainerSpecGcsPath: "gs://template/Cloud_Datastream_to_Spanner"},
			Parameters: getParameters(),
			Environment: &dataflowpb.FlexTemplateRuntimeEnvironment{
				MaxWorkers:            50,
				NumWorkers:            10,
				ServiceAccountEmail:   "svc-account@google.com",
				MachineType:           "n2-standard-64",
				AdditionalUserLabels:  map[string]string{"name": "wrench"},
				KmsKeyName:            "sample-kms-key",
				Network:               "my-network",
				Subnetwork:            "https://www.googleapis.com/compute/v1/projects/host-project/regions/us-central1/subnetworks/my-subnetwork",
				IpConfiguration:       dataflowpb.WorkerIPAddressConfiguration_WORKER_IP_PRIVATE,
				AdditionalExperiments: []string{"use_runner_V2", "test-experiment"},
				EnableStreamingEngine: true,
			},
		},
	}
}

func TestGetDataflowLaunchRequestBasic(t *testing.T) {
	params := getParameters()
	cfg := getTuningConfig()
	actual, err := dataflowutils.GetDataflowLaunchRequest(params, cfg)
	if err != nil {
		t.Fail()
	}
	expected := getTemplateDfRequest1()
	assert.True(t, EquateLaunchFlexTemplateRequest(expected, actual))
}

func TestGetDataflowLaunchRequestMissingVpcHost(t *testing.T) {
	params := getParameters()
	cfg := getTuningConfig()
	cfg.VpcHostProjectId = ""
	_, err := dataflowutils.GetDataflowLaunchRequest(params, cfg)
	assert.True(t, err != nil)
}

func TestGetDataflowLaunchRequestNameToLowerCase(t *testing.T) {
	params := getParameters()
	cfg := getTuningConfig()
	cfg.JobName = "CAPITalJobName"
	actual, err := dataflowutils.GetDataflowLaunchRequest(params, cfg)
	if err != nil {
		t.Fail()
	}
	expected := getTemplateDfRequest1()
	expected.LaunchParameter.JobName = "capitaljobname"
	assert.True(t, EquateLaunchFlexTemplateRequest(expected, actual))
}

func getTemplateDfRequest2() *dataflowpb.LaunchFlexTemplateRequest {
	launchParameters := &dataflowpb.LaunchFlexTemplateParameter{
		JobName:    "test-job",
		Template:   &dataflowpb.LaunchFlexTemplateParameter_ContainerSpecGcsPath{ContainerSpecGcsPath: "gs://template/Cloud_Datastream_to_Spanner"},
		Parameters: getParameters(),
		Environment: &dataflowpb.FlexTemplateRuntimeEnvironment{
			MaxWorkers:            50,
			NumWorkers:            10,
			ServiceAccountEmail:   "svc-account@google.com",
			TempLocation:          "gs://temp-location",
			MachineType:           "n2-standard-64",
			AdditionalExperiments: []string{"use_runner_V2", "test-experiment"},
			Network:               "my-network",
			Subnetwork:            "https://www.googleapis.com/compute/v1/projects/host-project/regions/us-central1/subnetworks/my-subnetwork",
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

	req := getTemplateDfRequest2()
	expectedCmd := "gcloud dataflow flex-template run test-job " +
		"--project=test-project --region=us-central1 " +
		"--template-file-gcs-location=gs://template/Cloud_Datastream_to_Spanner " +
		"--num-workers 10 --max-workers 50 --service-account-email svc-account@google.com " +
		"--temp-location gs://temp-location --worker-machine-type n2-standard-64 " +
		"--additional-experiments use_runner_V2,test-experiment --network my-network " +
		"--subnetwork https://www.googleapis.com/compute/v1/projects/host-project/regions/us-central1/subnetworks/my-subnetwork --additional-user-labels name=wrench " +
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

	req := getTemplateDfRequest2()
	req.LaunchParameter.Parameters = make(map[string]string)
	req.LaunchParameter.Environment.FlexrsGoal = 0
	req.LaunchParameter.Environment.IpConfiguration = 0
	req.LaunchParameter.Environment.EnableStreamingEngine = false
	req.LaunchParameter.Environment.AdditionalExperiments = []string{}
	req.LaunchParameter.Environment.AdditionalUserLabels = make(map[string]string)
	req.LaunchParameter.Environment.WorkerRegion = ""
	req.LaunchParameter.Environment.NumWorkers = 0
	req.LaunchParameter.Environment.Network = ""
	req.LaunchParameter.Environment.Subnetwork = ""

	expectedCmd := "gcloud dataflow flex-template run test-job " +
		"--project=test-project --region=us-central1 " +
		"--template-file-gcs-location=gs://template/Cloud_Datastream_to_Spanner " +
		"--max-workers 50 --service-account-email svc-account@google.com " +
		"--temp-location gs://temp-location --worker-machine-type n2-standard-64 " +
		"--dataflow-kms-key sample-kms-key " +
		"--worker-zone test-worker-zone " +
		"--staging-location gs://staging-location"
	assert.Equal(t, expectedCmd, dataflowutils.GetGcloudDataflowCommand(req))
}

func EquateLaunchFlexTemplateRequest(df1 *dataflowpb.LaunchFlexTemplateRequest, df2 *dataflowpb.LaunchFlexTemplateRequest) bool {
	lp1 := df1.LaunchParameter
	lp2 := df2.LaunchParameter
	return (df1.ProjectId == df2.ProjectId &&
		df1.Location == df2.Location &&
		lp1.JobName == lp2.JobName &&
		lp1.Environment.MaxWorkers == lp2.Environment.MaxWorkers &&
		lp1.Environment.NumWorkers == lp2.Environment.NumWorkers &&
		lp1.Environment.ServiceAccountEmail == lp2.Environment.ServiceAccountEmail &&
		lp1.Environment.MachineType == lp2.Environment.MachineType &&
		lp1.Environment.KmsKeyName == lp2.Environment.KmsKeyName &&
		lp1.Environment.Network == lp2.Environment.Network &&
		lp1.Environment.Subnetwork == lp2.Environment.Subnetwork &&
		lp1.Environment.GetIpConfiguration().String() == lp2.Environment.GetIpConfiguration().String() &&
		lp1.Environment.EnableStreamingEngine == lp2.Environment.EnableStreamingEngine &&
		cmp.Equal(lp1.Environment.AdditionalUserLabels, lp2.Environment.AdditionalUserLabels) &&
		cmp.Equal(lp1.Environment.AdditionalExperiments, lp2.Environment.AdditionalExperiments) &&
		lp1.GetContainerSpecGcsPath() == lp2.GetContainerSpecGcsPath())
}

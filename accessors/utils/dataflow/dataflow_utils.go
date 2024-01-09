// Copyright 2024 Google LLC
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
	"fmt"
	"sort"
	"strings"

	"cloud.google.com/go/dataflow/apiv1beta3/dataflowpb"
	dataflowaccessor "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/dataflow"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"golang.org/x/exp/maps"
)

func GetDataflowLaunchRequest(parameters map[string]string, cfg dataflowaccessor.DataflowTuningConfig) (*dataflowpb.LaunchFlexTemplateRequest, error) {
	// If custom network is not selected, use public IP. Typical for internal testing flow.
	vpcSubnetwork := ""
	workerIpAddressConfig := dataflowpb.WorkerIPAddressConfiguration_WORKER_IP_PUBLIC
	if cfg.Network != "" || cfg.Subnetwork != "" {
		workerIpAddressConfig = dataflowpb.WorkerIPAddressConfiguration_WORKER_IP_PRIVATE
		// If subnetwork is not provided, assume network has auto subnet configuration.
		if cfg.Subnetwork != "" {
			if cfg.VpcHostProjectId == "" || cfg.Location == "" {
				return nil, fmt.Errorf("vpc host project id and location must be specified when specifying subnetwork")
			}
			vpcSubnetwork = fmt.Sprintf("https://www.googleapis.com/compute/v1/projects/%s/regions/%s/subnetworks/%s", cfg.VpcHostProjectId, cfg.Location, cfg.Subnetwork)
		}
	}
	request := &dataflowpb.LaunchFlexTemplateRequest{
		ProjectId: cfg.ProjectId,
		LaunchParameter: &dataflowpb.LaunchFlexTemplateParameter{
			JobName:    cfg.JobName,
			Template:   &dataflowpb.LaunchFlexTemplateParameter_ContainerSpecGcsPath{ContainerSpecGcsPath: cfg.GcsTemplatePath},
			Parameters: parameters,
			Environment: &dataflowpb.FlexTemplateRuntimeEnvironment{
				MaxWorkers:            cfg.MaxWorkers,
				NumWorkers:            cfg.NumWorkers,
				ServiceAccountEmail:   cfg.ServiceAccountEmail,
				MachineType:           cfg.MachineType,
				AdditionalUserLabels:  cfg.AdditionalUserLabels,
				KmsKeyName:            cfg.KmsKeyName,
				Network:               cfg.Network,
				Subnetwork:            vpcSubnetwork,
				IpConfiguration:       workerIpAddressConfig,
				AdditionalExperiments: cfg.AdditionalExperiments,
				EnableStreamingEngine: cfg.EnableStreamingEngine,
			},
		},
		Location: cfg.Location,
	}
	logger.Log.Debug(fmt.Sprintf("Flex Template request generated: %+v", request))
	return request, nil
}

// Generate the equivalent gCloud CLI command to launch a dataflow job with the same parameters and environment flags
// as the input body.
func GetGcloudDataflowCommand(req *dataflowpb.LaunchFlexTemplateRequest) string {
	lp := req.LaunchParameter
	templatePath := lp.Template.(*dataflowpb.LaunchFlexTemplateParameter_ContainerSpecGcsPath).ContainerSpecGcsPath
	cmd := fmt.Sprintf("gcloud dataflow flex-template run %s --project=%s --region=%s --template-file-gcs-location=%s %s %s",
		lp.JobName, req.ProjectId, req.Location, templatePath, getEnvironmentFlags(lp.Environment), getParametersFlag(lp.Parameters))
	return strings.Trim(cmd, " ")
}

// Generate the equivalent parameter flag string, returning empty string if none are specified.
func getParametersFlag(parameters map[string]string) string {
	if len(parameters) == 0 {
		return ""
	}
	params := ""
	keys := maps.Keys(parameters)
	sort.Strings(keys)
	for _, k := range keys {
		params = params + k + "=" + parameters[k] + ","
	}
	params = strings.TrimSuffix(params, ",")
	return fmt.Sprintf("--parameters %s", params)
}

// We don't populate all flags in the API because certain flags (like AutoscalingAlgorithm, DumpHeapOnOom etc.)
// are not supported in gCloud.
func getEnvironmentFlags(environment *dataflowpb.FlexTemplateRuntimeEnvironment) string {
	flag := ""
	if environment.NumWorkers != 0 {
		flag += fmt.Sprintf("--num-workers %d ", environment.NumWorkers)
	}
	if environment.MaxWorkers != 0 {
		flag += fmt.Sprintf("--max-workers %d ", environment.MaxWorkers)
	}
	if environment.ServiceAccountEmail != "" {
		flag += fmt.Sprintf("--service-account-email %s ", environment.ServiceAccountEmail)
	}
	if environment.TempLocation != "" {
		flag += fmt.Sprintf("--temp-location %s ", environment.TempLocation)
	}
	if environment.MachineType != "" {
		flag += fmt.Sprintf("--worker-machine-type %s ", environment.MachineType)
	}
	if environment.AdditionalExperiments != nil && len(environment.AdditionalExperiments) > 0 {
		flag += fmt.Sprintf("--additional-experiments %s ", strings.Join(environment.AdditionalExperiments, ","))
	}
	if environment.Network != "" {
		flag += fmt.Sprintf("--network %s ", environment.Network)
	}
	if environment.Subnetwork != "" {
		flag += fmt.Sprintf("--subnetwork %s ", environment.Subnetwork)
	}
	if environment.AdditionalUserLabels != nil && len(environment.AdditionalUserLabels) > 0 {
		flag += fmt.Sprintf("--additional-user-labels %s ", formatAdditionalUserLabels(environment.AdditionalUserLabels))
	}
	if environment.KmsKeyName != "" {
		flag += fmt.Sprintf("--dataflow-kms-key %s ", environment.KmsKeyName)
	}
	if environment.IpConfiguration == dataflowpb.WorkerIPAddressConfiguration_WORKER_IP_PRIVATE {
		flag += "--disable-public-ips "
	}
	if environment.WorkerRegion != "" {
		flag += fmt.Sprintf("--worker-region %s ", environment.WorkerRegion)
	}
	if environment.WorkerZone != "" {
		flag += fmt.Sprintf("--worker-zone %s ", environment.WorkerZone)
	}
	if environment.EnableStreamingEngine {
		flag += "--enable-streaming-engine "
	}
	if environment.FlexrsGoal != dataflowpb.FlexResourceSchedulingGoal_FLEXRS_UNSPECIFIED {
		flag += fmt.Sprintf("--flexrs-goal %s ", environment.FlexrsGoal)
	}
	if environment.StagingLocation != "" {
		flag += fmt.Sprintf("--staging-location %s ", environment.StagingLocation)
	}
	return strings.Trim(flag, " ")
}

func formatAdditionalUserLabels(labels map[string]string) string {
	res := []string{}
	for key, value := range labels {
		res = append(res, fmt.Sprintf("%s=%s", key, value))
	}
	return strings.Join(res, ",")
}

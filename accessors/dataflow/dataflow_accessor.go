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
package dataflowacc

import (
	"context"
	"encoding/json"
	"fmt"

	"cloud.google.com/go/dataflow/apiv1beta3/dataflowpb"
	dataflowclient "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/clients/dataflow"
	storageacc "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/storage"
)

type DataflowTuningConfig struct {
	ProjectId             string            `json:"projectId"`
	JobName               string            `json:"jobName"`
	Location              string            `json:"location"`
	VpcHostProjectId      string            `json:"hostProjectId"`
	Network               string            `json:"network"`
	Subnetwork            string            `json:"subnetwork"`
	MaxWorkers            int32             `json:"maxWorkers"`
	NumWorkers            int32             `json:"numWorkers"`
	ServiceAccountEmail   string            `json:"serviceAccountEmail"`
	MachineType           string            `json:"machineType"`
	AdditionalUserLabels  map[string]string `json:"additionalUserLabels"`
	KmsKeyName            string            `json:"kmsKeyName"`
	GcsTemplatePath       string            `json:"gcsTemplatePath"`
	AdditionalExperiments []string          `json:"additionalExperiments"`
	EnableStreamingEngine bool
}

func GetDataflowLaunchRequest(parameters map[string]string, cfg DataflowTuningConfig) (*dataflowpb.LaunchFlexTemplateRequest, error) {
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
	return &dataflowpb.LaunchFlexTemplateRequest{
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
	}, nil
}

func LaunchDataflowJob(ctx context.Context, launchRequest *dataflowpb.LaunchFlexTemplateRequest) (*dataflowpb.LaunchFlexTemplateResponse, error) {
	dfClient, err := dataflowclient.GetOrCreateClient(ctx)
	if err != nil {
		return nil, err
	}
	respDf, err := dfClient.LaunchFlexTemplate(ctx, launchRequest)
	if err != nil {
		fmt.Printf("flexTemplateRequest: %+v\n", launchRequest)
		return nil, fmt.Errorf("error launching dataflow template: %v", err)
	}
	return respDf, nil
}

func UnmarshalDataflowTuningConfig(ctx context.Context, filePath string) (DataflowTuningConfig, error) {
	jsonStr, err := storageacc.ReadAnyFile(ctx, filePath)
	if err != nil {
		return DataflowTuningConfig{}, err
	}
	tuningCfg := DataflowTuningConfig{}
	err = json.Unmarshal([]byte(jsonStr), &tuningCfg)
	if err != nil {
		return DataflowTuningConfig{}, err
	}
	return tuningCfg, nil
}

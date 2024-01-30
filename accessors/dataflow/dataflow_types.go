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
package dataflowaccessor

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
	EnableStreamingEngine bool              `json:"enableStreamingEngine"`
}

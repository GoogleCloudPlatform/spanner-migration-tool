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
package reverserepl

import (
	"cloud.google.com/go/dataflow/apiv1beta3/dataflowpb"
	dataflowacc "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/dataflow"
)

type JobData struct {
	// Required parameters.
	InstanceId             string `json:"instanceId"`
	DatabaseId             string `json:"databaseId"`
	SessionFilePath        string `json:"sessionFilePath"`
	SourceConnectionConfig string `json:"sourceConnectionConfig"`
	SpannerProjectId       string `json:"spannerProjectId"`
	// Optional parameters.
	JobName                string `json:"jobName"`
	SourceType             string `json:"sourceType"`
	MetadataInstance       string `json:"metadataInstance"`
	MetadataDatabase       string `json:"metadataDatabase"`
	GcsDataDirectory       string `json:"gcsDataDirectory"`
	ChangeStreamName       string `json:"changeStreamName"`
	StartTimestamp         string `json:"startTimestamp"`
	EndTimestamp           string `json:"endTimestamp"`
	WindowDuration         string `json:"windowDuration"`
	FiltrationMode         string `json:"filtrationMode"`
	SourceDbTimezoneOffset string `json:"sourceDbTimezoneOffset"`
	TimerInterval          int    `json:"timerInterval"`
	MetadataTableSuffix    string `json:"metadataTableSuffix"`
	SkipDirectoryName      string `json:"skipDirectoryName"`
	ReaderCfg              string `json:"readerCfg"`
	WriterCfg              string `json:"writerCfg"`
	// SMT generated
	IsSMTBucketRequired bool   `json:"isSMTBucketRequired"`
	SmtBucketName       string `json:"smtBucketName"`
	SpannerLocation     string `json:"spannerLocation"`
}

type CreateReaderJobRequest struct {
	ChangeStreamName     string                           `json:"changeStreamName"`
	InstanceId           string                           `json:"instanceId"`
	DatabaseId           string                           `json:"databaseId"`
	SpannerProjectId     string                           `json:"spannerProjectId"`
	SessionFilePath      string                           `json:"sessionFilePath"`
	SourceShardsFilePath string                           `json:"sourceShardsFilePath"`
	MetadataInstance     string                           `json:"metadataInstance"`
	MetadataDatabase     string                           `json:"metadataDatabase"`
	GcsOutputDirectory   string                           `json:"gcsOutputDirectory"`
	StartTimestamp       string                           `json:"startTimestamp"`
	EndTimestamp         string                           `json:"endTimestamp"`
	WindowDuration       string                           `json:"windowDuration"`
	FiltrationMode       string                           `json:"filtrationMode"`
	MetadataTableSuffix  string                           `json:"metadataTableSuffix"`
	SkipDirectoryName    string                           `json:"skipDirectoryName"`
	TuningCfg            dataflowacc.DataflowTuningConfig `json:"tuningCfg"`
}

type CreateWriterJobRequest struct {
	SourceShardsFilePath   string                           `json:"sourceShardsFilePath"`
	SessionFilePath        string                           `json:"sessionFilePath"`
	SourceType             string                           `json:"sourceType"`
	SourceDbTimezoneOffset string                           `json:"sourceDbTimezoneOffset"`
	TimerInterval          int                              `json:"timerInterval"`
	StartTimestamp         string                           `json:"startTimestamp"`
	WindowDuration         string                           `json:"windowDuration"`
	GCSInputDirectoryPath  string                           `json:"GCSInputDirectoryPath"`
	SpannerProjectId       string                           `json:"spannerProjectId"`
	MetadataInstance       string                           `json:"metadataInstance"`
	MetadataDatabase       string                           `json:"metadataDatabase"`
	MetadataTableSuffix    string                           `json:"metadataTableSuffix"`
	TuningCfg              dataflowacc.DataflowTuningConfig `json:"tuningCfg"`
}

type ResourceData_ChangeStream struct {
	DbURI string `json:"dbURI"`
}

type ResourceData_MetadataDb struct {
	DbURI string `json:"dbURI"`
}

type ResourceData_GCSBucket struct {
	Name          string   `json:"name"`
	ProjectId     string   `json:"projectId"`
	Location      string   `json:"location"`
	Ttl           int64    `json:"ttl"`
	MatchesPrefix []string `json:"matchesPrefix"`
}

type ResourceData_Dataflow struct {
	LaunchRequest       *dataflowpb.LaunchFlexTemplateRequest `json:"launchRequest"`
	EquivalentGcloudCmd string                                `json:"equivalentGcloudCmd"`
}

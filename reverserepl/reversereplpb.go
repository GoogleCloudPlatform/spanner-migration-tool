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
	// SMT generated - These fields are for use internally by SMT. These should not be configured when passing input.
	IsSMTBucketRequired bool   `json:"isSMTBucketRequired"`
	SmtBucketName       string `json:"smtBucketName"`
	// Location of Spanner leader.
	SpannerLocation string `json:"spannerLocation"`
	// GCS location of session file path.
	SessionFileGcsPath string `json:"sessionFileGcsPath"`
	// GCS location of source connection config.
	SourceConnectionConfigGcsPath string `json:"sourceConnectionConfigGcsPath"`
}

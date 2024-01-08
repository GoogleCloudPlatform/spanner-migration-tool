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
package activity

import (
	"context"
	"fmt"

	dataflowacc "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/dataflow"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/utils"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	resource "github.com/GoogleCloudPlatform/spanner-migration-tool/reverserepl/resource"
)

type PrepareDataflowWriterInput struct {
	SmtJobId               string
	SourceShardsFilePath   string
	SessionFilePath        string
	SourceType             string
	SourceDbTimezoneOffset string
	TimerInterval          int
	StartTimestamp         string
	WindowDuration         string
	GCSInputDirectoryPath  string
	SpannerProjectId       string
	MetadataInstance       string
	MetadataDatabase       string
	MetadataTableSuffix    string
	TuningCfg              string
	SpannerLocation        string
}

type PrepareDataflowWriterOutput struct {
	JobId string
}

type PrepareDataflowWriter struct {
	Input  *PrepareDataflowWriterInput
	Output *PrepareDataflowWriterOutput
}

// Launches the writer dataflow job.
func (p *PrepareDataflowWriter) Transaction(ctx context.Context) error {
	input := p.Input
	writerTuningCfg, err := dataflowacc.UnmarshalDataflowTuningConfig(ctx, input.TuningCfg)
	if err != nil {
		return fmt.Errorf("error reading writer tuning config %s: %v", input.TuningCfg, err)
	}
	logger.Log.Debug(fmt.Sprintf("writerTuningCfg: %+v", writerTuningCfg))
	validateUpdateWriterTuningCfg(&writerTuningCfg, input.SpannerProjectId, input.SpannerLocation, input.SmtJobId)
	logger.Log.Debug(fmt.Sprintf("Updated writerTuningCfg: %+v", writerTuningCfg))
	params := map[string]string{
		"sourceShardsFilePath":   input.SourceShardsFilePath,
		"sessionFilePath":        input.SessionFilePath,
		"sourceType":             input.SourceType,
		"sourceDbTimezoneOffset": input.SourceDbTimezoneOffset,
		"timerInterval":          fmt.Sprintf("%v", input.TimerInterval),
		"windowDuration":         input.WindowDuration,
		"GCSInputDirectoryPath":  input.GCSInputDirectoryPath,
		"metadataTableSuffix":    input.MetadataTableSuffix,
		"spannerProjectId":       input.SpannerProjectId,
		"metadataInstance":       input.MetadataInstance,
		"metadataDatabase":       input.MetadataDatabase,
		"startTimestamp":         input.StartTimestamp,
		"runIdentifier":          input.SmtJobId,
		"runMode":                constants.RR_WRITER_REGULAR_MODE,
	}
	dfLaunchReq, err := dataflowacc.GetDataflowLaunchRequest(params, writerTuningCfg)
	if err != nil {
		return err
	}
	dfJobId, err := resource.CreateDataflowSMTResource(ctx, input.SmtJobId, dfLaunchReq)
	if err != nil {
		return err
	}
	logger.Log.Info(fmt.Sprintf("Launched writer job with id: %s", dfJobId))
	p.Output.JobId = dfJobId
	return nil
}

func (p *PrepareDataflowWriter) Compensation(ctx context.Context) error {
	return nil
}

func validateUpdateWriterTuningCfg(cfg *dataflowacc.DataflowTuningConfig, spannerProjectId, spannerLocation, smtJobId string) {
	if cfg.ProjectId == "" {
		cfg.ProjectId = spannerProjectId
	}
	if cfg.JobName == "" {
		cfg.JobName = fmt.Sprintf("smt-writer-job-%s", utils.GenerateHashStr())
	}
	if cfg.Location == "" {
		cfg.Location = spannerLocation
	}
	if cfg.MaxWorkers == 0 {
		cfg.MaxWorkers = 50
	}
	if cfg.NumWorkers == 0 {
		cfg.NumWorkers = 5
	}
	if cfg.MachineType == "" {
		cfg.MachineType = "n1-standard-2"
	}
	cfg.AdditionalUserLabels["smt-writer-job"] = smtJobId
	if cfg.GcsTemplatePath == "" {
		cfg.GcsTemplatePath = constants.REVERSE_REPLICATION_WRITER_TEMPLATE_PATH
	}
}

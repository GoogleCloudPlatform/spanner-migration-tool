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
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"cloud.google.com/go/spanner"
	dataflowacc "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/dataflow"
	spanneracc "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/spanner"
	storageacc "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/storage"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/utils"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/dao"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/helpers"
)

func CreateReverseReplicationJob(ctx context.Context, request JobData) error {
	fmt.Printf("Received Create Reverse Replication job request: %+v\n", request)
	uuid := utils.GenerateHashStr()

	err := validateAndUpdateJobData(ctx, &request, uuid)
	if err != nil {
		fmt.Println("error in validateCreateRequest: %v\n", err)
		return fmt.Errorf("error in validateCreateRequest: %v", err)
	}
	// Check or create the internal metadata database for all flows.
	helpers.CheckOrCreateMetadataDb(request.SpannerProjectId, request.InstanceId)
	smtMetadataDBURI := helpers.GetSpannerUri(request.SpannerProjectId, request.InstanceId)
	// Init dao client.
	_, err = dao.GetOrCreateClient(ctx, smtMetadataDBURI)
	if err != nil {
		return err
	}

	smtJobId := fmt.Sprintf("smt-job-%s", uuid)

	err = InsertReverseReplicationJobEntry(ctx, smtJobId, request)
	if err != nil {
		return err
	}

	if request.IsSMTBucketRequired {
		fmt.Println("Creating bucket")
		err = createBucketSMTResource(ctx, smtJobId, request.SmtBucketName, request.SpannerProjectId, request.SpannerLocation, nil, 45)
		if err != nil {
			return err
		}
		if !strings.HasPrefix(request.SessionFilePath, constants.GCS_FILE_PREFIX) {
			err := storageacc.UploadLocalFileToGCS(ctx, fmt.Sprintf("%s%s/", constants.GCS_FILE_PREFIX, request.SmtBucketName), "session.json", request.SessionFilePath)
			if err != nil {
				return fmt.Errorf("could not upload session file to GCS: %v", err)
			}
			request.SessionFilePath = fmt.Sprintf("%s%s/session.json", constants.GCS_FILE_PREFIX, request.SmtBucketName)
		}
		if !strings.HasPrefix(request.SourceConnectionConfig, constants.GCS_FILE_PREFIX) {
			err := storageacc.UploadLocalFileToGCS(ctx, fmt.Sprintf("%s%s/", constants.GCS_FILE_PREFIX, request.SmtBucketName), "source-connection-config.json", request.SourceConnectionConfig)
			if err != nil {
				return fmt.Errorf("could not upload source connection config file to GCS: %v", err)
			}
			request.SourceConnectionConfig = fmt.Sprintf("%s%s/source-connection-config.json", constants.GCS_FILE_PREFIX, request.SmtBucketName)
		}
		fmt.Println("Created bucket succesfully")
	}

	fmt.Println("Creating changestream")
	targetDbUri := fmt.Sprintf("projects/%s/instances/%s/databases/%s", request.SpannerProjectId, request.InstanceId, request.DatabaseId)
	err = checkOrCreateChangeStream(ctx, request.ChangeStreamName, targetDbUri, smtJobId)
	if err != nil {
		return err
	}
	fmt.Println("Created changestream succesfully")

	fmt.Println("Creating metadata db")
	rrmetadataDbUri := fmt.Sprintf("projects/%s/instances/%s/databases/%s", request.SpannerProjectId, request.MetadataInstance, request.MetadataDatabase)
	err = checkOrCreateMetadataDb(ctx, smtJobId, request.MetadataDatabase, rrmetadataDbUri)
	if err != nil {
		return err
	}
	fmt.Println("Created metadata db succesfuly")

	fmt.Println("launching reader job")
	readerTuningCfg, err := UnmarshalDataflowTuningConfig(ctx, request.ReaderCfg)
	if err != nil {
		return err
	}
	validateUpdateReaderTuningCfg(&readerTuningCfg, request, uuid, smtJobId)
	err = createReaderJob(ctx, CreateReaderJobRequest{
		ChangeStreamName:     request.ChangeStreamName,
		InstanceId:           request.InstanceId,
		DatabaseId:           request.DatabaseId,
		SpannerProjectId:     request.SpannerProjectId,
		SessionFilePath:      request.SessionFilePath,
		SourceShardsFilePath: request.SourceConnectionConfig,
		MetadataInstance:     request.MetadataInstance,
		MetadataDatabase:     request.MetadataDatabase,
		GcsOutputDirectory:   request.GcsDataDirectory,
		StartTimestamp:       request.StartTimestamp,
		EndTimestamp:         request.EndTimestamp,
		WindowDuration:       request.WindowDuration,
		FiltrationMode:       request.FiltrationMode,
		MetadataTableSuffix:  request.MetadataTableSuffix,
		SkipDirectoryName:    request.SkipDirectoryName,
		TuningCfg:            readerTuningCfg,
	}, smtJobId)
	if err != nil {
		return fmt.Errorf("error launching reader job: %v", err)
	}
	fmt.Println("launched reader job")

	fmt.Println("launching writer job")
	writerTuningCfg, err := UnmarshalDataflowTuningConfig(ctx, request.WriterCfg)
	if err != nil {
		return err
	}
	validateUpdateWriterTuningCfg(&writerTuningCfg, request, uuid, smtJobId)
	err = createWriterJob(ctx, CreateWriterJobRequest{
		SourceShardsFilePath:   request.SourceConnectionConfig,
		SessionFilePath:        request.SessionFilePath,
		SourceType:             request.SourceType,
		SourceDbTimezoneOffset: request.SourceDbTimezoneOffset,
		TimerInterval:          request.TimerInterval,
		StartTimestamp:         request.StartTimestamp,
		WindowDuration:         request.WindowDuration,
		GCSInputDirectoryPath:  request.GcsDataDirectory,
		SpannerProjectId:       request.SpannerProjectId,
		MetadataInstance:       request.MetadataInstance,
		MetadataDatabase:       request.MetadataDatabase,
		MetadataTableSuffix:    request.MetadataTableSuffix,
		TuningCfg:              writerTuningCfg,
	}, smtJobId)
	if err != nil {
		return fmt.Errorf("error launching writer job: %v", err)
	}
	fmt.Println("launched writer job")

	dao.UpdateSMTJobState(ctx, smtJobId, "RUNNING")
	fmt.Println("Done creation flow")
	return nil
}

func UnmarshalDataflowTuningConfig(ctx context.Context, filePath string) (dataflowacc.DataflowTuningConfig, error) {
	jsonStr, err := storageacc.ReadAnyFile(ctx, filePath)
	if err != nil {
		return dataflowacc.DataflowTuningConfig{}, err
	}
	tuningCfg := dataflowacc.DataflowTuningConfig{}
	err = json.Unmarshal([]byte(jsonStr), &tuningCfg)
	if err != nil {
		return dataflowacc.DataflowTuningConfig{}, err
	}
	return tuningCfg, nil
}

func InsertReverseReplicationJobEntry(ctx context.Context, smtJobId string, request JobData) error {
	dialect, err := spanneracc.GetDatabaseDialect(ctx, fmt.Sprintf("projects/%s/instances/%s/databases/%s", request.SpannerProjectId, request.InstanceId, request.DatabaseId))
	if err != nil {
		return fmt.Errorf("could not fetch database dialect: %v", err)
	}
	jobData := spanner.NullJSON{Valid: true, Value: request}
	return dao.InsertSMTJobEntry(ctx, smtJobId, request.JobName, constants.REVERSE_REPLICATION_JOB_TYPE, dialect, request.DatabaseId, jobData)
}

func validateAndUpdateJobData(ctx context.Context, request *JobData, uuid string) (err error) {
	request.IsSMTBucketRequired = true
	request.SmtBucketName = fmt.Sprintf("smt-rr-gcs-%s", uuid)
	if strings.HasPrefix(request.SessionFilePath, constants.GCS_FILE_PREFIX) && strings.HasPrefix(request.SourceConnectionConfig, constants.GCS_FILE_PREFIX) && request.GcsDataDirectory != "" {
		request.IsSMTBucketRequired = false
		request.SmtBucketName = ""
	}
	if request.InstanceId == "" {
		return fmt.Errorf("found empty InstanceId which is a required parameter")
	}
	if request.DatabaseId == "" {
		return fmt.Errorf("found empty DatabaseId which is a required parameter")
	}
	if request.SessionFilePath == "" {
		return fmt.Errorf("found empty SessionFilePath which is a required parameter")
	}
	if request.SourceConnectionConfig == "" {
		return fmt.Errorf("found empty SourceConnectionConfig which is a required parameter")
	}
	if request.SpannerProjectId == "" {
		return fmt.Errorf("found empty SpannerProjectId which is a required parameter")
	}
	if request.JobName == "" {
		request.JobName = fmt.Sprintf("smt-job-%s", uuid)
	}
	if request.SourceType == "" {
		request.SourceType = constants.MYSQL
	}
	if request.SourceType != constants.MYSQL {
		return fmt.Errorf("%s is not a valid source type for reverse replication. Only supported source type is mysql", request.SourceType)
	}
	if request.MetadataInstance == "" {
		request.MetadataInstance = request.InstanceId
	}
	if request.MetadataDatabase == "" {
		request.MetadataDatabase = fmt.Sprintf("smt-rr-metadata-%s", uuid)
	}
	if request.GcsDataDirectory == "" {
		request.GcsDataDirectory = fmt.Sprintf("gs://smt-rr-gcs-%s/reverse-replication/data", uuid)
	} else if !strings.HasPrefix(request.GcsDataDirectory, constants.GCS_FILE_PREFIX) {
		return fmt.Errorf("invalid gcs path for GcsDataDirectory: %s", request.GcsDataDirectory)
	}
	if request.ChangeStreamName == "" {
		request.ChangeStreamName = fmt.Sprintf("smt-rr-cs-%s", uuid)
	}
	if request.FiltrationMode == "" {
		request.FiltrationMode = constants.RR_READER_FILTER_FWD
	} else if !slices.Contains([]string{constants.RR_READER_FILTER_FWD, constants.RR_READER_FILTER_NONE}, request.FiltrationMode) {
		return fmt.Errorf("found filtrationMode %s, only allowed values are [%s, %s]", request.FiltrationMode, constants.RR_READER_FILTER_FWD, constants.RR_READER_FILTER_NONE)
	}
	if request.TimerInterval < 1 {
		request.TimerInterval = 1
	}
	if request.WindowDuration == "" {
		request.WindowDuration = "10s"
	}

	// Replace '-' with '_' since hyphens are not allowed in cs names.
	request.ChangeStreamName = strings.Replace(request.ChangeStreamName, "-", "_", -1)

	request.SpannerLocation, err = spanneracc.GetSpannerLeaderLocation(ctx, fmt.Sprintf("projects/%s/instances/%s", request.SpannerProjectId, request.InstanceId))
	return err
}

func validateUpdateReaderTuningCfg(cfg *dataflowacc.DataflowTuningConfig, jobData JobData, uuid, smtJobId string) {
	if cfg.ProjectId == "" {
		cfg.ProjectId = jobData.SpannerProjectId
	}
	if cfg.JobName == "" {
		cfg.JobName = fmt.Sprintf("smt-reader-job-%s", utils.GenerateHashStr())
	}
	if cfg.Location == "" {
		cfg.Location = jobData.SpannerLocation
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
	cfg.AdditionalUserLabels["smt-reader-job"] = smtJobId
	if cfg.GcsTemplatePath == "" {
		cfg.GcsTemplatePath = constants.REVERSE_REPLICATION_READER_TEMPLATE_PATH
	}
	if cfg.AdditionalExperiments == nil {
		cfg.AdditionalExperiments = []string{"use_runner_v2"}
	} else if !slices.Contains(cfg.AdditionalExperiments, "use_runner_v2") {
		cfg.AdditionalExperiments = append(cfg.AdditionalExperiments, "use_runner_v2")
	}
	cfg.EnableStreamingEngine = true
}

func validateUpdateWriterTuningCfg(cfg *dataflowacc.DataflowTuningConfig, jobData JobData, uuid, smtJobId string) {
	if cfg.ProjectId == "" {
		cfg.ProjectId = jobData.SpannerProjectId
	}
	if cfg.JobName == "" {
		cfg.JobName = fmt.Sprintf("smt-writer-job-%s", utils.GenerateHashStr())
	}
	if cfg.Location == "" {
		cfg.Location = jobData.SpannerLocation
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

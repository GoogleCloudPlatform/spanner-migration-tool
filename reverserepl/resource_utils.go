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
	"fmt"

	dataflowacc "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/dataflow"
	spanneracc "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/spanner"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
)

func checkOrCreateChangeStream(ctx context.Context, changeStreamName, dbURI, smtJobId string) error {
	csExists, err := spanneracc.CheckIfChangeStreamExists(ctx, changeStreamName, dbURI)
	if err != nil {
		return err
	}
	if csExists {
		err = spanneracc.ValidateChangeStreamOptions(ctx, changeStreamName, dbURI)
		if err != nil {
			return err
		}
		fmt.Println("Provided change stream already exists, skipping change stream creation")
		return nil
	}
	err = createChangeStreamSMTResource(ctx, smtJobId, changeStreamName, dbURI)
	if err != nil {
		return fmt.Errorf("could not create changestream resource: %v", err)
	}
	return nil
}

func checkOrCreateMetadataDb(ctx context.Context, smtJobId, dbName, dbURI string) error {
	dbExists, err := spanneracc.CheckExistingDb(ctx, dbURI)
	if err != nil {
		return err
	}
	if dbExists {
		return nil
	}
	return createMetadataDbSMTResource(ctx, smtJobId, dbName, dbURI)
}

func createReaderJob(ctx context.Context, req CreateReaderJobRequest, smtJobId string) error {
	params := map[string]string{
		"changeStreamName":     req.ChangeStreamName,
		"instanceId":           req.InstanceId,
		"databaseId":           req.DatabaseId,
		"spannerProjectId":     req.SpannerProjectId,
		"metadataInstance":     req.MetadataInstance,
		"metadataDatabase":     req.MetadataDatabase,
		"gcsOutputDirectory":   req.GcsOutputDirectory,
		"sessionFilePath":      req.SessionFilePath,
		"sourceShardsFilePath": req.SourceShardsFilePath,
		"endTimestamp":         req.EndTimestamp,
		"windowDuration":       req.WindowDuration,
		"filtrationMode":       req.FiltrationMode,
		"metadataTableSuffix":  req.MetadataTableSuffix,
		"skipDirectoryName":    req.SkipDirectoryName,
		"startTimestamp":       req.StartTimestamp,
		"runIdentifier":        smtJobId,
		"runMode":              constants.RR_READER_REGULAR_MODE,
	}
	dfLaunchReq, err := dataflowacc.GetDataflowLaunchRequest(params, req.TuningCfg)
	if err != nil {
		return err
	}
	err = createDataflowSMTResource(ctx, smtJobId, dfLaunchReq)
	if err != nil {
		return err
	}
	return nil
}

func createWriterJob(ctx context.Context, req CreateWriterJobRequest, smtJobId string) error {
	params := map[string]string{
		"sourceShardsFilePath":   req.SourceShardsFilePath,
		"sessionFilePath":        req.SessionFilePath,
		"sourceType":             req.SourceType,
		"sourceDbTimezoneOffset": req.SourceDbTimezoneOffset,
		"timerInterval":          fmt.Sprintf("%v", req.TimerInterval),
		"windowDuration":         req.WindowDuration,
		"GCSInputDirectoryPath":  req.GCSInputDirectoryPath,
		"metadataTableSuffix":    req.MetadataTableSuffix,
		"spannerProjectId":       req.SpannerProjectId,
		"metadataInstance":       req.MetadataInstance,
		"metadataDatabase":       req.MetadataDatabase,
		"startTimestamp":         req.StartTimestamp,
		"runIdentifier":          smtJobId,
		"runMode":                constants.RR_WRITER_REGULAR_MODE,
	}
	dfLaunchReq, err := dataflowacc.GetDataflowLaunchRequest(params, req.TuningCfg)
	if err != nil {
		return err
	}
	err = createDataflowSMTResource(ctx, smtJobId, dfLaunchReq)
	if err != nil {
		return err
	}
	return nil
}

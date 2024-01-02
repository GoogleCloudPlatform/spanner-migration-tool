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

	"cloud.google.com/go/dataflow/apiv1beta3/dataflowpb"
	"cloud.google.com/go/spanner"
	dataflowacc "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/dataflow"
	spanneracc "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/spanner"
	storageacc "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/storage"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/utils"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/dao"
)

func createChangeStreamSMTResource(ctx context.Context, smtJobId, changeStreamName, dbURI string) error {
	resourceId := fmt.Sprintf("smt-resource-%s", utils.GenerateHashStr())
	resourceData := spanner.NullJSON{Valid: true, Value: ResourceData_ChangeStream{DbURI: dbURI}}
	err := dao.InsertSMTResourceEntry(ctx, resourceId, smtJobId, changeStreamName, changeStreamName, "change-stream", resourceData)
	if err != nil {
		return err
	}
	err = spanneracc.CreateChangeStream(ctx, changeStreamName, dbURI)
	if err != nil {
		return err
	}
	return dao.UpdateSMTResourceState(ctx, resourceId, "CREATED")
}

func createMetadataDbSMTResource(ctx context.Context, smtJobId, dbName, dbURI string) error {
	resourceId := fmt.Sprintf("smt-resource-%s", utils.GenerateHashStr())
	resourceData := spanner.NullJSON{Valid: true, Value: ResourceData_MetadataDb{DbURI: dbURI}}
	err := dao.InsertSMTResourceEntry(ctx, resourceId, smtJobId, dbName, dbName, "rr-metadata-db", resourceData)
	if err != nil {
		return err
	}
	err = spanneracc.CreateEmptyDatabase(ctx, dbURI)
	if err != nil {
		return err
	}
	return dao.UpdateSMTResourceState(ctx, resourceId, "CREATED")
}

func createBucketSMTResource(ctx context.Context, smtJobId, bucketName, projectId, location string, matchesPrefix []string, ttl int64) error {
	resourceId := fmt.Sprintf("smt-resource-%s", utils.GenerateHashStr())
	resourceData := spanner.NullJSON{Valid: true, Value: ResourceData_GCSBucket{
		Name:          bucketName,
		ProjectId:     projectId,
		Location:      location,
		MatchesPrefix: matchesPrefix,
		Ttl:           ttl,
	}}
	err := dao.InsertSMTResourceEntry(ctx, resourceId, smtJobId, bucketName, bucketName, "gcs-bucket", resourceData)
	if err != nil {
		return err
	}
	err = storageacc.CreateGCSBucketWithLifecycle(ctx, bucketName, projectId, location, matchesPrefix, ttl)
	if err != nil {
		return err
	}
	return dao.UpdateSMTResourceState(ctx, resourceId, "CREATED")
}

func createDataflowSMTResource(ctx context.Context, smtJobId string, launchRequest *dataflowpb.LaunchFlexTemplateRequest) error {
	resourceId := fmt.Sprintf("smt-resource-%s", utils.GenerateHashStr())
	resourceData := spanner.NullJSON{Valid: true, Value: ResourceData_Dataflow{LaunchRequest: launchRequest, EquivalentGcloudCmd: utils.GetGcloudDataflowCommand(launchRequest)}}
	err := dao.InsertSMTResourceEntry(ctx, resourceId, smtJobId, "", launchRequest.LaunchParameter.JobName, "dataflow", resourceData)
	if err != nil {
		return err
	}
	response, err := dataflowacc.LaunchDataflowJob(ctx, launchRequest)
	if err != nil {
		return err
	}
	err = dao.UpdateSMTResourceExternalId(ctx, resourceId, response.Job.Id)
	if err != nil {
		return fmt.Errorf("error updating external id with dataflow job id: %v", err)
	}
	return dao.UpdateSMTResourceState(ctx, resourceId, "CREATED")
}

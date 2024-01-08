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
package resource

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

func CreateChangeStreamSMTResource(ctx context.Context, smtJobId, changeStreamName, dbURI string) error {
	resourceId := fmt.Sprintf("smt-resource-%s", utils.GenerateHashStr())
	resourceData := spanner.NullJSON{Valid: true, Value: ResourceData_ChangeStream{DbURI: dbURI}}
	err := dao.InsertSMTResourceEntry(ctx, resourceId, smtJobId, changeStreamName, changeStreamName, "change-stream", resourceData)
	if err != nil {
		return fmt.Errorf("error inserting SMT change stream resource: %v", err)
	}
	err = spanneracc.CreateChangeStream(ctx, changeStreamName, dbURI)
	if err != nil {
		return fmt.Errorf("error in change stream creation: %v", err)
	}
	return dao.UpdateSMTResourceState(ctx, resourceId, "CREATED")
}

func CreateMetadataDbSMTResource(ctx context.Context, smtJobId, dbURI string) error {
	_, _, dbName := utils.ParseDbURI(dbURI)
	resourceId := fmt.Sprintf("smt-resource-%s", utils.GenerateHashStr())
	resourceData := spanner.NullJSON{Valid: true, Value: ResourceData_MetadataDb{DbURI: dbURI}}
	err := dao.InsertSMTResourceEntry(ctx, resourceId, smtJobId, dbName, dbName, "rr-metadata-db", resourceData)
	if err != nil {
		return fmt.Errorf("error inserting SMT metadata db resource: %v", err)
	}
	err = spanneracc.CreateEmptyDatabase(ctx, dbURI)
	if err != nil {
		return fmt.Errorf("error creating db: %v", err)
	}
	return dao.UpdateSMTResourceState(ctx, resourceId, "CREATED")
}

func CreateBucketSMTResource(ctx context.Context, smtJobId, bucketName, projectId, location string, matchesPrefix []string, ttl int64) error {
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
		return fmt.Errorf("error inserting SMT bucket resource: %v", err)
	}
	err = storageacc.CreateGCSBucketWithLifecycle(ctx, bucketName, projectId, location, matchesPrefix, ttl)
	if err != nil {
		return fmt.Errorf("error in bucket creation: %v", err)
	}
	return dao.UpdateSMTResourceState(ctx, resourceId, "CREATED")
}

func CreateDataflowSMTResource(ctx context.Context, smtJobId string, launchRequest *dataflowpb.LaunchFlexTemplateRequest) (string, error) {
	resourceId := fmt.Sprintf("smt-resource-%s", utils.GenerateHashStr())
	resourceData := spanner.NullJSON{Valid: true, Value: ResourceData_Dataflow{LaunchRequest: launchRequest, EquivalentGcloudCmd: utils.GetGcloudDataflowCommand(launchRequest)}}
	err := dao.InsertSMTResourceEntry(ctx, resourceId, smtJobId, "", launchRequest.LaunchParameter.JobName, "dataflow", resourceData)
	if err != nil {
		return "", fmt.Errorf("error inserting SMT dataflow resource: %v", err)
	}
	response, err := dataflowacc.LaunchDataflowJob(ctx, launchRequest)
	if err != nil {
		return "", fmt.Errorf("error in launching dataflow job: %v", err)
	}
	err = dao.UpdateSMTResourceExternalId(ctx, resourceId, response.Job.Id)
	if err != nil {
		return "", fmt.Errorf("error updating external id for dataflow job: %v", err)
	}
	err = dao.UpdateSMTResourceState(ctx, resourceId, "CREATED")
	if err != nil {
		return "", fmt.Errorf("error updating state for dataflow job: %v", err)
	}
	return response.Job.Id, nil
}

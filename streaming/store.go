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
package streaming

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/utils"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/profiles"
)

// This file contains functions specific to storing state for a minimal downtime migration.

// Struct to store minimal downtime migration specific metadata in the JobMetadata field in the JobDetails table
type MinimalDowntimeJobMetadata struct {
	IsSharded bool
}

// PersistJobDetails stores all the metadata associated with a job orchestration for a minimal downtime migration in the metadata db. An example of this metadata is job level data such as the spanner database name.
func PersistJobDetails(ctx context.Context, targetProfile profiles.TargetProfile, sourceProfile profiles.SourceProfile, conv *internal.Conv, migrationJobId string, isSharded bool) (err error) {
	project, instance, dbName, err := targetProfile.GetResourceIds(ctx, time.Now(), sourceProfile.Driver, nil)
	if err != nil {
		err = fmt.Errorf("can't get resource ids: %v", err)
		return err
	}
	dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", project, instance, constants.METADATA_DB)
	client, err := utils.GetClient(ctx, dbURI)
	if err != nil {
		err = fmt.Errorf("can't create client for db %s: %v", dbURI, err)
		return err
	}
	defer client.Close()
	if err != nil {
		err = fmt.Errorf("can't create database client: %v", err)
		return err
	}
	err = writeJobDetails(ctx, migrationJobId, isSharded, conv, dbName, time.Now(), client)
	if err != nil {
		err = fmt.Errorf("can't store generated resources for datashard: %v", err)
		return err
	}
	logger.Log.Info(fmt.Sprintf("Generated resources stored successfully for migration jobId: %s. You can also look at the 'spannermigrationtool_metadata' database in your spanner instance to get this jobId at a later point of time.\n", migrationJobId))
	return nil
}

func PersistAggregateMonitoringResources(ctx context.Context, targetProfile profiles.TargetProfile, sourceProfile profiles.SourceProfile, conv *internal.Conv, migrationJobId string) error {
	logger.Log.Debug(fmt.Sprintf("Storing aggregate monitoring dashboard for migration jobId: %s\n", migrationJobId))
	project, instance, _, err := targetProfile.GetResourceIds(ctx, time.Now(), sourceProfile.Driver, nil)
	if err != nil {
		err = fmt.Errorf("can't get resource ids: %v", err)
		return err
	}
	dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", project, instance, constants.METADATA_DB)
	client, err := utils.GetClient(ctx, dbURI)
	if err != nil {
		err = fmt.Errorf("can't create client for db %s: %v", dbURI, err)
		return err
	}
	defer client.Close()
	aggMonitoringResourcesBytes, err := json.Marshal(conv.Audit.StreamingStats.AggMonitoringResources)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("internal error occurred while persisting metadata for migration job %s: %v\n", migrationJobId, err))
		return err
	}
	_, err = client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		mutation, err := createResourceMutation(migrationJobId, constants.DEFAULT_SHARD_ID, conv.Audit.StreamingStats.AggMonitoringResources.DashboardName, constants.AGG_MONITORING_RESOURCE, conv.Audit.StreamingStats.AggMonitoringResources.DashboardName, string(aggMonitoringResourcesBytes))
		if err != nil {
			return err
		}
		err = txn.BufferWrite([]*spanner.Mutation{mutation})
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		err = fmt.Errorf("can't store aggregate monitoring resources for migration job %s: %v", migrationJobId, err)
		return err
	}
	return nil
}

// PersistResources stores all the metadata associated with a shard orchestration for a minimal downtime migration in the metadata db. An example of this metadata is generated resources.
func PersistResources(ctx context.Context, targetProfile profiles.TargetProfile, sourceProfile profiles.SourceProfile, conv *internal.Conv, migrationJobId string, dataShardId string) (err error) {
	project, instance, _, err := targetProfile.GetResourceIds(ctx, time.Now(), sourceProfile.Driver, nil)
	if err != nil {
		err = fmt.Errorf("can't get resource ids: %v", err)
		return err
	}
	dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", project, instance, constants.METADATA_DB)
	client, err := utils.GetClient(ctx, dbURI)
	if err != nil {
		err = fmt.Errorf("can't create client for db %s: %v", dbURI, err)
		return err
	}
	defer client.Close()
	if err != nil {
		err = fmt.Errorf("can't create database client: %v", err)
		return err
	}
	err = writeJobResources(ctx, migrationJobId, dataShardId, conv.Audit.StreamingStats.DataflowResources, conv.Audit.StreamingStats.DatastreamResources, conv.Audit.StreamingStats.GcsResources, conv.Audit.StreamingStats.PubsubResources, conv.Audit.StreamingStats.MonitoringResources, time.Now(), client)
	if err != nil {
		err = fmt.Errorf("can't store generated resources for datashard: %v", err)
		return err
	}
	logger.Log.Info(fmt.Sprintf("Generated resources stored successfully for migration jobId: %s, dataShardId: %s. You can also look at the 'spannermigrationtool_metadata' database in your spanner instance to get this jobId at a later point of time.\n", migrationJobId, dataShardId))
	return nil
}

func writeJobDetails(ctx context.Context, migrationJobId string, isShardedMigration bool, conv *internal.Conv, spannerDatabaseName string, createTimestamp time.Time, client *spanner.Client) error {
	jobMetadataBytes, err := json.Marshal(MinimalDowntimeJobMetadata{IsSharded: isShardedMigration})
	if err != nil {
		logger.Log.Error(fmt.Sprintf("internal error occurred while persisting metadata for migration job %s: %v\n", migrationJobId, err))
		return err
	}
	sessionBytes, err := json.Marshal(conv)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("internal error occurred while persisting metadata for migration job %s: %v\n", migrationJobId, err))
		return err
	}
	jobDetails := JobDetails{
		JobId:               migrationJobId,
		JobName:             migrationJobId,
		JobType:             constants.MINIMAL_DOWNTIME_MIGRATION,
		Dialect:             conv.SpDialect,
		Session:             string(sessionBytes),
		JobMetadata:         string(jobMetadataBytes),
		SpannerDatabaseName: spannerDatabaseName,
		CreatedAt:           createTimestamp,
		UpdatedAt:           createTimestamp,
	}
	_, err = client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		mutation, err := spanner.InsertStruct("JobDetails", jobDetails)
		if err != nil {
			return err
		}
		err = txn.BufferWrite([]*spanner.Mutation{mutation})
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		logger.Log.Error(fmt.Sprintf("can't store generated resources for migration job %s: %v\n", migrationJobId, err))
		return err
	}
	return nil
}

func writeJobResources(ctx context.Context, migrationJobId string, dataShardId string, dataflowResources internal.DataflowResources, datastreamResources internal.DatastreamResources, gcsResources internal.GcsResources, pubsubResources internal.PubsubResources, monitoringResources internal.MonitoringResources, createTimestamp time.Time, client *spanner.Client) error {
	datastreamResourcesBytes, err := json.Marshal(datastreamResources)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("can't marshal datastream resources for data shard %s: %v\n", dataShardId, err))
		return err
	}
	dataflowResourcesBytes, err := json.Marshal(dataflowResources)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("can't marshal dataflow resources for data shard %s: %v\n", dataShardId, err))
		return err
	}
	gcsResourcesBytes, err := json.Marshal(gcsResources)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("can't marshal gcs resources for data shard %s: %v\n", dataShardId, err))
		return err
	}
	pubsubResourcesBytes, err := json.Marshal(pubsubResources)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("can't marshal pubsub resources for data shard %s: %v\n", dataShardId, err))
		return err
	}
	monitoringResourcesBytes, err := json.Marshal(monitoringResources)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("can't marshal monitoring resources for data shard %s: %v\n", dataShardId, err))
		return err
	}
	logger.Log.Debug(fmt.Sprintf("Storing generated resources for data shard %s...\n", dataShardId))
	_, err = client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		datastreamMutation, err := createResourceMutation(migrationJobId, dataShardId, datastreamResources.DatastreamName, constants.DATASTREAM_RESOURCE, datastreamResources.DatastreamName, string(datastreamResourcesBytes))
		if err != nil {
			return err
		}
		dataflowMutation, err := createResourceMutation(migrationJobId, dataShardId, dataflowResources.JobId, constants.DATAFLOW_RESOURCE, dataflowResources.JobId, string(dataflowResourcesBytes))
		if err != nil {
			return err
		}
		gcsMutation, err := createResourceMutation(migrationJobId, dataShardId, gcsResources.BucketName, constants.GCS_RESOURCE, gcsResources.BucketName, string(gcsResourcesBytes))
		if err != nil {
			return err
		}
		pubsubMutation, errr := createResourceMutation(migrationJobId, dataShardId, pubsubResources.TopicId, constants.PUBSUB_RESOURCE, pubsubResources.TopicId, string(pubsubResourcesBytes))
		if errr != nil {
			return errr
		}
		monitoringMutation, err := createResourceMutation(migrationJobId, dataShardId, monitoringResources.DashboardName, constants.MONITORING_RESOURCE, monitoringResources.DashboardName, string(monitoringResourcesBytes))
		if err != nil {
			return err
		}
		err = txn.BufferWrite([]*spanner.Mutation{datastreamMutation, dataflowMutation, gcsMutation, pubsubMutation, monitoringMutation})
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		logger.Log.Error(fmt.Sprintf("can't store generated resources for data shard %s: %v\n", dataShardId, err))
		return err
	}
	return nil
}

func createResourceMutation(jobId string, dataShardId string, externalResourceId string, resourceType string, resourceName string, ResourceMetadata string) (*spanner.Mutation, error) {
	resourceId, _ := utils.GenerateName("smt-resource")
	resourceId = strings.Replace(resourceId, "_", "-", -1)
	jobResource := JobResources{
		ResourceId:         resourceId,
		JobId:              jobId,
		DataShardId:        dataShardId,
		ExternalResourceId: externalResourceId,
		ResourceType:       resourceType,
		ResourceName:       resourceName,
		ResourceMetadata:   ResourceMetadata,
		CreatedAt:          time.Now(),
	}
	mutation, err := spanner.InsertStruct("JobResources", jobResource)
	if err != nil {
		return nil, err
	}
	return mutation, nil
}

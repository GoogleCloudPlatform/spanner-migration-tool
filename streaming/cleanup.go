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
	"os"

	dataflow "cloud.google.com/go/dataflow/apiv1beta3"
	"cloud.google.com/go/dataflow/apiv1beta3/dataflowpb"
	datastream "cloud.google.com/go/datastream/apiv1"
	"cloud.google.com/go/datastream/apiv1/datastreampb"
	dashboard "cloud.google.com/go/monitoring/dashboard/apiv1"
	"cloud.google.com/go/monitoring/dashboard/apiv1/dashboardpb"
	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/spanner"
	"cloud.google.com/go/storage"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/utils"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/profiles"
	"google.golang.org/api/iterator"
)

type JobCleanupOptions struct {
	Dataflow   bool
	Datastream bool
	Pubsub     bool
	Monitoring bool
}

func InitiateJobCleanup(ctx context.Context, migrationJobId string, dataShardIds []string, jobCleanupOptions JobCleanupOptions, migrationProjectId string, spannerProjectId string, instance string) {
	//initiate resource cleanup
	if jobCleanupOptions.Dataflow {
		//fetch dataflow resources
		dataflowResourcesList, err := FetchResources(ctx, migrationJobId, constants.DATAFLOW_RESOURCE, dataShardIds, spannerProjectId, instance)
		if err != nil {
			logger.Log.Debug(fmt.Sprintf("Unable to fetch dataflow resources for jobId: %s: %v\n", migrationJobId, err))
		}
		//cleanup
		for _, resources := range dataflowResourcesList {
			var dataflowResources internal.DataflowResources
			var minimalDowntimeResourceData MinimalDowntimeResourceData
			json.Unmarshal([]byte(resources.ResourceData), &minimalDowntimeResourceData)
			err = json.Unmarshal([]byte(minimalDowntimeResourceData.ResourcePayload), &dataflowResources)
			if err != nil {
				logger.Log.Debug("Unable to read Dataflow metadata for deletion\n")
			} else {
				cleanupDataflowJob(ctx, dataflowResources, migrationProjectId)
			}
		}
	}
	if jobCleanupOptions.Datastream {
		//fetch dataflow resources
		datastreamResourcesList, err := FetchResources(ctx, migrationJobId, constants.DATASTREAM_RESOURCE, dataShardIds, spannerProjectId, instance)
		if err != nil {
			logger.Log.Debug(fmt.Sprintf("Unable to fetch datastream resources for jobId: %s: %v\n", migrationJobId, err))
		}
		//cleanup
		for _, resources := range datastreamResourcesList {
			var datastreamResources internal.DatastreamResources
			var minimalDowntimeResourceData MinimalDowntimeResourceData
			json.Unmarshal([]byte(resources.ResourceData), &minimalDowntimeResourceData)
			err := json.Unmarshal([]byte(minimalDowntimeResourceData.ResourcePayload), &datastreamResources)
			if err != nil {
				logger.Log.Debug("Unable to read Datastream metadata for deletion\n")
			} else {
				cleanupDatastream(ctx, datastreamResources, migrationJobId)
			}
		}
	}
	if jobCleanupOptions.Pubsub {
		//fetch pubsub resources
		pubsubResourcesList, err := FetchResources(ctx, migrationJobId, constants.PUBSUB_RESOURCE, dataShardIds, spannerProjectId, instance)
		if err != nil {
			logger.Log.Debug(fmt.Sprintf("Unable to fetch pubsub resources for jobId: %s: %v\n", migrationJobId, err))
		}
		//cleanup
		for _, resources := range pubsubResourcesList {
			var pubsubResources internal.PubsubResources
			var minimalDowntimeResourceData MinimalDowntimeResourceData
			json.Unmarshal([]byte(resources.ResourceData), &minimalDowntimeResourceData)
			err := json.Unmarshal([]byte(minimalDowntimeResourceData.ResourcePayload), &pubsubResources)
			if err != nil {
				logger.Log.Debug("Unable to read Pubsub metadata for deletion\n")
			} else {
				cleanupPubsubResources(ctx, pubsubResources, migrationProjectId)
			}
		}
	}
	if jobCleanupOptions.Monitoring {
		//fetch monitoring resources
		shardMonitoringResourcesList, err := FetchResources(ctx, migrationJobId, constants.MONITORING_RESOURCE, dataShardIds, spannerProjectId, instance)
		if err != nil {
			logger.Log.Debug(fmt.Sprintf("Unable to fetch shard monitoring resources for jobId: %s: %v\n", migrationJobId, err))
		}
		jobMonitoringResourcesList, err := FetchResources(ctx, migrationJobId, constants.AGG_MONITORING_RESOURCE, dataShardIds, spannerProjectId, instance)
		if err != nil {
			logger.Log.Debug(fmt.Sprintf("Unable to fetch aggregate monitoring resources for jobId: %s: %v\n", migrationJobId, err))
		}
		monitoringResourcesList := append(shardMonitoringResourcesList, jobMonitoringResourcesList...)
		//cleanup
		for _, resources := range monitoringResourcesList {
			var monitoringResources internal.MonitoringResources
			var minimalDowntimeResourceData MinimalDowntimeResourceData
			json.Unmarshal([]byte(resources.ResourceData), &minimalDowntimeResourceData)
			err := json.Unmarshal([]byte(minimalDowntimeResourceData.ResourcePayload), &monitoringResources)
			if err != nil {
				logger.Log.Debug("Unable to read monitoring metadata for deletion\n")
			} else {
				cleanupMonitoringDashboard(ctx, monitoringResources, migrationProjectId)
			}
		}
	}
}

func FetchResources(ctx context.Context, migrationJobId string, resourceType string, dataShardIds []string, spannerProjectId string, instance string) ([]SmtResource, error) {
	dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", spannerProjectId, instance, constants.METADATA_DB)
	client, err := utils.GetClient(ctx, dbURI)
	if err != nil {
		err = fmt.Errorf("can't create client for db %s: %v", dbURI, err)
		return nil, err
	}
	defer client.Close()
	txn := client.ReadOnlyTransaction()
	defer txn.Close()

	//fetch all resources
	var resourceQuery spanner.Statement
	if dataShardIds != nil {
		//query the provided data shards only
		resourceQuery = spanner.Statement{
			SQL: `SELECT 
					ResourceId,
					JobId,
					ExternalId,
					ResourceName,
					ResourceType,
					TO_JSON_STRING(ResourceData) AS ResourceData
				FROM SMT_RESOURCE 
				WHERE JobId = @migrationJobId and ResourceType = @resourceType and JSON_VALUE(ResourceData, '$.DataShardId') IN UNNEST (@dataShardIds)`,
			Params: map[string]interface{}{
				"migrationJobId": migrationJobId,
				"resourceType":   resourceType,
				"dataShardIds":   dataShardIds,
			},
		}
	} else {
		//query all data shards
		resourceQuery = spanner.Statement{
			SQL: `SELECT 
					ResourceId,
					JobId,
					ExternalId,
					ResourceName,
					ResourceType,
					TO_JSON_STRING(ResourceData) AS ResourceData
				FROM SMT_RESOURCE
				WHERE JobId = @migrationJobId and ResourceType = @resourceType`,
			Params: map[string]interface{}{
				"migrationJobId": migrationJobId,
				"resourceType":   resourceType,
			},
		}
	}
	iter := txn.Query(ctx, resourceQuery)
	jobResourcesList := []SmtResource{}
	for {
		row, e := iter.Next()
		if e == iterator.Done {
			break
		}
		if e != nil {
			err = e
			break
		}
		var jobResource SmtResource
		row.ToStruct(&jobResource)
		jobResourcesList = append(jobResourcesList, jobResource)
	}
	return jobResourcesList, err
}

func GetInstanceDetails(ctx context.Context, targetProfile profiles.TargetProfile) (string, string, error) {
	var err error
	project := targetProfile.Conn.Sp.Project
	g := utils.GetUtilInfoImpl{}
	if project == "" {
		project, err = g.GetProject()
		if err != nil {
			return "", "", fmt.Errorf("can't get project: %v", err)
		}
	}

	instance := targetProfile.Conn.Sp.Instance
	if instance == "" {
		instance, err = g.GetInstance(ctx, project, os.Stdout)
		if err != nil {
			return "", "", fmt.Errorf("can't get instance: %v", err)
		}
	}
	return project, instance, nil
}

func cleanupPubsubResources(ctx context.Context, pubsubResources internal.PubsubResources, project string) {
	logger.Log.Debug("Attempting to delete pubsub topic and subscription...\n")
	pubsubClient, err := pubsub.NewClient(ctx, project)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("pubsub client can not be created: %v", err))
		return
	}
	defer pubsubClient.Close()
	storageClient, err := storage.NewClient(ctx)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("storage client can not be created: %v", err))
		return
	}
	defer storageClient.Close()
	subscription := pubsubClient.Subscription(pubsubResources.SubscriptionId)
	err = subscription.Delete(ctx)
	if err != nil {
		logger.Log.Info(fmt.Sprintf("Cleanup of the pubsub subscription: %s Failed, please clean up the pubsub subscription manually\n error=%v\n", pubsubResources.SubscriptionId, err))
	} else {
		logger.Log.Info(fmt.Sprintf("Successfully deleted subscription: %s\n\n", pubsubResources.SubscriptionId))
	}

	topic := pubsubClient.Topic(pubsubResources.TopicId)
	err = topic.Delete(ctx)
	if err != nil {
		logger.Log.Info(fmt.Sprintf("Cleanup of the pubsub topic: %s Failed, please clean up the pubsub topic manually\n error=%v\n", pubsubResources.TopicId, err))
	} else {
		logger.Log.Info(fmt.Sprintf("Successfully deleted topic: %s\n\n", pubsubResources.TopicId))
	}

	bucket := storageClient.Bucket(pubsubResources.BucketName)
	if err := bucket.DeleteNotification(ctx, pubsubResources.NotificationId); err != nil {
		logger.Log.Info(fmt.Sprintf("Cleanup of GCS pubsub notification: %s failed.\n error=%v\n", pubsubResources.NotificationId, err))
	} else {
		logger.Log.Info(fmt.Sprintf("Successfully deleted GCS pubsub notification: %s\n\n", pubsubResources.NotificationId))
	}
}

func cleanupMonitoringDashboard(ctx context.Context, monitoringResources internal.MonitoringResources, projectID string) {
	logger.Log.Debug("Attempting to delete monitoring resources...\n")
	client, err := dashboard.NewDashboardsClient(ctx)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("Cleanup of the monitoring dashboard: %s Failed, please clean up the dashboard manually\n error=%v\n", monitoringResources.DashboardName, err))
		return
	}
	defer client.Close()
	req := &dashboardpb.DeleteDashboardRequest{
		Name: fmt.Sprintf("projects/%s/dashboards/%s", projectID, monitoringResources.DashboardName),
	}
	err = client.DeleteDashboard(ctx, req)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("Cleanup of the monitoring dashboard: %s Failed, please clean up the dashboard manually\n error=%v\n", monitoringResources.DashboardName, err))
	} else {
		logger.Log.Info(fmt.Sprintf("Successfully deleted Monitoring Dashboard: %s\n\n", monitoringResources.DashboardName))
	}
}

func cleanupDatastream(ctx context.Context, datastreamResources internal.DatastreamResources, project string) {
	logger.Log.Debug("Attempting to delete datastream stream...\n")
	datastreamClient, err := datastream.NewClient(ctx)
	logger.Log.Debug("Created datastream client...")
	if err != nil {
		logger.Log.Error(fmt.Sprintf("datastream client can not be created: %v", err))
		return
	}
	defer datastreamClient.Close()
	req := &datastreampb.DeleteStreamRequest{
		Name: fmt.Sprintf("projects/%s/locations/%s/streams/%s", project, datastreamResources.Region, datastreamResources.DatastreamName),
	}
	_, err = datastreamClient.DeleteStream(ctx, req)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("Cleanup of the datastream stream: %s Failed, please clean up the datastream stream manually\n error=%v\n", datastreamResources.DatastreamName, err))
	} else {
		logger.Log.Info(fmt.Sprintf("Successfully deleted datastream stream: %s\n\n", datastreamResources.DatastreamName))
	}
}

func cleanupDataflowJob(ctx context.Context, dataflowResources internal.DataflowResources, project string) {
	logger.Log.Debug("Attempting to delete dataflow job...\n")
	dataflowClient, err := dataflow.NewJobsV1Beta3Client(ctx)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("dataflow client can not be created: %v", err))
		return
	}
	defer dataflowClient.Close()
	job := &dataflowpb.Job{
		Id:             dataflowResources.JobId,
		ProjectId:      project,
		RequestedState: dataflowpb.JobState_JOB_STATE_CANCELLED,
	}

	dfReq := &dataflowpb.UpdateJobRequest{
		ProjectId: project,
		JobId:     dataflowResources.JobId,
		Location:  dataflowResources.Region,
		Job:       job,
	}
	_, err = dataflowClient.UpdateJob(ctx, dfReq)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("Cleanup of the dataflow job: %s Failed, please clean up the dataflow job manually\n error=%v\n", dataflowResources.JobId, err))
	} else {
		logger.Log.Info(fmt.Sprintf("Successfully deleted dataflow job: %s\n\n", dataflowResources.JobId))
	}
}

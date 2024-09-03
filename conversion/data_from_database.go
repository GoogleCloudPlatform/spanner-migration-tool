// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package conversion

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"cloud.google.com/go/datastream/apiv1/datastreampb"
	sp "cloud.google.com/go/spanner"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/clients/datastream"
	spinstanceadmin "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/clients/spanner/instanceadmin"
	storageclient "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/clients/storage"
	datastream_accessor "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/datastream"
	spanneraccessor "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/spanner"
	storageaccessor "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/storage"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/metrics"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/utils"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/profiles"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/common"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/writer"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/streaming"
	"go.uber.org/zap"
)

type DataFromDatabaseInterface interface {
	dataFromDatabaseForDMSMigration() (*writer.BatchWriter, error)
	dataFromDatabaseForDataflowMigration(migrationProjectId string, targetProfile profiles.TargetProfile, ctx context.Context, sourceProfile profiles.SourceProfile, conv *internal.Conv, is common.InfoSchemaInterface) (*writer.BatchWriter, error)
	dataFromDatabaseForBulkMigration(migrationProjectId string, sourceProfile profiles.SourceProfile, targetProfile profiles.TargetProfile, config writer.BatchWriterConfig, conv *internal.Conv, client *sp.Client, gi GetInfoInterface, sm SnapshotMigrationInterface) (*writer.BatchWriter, error)
}

type DataFromDatabaseImpl struct{}

// TODO: Define the data processing logic for DMS migrations here.
func (dd *DataFromDatabaseImpl) dataFromDatabaseForDMSMigration() (*writer.BatchWriter, error) {
	return nil, fmt.Errorf("dms configType is not implemented yet, please use one of 'bulk' or 'dataflow'")
}

// 1. Create batch for each physical shard
// 2. Create streaming cfg from the config source type.
// 3. Verify the CFG and update it with SMT defaults
// 4. Launch the stream for the physical shard
// 5. Perform streaming migration via dataflow
func (dd *DataFromDatabaseImpl) dataFromDatabaseForDataflowMigration(migrationProjectId string, targetProfile profiles.TargetProfile, ctx context.Context, sourceProfile profiles.SourceProfile, conv *internal.Conv, is common.InfoSchemaInterface) (*writer.BatchWriter, error) {
	// Fetch Spanner Region
	if conv.SpRegion == "" {
		spInstanceAdmin, err := spinstanceadmin.NewInstanceAdminClientImpl(ctx)
		if err != nil {
			return nil, fmt.Errorf("unable to fetch Spanner Region for resource creation: %v", err)
		}
		spAcc := spanneraccessor.SpannerAccessorImpl{}
		spannerRegion, err := spAcc.GetSpannerLeaderLocation(ctx, spInstanceAdmin, "projects/"+targetProfile.Conn.Sp.Project+"/instances/"+targetProfile.Conn.Sp.Instance)
		if err != nil {
			return nil, fmt.Errorf("unable to fetch Spanner Region for resource creation: %v", err)
		}
		conv.SpRegion = spannerRegion
	}

	storageClient, err := storageclient.NewStorageClientImpl(ctx)
	if err != nil {
		return nil, err
	}

	// Create Resources required for migration
	if conv.ResourceValidation {
		dsClient, err := datastreamclient.NewDatastreamClientImpl(ctx)
		if err != nil {
			return nil, err
		}
		createResources := NewValidateOrCreateResourcesImpl(&datastream_accessor.DatastreamAccessorImpl{}, dsClient, &storageaccessor.StorageAccessorImpl{}, storageClient)
		err = createResources.ValidateOrCreateResourcesForShardedMigration(ctx, migrationProjectId, targetProfile.Conn.Sp.Instance, false, conv.SpRegion, sourceProfile)
		if err != nil {
			return nil, fmt.Errorf("unable to create connection profiles: %v", err)
		}
	}

	//Set the TmpDir from the sessionState bucket which is derived from the target connection profile
	for _, dataShard := range sourceProfile.Config.ShardConfigurationDataflow.DataShards {
		if dataShard.TmpDir == "" {
			bucket, rootPath, err := GetBucketFromDatastreamProfile(migrationProjectId, conv.SpRegion, dataShard.DstConnectionProfile.Name)
			if err != nil {
				return nil, fmt.Errorf("error while getting target bucket: %v", err)
			}
			dataShard.TmpDir = "gs://" + bucket + rootPath
		}
	}

	updateShardsWithTuningConfigs(sourceProfile.Config.ShardConfigurationDataflow)
	//Generate a job Id
	migrationJobId := conv.Audit.MigrationRequestId
	fmt.Printf("Creating a migration job with id: %v. This jobId can be used in future commmands (such as cleanup) to refer to this job.\n", migrationJobId)
	conv.Audit.StreamingStats.ShardToShardResourcesMap = make(map[string]internal.ShardResources)
	schemaDetails, err := is.GetIncludedSrcTablesFromConv(conv)
	if err != nil {
		fmt.Printf("unable to determine tableList from schema, falling back to full database")
		schemaDetails = map[string]internal.SchemaDetails{}
	}
	err = streaming.PersistJobDetails(ctx, targetProfile, sourceProfile, conv, migrationJobId, true)
	if err != nil {
		logger.Log.Info(fmt.Sprintf("Error storing job details in SMT metadata store...the migration job will still continue as intended. %v", err))
	}
	asyncProcessShards := func(p *profiles.DataShard, mutex *sync.Mutex) common.TaskResult[*profiles.DataShard] {
		dbNameToShardIdMap := make(map[string]string)
		for _, l := range p.LogicalShards {
			dbNameToShardIdMap[l.DbName] = l.LogicalShardId
		}
		if p.DataShardId == "" {
			dataShardId, err := utils.GenerateName("smt-datashard")
			dataShardId = strings.Replace(dataShardId, "_", "-", -1)
			if err != nil {
				return common.TaskResult[*profiles.DataShard]{Result: p, Err: err}
			}
			p.DataShardId = dataShardId
			fmt.Printf("Data shard id generated: %v\n", p.DataShardId)
		}
		streamingCfg := streaming.CreateStreamingConfig(*p)
		err := streaming.VerifyAndUpdateCfg(&streamingCfg, targetProfile.Conn.Sp.Dbname, schemaDetails)
		if err != nil {
			err = fmt.Errorf("failed to process shard: %s, there seems to be an error in the sharding configuration, error: %v", p.DataShardId, err)
			return common.TaskResult[*profiles.DataShard]{Result: p, Err: err}
		}
		fmt.Printf("Initiating migration for shard: %v\n", p.DataShardId)
		pubsubCfg, err := streaming.CreatePubsubResources(ctx, migrationProjectId, streamingCfg.DatastreamCfg.DestinationConnectionConfig, targetProfile.Conn.Sp.Dbname)
		if err != nil {
			return common.TaskResult[*profiles.DataShard]{Result: p, Err: err}
		}
		streamingCfg.PubsubCfg = *pubsubCfg
		err = streaming.LaunchStream(ctx, sourceProfile, p.LogicalShards, migrationProjectId, streamingCfg.DatastreamCfg)
		if err != nil {
			return common.TaskResult[*profiles.DataShard]{Result: p, Err: err}
		}
		streamingCfg.DataflowCfg.DbNameToShardIdMap = dbNameToShardIdMap
		dfOutput, err := streaming.StartDataflow(ctx, migrationProjectId, targetProfile, streamingCfg, conv)
		if err != nil {
			return common.TaskResult[*profiles.DataShard]{Result: p, Err: err}
		}
		// store the generated resources locally in conv, this is used as source of truth for persistence and the UI (should change to persisted values)

		// Fetch and store the GCS bucket associated with the datastream
		dsClient := GetDatastreamClient(ctx)
		gcsBucket, gcsDestPrefix, fetchGcsErr := streaming.FetchTargetBucketAndPath(ctx, dsClient, migrationProjectId, streamingCfg.DatastreamCfg.DestinationConnectionConfig)
		if fetchGcsErr != nil {
			logger.Log.Info(fmt.Sprintf("Could not fetch GCS Bucket for Shard %s hence Monitoring Dashboard will not contain Metrics for the gcs bucket\n", p.DataShardId))
			logger.Log.Debug("Error", zap.Error(fetchGcsErr))
		}

		// Try to apply lifecycle rule to Datastream destination bucket.
		gcsConfig := streamingCfg.GcsCfg
		sc, err := storageclient.NewStorageClientImpl(ctx)
		if err != nil {
			return common.TaskResult[*profiles.DataShard]{Result: p, Err: err}
		}
		sa := storageaccessor.StorageAccessorImpl{}
		if gcsConfig.TtlInDaysSet {
			err = sa.ApplyBucketLifecycleDeleteRule(ctx, sc, storageaccessor.StorageBucketMetadata{
				BucketName:    gcsBucket,
				Ttl:           gcsConfig.TtlInDays,
				MatchesPrefix: []string{gcsDestPrefix},
			})
			if err != nil {
				logger.Log.Warn(fmt.Sprintf("\nWARNING: could not update Datastream destination GCS bucket with lifecycle rule, error: %v\n", err))
				logger.Log.Warn("Please apply the lifecycle rule manually. Continuing...\n")
			}
		}

		// create monitoring dashboard for a single shard
		monitoringResources := metrics.MonitoringMetricsResources{
			MigrationProjectId:   migrationProjectId,
			DataflowJobId:        dfOutput.JobID,
			DatastreamId:         streamingCfg.DatastreamCfg.StreamId,
			JobMetadataGcsBucket: gcsBucket,
			PubsubSubscriptionId: streamingCfg.PubsubCfg.SubscriptionId,
			SpannerProjectId:     targetProfile.Conn.Sp.Project,
			SpannerInstanceId:    targetProfile.Conn.Sp.Instance,
			SpannerDatabaseId:    targetProfile.Conn.Sp.Dbname,
			ShardId:              p.DataShardId,
			MigrationRequestId:   conv.Audit.MigrationRequestId,
		}
		respDash, dashboardErr := monitoringResources.CreateDataflowShardMonitoringDashboard(ctx)
		var dashboardName string
		if dashboardErr != nil {
			dashboardName = ""
			logger.Log.Info(fmt.Sprintf("Creation of the monitoring dashboard for shard %s failed, please create the dashboard manually\n", p.DataShardId))
			logger.Log.Debug("Error", zap.Error(dashboardErr))
		} else {
			dashboardName = strings.Split(respDash.Name, "/")[3]
			fmt.Printf("Monitoring Dashboard for shard %v: %+v\n", p.DataShardId, dashboardName)
		}
		streaming.StoreGeneratedResources(conv, streamingCfg, dfOutput.JobID, dfOutput.GCloudCmd, migrationProjectId, p.DataShardId, internal.GcsResources{BucketName: gcsBucket}, dashboardName)
		//persist the generated resources in a metadata db
		err = streaming.PersistResources(ctx, targetProfile, sourceProfile, conv, migrationJobId, p.DataShardId)
		if err != nil {
			fmt.Printf("Error storing generated resources in SMT metadata store for dataShardId: %s...the migration job will still continue as intended, error: %v\n", p.DataShardId, err)
		}
		return common.TaskResult[*profiles.DataShard]{Result: p, Err: err}
	}
	r := common.RunParallelTasksImpl[*profiles.DataShard, *profiles.DataShard]{}
	_, err = r.RunParallelTasks(sourceProfile.Config.ShardConfigurationDataflow.DataShards, 20, asyncProcessShards, false)
	if err != nil {
		return nil, fmt.Errorf("unable to start minimal downtime migrations: %v", err)
	}

	// create monitoring aggregated dashboard for sharded migration
	aggMonitoringResources := metrics.MonitoringMetricsResources{
		MigrationProjectId:       migrationProjectId,
		SpannerProjectId:         targetProfile.Conn.Sp.Project,
		SpannerInstanceId:        targetProfile.Conn.Sp.Instance,
		SpannerDatabaseId:        targetProfile.Conn.Sp.Dbname,
		ShardToShardResourcesMap: conv.Audit.StreamingStats.ShardToShardResourcesMap,
		MigrationRequestId:       conv.Audit.MigrationRequestId,
	}
	aggRespDash, dashboardErr := aggMonitoringResources.CreateDataflowAggMonitoringDashboard(ctx)
	if dashboardErr != nil {
		logger.Log.Error(fmt.Sprintf("Creation of the aggregated monitoring dashboard failed, please create the dashboard manually\n error=%v\n", dashboardErr))
	} else {
		fmt.Printf("Aggregated Monitoring Dashboard: %+v\n", strings.Split(aggRespDash.Name, "/")[3])
		conv.Audit.StreamingStats.AggMonitoringResources = internal.MonitoringResources{DashboardName: strings.Split(aggRespDash.Name, "/")[3]}
	}
	err = streaming.PersistAggregateMonitoringResources(ctx, targetProfile, sourceProfile, conv, migrationJobId)
	if err != nil {
		logger.Log.Info(fmt.Sprintf("Unable to store aggregated monitoring dashboard in metadata database\n error=%v\n", err))
	} else {
		logger.Log.Debug("Aggregate monitoring resources stored successfully.\n")
	}
	return &writer.BatchWriter{}, nil
}

// 1. Migrate the data from the data shards, the schema shard needs to be specified here again.
// 2. Create a connection profile object for it
// 3. Perform a snapshot migration for the shard
// 4. Once all shard migrations are complete, return the batch writer object
func (dd *DataFromDatabaseImpl) dataFromDatabaseForBulkMigration(migrationProjectId string, sourceProfile profiles.SourceProfile, targetProfile profiles.TargetProfile, config writer.BatchWriterConfig, conv *internal.Conv, client *sp.Client, gi GetInfoInterface, sm SnapshotMigrationInterface) (*writer.BatchWriter, error) {
	var bw *writer.BatchWriter
	for _, dataShard := range sourceProfile.Config.ShardConfigurationBulk.DataShards {

		fmt.Printf("Initiating migration for shard: %v\n", dataShard.DbName)
		infoSchema, err := gi.getInfoSchemaForShard(migrationProjectId, dataShard, sourceProfile.Driver, targetProfile, &profiles.SourceProfileDialectImpl{}, &GetInfoImpl{})
		if err != nil {
			return nil, err
		}
		additionalDataAttributes := internal.AdditionalDataAttributes{
			ShardId: dataShard.DataShardId,
		}
		bw = sm.performSnapshotMigration(config, conv, client, infoSchema, additionalDataAttributes, &common.InfoSchemaImpl{}, &PopulateDataConvImpl{})
	}

	return bw, nil
}

func GetBucketFromDatastreamProfile(project, location, profileName string) (string, string, error) {
	ctx := context.Background()
	dsClient, err := datastreamclient.NewDatastreamClientImpl(ctx)
	if err != nil {
		return "", "", fmt.Errorf("datastream client can not be created: %v", err)
	}
	// Fetch the GCS path from the destination connection profile.
	dstProf := fmt.Sprintf("projects/%s/locations/%s/connectionProfiles/%s", project, location, profileName)
	res, err := dsClient.GetConnectionProfile(ctx, dstProf)
	if err != nil {
		return "", "", fmt.Errorf("could not get connection profile: %v", err)
	}
	gcsProfile := res.Profile.(*datastreampb.ConnectionProfile_GcsProfile).GcsProfile
	return gcsProfile.GetBucket(), gcsProfile.GetRootPath(), nil
}

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
	"bufio"
	"context"
	"fmt"
	"strings"

	sp "cloud.google.com/go/spanner"
	storageclient "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/clients/storage"
	storageaccessor "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/storage"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/metrics"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/utils"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/expressions_api"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/profiles"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/common"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/csv"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/writer"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/streaming"
	"go.uber.org/zap"
)

type SchemaFromSourceInterface interface {
	schemaFromDatabase(migrationProjectId string, sourceProfile profiles.SourceProfile, targetProfile profiles.TargetProfile, getInfo GetInfoInterface, processSchema common.ProcessSchemaInterface) (*internal.Conv, error)
	SchemaFromDump(SpProjectId string, SpInstanceId string, driver string, spDialect string, ioHelper *utils.IOStreams, processDump ProcessDumpByDialectInterface) (*internal.Conv, error)
}

type SchemaFromSourceImpl struct {
	DdlVerifier expressions_api.DDLVerifier
}

type DataFromSourceInterface interface {
	dataFromDatabase(ctx context.Context, migrationProjectId string, sourceProfile profiles.SourceProfile, targetProfile profiles.TargetProfile, config writer.BatchWriterConfig, conv *internal.Conv, client *sp.Client, getInfo GetInfoInterface, dataFromDb DataFromDatabaseInterface, snapshotMigration SnapshotMigrationInterface) (*writer.BatchWriter, error)
	dataFromDump(driver string, config writer.BatchWriterConfig, ioHelper *utils.IOStreams, client *sp.Client, conv *internal.Conv, dataOnly bool, processDump ProcessDumpByDialectInterface, populateDataConv PopulateDataConvInterface) (*writer.BatchWriter, error)
	dataFromCSV(ctx context.Context, sourceProfile profiles.SourceProfile, targetProfile profiles.TargetProfile, config writer.BatchWriterConfig, conv *internal.Conv, client *sp.Client, populateDataConv PopulateDataConvInterface, csv csv.CsvInterface) (*writer.BatchWriter, error)
}

type DataFromSourceImpl struct{}

func (sads *SchemaFromSourceImpl) schemaFromDatabase(migrationProjectId string, sourceProfile profiles.SourceProfile, targetProfile profiles.TargetProfile, getInfo GetInfoInterface, processSchema common.ProcessSchemaInterface) (*internal.Conv, error) {
	conv := internal.MakeConv()
	conv.SpDialect = targetProfile.Conn.Sp.Dialect
	conv.SpProjectId = targetProfile.Conn.Sp.Project
	conv.SpInstanceId = targetProfile.Conn.Sp.Instance
	conv.Source = sourceProfile.Driver
	//handle fetching schema differently for sharded migrations, we only connect to the primary shard to
	//fetch the schema. We reuse the SourceProfileConnection object for this purpose.
	var infoSchema common.InfoSchema
	var err error
	isSharded := false
	switch sourceProfile.Ty {
	case profiles.SourceProfileTypeConfig:
		isSharded = true
		//Find Primary Shard Name
		if sourceProfile.Config.ConfigType == constants.BULK_MIGRATION {
			schemaSource := sourceProfile.Config.ShardConfigurationBulk.SchemaSource
			infoSchema, err = getInfo.getInfoSchemaForShard(migrationProjectId, schemaSource, sourceProfile.Driver, targetProfile, &profiles.SourceProfileDialectImpl{}, &GetInfoImpl{})
			if err != nil {
				return conv, err
			}
		} else if sourceProfile.Config.ConfigType == constants.DATAFLOW_MIGRATION {
			schemaSource := sourceProfile.Config.ShardConfigurationDataflow.SchemaSource
			infoSchema, err = getInfo.getInfoSchemaForShard(migrationProjectId, schemaSource, sourceProfile.Driver, targetProfile, &profiles.SourceProfileDialectImpl{}, &GetInfoImpl{})
			if err != nil {
				return conv, err
			}
		} else if sourceProfile.Config.ConfigType == constants.DMS_MIGRATION {
			// TODO: Define the schema processing logic for DMS migrations here.
			return conv, fmt.Errorf("dms based migrations are not implemented yet")
		} else {
			return conv, fmt.Errorf("unknown type of migration, please select one of bulk, dataflow or dms")
		}
	case profiles.SourceProfileTypeCloudSQL:
		infoSchema, err = getInfo.GetInfoSchemaFromCloudSQL(migrationProjectId, sourceProfile, targetProfile)
		if err != nil {
			return conv, err
		}

	default:
		infoSchema, err = getInfo.GetInfoSchema(migrationProjectId, sourceProfile, targetProfile)
		if err != nil {
			return conv, err
		}
	}
	additionalSchemaAttributes := internal.AdditionalSchemaAttributes{
		IsSharded: isSharded,
	}

	ctx := context.Background()
	expressionVerificationAccessor, _ := expressions_api.NewExpressionVerificationAccessorImpl(ctx, conv.SpProjectId, conv.SpInstanceId)
	schemaToSpanner := common.SchemaToSpannerImpl{
		DdlV:                           sads.DdlVerifier,
		ExpressionVerificationAccessor: expressionVerificationAccessor,
	}
	return conv, processSchema.ProcessSchema(conv, infoSchema, common.DefaultWorkers, additionalSchemaAttributes, &schemaToSpanner, &common.UtilsOrderImpl{}, &common.InfoSchemaImpl{})
}

func (sads *SchemaFromSourceImpl) SchemaFromDump(SpProjectId string, SpInstanceId string, driver string, spDialect string, ioHelper *utils.IOStreams, processDump ProcessDumpByDialectInterface) (*internal.Conv, error) {
	f, n, err := getSeekable(ioHelper.In)
	if err != nil {
		utils.PrintSeekError(driver, err, ioHelper.Out)
		return nil, fmt.Errorf("can't get seekable input file")
	}
	ioHelper.SeekableIn = f
	ioHelper.BytesRead = n
	conv := internal.MakeConv()
	conv.SpDialect = spDialect
	conv.Source = driver
	conv.SpProjectId = SpProjectId
	conv.SpInstanceId = SpInstanceId
	p := internal.NewProgress(n, "Generating schema", internal.Verbose(), false, int(internal.SchemaCreationInProgress))
	r := internal.NewReader(bufio.NewReader(f), p)
	conv.SetSchemaMode() // Build schema and ignore data in dump.
	conv.SetDataSink(nil)
	err = processDump.ProcessDump(driver, conv, r)
	if err != nil {
		fmt.Fprintf(ioHelper.Out, "Failed to parse the data file: %v", err)
		return nil, fmt.Errorf("failed to parse the data file")
	}
	p.Done()
	return conv, nil
}

func (sads *DataFromSourceImpl) dataFromDump(driver string, config writer.BatchWriterConfig, ioHelper *utils.IOStreams, client *sp.Client, conv *internal.Conv, dataOnly bool, processDump ProcessDumpByDialectInterface, populateDataConv PopulateDataConvInterface) (*writer.BatchWriter, error) {
	// TODO: refactor of the way we handle getSeekable
	// to avoid the code duplication here
	if !dataOnly {
		_, err := ioHelper.SeekableIn.Seek(0, 0)
		if err != nil {
			fmt.Printf("\nCan't seek to start of file (preparation for second pass): %v\n", err)
			return nil, fmt.Errorf("can't seek to start of file")
		}
	} else {
		// Note: input file is kept seekable to plan for future
		// changes in showing progress for data migration.
		f, n, err := getSeekable(ioHelper.In)
		if err != nil {
			utils.PrintSeekError(driver, err, ioHelper.Out)
			return nil, fmt.Errorf("can't get seekable input file")
		}
		ioHelper.SeekableIn = f
		ioHelper.BytesRead = n
	}
	totalRows := conv.Rows()

	conv.Audit.Progress = *internal.NewProgress(totalRows, "Writing data to Spanner", internal.Verbose(), false, int(internal.DataWriteInProgress))
	r := internal.NewReader(bufio.NewReader(ioHelper.SeekableIn), nil)
	batchWriter := populateDataConv.populateDataConv(conv, config, client)
	processDump.ProcessDump(driver, conv, r)
	batchWriter.Flush()
	conv.Audit.Progress.Done()

	return batchWriter, nil
}

func (sads *DataFromSourceImpl) dataFromCSV(ctx context.Context, sourceProfile profiles.SourceProfile, targetProfile profiles.TargetProfile, config writer.BatchWriterConfig, conv *internal.Conv, client *sp.Client, populateDataConv PopulateDataConvInterface, csv csv.CsvInterface) (*writer.BatchWriter, error) {
	if targetProfile.Conn.Sp.Dbname == "" {
		return nil, fmt.Errorf("dbName is mandatory in target-profile for csv source")
	}
	conv.SpDialect = targetProfile.Conn.Sp.Dialect
	conv.SpProjectId = targetProfile.Conn.Sp.Project
	conv.SpInstanceId = targetProfile.Conn.Sp.Instance
	conv.Source = sourceProfile.Driver
	dialect, err := targetProfile.FetchTargetDialect(ctx)
	if err != nil {
		return nil, fmt.Errorf("could not fetch dialect: %v", err)
	}
	if strings.ToLower(dialect) != constants.DIALECT_POSTGRESQL {
		dialect = constants.DIALECT_GOOGLESQL
	}

	if dialect != conv.SpDialect {
		return nil, fmt.Errorf("dialect specified in target profile does not match spanner dialect")
	}

	delimiterStr := sourceProfile.Csv.Delimiter
	if len(delimiterStr) != 1 {
		return nil, fmt.Errorf("delimiter should only be a single character long, found '%s'", delimiterStr)
	}

	delimiter := rune(delimiterStr[0])

	err = utils.ReadSpannerSchema(ctx, conv, client)
	if err != nil {
		return nil, fmt.Errorf("error trying to read and convert spanner schema: %v", err)
	}

	tables, err := csv.GetCSVFiles(conv, sourceProfile)
	if err != nil {
		return nil, fmt.Errorf("error finding csv files: %v", err)
	}

	// Find the number of rows in each csv file for generating stats.
	err = csv.SetRowStats(conv, tables, delimiter)
	if err != nil {
		return nil, err
	}

	totalRows := conv.Rows()
	conv.Audit.Progress = *internal.NewProgress(totalRows, "Writing data to Spanner", internal.Verbose(), false, int(internal.DataWriteInProgress))
	batchWriter := populateDataConv.populateDataConv(conv, config, client)
	err = csv.ProcessCSV(conv, tables, sourceProfile.Csv.NullStr, delimiter)
	if err != nil {
		return nil, fmt.Errorf("can't process csv: %v", err)
	}
	batchWriter.Flush()
	conv.Audit.Progress.Done()
	return batchWriter, nil
}

func (sads *DataFromSourceImpl) dataFromDatabase(ctx context.Context, migrationProjectId string, sourceProfile profiles.SourceProfile, targetProfile profiles.TargetProfile, config writer.BatchWriterConfig, conv *internal.Conv, client *sp.Client, getInfo GetInfoInterface, dataFromDb DataFromDatabaseInterface, snapshotMigration SnapshotMigrationInterface) (*writer.BatchWriter, error) {
	//handle migrating data for sharded migrations differently
	//sharded migrations are identified via the config= flag, if that flag is not present
	//carry on with the existing code path in the else block
	switch sourceProfile.Ty {
	case profiles.SourceProfileTypeConfig:
		////There are three cases to cover here, bulk migrations and sharded migrations (and later DMS)
		//We provide an if-else based handling for each within the sharded code branch
		//This will be determined via the configType, which can be "bulk", "dataflow" or "dms"
		if sourceProfile.Config.ConfigType == constants.BULK_MIGRATION {
			return dataFromDb.dataFromDatabaseForBulkMigration(migrationProjectId, sourceProfile, targetProfile, config, conv, client, getInfo, snapshotMigration)
		} else if sourceProfile.Config.ConfigType == constants.DATAFLOW_MIGRATION {
			return dataFromDb.dataFromDatabaseForDataflowMigration(migrationProjectId, targetProfile, ctx, sourceProfile, conv, &common.InfoSchemaImpl{})
		} else if sourceProfile.Config.ConfigType == constants.DMS_MIGRATION {
			return dataFromDb.dataFromDatabaseForDMSMigration()
		} else {
			return nil, fmt.Errorf("configType should be one of 'bulk', 'dataflow' or 'dms'")
		}
	default:
		var infoSchema common.InfoSchema
		var err error
		if sourceProfile.Ty == profiles.SourceProfileTypeCloudSQL {
			infoSchema, err = getInfo.GetInfoSchemaFromCloudSQL(migrationProjectId, sourceProfile, targetProfile)
			if err != nil {
				return nil, err
			}
		} else {
			infoSchema, err = getInfo.GetInfoSchema(migrationProjectId, sourceProfile, targetProfile)
			if err != nil {
				return nil, err
			}
		}
		var streamInfo map[string]interface{}
		// minimal downtime migration for a single shard
		if sourceProfile.Conn.Streaming {
			//Generate a job Id
			migrationJobId := conv.Audit.MigrationRequestId
			logger.Log.Info(fmt.Sprintf("Creating a migration job with id: %v. This jobId can be used in future commmands (such as cleanup) to refer to this job.\n", migrationJobId))
			streamInfo, err = infoSchema.StartChangeDataCapture(ctx, conv)
			if err != nil {
				return nil, err
			}
			bw, err := snapshotMigration.snapshotMigrationHandler(sourceProfile, config, conv, client, infoSchema)
			if err != nil {
				return nil, err
			}
			dfOutput, err := infoSchema.StartStreamingMigration(ctx, migrationProjectId, client, conv, streamInfo)
			if err != nil {
				return nil, err
			}
			dfJobId := dfOutput.JobID
			gcloudCmd := dfOutput.GCloudCmd
			streamingCfg, _ := streamInfo["streamingCfg"].(streaming.StreamingCfg)
			// Fetch and store the GCS bucket associated with the datastream
			dsClient := GetDatastreamClient(ctx)
			gcsBucket, gcsDestPrefix, fetchGcsErr := streaming.FetchTargetBucketAndPath(ctx, dsClient, migrationProjectId, streamingCfg.DatastreamCfg.DestinationConnectionConfig, "data")
			if fetchGcsErr != nil {
				logger.Log.Info("Could not fetch GCS Bucket, hence Monitoring Dashboard will not contain Metrics for the gcs bucket\n")
				logger.Log.Debug("Error", zap.Error(fetchGcsErr))
			}

			// Try to apply lifecycle rule to Datastream destination bucket.
			gcsConfig := streamingCfg.GcsCfg
			sc, err := storageclient.NewStorageClientImpl(ctx)
			if err != nil {
				return nil, err
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

			monitoringResources := metrics.MonitoringMetricsResources{
				MigrationProjectId:   migrationProjectId,
				DataflowJobId:        dfOutput.JobID,
				DatastreamId:         streamingCfg.DatastreamCfg.StreamId,
				JobMetadataGcsBucket: gcsBucket,
				PubsubSubscriptionId: streamingCfg.PubsubCfg.SubscriptionId,
				SpannerProjectId:     targetProfile.Conn.Sp.Project,
				SpannerInstanceId:    targetProfile.Conn.Sp.Instance,
				SpannerDatabaseId:    targetProfile.Conn.Sp.Dbname,
				ShardId:              "",
				MigrationRequestId:   conv.Audit.MigrationRequestId,
			}
			respDash, dashboardErr := monitoringResources.CreateDataflowShardMonitoringDashboard(ctx)
			var dashboardName string
			if dashboardErr != nil {
				dashboardName = ""
				logger.Log.Info("Creation of the monitoring dashboard failed, please create the dashboard manually")
				logger.Log.Debug("Error", zap.Error(dashboardErr))
			} else {
				dashboardName = strings.Split(respDash.Name, "/")[3]
				fmt.Printf("Monitoring Dashboard: %+v\n", dashboardName)
			}
			// store the generated resources locally in conv, this is used as source of truth for persistence and the UI (should change to persisted values)
			streaming.StoreGeneratedResources(conv, streamingCfg, dfJobId, gcloudCmd, migrationProjectId, "", internal.GcsResources{BucketName: gcsBucket}, dashboardName)
			//persist job and shard level data in the metadata db
			err = streaming.PersistJobDetails(ctx, targetProfile, sourceProfile, conv, migrationJobId, false)
			if err != nil {
				logger.Log.Info(fmt.Sprintf("Error storing job details in SMT metadata store...the migration job will still continue as intended. %v", err))
			} else {
				//only attempt persisting shard level data if the job level data is persisted
				err = streaming.PersistResources(ctx, targetProfile, sourceProfile, conv, migrationJobId, constants.DEFAULT_SHARD_ID)
				if err != nil {
					logger.Log.Info(fmt.Sprintf("Error storing details for migration job: %s, data shard: %s in SMT metadata store...the migration job will still continue as intended. err = %v\n", migrationJobId, constants.DEFAULT_SHARD_ID, err))
				}
			}
			return bw, nil
		}
		//bulk migration for a single shard
		return snapshotMigration.performSnapshotMigration(config, conv, client, infoSchema, internal.AdditionalDataAttributes{ShardId: ""}, &common.InfoSchemaImpl{}, &PopulateDataConvImpl{}), nil
	}
}

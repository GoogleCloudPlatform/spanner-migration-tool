// Copyright 2023 Google LLC
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

// Package utils contains common helper functions used across multiple other packages.
// Utils should not import any Spanner migration tool packages.
package metrics

import (
	"context"
	"fmt"
	"math"

	"cloud.google.com/go/monitoring/dashboard/apiv1/dashboardpb"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal/reports"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/proto/migration"
)

// GetMigrationData returns migration data comprising source schema details,
// request id, target dialect, connection mechanism etc based on
// the conv object, source driver and target db
func GetMigrationData(conv *internal.Conv, driver, typeOfConv string) *migration.MigrationData {

	migrationData := migration.MigrationData{
		MigrationRequestId: &conv.Audit.MigrationRequestId,
	}
	if typeOfConv == constants.DataConv {
		return &migrationData
	}
	migrationData.MigrationType = conv.Audit.MigrationType
	migrationData.SourceConnectionMechanism, migrationData.Source = getMigrationDataSourceDetails(driver, &migrationData)
	migrationData.SchemaPatterns = getMigrationDataSchemaPatterns(conv, &migrationData)

	switch conv.SpDialect {
	case constants.DIALECT_GOOGLESQL:
		migrationData.TargetDialect = migration.MigrationData_GOOGLE_STANDARD_SQL.Enum()
	case constants.DIALECT_POSTGRESQL:
		migrationData.TargetDialect = migration.MigrationData_POSTGRESQL_STANDARD_SQL.Enum()
	default:
		migrationData.TargetDialect = migration.MigrationData_TARGET_DIALECT_UNSPECIFIED.Enum()
	}
	return &migrationData
}

// getMigrationDataSchemaPatterns returns schema petterns like number of tables, foreign key, primary key,
// indexes, interleaves, max interleave depth in the spanner schema and count of missing primary keys
// if any in source schema
func getMigrationDataSchemaPatterns(conv *internal.Conv, migrationData *migration.MigrationData) *migration.MigrationData_SchemaPatterns {

	numTables := int32(len(conv.SrcSchema))
	var numForeignKey, numIndexes, numMissingPrimaryKey, numInterleaves, maxInterleaveDepth, numColumns, numWarnings int32 = 0, 0, 0, 0, 0, 0, 0

	for srcTableId, srcSchema := range conv.SrcSchema {
		if len(srcSchema.PrimaryKeys) == 0 {
			numMissingPrimaryKey++
		}
		_, cols, warnings := reports.AnalyzeCols(conv, srcTableId)
		numColumns += int32(cols)
		numWarnings += int32(warnings)
	}

	for _, table := range conv.SpSchema {
		numForeignKey += int32(len(table.ForeignKeys))
		numIndexes += int32(len(table.Indexes))
		depth := 0
		tableId := table.Id
		for conv.SpSchema[tableId].ParentTable.Id != "" {
			numInterleaves++
			depth++
			tableId = conv.SpSchema[tableId].ParentTable.Id
		}
		maxInterleaveDepth = int32(math.Max(float64(maxInterleaveDepth), float64(depth)))
	}

	return &migration.MigrationData_SchemaPatterns{
		NumTables:            &numTables,
		NumForeignKey:        &numForeignKey,
		NumInterleaves:       &numInterleaves,
		MaxInterleaveDepth:   &maxInterleaveDepth,
		NumIndexes:           &numIndexes,
		NumMissingPrimaryKey: &numMissingPrimaryKey,
		NumColumns:           &numColumns,
		NumWarnings:          &numWarnings,
	}
}

// getMigrationDataSourceDetails returns source database type and
// source connection mechanism in migrationData object
func getMigrationDataSourceDetails(driver string, migrationData *migration.MigrationData) (*migration.MigrationData_SourceConnectionMechanism, *migration.MigrationData_Source) {

	switch driver {
	case constants.PGDUMP:
		return migration.MigrationData_DB_DUMP.Enum(), migration.MigrationData_POSTGRESQL.Enum()
	case constants.MYSQLDUMP:
		return migration.MigrationData_DB_DUMP.Enum(), migration.MigrationData_MYSQL.Enum()
	case constants.POSTGRES:
		return migration.MigrationData_DIRECT_CONNECTION.Enum(), migration.MigrationData_POSTGRESQL.Enum()
	case constants.MYSQL:
		return migration.MigrationData_DIRECT_CONNECTION.Enum(), migration.MigrationData_MYSQL.Enum()
	case constants.DYNAMODB:
		return migration.MigrationData_DIRECT_CONNECTION.Enum(), migration.MigrationData_DYNAMODB.Enum()
	case constants.ORACLE:
		return migration.MigrationData_DIRECT_CONNECTION.Enum(), migration.MigrationData_ORACLE.Enum()
	case constants.SQLSERVER:
		return migration.MigrationData_DIRECT_CONNECTION.Enum(), migration.MigrationData_SQL_SERVER.Enum()
	case constants.CSV:
		return migration.MigrationData_FILE.Enum(), migration.MigrationData_CSV.Enum()
	default:
		return migration.MigrationData_SOURCE_CONNECTION_MECHANISM_UNSPECIFIED.Enum(), migration.MigrationData_SOURCE_UNSPECIFIED.Enum()
	}
}

// CreateDataflowShardMonitoringDashboard returns a monitoring dashboard for a single shard
func (resourceIds MonitoringMetricsResources) CreateDataflowShardMonitoringDashboard(ctx context.Context) (*dashboardpb.Dashboard, error) {
	var mosaicGroups = []MosaicGroup{
		{groupTitle: fmt.Sprintf("Dataflow Job: %s", resourceIds.DataflowJobId), groupCreateTileFunction: createShardDataflowMetrics},
		{groupTitle: fmt.Sprintf("Datastream: %s", resourceIds.DatastreamId), groupCreateTileFunction: createShardDatastreamMetrics},
		{groupTitle: fmt.Sprintf("GCS Bucket: %s", resourceIds.JobMetadataGcsBucket), groupCreateTileFunction: createShardGcsMetrics},
		{groupTitle: fmt.Sprintf("Pubsub: %s", resourceIds.PubsubSubscriptionId), groupCreateTileFunction: createShardPubsubMetrics},
		{groupTitle: fmt.Sprintf("Spanner: instances/%s/databases/%s", resourceIds.SpannerInstanceId, resourceIds.SpannerDatabaseId), groupCreateTileFunction: createSpannerMetrics},
	}

	var dashboardDisplayName string
	if resourceIds.ShardId != "" {
		dashboardDisplayName = fmt.Sprintf("Migration Shard %s", resourceIds.ShardId)
	} else {
		dashboardDisplayName = fmt.Sprintf("Migration Dashboard %s", resourceIds.MigrationRequestId)
	}

	createDashboardReq := getCreateMonitoringDashboardRequest(resourceIds, createShardIndependentTopMetrics, mosaicGroups, nil, dashboardDisplayName)
	client := getDashboardClient(ctx)
	if client == nil {
		return nil, fmt.Errorf("dashboard client could not be created")
	}
	resp, err := client.CreateDashboard(ctx, createDashboardReq)
	if err != nil {
		return nil, err
	}
	return resp, err
}

// CreateDataflowAggMonitoringDashboard returns a monitoring dashboard for a sharded migration, aggregated across all shards
func (resourceIds MonitoringMetricsResources) CreateDataflowAggMonitoringDashboard(ctx context.Context) (*dashboardpb.Dashboard, error) {
	var mosaicGroups = []MosaicGroup{
		{groupTitle: "Summary of Dataflow Jobs", groupCreateTileFunction: createAggDataflowMetrics},
		{groupTitle: "Summary of Datastreams", groupCreateTileFunction: createAggDatastreamMetrics},
		{groupTitle: "Summary of GCS Buckets", groupCreateTileFunction: createAggGcsMetrics},
		{groupTitle: "Summary of Pubsubs", groupCreateTileFunction: createAggPubsubMetrics},
		{groupTitle: fmt.Sprintf("Spanner: instances/%s/databases/%s", resourceIds.SpannerInstanceId, resourceIds.SpannerDatabaseId), groupCreateTileFunction: createSpannerMetrics},
	}
	createDashboardReq := getCreateMonitoringDashboardRequest(resourceIds, createAggIndependentTopMetrics, mosaicGroups, createAggIndependentBottomMetrics, fmt.Sprintf("Migration Aggregated %s", resourceIds.MigrationRequestId))
	client := getDashboardClient(ctx)
	if client == nil {
		return nil, fmt.Errorf("dashboard client could not be created")
	}
	resp, err := client.CreateDashboard(ctx, createDashboardReq)
	if err != nil {
		return nil, err
	}
	return resp, err
}

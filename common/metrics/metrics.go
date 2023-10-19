package metrics

import (
	"math"

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
		for conv.SpSchema[tableId].ParentId != "" {
			numInterleaves++
			depth++
			tableId = conv.SpSchema[tableId].ParentId
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

type TileInfo struct {
	Title           string
	TimeSeriesQuery string
	XPos            int32
	YPos            int32
}

func GetDataflowCpuUtilMetric(projectId string, XPos int32, YPos int32, dataflowJobId string) TileInfo {
	return TileInfo{
		Title: "Dataflow Worker CPU Utilization",
		TimeSeriesQuery: "fetch gce_instance| metric 'compute.googleapis.com/instance/cpu/utilization'| filter " +
			"  resource.project_id == '" + projectId +
			"' && (metadata.user_labels.dataflow_job_id ==   '" + dataflowJobId +
			"')| group_by 1m, [value_utilization_mean: mean(value.utilization)]|" +
			" every 1m| group_by [metric.instance_name],  " +
			"  [value_utilization_mean_mean: mean(value_utilization_mean)]",
		XPos: XPos,
		YPos: YPos,
	}
}

func GetDatastreamThroughputMetric(projectId string, XPos int32, YPos int32, streamId string) TileInfo {
	return TileInfo{
		Title: "Datastream Throughput",
		TimeSeriesQuery: "fetch datastream.googleapis.com/Stream| metric 'datastream.googleapis.com/stream/event_count'" +
			"| filter (resource.stream_id == '" + streamId +
			"')| filter (resource.resource_container == '" + projectId +
			"')| align rate(1m)| group_by []," +
			"    [value_event_count_sum:     sum(value.event_count)]| " +
			"every 1m",
		XPos: XPos,
		YPos: YPos,
	}
}

func GetObjectCountGcsBucketMetric(projectId string, XPos int32, YPos int32, gcsBucketName string) TileInfo {
	return TileInfo{
		Title: "GCS Bucket Object Count",
		TimeSeriesQuery: "fetch gcs_bucket| metric 'storage.googleapis.com/storage/object_count'" +
			"| filter    resource.project_id == '" + projectId +
			"'&&   (resource.bucket_name     == '" + gcsBucketName +
			"'&& resource.location == 'us-central1')| group_by 1m, [value_object_count_mean:" +
			" mean(value.object_count)]| every 1m| group_by [], " +
			"[value_object_count_mean_mean: mean(value_object_count_mean)]",
		XPos: XPos,
		YPos: YPos,
	}
}

func GetDataflowSuccessfulEventsMetric(projectId string, XPos int32, YPos int32, dataflowJobId string) TileInfo {
	return TileInfo{
		Title: "Dataflow Successful Events",
		TimeSeriesQuery: "fetch dataflow_job | metric 'dataflow.googleapis.com/job/user_counter'" +
			"| filter resource.project_id == '" + projectId +
			"'  && (metric.job_id == '" + dataflowJobId +
			"'  && metric.metric_name == 'Successful events') | " +
			"group_by 1m, [value_user_counter_mean: mean(value.user_counter)] " +
			"| every 1m | group_by [], [value_user_counter_mean_mean: mean(value_user_counter_mean)]",
		XPos: XPos,
		YPos: YPos,
	}
}

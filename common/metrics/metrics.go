package metrics

import (
	"context"
	"math"

	dashboard "cloud.google.com/go/monitoring/dashboard/apiv1"
	"cloud.google.com/go/monitoring/dashboard/apiv1/dashboardpb"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal/reports"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/proto/migration"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/streaming"
	dataflowpb "google.golang.org/genproto/googleapis/dataflow/v1beta3"
)

const (
	dataflowCpuUtilQuery = "fetch datastream.googleapis.com/Stream| metric 'datastream.googleapis.com/stream/event_count'" +
		"| filter (resource.stream_id == '%s')| filter (resource.resource_container == '%s')| align rate(1m)| group_by [], " +
		"[value_event_count_sum:     sum(value.event_count)]| every 1m"
	dataflowBacklogTimeQuery = "fetch dataflow_job | metric 'dataflow.googleapis.com/job/estimated_backlog_processing_time' | " +
		"filter resource.project_id == '%s' && (metric.job_id == '%s') | group_by 1m, " +
		"[value_estimated_backlog_processing_time_mean: mean(value.estimated_backlog_processing_time)] | every 1m"
	datastreamTotalLatencyQuery = "fetch datastream.googleapis.com/Stream | metric 'datastream.googleapis.com/stream/total_latencies' " +
		"| filter resource.resource_container == '%s' | filter (resource.stream_id == '%s') | " +
		"align delta(1m) | every 1m | group_by [], [value_total_latencies_percentile: percentile(value.total_latencies, %s)]"
	datastreamUnsupportedEventsQuery = "fetch datastream.googleapis.com/Stream| metric 'datastream.googleapis.com/stream/unsupported_event_count'| " +
		"filter (resource.resource_container == '%s')| filter (resource.stream_id == '%s')| align delta(10m)| every 10m| group_by [], " +
		"[value_unsupported_event_count_sum: sum(value.unsupported_event_count)]"
	datastreamThroughputQuery = "fetch datastream.googleapis.com/Stream| metric 'datastream.googleapis.com/stream/event_count'" +
		"| filter (resource.resource_container == '%s') | filter (resource.stream_id == '%s')| align rate(1m)| group_by [], " +
		"[value_event_count_sum: sum(value.event_count)]| every 1m"
	gcsTotalBytesQuery = "fetch gcs_bucket | metric 'storage.googleapis.com/storage/total_bytes' | filter resource.project_id == '%s' && " +
		"(resource.bucket_name == '%s') | group_by 1m, [value_total_bytes_mean: mean(value.total_bytes)] | every 1m | " +
		"group_by [], [value_total_bytes_mean_aggregate: aggregate(value_total_bytes_mean)]"
	pubsubSubscriptionSentMessageCountQuery = "fetch pubsub_subscription | metric 'pubsub.googleapis.com/subscription/sent_message_count' | " +
		"filter resource.project_id == '%s' && (resource.subscription_id == '%s') | align rate(1m) | every 1m | group_by [], " +
		"[value_sent_message_count_aggregate: aggregate(value.sent_message_count)]"
	pubsubOldestUnackedMessageAgeQuery = "fetch pubsub_subscription | metric 'pubsub.googleapis.com/subscription/oldest_unacked_message_age' | " +
		"filter resource.project_id == '%s' && (resource.subscription_id == '%s') | group_by 1m, " +
		"[value_oldest_unacked_message_age_mean: mean(value.oldest_unacked_message_age)] | every 1m | group_by [], " +
		"[value_oldest_unacked_message_age_mean_max: max(value_oldest_unacked_message_age_mean)]"

	DEFAULT_MONITORING_METRIC_HEIGHT int32 = 16
	DEFAULT_MONITORING_METRIC_WIDTH  int32 = 24
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

func CreateDataflowMonitoringDashboard(ctx context.Context, project string, datastreamCfg streaming.DatastreamCfg, respDf *dataflowpb.LaunchFlexTemplateResponse, streamingCfg streaming.StreamingCfg) (*dashboardpb.Dashboard, error) {
	mosaicLayoutTiles := []*dashboardpb.MosaicLayout_Tile{
		createTile(GetDataflowCpuUtilMetric(project, getNthTileXCoordinate(0, 2), getNthTileYCoordinate(0, 2), respDf.Job.Id)),
		createTile(GetDatastreamThroughputMetric(project, getNthTileXCoordinate(1, 2), getNthTileYCoordinate(1, 2), datastreamCfg.StreamId)),
		createTile(GetObjectCountGcsBucketMetric(project, getNthTileXCoordinate(2, 2), getNthTileYCoordinate(2, 2), streamingCfg.TmpDir)),
		createTile(GetDataflowSuccessfulEventsMetric(project, getNthTileXCoordinate(3, 2), getNthTileYCoordinate(3, 2), respDf.Job.Id)),
	}
	mosaicLayout := dashboardpb.MosaicLayout{
		Columns: 48,
		Tiles:   mosaicLayoutTiles,
	}
	layout := dashboardpb.Dashboard_MosaicLayout{
		MosaicLayout: &mosaicLayout,
	}
	db := dashboardpb.Dashboard{
		DisplayName: "sample migration dashboard",
		Layout:      &layout,
	}
	req := &dashboardpb.CreateDashboardRequest{
		Parent:    "projects/" + project,
		Dashboard: &db,
	}
	client, _ := dashboard.NewDashboardsClient(ctx)
	defer client.Close()
	resp, err := client.CreateDashboard(ctx, req)
	return resp, err
}

func createDataflowMetrics(ctx context.Context, projectId string, dataflowJobId string) []*dashboardpb.MosaicLayout_Tile {
	var dataflowTiles []dashboardpb.MosaicLayout_Tile

}
func createTile(tileInfo TileInfo) *dashboardpb.MosaicLayout_Tile {
	tile := dashboardpb.MosaicLayout_Tile{
		Height: 24,
		Width:  16,
		XPos:   tileInfo.Height,
		YPos:   tileInfo.Width,
		Widget: &dashboardpb.Widget{
			Title: tileInfo.Title,
			Content: &dashboardpb.Widget_XyChart{
				XyChart: &dashboardpb.XyChart{
					ChartOptions: &dashboardpb.ChartOptions{
						Mode: dashboardpb.ChartOptions_COLOR,
					},
					DataSets: []*dashboardpb.XyChart_DataSet{
						{
							PlotType:   dashboardpb.XyChart_DataSet_LINE,
							TargetAxis: dashboardpb.XyChart_DataSet_Y1,
							TimeSeriesQuery: &dashboardpb.TimeSeriesQuery{
								Source: &dashboardpb.TimeSeriesQuery_TimeSeriesQueryLanguage{
									TimeSeriesQueryLanguage: tileInfo.TimeSeriesQuery,
								},
							},
						},
					},
				},
			},
		},
	}
	return &tile
}

type TileInfo struct {
	Title           string
	TimeSeriesQuery string
	Height          int32
	Width           int32
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
		Height: XPos,
		Width:  YPos,
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
		Height: XPos,
		Width:  YPos,
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
		Height: XPos,
		Width:  YPos,
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
		Height: XPos,
		Width:  YPos,
	}
}

func getNthTileXCoordinate(n int32, numColumns int32) int32 {
	return (n % numColumns) * 16
}

func getNthTileYCoordinate(n int32, numColumns int32) int32 {
	return (n / numColumns) * 24
}

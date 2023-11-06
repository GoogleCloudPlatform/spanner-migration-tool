package metrics

import (
	"context"
	"fmt"
	"math"
	"strings"

	dashboard "cloud.google.com/go/monitoring/dashboard/apiv1"
	"cloud.google.com/go/monitoring/dashboard/apiv1/dashboardpb"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal/reports"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/proto/migration"
)

const (
	dataflowCpuUtilQuery = "fetch gce_instance | metric 'compute.googleapis.com/instance/cpu/utilization' | filter " +
		"(metadata.user_labels.dataflow_job_id == '%s') && resource.project_id == '%s' | group_by 1m, " +
		"[value_utilization_mean: mean(value.utilization)] | every 1m | group_by [metric.instance_name], " +
		"[value_utilization_mean_percentile: percentile(value_utilization_mean, 90)]"
	dataflowMemoryUtilQuery = "fetch gce_instance | metric 'compute.googleapis.com/guest/memory/bytes_used' | filter (metadata.user_labels.dataflow_job_id == '%s') " +
		"&& resource.project_id == '%s' && (metric.state == 'used') | align next_older(1m) | every 1m | " +
		"group_by [metric.instance_name], [value_bytes_used_percentile: percentile(value.bytes_used, 90 )]"
	dataflowBacklogTimeQuery = "fetch dataflow_job | metric 'dataflow.googleapis.com/job/estimated_backlog_processing_time' | " +
		"filter (metric.job_id == '%s') && resource.project_id == '%s' | group_by 1m, " +
		"[value_estimated_backlog_processing_time_mean: mean(value.estimated_backlog_processing_time)] | every 1m"
	datastreamTotalLatencyQuery = "fetch datastream.googleapis.com/Stream | metric 'datastream.googleapis.com/stream/total_latencies' " +
		"| filter (resource.stream_id == '%s') | filter resource.resource_container == '%s' | " +
		"align delta(1m) | every 1m | group_by [], [value_total_latencies_percentile: percentile(value.total_latencies, %s)]"
	datastreamUnsupportedEventsQuery = "fetch datastream.googleapis.com/Stream| metric 'datastream.googleapis.com/stream/unsupported_event_count'| " +
		"filter (resource.stream_id == '%s') | filter (resource.resource_container == '%s') | align delta(10m)| every 10m| group_by [], " +
		"[value_unsupported_event_count_sum: sum(value.unsupported_event_count)]"
	datastreamThroughputQuery = "fetch datastream.googleapis.com/Stream| metric 'datastream.googleapis.com/stream/event_count'" +
		"| filter (resource.stream_id == '%s') | filter (resource.resource_container == '%s')| align rate(1m)| group_by [], " +
		"[value_event_count_sum: mean(value.event_count)]| every 1m"
	gcsTotalBytesQuery = "fetch gcs_bucket | metric 'storage.googleapis.com/storage/total_bytes' | filter " +
		"(resource.bucket_name == '%s') && resource.project_id == '%s' | group_by 1m, [value_total_bytes_mean: mean(value.total_bytes)] | every 1m | " +
		"group_by [], [value_total_bytes_mean_aggregate: aggregate(value_total_bytes_mean)]"
	pubsubSubscriptionSentMessageCountQuery = "fetch pubsub_subscription | metric 'pubsub.googleapis.com/subscription/sent_message_count' | " +
		"filter (resource.subscription_id == '%s') && resource.project_id == '%s' | align rate(1m) | every 1m | group_by [], " +
		"[value_sent_message_count_aggregate: aggregate(value.sent_message_count)]"
	pubsubOldestUnackedMessageAgeQuery = "fetch pubsub_subscription | metric 'pubsub.googleapis.com/subscription/oldest_unacked_message_age' | " +
		"filter (resource.subscription_id == '%s') && resource.project_id == '%s' | group_by 1m, " +
		"[value_oldest_unacked_message_age_mean: mean(value.oldest_unacked_message_age)] | every 1m | group_by [], " +
		"[value_oldest_unacked_message_age_mean_max: max(value_oldest_unacked_message_age_mean)]"
	spannerCpuUtilDbQuery = "fetch spanner_instance | metric 'spanner.googleapis.com/instance/cpu/utilization' | " +
		"filter (resource.instance_id == '%s') && (metric.database == '%s')  && resource.project_id == '%s' " +
		"| group_by 1m, [value_utilization_mean: mean(value.utilization)] | every 1m | group_by [], " +
		"[value_utilization_mean_percentile: percentile(value_utilization_mean, 90)]"
	spannerCpuUtilInstanceQuery = "fetch spanner_instance | metric 'spanner.googleapis.com/instance/cpu/utilization' | filter (resource.instance_id == '%s') && resource.project_id == '%s' |" +
		" group_by 1m, [value_utilization_mean: mean(value.utilization)] | every 1m | group_by [], " +
		"[value_utilization_mean_percentile: percentile(value_utilization_mean, 90)]"
	spannerStorageUtilDbQuery = "fetch spanner_instance | metric 'spanner.googleapis.com/instance/storage/used_bytes' | " +
		"filter (resource.instance_id == '%s') && (metric.database == '%s') && resource.project_id == '%s' | " +
		"group_by 1m, [value_used_bytes_mean: mean(value.used_bytes)] | every 1m | group_by [], " +
		"[value_used_bytes_mean_aggregate: aggregate(value_used_bytes_mean)]"
	spannerStorageUtilInstanceQuery = "fetch spanner_instance | metric 'spanner.googleapis.com/instance/storage/used_bytes' | filter " +
		"(resource.instance_id == '%s') && resource.project_id == '%s' | group_by 1m, " +
		"[value_used_bytes_mean: mean(value.used_bytes)] | every 1m | group_by [], " +
		"[value_used_bytes_mean_aggregate: aggregate(value_used_bytes_mean)]"
	defaultMonitoringMetricHeight int32 = 16
	defaultMonitoringMetricWidth  int32 = 16
	defaultColumns                int32 = 3
)

type MonitoringMetricsResources struct {
	ProjectId            string
	DataflowJobId        string
	DatastreamId         string
	GcsBucketId          string
	PubsubSubscriptionId string
	SpannerInstanceId    string
	SpannerDatabaseId    string
	ShardId              string
}

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

func CreateDataflowMonitoringDashboard(ctx context.Context, resourceIds MonitoringMetricsResources) (*dashboardpb.Dashboard, error) {
	var mosaicLayoutTiles []*dashboardpb.MosaicLayout_Tile
	var heightOffset int32 = 0
	// create independent metrics tiles
	independentMetricsTiles := createIndependentMetrics(resourceIds)
	heightOffset += setWidgetPositions(independentMetricsTiles, heightOffset)
	mosaicLayoutTiles = append(mosaicLayoutTiles, independentMetricsTiles...)
	// create dataflow metrics
	dataflowMetricsTiles := createDataflowMetrics(resourceIds)
	dataflowMetricsGroupTile, newOffsetHeight := createCollapsibleGroupTile(TileInfo{Title: fmt.Sprintf("Dataflow Job: %s", resourceIds.DataflowJobId)}, dataflowMetricsTiles, heightOffset)
	heightOffset = newOffsetHeight
	mosaicLayoutTiles = append(append(mosaicLayoutTiles, dataflowMetricsTiles...), dataflowMetricsGroupTile)
	// create datastream metrics tiles
	datastreamMetricsTiles := createDatastreamMetrics(resourceIds)
	datastreamMetricsGroupTile, newOffsetHeight := createCollapsibleGroupTile(TileInfo{Title: fmt.Sprintf("Datastream: %s", resourceIds.DatastreamId)}, datastreamMetricsTiles, heightOffset)
	heightOffset = newOffsetHeight
	mosaicLayoutTiles = append(append(mosaicLayoutTiles, datastreamMetricsTiles...), datastreamMetricsGroupTile)
	// create gcs bucket metrics tiles
	gcsMetricsTiles := createGcsMetrics(resourceIds)
	gcsMetricsGroupTile, newOffsetHeight := createCollapsibleGroupTile(TileInfo{Title: fmt.Sprintf("GCS Bucket: %s", strings.Split(resourceIds.GcsBucketId, "/")[2])}, gcsMetricsTiles, heightOffset)
	heightOffset = newOffsetHeight
	mosaicLayoutTiles = append(append(mosaicLayoutTiles, gcsMetricsTiles...), gcsMetricsGroupTile)
	// create pubsub metrics tiles
	pubsubMetricsTiles := createPubsubMetrics(resourceIds)
	pubsubMetricsGroupTile, newOffsetHeight := createCollapsibleGroupTile(TileInfo{Title: fmt.Sprintf("Pubsub: %s", resourceIds.PubsubSubscriptionId)}, pubsubMetricsTiles, heightOffset)
	heightOffset = newOffsetHeight
	mosaicLayoutTiles = append(append(mosaicLayoutTiles, pubsubMetricsTiles...), pubsubMetricsGroupTile)
	// create spanner metrics tiles
	spannerMetricsTiles := createSpannerMetrics(resourceIds)
	spannerMetricsGroupTile, newOffsetHeight := createCollapsibleGroupTile(TileInfo{Title: fmt.Sprintf("Spanner: instances/%s/databases/%s", resourceIds.SpannerInstanceId, resourceIds.SpannerDatabaseId)}, spannerMetricsTiles, heightOffset)
	heightOffset = newOffsetHeight
	mosaicLayoutTiles = append(append(mosaicLayoutTiles, spannerMetricsTiles...), spannerMetricsGroupTile)
	mosaicLayout := dashboardpb.MosaicLayout{
		Columns: 48,
		Tiles:   mosaicLayoutTiles,
	}
	layout := dashboardpb.Dashboard_MosaicLayout{
		MosaicLayout: &mosaicLayout,
	}
	db := dashboardpb.Dashboard{
		DisplayName: fmt.Sprintf("Shard Migration Dashboard: %s", resourceIds.ShardId),
		Layout:      &layout,
	}
	req := &dashboardpb.CreateDashboardRequest{
		Parent:    "projects/" + resourceIds.ProjectId,
		Dashboard: &db,
	}
	client, err := dashboard.NewDashboardsClient(ctx)
	if err != nil {
		return nil, err
	}
	defer client.Close()
	resp, err := client.CreateDashboard(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp, err
}

func createDataflowMetrics(resourceIds MonitoringMetricsResources) []*dashboardpb.MosaicLayout_Tile {
	dataflowTiles := []*dashboardpb.MosaicLayout_Tile{
		createXYChartTile(TileInfo{"Dataflow Workers CPU Utilization", map[string]string{"": fmt.Sprintf(dataflowCpuUtilQuery, resourceIds.DataflowJobId, resourceIds.ProjectId)}}),
		createXYChartTile(TileInfo{"Dataflow Workers Memory Utilization", map[string]string{"": fmt.Sprintf(dataflowMemoryUtilQuery, resourceIds.DataflowJobId, resourceIds.ProjectId)}}),
		createXYChartTile(TileInfo{"Dataflow Workers Backlog Time Seconds", map[string]string{"": fmt.Sprintf(dataflowBacklogTimeQuery, resourceIds.DataflowJobId, resourceIds.ProjectId)}}),
	}
	return dataflowTiles
}
func createDatastreamMetrics(resourceIds MonitoringMetricsResources) []*dashboardpb.MosaicLayout_Tile {
	datastreamTiles := []*dashboardpb.MosaicLayout_Tile{
		createXYChartTile(TileInfo{
			"Datastream Total Latency",
			map[string]string{"p50 " + resourceIds.DatastreamId: fmt.Sprintf(datastreamTotalLatencyQuery, resourceIds.DatastreamId, resourceIds.ProjectId, "50"), "p90 " + resourceIds.DatastreamId: fmt.Sprintf(datastreamTotalLatencyQuery, resourceIds.DatastreamId, resourceIds.ProjectId, "90")}}),
		createXYChartTile(TileInfo{"Datastream Throughput Query", map[string]string{resourceIds.DatastreamId: fmt.Sprintf(datastreamThroughputQuery, resourceIds.DatastreamId, resourceIds.ProjectId)}}),
		createXYChartTile(TileInfo{"Datastream Unsupported Latency", map[string]string{resourceIds.DatastreamId: fmt.Sprintf(datastreamUnsupportedEventsQuery, resourceIds.DatastreamId, resourceIds.ProjectId)}}),
	}
	return datastreamTiles
}
func createGcsMetrics(resourceIds MonitoringMetricsResources) []*dashboardpb.MosaicLayout_Tile {
	gcsBucketTiles := []*dashboardpb.MosaicLayout_Tile{
		createXYChartTile(TileInfo{"GCS Bucket Total Bytes", map[string]string{resourceIds.GcsBucketId: fmt.Sprintf(gcsTotalBytesQuery, strings.Split(resourceIds.GcsBucketId, "/")[2], resourceIds.ProjectId)}}),
	}
	return gcsBucketTiles
}
func createSpannerMetrics(resourceIds MonitoringMetricsResources) []*dashboardpb.MosaicLayout_Tile {
	spannerTiles := []*dashboardpb.MosaicLayout_Tile{
		createXYChartTile(
			TileInfo{"Spanner CPU Utilisation",
				map[string]string{"Database CPU Utilisation": fmt.Sprintf(spannerCpuUtilDbQuery, resourceIds.SpannerInstanceId, resourceIds.SpannerDatabaseId, resourceIds.ProjectId), "Instance CPU Utilisation": fmt.Sprintf(spannerCpuUtilInstanceQuery, resourceIds.SpannerInstanceId, resourceIds.ProjectId)}}),
		createXYChartTile(
			TileInfo{"Spanner Storage",
				map[string]string{"Database Storage": fmt.Sprintf(spannerStorageUtilDbQuery, resourceIds.SpannerInstanceId, resourceIds.SpannerDatabaseId, resourceIds.ProjectId), "Instance Storage": fmt.Sprintf(spannerStorageUtilInstanceQuery, resourceIds.SpannerInstanceId, resourceIds.ProjectId)}}),
	}
	return spannerTiles
}
func createPubsubMetrics(resourceIds MonitoringMetricsResources) []*dashboardpb.MosaicLayout_Tile {
	pubsubTiles := []*dashboardpb.MosaicLayout_Tile{
		createXYChartTile(TileInfo{"Pubsub Subscription Sent Message Count", map[string]string{resourceIds.PubsubSubscriptionId: fmt.Sprintf(pubsubSubscriptionSentMessageCountQuery, resourceIds.PubsubSubscriptionId, resourceIds.ProjectId)}}),
		createXYChartTile(TileInfo{"Pubsub Age of Oldest Unacknowledged Message", map[string]string{resourceIds.PubsubSubscriptionId: fmt.Sprintf(pubsubOldestUnackedMessageAgeQuery, resourceIds.PubsubSubscriptionId, resourceIds.ProjectId)}}),
	}
	return pubsubTiles
}
func createIndependentMetrics(resourceIds MonitoringMetricsResources) []*dashboardpb.MosaicLayout_Tile {
	independentMetricsTiles := []*dashboardpb.MosaicLayout_Tile{
		createXYChartTile(TileInfo{"Dataflow Workers CPU Utilization", map[string]string{"": fmt.Sprintf(dataflowCpuUtilQuery, resourceIds.DataflowJobId, resourceIds.ProjectId)}}),
		createXYChartTile(TileInfo{"Datastream Throughput Query", map[string]string{resourceIds.DatastreamId: fmt.Sprintf(datastreamThroughputQuery, resourceIds.DatastreamId, resourceIds.ProjectId)}}),
		createXYChartTile(TileInfo{"Datastream Unsupported Latency", map[string]string{resourceIds.DatastreamId: fmt.Sprintf(datastreamUnsupportedEventsQuery, resourceIds.DatastreamId, resourceIds.ProjectId)}}),
		createXYChartTile(TileInfo{"Pubsub Age of Oldest Unacknowledged Message", map[string]string{resourceIds.PubsubSubscriptionId: fmt.Sprintf(pubsubOldestUnackedMessageAgeQuery, resourceIds.PubsubSubscriptionId, resourceIds.ProjectId)}}), createXYChartTile(
			TileInfo{"Spanner CPU Utilisation",
				map[string]string{"Database CPU Utilisation": fmt.Sprintf(spannerCpuUtilDbQuery, resourceIds.SpannerInstanceId, resourceIds.SpannerDatabaseId, resourceIds.ProjectId), "Instance CPU Utilisation": fmt.Sprintf(spannerCpuUtilInstanceQuery, resourceIds.SpannerInstanceId, resourceIds.ProjectId)}}),
	}
	return independentMetricsTiles
}
func createXYChartTile(tileInfo TileInfo) *dashboardpb.MosaicLayout_Tile {
	var dataSets []*dashboardpb.XyChart_DataSet
	for legendTemplate, query := range tileInfo.TimeSeriesQueries {
		ds := &dashboardpb.XyChart_DataSet{
			PlotType:   dashboardpb.XyChart_DataSet_LINE,
			TargetAxis: dashboardpb.XyChart_DataSet_Y1,
			TimeSeriesQuery: &dashboardpb.TimeSeriesQuery{
				Source: &dashboardpb.TimeSeriesQuery_TimeSeriesQueryLanguage{
					TimeSeriesQueryLanguage: query,
				},
			},
		}
		if legendTemplate != "" {
			ds.LegendTemplate = legendTemplate
		}
		dataSets = append(dataSets, ds)
	}
	tile := dashboardpb.MosaicLayout_Tile{
		Widget: &dashboardpb.Widget{
			Title: tileInfo.Title,
			Content: &dashboardpb.Widget_XyChart{
				XyChart: &dashboardpb.XyChart{
					ChartOptions: &dashboardpb.ChartOptions{
						Mode: dashboardpb.ChartOptions_COLOR,
					},
					DataSets: dataSets,
				},
			},
		},
	}
	return &tile
}

func createCollapsibleGroupTile(tileInfo TileInfo, tiles []*dashboardpb.MosaicLayout_Tile, heightOffset int32) (*dashboardpb.MosaicLayout_Tile, int32) {
	groupTileHeight := setWidgetPositions(tiles, heightOffset)
	groupTile := dashboardpb.MosaicLayout_Tile{
		XPos:   0,
		YPos:   heightOffset,
		Width:  defaultMonitoringMetricWidth * defaultColumns,
		Height: groupTileHeight,
		Widget: &dashboardpb.Widget{
			Title: tileInfo.Title,
			Content: &dashboardpb.Widget_CollapsibleGroup{
				CollapsibleGroup: &dashboardpb.CollapsibleGroup{
					Collapsed: true,
				},
			},
		},
	}
	return &groupTile, heightOffset + groupTileHeight
}

func setWidgetPositions(tiles []*dashboardpb.MosaicLayout_Tile, heightOffset int32) int32 {
	for tilePosition, tile := range tiles {
		tile.XPos = (int32(tilePosition) % defaultColumns) * defaultMonitoringMetricWidth
		tile.YPos = heightOffset + (int32(tilePosition)/defaultColumns)*defaultMonitoringMetricHeight
		tile.Width = defaultMonitoringMetricWidth
		tile.Height = defaultMonitoringMetricHeight
	}
	return ((int32(len(tiles)-1) / defaultColumns) + 1) * defaultMonitoringMetricHeight
}

type TileInfo struct {
	Title             string
	TimeSeriesQueries map[string]string
}

package metrics

import (
	"context"
	"fmt"
	"math"
	"sync"

	dashboard "cloud.google.com/go/monitoring/dashboard/apiv1"
	"cloud.google.com/go/monitoring/dashboard/apiv1/dashboardpb"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal/reports"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/proto/migration"
)

// Defines dimensions for Monitoring Dashboard Metrics
const (
	// Default height of a tile in the monitoring dashboard
	defaultMonitoringMetricHeight int32 = 16
	// Default width of a tile in the monitoring dashboard
	defaultMonitoringMetricWidth int32 = 16
	// Default columns in the monitoring dashboard
	defaultColumns       int32 = 3
	defaultMosaicColumns int32 = 48
)

var once sync.Once
var dashboardClient *dashboard.DashboardsClient

// MonitoringMetricsResources contains information required to create the monitoring dashboard
type MonitoringMetricsResources struct {
	ProjectId                string
	DataflowJobId            string
	DatastreamId             string
	GcsBucketId              string
	PubsubSubscriptionId     string
	SpannerInstanceId        string
	SpannerDatabaseId        string
	ShardToDataStreamNameMap map[string]string
	ShardToDataflowInfoMap   map[string]internal.ShardedDataflowJobResources
	ShardToPubsubIdMap       map[string]internal.PubsubCfg
	ShardToGcsMap            map[string]internal.GcsResources
	ShardId                  string
}

type TileInfo struct {
	Title             string
	TimeSeriesQueries map[string]string // Map of legend template and their corresponding queries
}

type MosaicGroup struct {
	groupTitle              string
	groupCreateTileFunction func(resourceIds MonitoringMetricsResources) []*dashboardpb.MosaicLayout_Tile
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

func getDashboardClient(ctx context.Context) *dashboard.DashboardsClient {
	if dashboardClient == nil {
		once.Do(func() {
			dashboardClient, _ = dashboard.NewDashboardsClient(ctx)
		})
		return dashboardClient
	}
	return dashboardClient
}

// CreateDataflowShardMonitoringDashboard returns a monitoring dashboard for a single shard
func (resourceIds MonitoringMetricsResources) CreateDataflowShardMonitoringDashboard(ctx context.Context) (*dashboardpb.Dashboard, error) {
	var mosaicGroups = []MosaicGroup{
		{groupTitle: fmt.Sprintf("Dataflow Job: %s", resourceIds.DataflowJobId), groupCreateTileFunction: createShardDataflowMetrics},
		{groupTitle: fmt.Sprintf("Datastream: %s", resourceIds.DatastreamId), groupCreateTileFunction: createShardDatastreamMetrics},
		{groupTitle: fmt.Sprintf("GCS Bucket: %s", resourceIds.GcsBucketId), groupCreateTileFunction: createShardGcsMetrics},
		{groupTitle: fmt.Sprintf("Pubsub: %s", resourceIds.PubsubSubscriptionId), groupCreateTileFunction: createShardPubsubMetrics},
		{groupTitle: fmt.Sprintf("Spanner: instances/%s/databases/%s", resourceIds.SpannerInstanceId, resourceIds.SpannerDatabaseId), groupCreateTileFunction: createSpannerMetrics},
	}
	dashboardDisplayName := "Migration Dashboard"
	if resourceIds.ShardId != "" {
		dashboardDisplayName = fmt.Sprintf("Shard Migration Dashboard %s", resourceIds.ShardId)
	}
	createDashboardReq := getCreateMonitoringDashboardRequest(resourceIds, createShardIndependentMetrics, mosaicGroups, dashboardDisplayName)
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
	noOfShard := len(resourceIds.ShardToDataflowInfoMap)
	createDashboardReq := getCreateMonitoringDashboardRequest(resourceIds, createAggIndependentMetrics, mosaicGroups, fmt.Sprintf("Aggregated Migration Dashboard of %v shards", noOfShard))
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

func createSpannerMetrics(resourceIds MonitoringMetricsResources) []*dashboardpb.MosaicLayout_Tile {
	spannerTiles := []*dashboardpb.MosaicLayout_Tile{
		createXYChartTile(
			TileInfo{"Spanner CPU Utilisation",
				map[string]string{"Database CPU Utilisation": fmt.Sprintf(spannerCpuUtilDbQuery, resourceIds.SpannerInstanceId, resourceIds.SpannerDatabaseId), "Instance CPU Utilisation": fmt.Sprintf(spannerCpuUtilInstanceQuery, resourceIds.SpannerInstanceId)}}),
		createXYChartTile(
			TileInfo{"Spanner Storage",
				map[string]string{"Database Storage": fmt.Sprintf(spannerStorageUtilDbQuery, resourceIds.SpannerDatabaseId, resourceIds.SpannerInstanceId), "Instance Storage": fmt.Sprintf(spannerStorageUtilInstanceQuery, resourceIds.SpannerInstanceId)}}),
	}
	return spannerTiles
}

func createShardDataflowMetrics(resourceIds MonitoringMetricsResources) []*dashboardpb.MosaicLayout_Tile {
	dataflowTiles := []*dashboardpb.MosaicLayout_Tile{
		createXYChartTile(TileInfo{
			"Dataflow Workers CPU Utilization",
			map[string]string{
				"p50 worker": fmt.Sprintf(dataflowCpuUtilPercentileQuery, resourceIds.DataflowJobId, "50"),
				"p90 worker": fmt.Sprintf(dataflowCpuUtilPercentileQuery, resourceIds.DataflowJobId, "90"),
				"Max worker": fmt.Sprintf(dataflowCpuUtilMaxQuery, resourceIds.DataflowJobId),
			}}),
		createXYChartTile(TileInfo{"Dataflow Workers Memory Utilization", map[string]string{
			"p50 worker": fmt.Sprintf(dataflowMemoryUtilPercentileQuery, resourceIds.DataflowJobId, "50"),
			"p90 worker": fmt.Sprintf(dataflowMemoryUtilPercentileQuery, resourceIds.DataflowJobId, "90"),
			"Max worker": fmt.Sprintf(dataflowMemoryUtilMaxQuery, resourceIds.DataflowJobId),
		}}),
		createXYChartTile(TileInfo{"Dataflow Workers Max Backlog Time Seconds", map[string]string{"": fmt.Sprintf(dataflowBacklogTimeQuery, resourceIds.DataflowJobId)}}),
	}
	return dataflowTiles
}

func createShardDatastreamMetrics(resourceIds MonitoringMetricsResources) []*dashboardpb.MosaicLayout_Tile {
	datastreamTiles := []*dashboardpb.MosaicLayout_Tile{
		createXYChartTile(TileInfo{
			"Datastream Total Latency",
			map[string]string{"p50 " + resourceIds.DatastreamId: fmt.Sprintf(datastreamTotalLatencyQuery, resourceIds.DatastreamId, "50"), "p90 " + resourceIds.DatastreamId: fmt.Sprintf(datastreamTotalLatencyQuery, resourceIds.DatastreamId, "90")}}),
		createXYChartTile(TileInfo{"Datastream Throughput", map[string]string{resourceIds.DatastreamId: fmt.Sprintf(datastreamThroughputQuery, resourceIds.DatastreamId)}}),
		createXYChartTile(TileInfo{"Datastream Unsupported Events", map[string]string{resourceIds.DatastreamId: fmt.Sprintf(datastreamUnsupportedEventsQuery, resourceIds.DatastreamId)}}),
	}
	return datastreamTiles
}

func createShardGcsMetrics(resourceIds MonitoringMetricsResources) []*dashboardpb.MosaicLayout_Tile {
	// If fetching gcs bucket failed, don't return any tiles
	if resourceIds.GcsBucketId == "" {
		return []*dashboardpb.MosaicLayout_Tile{}
	}
	gcsBucketTiles := []*dashboardpb.MosaicLayout_Tile{
		createXYChartTile(TileInfo{"GCS Bucket Total Bytes", map[string]string{resourceIds.GcsBucketId: fmt.Sprintf(gcsTotalBytesQuery, resourceIds.GcsBucketId)}}),
	}
	return gcsBucketTiles
}

func createShardPubsubMetrics(resourceIds MonitoringMetricsResources) []*dashboardpb.MosaicLayout_Tile {
	pubsubTiles := []*dashboardpb.MosaicLayout_Tile{
		createXYChartTile(TileInfo{"Pubsub Subscription Sent Message Count", map[string]string{resourceIds.PubsubSubscriptionId: fmt.Sprintf(pubsubSubscriptionSentMessageCountQuery, resourceIds.PubsubSubscriptionId)}}),
		createXYChartTile(TileInfo{"Pubsub Age of Oldest Unacknowledged Message", map[string]string{resourceIds.PubsubSubscriptionId: fmt.Sprintf(pubsubOldestUnackedMessageAgeQuery, resourceIds.PubsubSubscriptionId)}}),
	}
	return pubsubTiles
}

func createShardIndependentMetrics(resourceIds MonitoringMetricsResources) []*dashboardpb.MosaicLayout_Tile {
	independentMetricsTiles := []*dashboardpb.MosaicLayout_Tile{
		createXYChartTile(TileInfo{
			"Dataflow Workers CPU Utilization",
			map[string]string{
				"p50 worker": fmt.Sprintf(dataflowCpuUtilPercentileQuery, resourceIds.DataflowJobId, "50"),
				"p90 worker": fmt.Sprintf(dataflowCpuUtilPercentileQuery, resourceIds.DataflowJobId, "90"),
				"Max worker": fmt.Sprintf(dataflowCpuUtilMaxQuery, resourceIds.DataflowJobId),
			}}),
		createXYChartTile(TileInfo{"Datastream Throughput", map[string]string{resourceIds.DatastreamId: fmt.Sprintf(datastreamThroughputQuery, resourceIds.DatastreamId)}}),
		createXYChartTile(TileInfo{"Datastream Unsupported Events", map[string]string{resourceIds.DatastreamId: fmt.Sprintf(datastreamUnsupportedEventsQuery, resourceIds.DatastreamId)}}),
		createXYChartTile(TileInfo{"Pubsub Age of Oldest Unacknowledged Message", map[string]string{resourceIds.PubsubSubscriptionId: fmt.Sprintf(pubsubOldestUnackedMessageAgeQuery, resourceIds.PubsubSubscriptionId)}}),
		createXYChartTile(
			TileInfo{"Spanner CPU Utilisation",
				map[string]string{"Database CPU Utilisation": fmt.Sprintf(spannerCpuUtilDbQuery, resourceIds.SpannerInstanceId, resourceIds.SpannerDatabaseId), "Instance CPU Utilisation": fmt.Sprintf(spannerCpuUtilInstanceQuery, resourceIds.SpannerInstanceId)}}),
	}
	return independentMetricsTiles
}

func createAggFilterCondition(resourceName string, resourceValues []string) string {
	condition := ""
	for _, id := range resourceValues {
		if condition == "" {
			condition = fmt.Sprintf("%s == '%s'", resourceName, id)
		} else {
			condition += fmt.Sprintf("|| %s == '%s'", resourceName, id)
		}
	}
	return condition
}

func createAggDataflowMetrics(resourceIds MonitoringMetricsResources) []*dashboardpb.MosaicLayout_Tile {
	var dataflowJobs []string
	for _, value := range resourceIds.ShardToDataflowInfoMap {
		dataflowJobs = append(dataflowJobs, value.JobId)
	}
	dataflowTiles := []*dashboardpb.MosaicLayout_Tile{
		createXYChartTile(TileInfo{
			"Dataflow Workers CPU Utilization",
			map[string]string{
				"p50 shard": fmt.Sprintf(dataflowAggCpuUtilPercentileQuery, createAggFilterCondition("metadata.user_labels.dataflow_job_id", dataflowJobs), "50"),
				"p90 shard": fmt.Sprintf(dataflowAggCpuUtilPercentileQuery, createAggFilterCondition("metadata.user_labels.dataflow_job_id", dataflowJobs), "90"),
				"Max shard": fmt.Sprintf(dataflowAggCpuUtilMaxQuery, createAggFilterCondition("metadata.user_labels.dataflow_job_id", dataflowJobs)),
			}}),
		createXYChartTile(TileInfo{"Dataflow Workers Memory Utilization", map[string]string{
			"p50 shard": fmt.Sprintf(dataflowAggMemoryUtilPercentileQuery, createAggFilterCondition("metadata.user_labels.dataflow_job_id", dataflowJobs), "50"),
			"p90 shard": fmt.Sprintf(dataflowAggMemoryUtilPercentileQuery, createAggFilterCondition("metadata.user_labels.dataflow_job_id", dataflowJobs), "90"),
			"Max shard": fmt.Sprintf(dataflowAggMemoryUtilMaxQuery, createAggFilterCondition("metadata.user_labels.dataflow_job_id", dataflowJobs)),
		}}),
		createXYChartTile(TileInfo{"p90 Dataflow Workers CPU Utilization", map[string]string{"Dataflow p90 CPU Utilization": fmt.Sprintf(dataflowAggCpuUtilPercentileQuery, createAggFilterCondition("metadata.user_labels.dataflow_job_id", dataflowJobs), "50")}}),
		createXYChartTile(TileInfo{"p90 Dataflow Workers Memory Utilization", map[string]string{"Dataflow p90 Memory Utilization": fmt.Sprintf(dataflowAggMemoryUtilPercentileQuery, createAggFilterCondition("metadata.user_labels.dataflow_job_id", dataflowJobs), "90")}}),
		createXYChartTile(TileInfo{"Dataflow Workers Max Backlog Time Seconds", map[string]string{"Dataflow Backlog Time Seconds": fmt.Sprintf(dataflowAggBacklogTimeQuery, createAggFilterCondition("metric.job_id", dataflowJobs))}}),
	}
	return dataflowTiles
}

func createAggDatastreamMetrics(resourceIds MonitoringMetricsResources) []*dashboardpb.MosaicLayout_Tile {
	var datastreamJobs []string
	for _, value := range resourceIds.ShardToDataStreamNameMap {
		datastreamJobs = append(datastreamJobs, value)
	}
	datastreamTiles := []*dashboardpb.MosaicLayout_Tile{
		createXYChartTile(TileInfo{
			"Datastream Total Latency",
			map[string]string{"p50 Datastream Latency": fmt.Sprintf(datastreamAggTotalLatencyQuery, createAggFilterCondition("resource.stream_id", datastreamJobs), "50", "50"), "p90 Datastream Latency": fmt.Sprintf(datastreamAggTotalLatencyQuery, createAggFilterCondition("resource.stream_id", datastreamJobs), "90", "90")}}),
		createXYChartTile(TileInfo{"Total Datastream Throughput", map[string]string{"Datastream Total Throughput": fmt.Sprintf(datastreamAggThroughputQuery, createAggFilterCondition("resource.stream_id", datastreamJobs))}}),
		createXYChartTile(TileInfo{"Total Datastream Unsupported Events", map[string]string{"Datastream Total Unsupported Events": fmt.Sprintf(datastreamAggUnsupportedEventsQuery, createAggFilterCondition("resource.stream_id", datastreamJobs))}}),
	}
	return datastreamTiles
}

func createAggGcsMetrics(resourceIds MonitoringMetricsResources) []*dashboardpb.MosaicLayout_Tile {
	var gcsBuckets []string
	for _, value := range resourceIds.ShardToGcsMap {
		if value.BucketName != "" {
			gcsBuckets = append(gcsBuckets, value.BucketName)
		}
	}
	if len(gcsBuckets) == 0 {
		return []*dashboardpb.MosaicLayout_Tile{}
	}
	// We fetch gcs buckets for dashboard creation it is possible due to an error we are not able to fetch gcs buckets for all the shards
	gcsBucketTiles := []*dashboardpb.MosaicLayout_Tile{
		createXYChartTile(TileInfo{fmt.Sprintf("GCS Bucket Total Bytes for %v shards", len(gcsBuckets)), map[string]string{resourceIds.GcsBucketId: fmt.Sprintf(gcsAggTotalBytesQuery, createAggFilterCondition("resource.bucket_name", gcsBuckets))}}),
	}
	return gcsBucketTiles
}

func createAggPubsubMetrics(resourceIds MonitoringMetricsResources) []*dashboardpb.MosaicLayout_Tile {
	var pubsubSubs []string
	for _, value := range resourceIds.ShardToPubsubIdMap {
		pubsubSubs = append(pubsubSubs, value.SubscriptionId)
	}
	pubsubTiles := []*dashboardpb.MosaicLayout_Tile{
		createXYChartTile(TileInfo{"Pubsub Subscription Sent Message Count", map[string]string{"Pubsub Subscription Sent Message Count": fmt.Sprintf(pubsubAggSubscriptionSentMessageCountQuery, createAggFilterCondition("resource.subscription_id", pubsubSubs))}}),
		createXYChartTile(TileInfo{"Pubsub Age of Oldest Unacknowledged Message", map[string]string{"Pubsub Age of Oldest Unacknowledged Message": fmt.Sprintf(pubsubAggOldestUnackedMessageAgeQuery, createAggFilterCondition("resource.subscription_id", pubsubSubs))}}),
	}
	return pubsubTiles
}

func createAggIndependentMetrics(resourceIds MonitoringMetricsResources) []*dashboardpb.MosaicLayout_Tile {
	var dataflowJobs []string
	for _, value := range resourceIds.ShardToDataflowInfoMap {
		dataflowJobs = append(dataflowJobs, value.JobId)
	}
	var datastreamJobs []string
	for _, value := range resourceIds.ShardToDataStreamNameMap {
		datastreamJobs = append(datastreamJobs, value)
	}
	var pubsubSubs []string
	for _, value := range resourceIds.ShardToPubsubIdMap {
		pubsubSubs = append(pubsubSubs, value.SubscriptionId)
	}
	independentMetricsTiles := []*dashboardpb.MosaicLayout_Tile{
		createXYChartTile(TileInfo{
			"Dataflow Workers CPU Utilization",
			map[string]string{
				"p50 shard": fmt.Sprintf(dataflowAggCpuUtilPercentileQuery, createAggFilterCondition("metadata.user_labels.dataflow_job_id", dataflowJobs), "50"),
				"p90 shard": fmt.Sprintf(dataflowAggCpuUtilPercentileQuery, createAggFilterCondition("metadata.user_labels.dataflow_job_id", dataflowJobs), "90"),
				"Max shard": fmt.Sprintf(dataflowAggCpuUtilMaxQuery, createAggFilterCondition("metadata.user_labels.dataflow_job_id", dataflowJobs)),
			}}),
		createXYChartTile(TileInfo{"Total Datastream Throughput", map[string]string{"Datastream Throughput": fmt.Sprintf(datastreamAggThroughputQuery, createAggFilterCondition("resource.stream_id", datastreamJobs))}}),
		createXYChartTile(TileInfo{"Total Datastream Unsupported Events", map[string]string{"Datastream Unsupported Events": fmt.Sprintf(datastreamAggUnsupportedEventsQuery, createAggFilterCondition("resource.stream_id", datastreamJobs))}}),
		createXYChartTile(TileInfo{"Pubsub Age of Oldest Unacknowledged Message", map[string]string{"Pubsub Age of Oldest Unacknowledged Message": fmt.Sprintf(pubsubAggOldestUnackedMessageAgeQuery, createAggFilterCondition("resource.subscription_id", pubsubSubs))}}),
		createXYChartTile(
			TileInfo{"Spanner CPU Utilisation",
				map[string]string{"Database CPU Utilisation": fmt.Sprintf(spannerCpuUtilDbQuery, resourceIds.SpannerInstanceId, resourceIds.SpannerDatabaseId), "Instance CPU Utilisation": fmt.Sprintf(spannerCpuUtilInstanceQuery, resourceIds.SpannerInstanceId)}}),
	}
	return independentMetricsTiles
}

// createXYChartTile returns a single tile in a mosaic layout dashboard
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

// createCollapsibleGroupTile returns a collapsible group tile in a mosaic layout dashboard
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

// setWidgetPositions positions the tiles in the monitoring dashboard
func setWidgetPositions(tiles []*dashboardpb.MosaicLayout_Tile, heightOffset int32) int32 {
	for tilePosition, tile := range tiles {
		tile.XPos = (int32(tilePosition) % defaultColumns) * defaultMonitoringMetricWidth
		tile.YPos = heightOffset + (int32(tilePosition)/defaultColumns)*defaultMonitoringMetricHeight
		tile.Width = defaultMonitoringMetricWidth
		tile.Height = defaultMonitoringMetricHeight
	}
	return ((int32(len(tiles)-1) / defaultColumns) + 1) * defaultMonitoringMetricHeight
}

// getCreateMonitoringDashboardRequest returns the request for generating the monitoring dashboard
func getCreateMonitoringDashboardRequest(
	resourceIds MonitoringMetricsResources,
	createIndependentMetric func(resourceIds MonitoringMetricsResources) []*dashboardpb.MosaicLayout_Tile,
	mosaicGroups []MosaicGroup,
	displayName string) *dashboardpb.CreateDashboardRequest {
	var mosaicLayoutTiles []*dashboardpb.MosaicLayout_Tile
	var heightOffset int32 = 0

	// create independent metrics tiles
	independentMetricsTiles := createIndependentMetric(resourceIds)
	heightOffset += setWidgetPositions(independentMetricsTiles, heightOffset)
	mosaicLayoutTiles = append(mosaicLayoutTiles, independentMetricsTiles...)

	for _, mosaicGroup := range mosaicGroups {
		metricTiles := mosaicGroup.groupCreateTileFunction(resourceIds)
		var groupTile *dashboardpb.MosaicLayout_Tile
		groupTile, heightOffset = createCollapsibleGroupTile(TileInfo{Title: mosaicGroup.groupTitle}, metricTiles, heightOffset)
		mosaicLayoutTiles = append(append(mosaicLayoutTiles, metricTiles...), groupTile)
	}

	mosaicLayout := dashboardpb.MosaicLayout{
		Columns: defaultMosaicColumns,
		Tiles:   mosaicLayoutTiles,
	}
	layout := dashboardpb.Dashboard_MosaicLayout{
		MosaicLayout: &mosaicLayout,
	}

	dashboardDisplayName := displayName
	db := dashboardpb.Dashboard{
		DisplayName: dashboardDisplayName,
		Layout:      &layout,
	}
	req := &dashboardpb.CreateDashboardRequest{
		Parent:    "projects/" + resourceIds.ProjectId,
		Dashboard: &db,
	}
	return req
}

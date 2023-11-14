package metrics

import (
	"context"
	"fmt"
	"math"
	"strings"
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
	ProjectId            string
	DataflowJobId        string
	DatastreamId         string
	GcsBucketId          string
	PubsubSubscriptionId string
	SpannerInstanceId    string
	SpannerDatabaseId    string
	ShardId              string
}

type TileInfo struct {
	Title             string
	TimeSeriesQueries map[string]string // Map of legend template and their corresponding queries
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
	var mosaicLayoutTiles []*dashboardpb.MosaicLayout_Tile
	var heightOffset int32 = 0

	// create independent metrics tiles
	independentMetricsTiles := createShardIndependentMetrics(resourceIds)
	heightOffset += setWidgetPositions(independentMetricsTiles, heightOffset)
	mosaicLayoutTiles = append(mosaicLayoutTiles, independentMetricsTiles...)

	var mosaicGroups = []struct {
		groupTitle              string
		groupCreateTileFunction func(resourceIds MonitoringMetricsResources) []*dashboardpb.MosaicLayout_Tile
	}{
		{groupTitle: fmt.Sprintf("Dataflow Job: %s", resourceIds.DataflowJobId), groupCreateTileFunction: createShardDataflowMetrics},
		{groupTitle: fmt.Sprintf("Datastream: %s", resourceIds.DatastreamId), groupCreateTileFunction: createShardDatastreamMetrics},
		{groupTitle: fmt.Sprintf("GCS Bucket: %s", strings.Split(resourceIds.GcsBucketId, "/")[2]), groupCreateTileFunction: createShardGcsMetrics},
		{groupTitle: fmt.Sprintf("Pubsub: %s", resourceIds.PubsubSubscriptionId), groupCreateTileFunction: createShardPubsubMetrics},
		{groupTitle: fmt.Sprintf("Spanner: instances/%s/databases/%s", resourceIds.SpannerInstanceId, resourceIds.SpannerDatabaseId), groupCreateTileFunction: createShardSpannerMetrics},
	}

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

	dashboardDisplayName := "Migration Dashboard"
	if resourceIds.ShardId != "" {
		dashboardDisplayName = fmt.Sprintf("Shard Migration Dashboard %s", resourceIds.ShardId)
	}
	db := dashboardpb.Dashboard{
		DisplayName: dashboardDisplayName,
		Layout:      &layout,
	}
	req := &dashboardpb.CreateDashboardRequest{
		Parent:    "projects/" + resourceIds.ProjectId,
		Dashboard: &db,
	}
	client := getDashboardClient(ctx)
	resp, err := client.CreateDashboard(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp, err
}

func createShardDataflowMetrics(resourceIds MonitoringMetricsResources) []*dashboardpb.MosaicLayout_Tile {
	dataflowTiles := []*dashboardpb.MosaicLayout_Tile{
		createXYChartTile(TileInfo{"Dataflow Workers CPU Utilization", map[string]string{"": fmt.Sprintf(dataflowCpuUtilQuery, resourceIds.DataflowJobId, resourceIds.ProjectId)}}),
		createXYChartTile(TileInfo{"Dataflow Workers Memory Utilization", map[string]string{"": fmt.Sprintf(dataflowMemoryUtilQuery, resourceIds.DataflowJobId, resourceIds.ProjectId)}}),
		createXYChartTile(TileInfo{"Dataflow Workers Backlog Time Seconds", map[string]string{"": fmt.Sprintf(dataflowBacklogTimeQuery, resourceIds.DataflowJobId, resourceIds.ProjectId)}}),
	}
	return dataflowTiles
}

func createShardDatastreamMetrics(resourceIds MonitoringMetricsResources) []*dashboardpb.MosaicLayout_Tile {
	datastreamTiles := []*dashboardpb.MosaicLayout_Tile{
		createXYChartTile(TileInfo{
			"Datastream Total Latency",
			map[string]string{"p50 " + resourceIds.DatastreamId: fmt.Sprintf(datastreamTotalLatencyQuery, resourceIds.DatastreamId, resourceIds.ProjectId, "50"), "p90 " + resourceIds.DatastreamId: fmt.Sprintf(datastreamTotalLatencyQuery, resourceIds.DatastreamId, resourceIds.ProjectId, "90")}}),
		createXYChartTile(TileInfo{"Datastream Throughput", map[string]string{resourceIds.DatastreamId: fmt.Sprintf(datastreamThroughputQuery, resourceIds.DatastreamId, resourceIds.ProjectId)}}),
		createXYChartTile(TileInfo{"Datastream Unsupported Events", map[string]string{resourceIds.DatastreamId: fmt.Sprintf(datastreamUnsupportedEventsQuery, resourceIds.DatastreamId, resourceIds.ProjectId)}}),
	}
	return datastreamTiles
}

func createShardGcsMetrics(resourceIds MonitoringMetricsResources) []*dashboardpb.MosaicLayout_Tile {
	gcsBucketTiles := []*dashboardpb.MosaicLayout_Tile{
		createXYChartTile(TileInfo{"GCS Bucket Total Bytes", map[string]string{resourceIds.GcsBucketId: fmt.Sprintf(gcsTotalBytesQuery, strings.Split(resourceIds.GcsBucketId, "/")[2], resourceIds.ProjectId)}}),
	}
	return gcsBucketTiles
}

func createShardSpannerMetrics(resourceIds MonitoringMetricsResources) []*dashboardpb.MosaicLayout_Tile {
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

func createShardPubsubMetrics(resourceIds MonitoringMetricsResources) []*dashboardpb.MosaicLayout_Tile {
	pubsubTiles := []*dashboardpb.MosaicLayout_Tile{
		createXYChartTile(TileInfo{"Pubsub Subscription Sent Message Count", map[string]string{resourceIds.PubsubSubscriptionId: fmt.Sprintf(pubsubSubscriptionSentMessageCountQuery, resourceIds.PubsubSubscriptionId, resourceIds.ProjectId)}}),
		createXYChartTile(TileInfo{"Pubsub Age of Oldest Unacknowledged Message", map[string]string{resourceIds.PubsubSubscriptionId: fmt.Sprintf(pubsubOldestUnackedMessageAgeQuery, resourceIds.PubsubSubscriptionId, resourceIds.ProjectId)}}),
	}
	return pubsubTiles
}

func createShardIndependentMetrics(resourceIds MonitoringMetricsResources) []*dashboardpb.MosaicLayout_Tile {
	independentMetricsTiles := []*dashboardpb.MosaicLayout_Tile{
		createXYChartTile(TileInfo{"Dataflow Workers CPU Utilization", map[string]string{"": fmt.Sprintf(dataflowCpuUtilQuery, resourceIds.DataflowJobId, resourceIds.ProjectId)}}),
		createXYChartTile(TileInfo{"Datastream Throughput", map[string]string{resourceIds.DatastreamId: fmt.Sprintf(datastreamThroughputQuery, resourceIds.DatastreamId, resourceIds.ProjectId)}}),
		createXYChartTile(TileInfo{"Datastream Unsupported Events", map[string]string{resourceIds.DatastreamId: fmt.Sprintf(datastreamUnsupportedEventsQuery, resourceIds.DatastreamId, resourceIds.ProjectId)}}),
		createXYChartTile(TileInfo{"Pubsub Age of Oldest Unacknowledged Message", map[string]string{resourceIds.PubsubSubscriptionId: fmt.Sprintf(pubsubOldestUnackedMessageAgeQuery, resourceIds.PubsubSubscriptionId, resourceIds.ProjectId)}}), createXYChartTile(
			TileInfo{"Spanner CPU Utilisation",
				map[string]string{"Database CPU Utilisation": fmt.Sprintf(spannerCpuUtilDbQuery, resourceIds.SpannerInstanceId, resourceIds.SpannerDatabaseId, resourceIds.ProjectId), "Instance CPU Utilisation": fmt.Sprintf(spannerCpuUtilInstanceQuery, resourceIds.SpannerInstanceId, resourceIds.ProjectId)}}),
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

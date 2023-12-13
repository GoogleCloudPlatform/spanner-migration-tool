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
	"sync"

	dashboard "cloud.google.com/go/monitoring/dashboard/apiv1"
	"cloud.google.com/go/monitoring/dashboard/apiv1/dashboardpb"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
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
	ProjectId                     string
	DataflowJobId                 string
	DatastreamId                  string
	JobMetadataGcsBucket          string
	PubsubSubscriptionId          string
	SpannerInstanceId             string
	SpannerDatabaseId             string
	ShardToDataStreamResourcesMap map[string]internal.DatastreamResources
	ShardToDataflowResourcesMap   map[string]internal.DataflowResources
	ShardToPubsubResourcesMap     map[string]internal.PubsubResources
	ShardToGcsMap                 map[string]internal.GcsResources
	ShardToMonitoringDashboardMap map [string] internal.MonitoringResources
	ShardId                       string
	MigrationRequestId       string
}

type TileInfo struct {
	Title             string
	TimeSeriesQueries map[string]string // Map of legend template and their corresponding queries
	TextContent		  string // string for text input
}

type MosaicGroup struct {
	groupTitle              string
	groupCreateTileFunction func(resourceIds MonitoringMetricsResources) []*dashboardpb.MosaicLayout_Tile
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

func createSpannerMetrics(resourceIds MonitoringMetricsResources) []*dashboardpb.MosaicLayout_Tile {
	spannerTiles := []*dashboardpb.MosaicLayout_Tile{
		TileInfo{Title: "Spanner CPU Utilisation", TimeSeriesQueries: map[string]string{"Database CPU Utilisation": fmt.Sprintf(spannerCpuUtilDbQuery, resourceIds.SpannerDatabaseId, resourceIds.SpannerInstanceId), "Instance CPU Utilisation": fmt.Sprintf(spannerCpuUtilInstanceQuery, resourceIds.SpannerInstanceId)}}.createXYChartTile(),
		TileInfo{Title: "Spanner Storage", TimeSeriesQueries: map[string]string{"Database Storage": fmt.Sprintf(spannerStorageUtilDbQuery, resourceIds.SpannerDatabaseId, resourceIds.SpannerInstanceId), "Instance Storage": fmt.Sprintf(spannerStorageUtilInstanceQuery, resourceIds.SpannerInstanceId)}}.createXYChartTile(),
	}
	return spannerTiles
}

func createShardDataflowMetrics(resourceIds MonitoringMetricsResources) []*dashboardpb.MosaicLayout_Tile {
	dataflowTiles := []*dashboardpb.MosaicLayout_Tile{
		TileInfo{
			Title: "Dataflow Workers CPU Utilization",
			TimeSeriesQueries: map[string]string{
				"p50 worker": fmt.Sprintf(dataflowCpuUtilPercentileQuery, resourceIds.DataflowJobId, "50"),
				"p90 worker": fmt.Sprintf(dataflowCpuUtilPercentileQuery, resourceIds.DataflowJobId, "90"),
				"Max worker": fmt.Sprintf(dataflowCpuUtilMaxQuery, resourceIds.DataflowJobId),
			}}.createXYChartTile(),
		TileInfo{
			Title: "Dataflow Workers Memory Utilization",
			TimeSeriesQueries: map[string]string{
			"p50 worker": fmt.Sprintf(dataflowMemoryUtilPercentileQuery, resourceIds.DataflowJobId, "50"),
			"p90 worker": fmt.Sprintf(dataflowMemoryUtilPercentileQuery, resourceIds.DataflowJobId, "90"),
			"Max worker": fmt.Sprintf(dataflowMemoryUtilMaxQuery, resourceIds.DataflowJobId),
		}}.createXYChartTile(),
		TileInfo{Title: "Dataflow Workers Max Backlog Time Seconds", TimeSeriesQueries: map[string]string{"": fmt.Sprintf(dataflowBacklogTimeQuery, resourceIds.DataflowJobId)}}.createXYChartTile(),
	}
	return dataflowTiles
}

func createShardDatastreamMetrics(resourceIds MonitoringMetricsResources) []*dashboardpb.MosaicLayout_Tile {
	datastreamTiles := []*dashboardpb.MosaicLayout_Tile{
		TileInfo{
			Title: "Datastream Total Latency",
			TimeSeriesQueries: map[string]string{"p50 " + resourceIds.DatastreamId: fmt.Sprintf(datastreamTotalLatencyQuery, resourceIds.DatastreamId, "50"), "p90 " + resourceIds.DatastreamId: fmt.Sprintf(datastreamTotalLatencyQuery, resourceIds.DatastreamId, "90")}}.createXYChartTile(),
		TileInfo{Title: "Datastream Throughput", TimeSeriesQueries: map[string]string{resourceIds.DatastreamId: fmt.Sprintf(datastreamThroughputQuery, resourceIds.DatastreamId)}}.createXYChartTile(),
		TileInfo{Title: "Datastream Unsupported Events", TimeSeriesQueries: map[string]string{resourceIds.DatastreamId: fmt.Sprintf(datastreamUnsupportedEventsQuery, resourceIds.DatastreamId)}}.createXYChartTile(),
	}
	return datastreamTiles
}

func createShardGcsMetrics(resourceIds MonitoringMetricsResources) []*dashboardpb.MosaicLayout_Tile {
	// If fetching gcs bucket failed, don't return any tiles
	if resourceIds.JobMetadataGcsBucket == "" {
		return []*dashboardpb.MosaicLayout_Tile{}
	}
	gcsBucketTiles := []*dashboardpb.MosaicLayout_Tile{
		TileInfo{Title: "GCS Bucket Total Bytes", TimeSeriesQueries: map[string]string{resourceIds.JobMetadataGcsBucket: fmt.Sprintf(gcsTotalBytesQuery, resourceIds.JobMetadataGcsBucket)}}.createXYChartTile(),
	}
	return gcsBucketTiles
}

func createShardPubsubMetrics(resourceIds MonitoringMetricsResources) []*dashboardpb.MosaicLayout_Tile {
	pubsubTiles := []*dashboardpb.MosaicLayout_Tile{
		TileInfo{Title: "Pubsub Subscription Sent Message Count", TimeSeriesQueries: map[string]string{resourceIds.PubsubSubscriptionId: fmt.Sprintf(pubsubSubscriptionSentMessageCountQuery, resourceIds.PubsubSubscriptionId)}}.createXYChartTile(),
		TileInfo{Title: "Pubsub Age of Oldest Unacknowledged Message", TimeSeriesQueries: map[string]string{resourceIds.PubsubSubscriptionId: fmt.Sprintf(pubsubOldestUnackedMessageAgeQuery, resourceIds.PubsubSubscriptionId)}}.createXYChartTile(),
	}
	return pubsubTiles
}

func createShardIndependentTopMetrics(resourceIds MonitoringMetricsResources) []*dashboardpb.MosaicLayout_Tile {
	independentTopMetricsTiles := []*dashboardpb.MosaicLayout_Tile{
		TileInfo{
			Title: "Dataflow Workers CPU Utilization",
			TimeSeriesQueries: map[string]string{
				"p50 worker": fmt.Sprintf(dataflowCpuUtilPercentileQuery, resourceIds.DataflowJobId, "50"),
				"p90 worker": fmt.Sprintf(dataflowCpuUtilPercentileQuery, resourceIds.DataflowJobId, "90"),
				"Max worker": fmt.Sprintf(dataflowCpuUtilMaxQuery, resourceIds.DataflowJobId),
			}}.createXYChartTile(),
		TileInfo{Title: "Datastream Throughput", TimeSeriesQueries: map[string]string{resourceIds.DatastreamId: fmt.Sprintf(datastreamThroughputQuery, resourceIds.DatastreamId)}}.createXYChartTile(),
		TileInfo{Title: "Datastream Unsupported Events", TimeSeriesQueries: map[string]string{resourceIds.DatastreamId: fmt.Sprintf(datastreamUnsupportedEventsQuery, resourceIds.DatastreamId)}}.createXYChartTile(),
		TileInfo{Title: "Pubsub Age of Oldest Unacknowledged Message", TimeSeriesQueries: map[string]string{resourceIds.PubsubSubscriptionId: fmt.Sprintf(pubsubOldestUnackedMessageAgeQuery, resourceIds.PubsubSubscriptionId)}}.createXYChartTile(),
	}
	spannerMetrics:=createSpannerMetrics(resourceIds)
	independentTopMetricsTiles=append(independentTopMetricsTiles,spannerMetrics...)
	return independentTopMetricsTiles
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
	for _, value := range resourceIds.ShardToDataflowResourcesMap {
		dataflowJobs = append(dataflowJobs, value.JobId)
	}
	dataflowTiles := []*dashboardpb.MosaicLayout_Tile{
		TileInfo{
			Title: "Dataflow Workers CPU Utilization",
			TimeSeriesQueries: map[string]string{
				"p50 shard": fmt.Sprintf(dataflowAggCpuUtilPercentileQuery, createAggFilterCondition("metadata.user_labels.dataflow_job_id", dataflowJobs), "50"),
				"p90 shard": fmt.Sprintf(dataflowAggCpuUtilPercentileQuery, createAggFilterCondition("metadata.user_labels.dataflow_job_id", dataflowJobs), "90"),
				"Max shard": fmt.Sprintf(dataflowAggCpuUtilMaxQuery, createAggFilterCondition("metadata.user_labels.dataflow_job_id", dataflowJobs)),
			}}.createXYChartTile(),
		TileInfo{
			Title: "Dataflow Workers Memory Utilization", 
			TimeSeriesQueries: map[string]string{
			"p50 shard": fmt.Sprintf(dataflowAggMemoryUtilPercentileQuery, createAggFilterCondition("metadata.user_labels.dataflow_job_id", dataflowJobs), "50"),
			"p90 shard": fmt.Sprintf(dataflowAggMemoryUtilPercentileQuery, createAggFilterCondition("metadata.user_labels.dataflow_job_id", dataflowJobs), "90"),
			"Max shard": fmt.Sprintf(dataflowAggMemoryUtilMaxQuery, createAggFilterCondition("metadata.user_labels.dataflow_job_id", dataflowJobs)),
		}}.createXYChartTile(),
		TileInfo{Title: "Dataflow Workers Max Backlog Time Seconds", TimeSeriesQueries: map[string]string{"Dataflow Backlog Time Seconds": fmt.Sprintf(dataflowAggBacklogTimeQuery, createAggFilterCondition("metric.job_id", dataflowJobs))}}.createXYChartTile(),
		TileInfo{Title: "Dataflow Per Shard Median CPU Utilization", TimeSeriesQueries: map[string]string{"": fmt.Sprintf(dataflowAggPerShardCpuUtil, createAggFilterCondition("metadata.user_labels.dataflow_job_id", dataflowJobs))}}.createXYChartTile(),
	}
	return dataflowTiles
}

func createAggDatastreamMetrics(resourceIds MonitoringMetricsResources) []*dashboardpb.MosaicLayout_Tile {
	var datastreamJobs []string
	for _, value := range resourceIds.ShardToDataStreamResourcesMap {
		datastreamJobs = append(datastreamJobs, value.DatastreamName)
	}
	datastreamTiles := []*dashboardpb.MosaicLayout_Tile{
		TileInfo{
			Title: "Datastream Total Latency",
			TimeSeriesQueries: map[string]string{"p50 Datastream Latency": fmt.Sprintf(datastreamAggTotalLatencyQuery, createAggFilterCondition("resource.stream_id", datastreamJobs), "50", "50"), "p90 Datastream Latency": fmt.Sprintf(datastreamAggTotalLatencyQuery, createAggFilterCondition("resource.stream_id", datastreamJobs), "90", "90")}}.createXYChartTile(),
		TileInfo{Title: "Total Datastream Throughput", TimeSeriesQueries: map[string]string{"Datastream Total Throughput": fmt.Sprintf(datastreamAggThroughputQuery, createAggFilterCondition("resource.stream_id", datastreamJobs))}}.createXYChartTile(),
		TileInfo{Title: "Total Datastream Unsupported Events", TimeSeriesQueries: map[string]string{"Datastream Total Unsupported Events": fmt.Sprintf(datastreamAggUnsupportedEventsQuery, createAggFilterCondition("resource.stream_id", datastreamJobs))}}.createXYChartTile(),
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
		TileInfo{Title: fmt.Sprintf("GCS Bucket Total Bytes for %v shards", len(gcsBuckets)), TimeSeriesQueries: map[string]string{resourceIds.JobMetadataGcsBucket: fmt.Sprintf(gcsAggTotalBytesQuery, createAggFilterCondition("resource.bucket_name", gcsBuckets))}}.createXYChartTile(),
	}
	return gcsBucketTiles
}

func createAggPubsubMetrics(resourceIds MonitoringMetricsResources) []*dashboardpb.MosaicLayout_Tile {
	var pubsubSubs []string
	for _, value := range resourceIds.ShardToPubsubResourcesMap {
		pubsubSubs = append(pubsubSubs, value.SubscriptionId)
	}
	pubsubTiles := []*dashboardpb.MosaicLayout_Tile{
		TileInfo{Title: "Pubsub Subscription Sent Message Count", TimeSeriesQueries: map[string]string{"Pubsub Subscription Sent Message Count": fmt.Sprintf(pubsubAggSubscriptionSentMessageCountQuery, createAggFilterCondition("resource.subscription_id", pubsubSubs))}}.createXYChartTile(),
		TileInfo{Title: "Pubsub Age of Oldest Unacknowledged Message", TimeSeriesQueries: map[string]string{"Pubsub Age of Oldest Unacknowledged Message": fmt.Sprintf(pubsubAggOldestUnackedMessageAgeQuery, createAggFilterCondition("resource.subscription_id", pubsubSubs))}}.createXYChartTile(),
	}
	return pubsubTiles
}

func createAggIndependentTopMetrics(resourceIds MonitoringMetricsResources) []*dashboardpb.MosaicLayout_Tile {
	var dataflowJobs []string
	for _, value := range resourceIds.ShardToDataflowResourcesMap {
		dataflowJobs = append(dataflowJobs, value.JobId)
	}
	var datastreamJobs []string
	for _, value := range resourceIds.ShardToDataStreamResourcesMap {
		datastreamJobs = append(datastreamJobs, value.DatastreamName)
	}
	var pubsubSubs []string
	for _, value := range resourceIds.ShardToPubsubResourcesMap {
		pubsubSubs = append(pubsubSubs, value.SubscriptionId)
	}
	independentTopMetricsTiles := []*dashboardpb.MosaicLayout_Tile{
		TileInfo{
			Title: "Dataflow Workers CPU Utilization",
			TimeSeriesQueries: map[string]string{
				"p50 shard": fmt.Sprintf(dataflowAggCpuUtilPercentileQuery, createAggFilterCondition("metadata.user_labels.dataflow_job_id", dataflowJobs), "50"),
				"p90 shard": fmt.Sprintf(dataflowAggCpuUtilPercentileQuery, createAggFilterCondition("metadata.user_labels.dataflow_job_id", dataflowJobs), "90"),
				"Max shard": fmt.Sprintf(dataflowAggCpuUtilMaxQuery, createAggFilterCondition("metadata.user_labels.dataflow_job_id", dataflowJobs)),
			}}.createXYChartTile(),
		TileInfo{Title: "Total Datastream Throughput", TimeSeriesQueries: map[string]string{"Datastream Throughput": fmt.Sprintf(datastreamAggThroughputQuery, createAggFilterCondition("resource.stream_id", datastreamJobs))}}.createXYChartTile(),
		TileInfo{Title: "Total Datastream Unsupported Events", TimeSeriesQueries: map[string]string{"Datastream Unsupported Events": fmt.Sprintf(datastreamAggUnsupportedEventsQuery, createAggFilterCondition("resource.stream_id", datastreamJobs))}}.createXYChartTile(),
		TileInfo{Title: "Pubsub Age of Oldest Unacknowledged Message", TimeSeriesQueries: map[string]string{"Pubsub Age of Oldest Unacknowledged Message": fmt.Sprintf(pubsubAggOldestUnackedMessageAgeQuery, createAggFilterCondition("resource.subscription_id", pubsubSubs))}}.createXYChartTile(),
		}
	spannerMetrics:=createSpannerMetrics(resourceIds)
	independentTopMetricsTiles=append(independentTopMetricsTiles,spannerMetrics...)
	return independentTopMetricsTiles
}

func createAggIndependentBottomMetrics(resourceIds MonitoringMetricsResources) []*dashboardpb.MosaicLayout_Tile {
	shardToDashboardMappingText := ""
	for shardId, monitoringResource := range resourceIds.ShardToMonitoringDashboardMap {
		shardUrl := fmt.Sprintf("https://console.cloud.google.com/monitoring/dashboards/builder/%v?project=%v", monitoringResource.DashboardName, resourceIds.ProjectId)
		shardString := fmt.Sprintf("Shard [%s](%s)", shardId, shardUrl)
		if(shardToDashboardMappingText == ""){
			shardToDashboardMappingText = shardString
		} else {
			shardToDashboardMappingText += " \\\n" + shardString
		}
	}
	independentBottomMetricsTiles := []*dashboardpb.MosaicLayout_Tile{
		TileInfo{
			Title: "Shard Dashboards",
			TextContent: shardToDashboardMappingText,
		}.createTextTile(),
	}
	return independentBottomMetricsTiles
}

// createXYChartTile returns a single tile in a mosaic layout dashboard
func (tileInfo TileInfo) createXYChartTile() *dashboardpb.MosaicLayout_Tile {
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
func (tileInfo TileInfo) createCollapsibleGroupTile(tiles []*dashboardpb.MosaicLayout_Tile, heightOffset int32) (*dashboardpb.MosaicLayout_Tile, int32) {
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

func (tileInfo TileInfo) createTextTile() (*dashboardpb.MosaicLayout_Tile){
	textTile :=  dashboardpb.MosaicLayout_Tile{
		Widget: &dashboardpb.Widget{
			Title: tileInfo.Title,
			Content: &dashboardpb.Widget_Text{
				Text: &dashboardpb.Text{
					Content: tileInfo.TextContent,
					Format: dashboardpb.Text_MARKDOWN,
				},
			},
		},
	}
	return &textTile
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
	createIndependentTopMetric func(resourceIds MonitoringMetricsResources) []*dashboardpb.MosaicLayout_Tile,
	mosaicGroups []MosaicGroup,
	createAggIndependentBottomMetrics func(resourceIds MonitoringMetricsResources) []*dashboardpb.MosaicLayout_Tile,
	displayName string) *dashboardpb.CreateDashboardRequest {
	var mosaicLayoutTiles []*dashboardpb.MosaicLayout_Tile
	var heightOffset int32 = 0

	// create top independent metrics tiles
	independentTopMetricsTiles := createIndependentTopMetric(resourceIds)
	heightOffset += setWidgetPositions(independentTopMetricsTiles, heightOffset)
	mosaicLayoutTiles = append(mosaicLayoutTiles, independentTopMetricsTiles...)

	// add group tiles
	for _, mosaicGroup := range mosaicGroups {
		metricTiles := mosaicGroup.groupCreateTileFunction(resourceIds)
		var groupTile *dashboardpb.MosaicLayout_Tile
		groupTile, heightOffset = TileInfo{Title: mosaicGroup.groupTitle}.createCollapsibleGroupTile(metricTiles, heightOffset)
		mosaicLayoutTiles = append(append(mosaicLayoutTiles, metricTiles...), groupTile)
	}

	// create bottom independent metrics tiles
	if createAggIndependentBottomMetrics!= nil{
		independentBottomMetricsTiles := createAggIndependentBottomMetrics(resourceIds)
		heightOffset += setWidgetPositions(independentBottomMetricsTiles, heightOffset)
		mosaicLayoutTiles = append(mosaicLayoutTiles, independentBottomMetricsTiles...)
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

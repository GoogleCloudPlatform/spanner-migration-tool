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

// Defines queries for Monitoring Dashboard Metrics
const (
	// Queries for Monitoring Dashboard of Shards
	dataflowCpuUtilPercentileQuery = "fetch gce_instance | metric 'compute.googleapis.com/instance/cpu/utilization' | " +
		"filter (metadata.user_labels.dataflow_job_id == '%s') | " +
		"group_by 1m, [value_utilization_mean: mean(value.utilization)] | every 1m | group_by [], " +
		"[value_utilization_mean_percentile: percentile(value_utilization_mean, %s)]"
	dataflowCpuUtilMaxQuery = "fetch gce_instance | metric 'compute.googleapis.com/instance/cpu/utilization' " +
		"| (metadata.user_labels.dataflow_job_id == '%s')| " +
		"group_by 1m, [value_utilization_max: max(value.utilization)] | every 1m | group_by [], " +
		"[value_utilization_max_max: max(value_utilization_max)]"
	dataflowMemoryUtilPercentileQuery = "fetch gce_instance | metric 'compute.googleapis.com/guest/memory/bytes_used' | " +
		"filter (metadata.user_labels.dataflow_job_id == '%s') | group_by 1m, " +
		"[value_bytes_used_mean: mean(value.bytes_used)] | every 1m | group_by [], " +
		"[value_bytes_used_mean_percentile: percentile(value_bytes_used_mean, %s)]"
	dataflowMemoryUtilMaxQuery = "fetch gce_instance | metric 'compute.googleapis.com/guest/memory/bytes_used' | " +
		"filter (metadata.user_labels.dataflow_job_id == '%s')  | group_by 1m, " +
		"[value_bytes_used_max: max(value.bytes_used)] | every 1m | group_by [], " +
		"[value_bytes_used_max_max: max(value_bytes_used_max)]"
	dataflowBacklogTimeQuery = "fetch dataflow_job | metric 'dataflow.googleapis.com/job/estimated_backlog_processing_time' | " +
		"filter (metric.job_id == '%s')  | group_by 1m, " +
		"[value_estimated_backlog_processing_time_mean: mean(value.estimated_backlog_processing_time)] | every 1m"
	datastreamTotalLatencyQuery = "fetch datastream.googleapis.com/Stream | metric 'datastream.googleapis.com/stream/total_latencies' " +
		"| filter (resource.stream_id == '%s') | " +
		"align delta(1m) | every 1m | group_by [], [value_total_latencies_percentile: percentile(value.total_latencies, %s)]"
	datastreamUnsupportedEventsQuery = "fetch datastream.googleapis.com/Stream| metric 'datastream.googleapis.com/stream/unsupported_event_count'| " +
		"filter (resource.stream_id == '%s') | align delta(10m)| every 10m| group_by [], " +
		"[value_unsupported_event_count_sum: sum(value.unsupported_event_count)]"
	datastreamThroughputQuery = "fetch datastream.googleapis.com/Stream| metric 'datastream.googleapis.com/stream/event_count'" +
		"| filter (resource.stream_id == '%s') | align rate(1m)| group_by [], " +
		"[value_event_count_sum: mean(value.event_count)]| every 1m"
	gcsTotalBytesQuery = "fetch gcs_bucket | metric 'storage.googleapis.com/storage/total_bytes' | filter " +
		"(resource.bucket_name == '%s') | group_by 1m, [value_total_bytes_mean: mean(value.total_bytes)] | every 1m | " +
		"group_by [], [value_total_bytes_mean_aggregate: aggregate(value_total_bytes_mean)]"
	pubsubSubscriptionSentMessageCountQuery = "fetch pubsub_subscription | metric 'pubsub.googleapis.com/subscription/sent_message_count' | " +
		"filter (resource.subscription_id == '%s') | align rate(1m) | every 1m | group_by [], " +
		"[value_sent_message_count_aggregate: aggregate(value.sent_message_count)]"
	pubsubOldestUnackedMessageAgeQuery = "fetch pubsub_subscription | metric 'pubsub.googleapis.com/subscription/oldest_unacked_message_age' | " +
		"filter (resource.subscription_id == '%s') | group_by 1m, " +
		"[value_oldest_unacked_message_age_mean: mean(value.oldest_unacked_message_age)] | every 1m | group_by [], " +
		"[value_oldest_unacked_message_age_mean_max: max(value_oldest_unacked_message_age_mean)]"
	spannerCpuUtilDbQuery = "fetch spanner_instance | metric 'spanner.googleapis.com/instance/cpu/utilization' | " +
		"filter (metric.database == '%s') && (resource.instance_id == '%s') | group_by 1m, " +
		"[value_utilization_mean: mean(value.utilization)] | every 1m | group_by [], [value_utilization_mean_aggregate: " +
		"aggregate(value_utilization_mean)]"
	spannerCpuUtilInstanceQuery = "fetch spanner_instance | metric 'spanner.googleapis.com/instance/cpu/utilization' | " +
		"filter (resource.instance_id == '%s') | group_by 1m, [value_utilization_mean: mean(value.utilization)] " +
		"| every 1m | group_by [], [value_utilization_mean_aggregate: aggregate(value_utilization_mean)]"
	spannerStorageUtilDbQuery = "fetch spanner_instance | metric 'spanner.googleapis.com/instance/storage/used_bytes' | " +
		"filter (metric.database == '%s') && (resource.instance_id == '%s') | " +
		"group_by 1m, [value_used_bytes_mean: mean(value.used_bytes)] | every 1m | group_by [], " +
		"[value_used_bytes_mean_aggregate: aggregate(value_used_bytes_mean)]"
	spannerStorageUtilInstanceQuery = "fetch spanner_instance | metric 'spanner.googleapis.com/instance/storage/used_bytes' | filter " +
		"(resource.instance_id == '%s') | group_by 1m, " +
		"[value_used_bytes_mean: mean(value.used_bytes)] | every 1m | group_by [], " +
		"[value_used_bytes_mean_aggregate: aggregate(value_used_bytes_mean)]"

	// Queries for Aggregated Monitoring Dashboard
	dataflowAggCpuUtilPercentileQuery = "fetch gce_instance | metric 'compute.googleapis.com/instance/cpu/utilization' | " +
		"filter (%s) | group_by 1m, [value_utilization_mean: mean(value.utilization)] " +
		"| every 1m | group_by [], [value_utilization_mean_percentile: percentile(value_utilization_mean, %s)]"
	dataflowAggCpuUtilMaxQuery = "fetch gce_instance | metric 'compute.googleapis.com/instance/cpu/utilization' | " +
		"filter (%s) | group_by 1m, [value_utilization_max: max(value.utilization)] | " +
		"every 1m | group_by [], [value_utilization_max_max: max(value_utilization_max)]"
	dataflowAggMemoryUtilPercentileQuery = "fetch gce_instance | metric 'compute.googleapis.com/guest/memory/bytes_used' | " +
		"filter(%s) | group_by 1m, [value_bytes_used_mean: mean(value.bytes_used)] " +
		"| every 1m | group_by [], [value_bytes_used_mean_percentile: percentile(value_bytes_used_mean, %s )]"
	dataflowAggMemoryUtilMaxQuery = "fetch gce_instance | metric 'compute.googleapis.com/guest/memory/bytes_used' | " +
		"filter (%s) | group_by 1m, [value_bytes_used_max: max(value.bytes_used)]" +
		" | every 1m | group_by [], [value_bytes_used_max_max: max(value_bytes_used_max)]"
	dataflowAggBacklogTimeQuery = "fetch dataflow_job | metric 'dataflow.googleapis.com/job/estimated_backlog_processing_time' | " +
		"filter && (%s) | group_by 1m, [value_estimated_backlog_processing_time_mean: " +
		"mean(value.estimated_backlog_processing_time)] | every 1m | group_by [], [value_estimated_backlog_processing_time_mean_mean: " +
		"mean(value_estimated_backlog_processing_time_mean)]"
	dataflowAggPerShardCpuUtil = "fetch gce_instance | metric 'compute.googleapis.com/instance/cpu/utilization' | filter (%s) " + 
		"| group_by 1m, [value_utilization_mean: mean(value.utilization)] | every 1m | group_by [metadata.user_labels.dataflow_job_id]," + 
		" [value_utilization_mean_percentile: percentile(value_utilization_mean, 50)]"
	datastreamAggThroughputQuery = "fetch datastream.googleapis.com/Stream | metric 'datastream.googleapis.com/stream/event_count' | " +
		"filter (%s) | align rate(1m) | every 1m | group_by [], [value_event_count_aggregate: aggregate(value.event_count)]"
	datastreamAggUnsupportedEventsQuery = "fetch datastream.googleapis.com/Stream | metric 'datastream.googleapis.com/stream/unsupported_event_count' " +
		"| filter (%s) | align rate(1m) | every 1m | group_by [], " +
		"[value_unsupported_event_count_aggregate: aggregate(value.unsupported_event_count)] | group_by 1m, " +
		"[value_unsupported_event_count_aggregate_mean: mean(value_unsupported_event_count_aggregate)] | every 1m | " +
		"group_by [], [value_unsupported_event_count_aggregate_mean_aggregate: aggregate(value_unsupported_event_count_aggregate_mean)]"
	datastreamAggTotalLatencyQuery = "fetch datastream.googleapis.com/Stream | metric 'datastream.googleapis.com/stream/total_latencies' | " +
		"filter (%s) |  align delta(1m) | every 1m | group_by [], " +
		"[value_total_latencies_percentile: percentile(value.total_latencies, %s)] | align rate(1m) | every 1m " +
		"| group_by [], [value_total_latencies_percentile_percentile: percentile(value_total_latencies_percentile, %s)]"
	pubsubAggSubscriptionSentMessageCountQuery = "fetch pubsub_subscription | metric 'pubsub.googleapis.com/subscription/sent_message_count' " +
		"| filter (%s ) | align rate(1m) | every 1m | group_by [], " +
		"[value_sent_message_count_aggregate: aggregate(value.sent_message_count)]"
	pubsubAggOldestUnackedMessageAgeQuery = "fetch pubsub_subscription | metric 'pubsub.googleapis.com/subscription/oldest_unacked_message_age' " +
		"| filter (%s) | group_by 1m, " +
		"[value_oldest_unacked_message_age_max: max(value.oldest_unacked_message_age)] | every 1m | group_by [], " +
		"[value_oldest_unacked_message_age_max_max: max(value_oldest_unacked_message_age_max)]"
	gcsAggTotalBytesQuery = "fetch gcs_bucket | metric 'storage.googleapis.com/storage/total_bytes' | " +
		"filter (%s) | group_by 1m, [value_total_bytes_mean: mean(value.total_bytes)] " +
		"| every 1m | group_by [], [value_total_bytes_mean_aggregate: aggregate(value_total_bytes_mean)]"
)

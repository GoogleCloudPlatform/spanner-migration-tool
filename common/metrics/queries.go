package metrics

// Defines queries for Monitoring Dashboard Metrics
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
)

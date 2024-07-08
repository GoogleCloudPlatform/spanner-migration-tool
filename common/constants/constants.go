// Copyright 2020 Google LLC
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

// Package constants contains constants used across multiple other packages.
// All string constants have a lower_case value and thus string matching is
// performend against other lower_case strings.
package constants

const (
	// PGDUMP is the driver name for pg_dump.
	PGDUMP string = "pg_dump"

	// POSTGRES is the driver name for PostgreSQL.
	POSTGRES string = "postgres"

	// MYSQLDUMP is the driver name for mysqldump.
	MYSQLDUMP string = "mysqldump"

	// MYSQL is the driver name for MySQL.
	MYSQL string = "mysql"

	// SQLSERVER is the driver name for sqlserver.
	SQLSERVER string = "sqlserver"

	// DYNAMODB is the driver name for AWS DynamoDB.
	// This is an experimental driver; implementation in progress.
	DYNAMODB string = "dynamodb"

	// CSV is the driver name when loading data using csv.
	CSV string = "csv"

	// ORACLE is the driver name for Oracle.
	// This is an experimental driver; implementation in progress.
	ORACLE string = "oracle"

	// Target db for which schema is being generated.
	// This can be removed once the support for global flags is removed.
	TargetSpanner              string = "spanner"
	TargetExperimentalPostgres string = "experimental_postgres"

	// Supported dialects for Cloud Spanner database.
	DIALECT_POSTGRESQL string = "postgresql"
	DIALECT_GOOGLESQL  string = "google_standard_sql"

	// Temp directory name to write data which we cleanup at the end.
	SMT_TMP_DIR string = "spanner_migration_tool_tmp_data"

	// Information on what conversion is happening (schema conv or data conv)
	SchemaConv string = "schema_conv"
	DataConv   string = "data_conv"

	// Information passed in metadata while using Cloud Spanner client.
	MigrationMetadataKey string = "cloud-spanner-migration-metadata"

	// Scheme used for GCS paths
	GCS_SCHEME      string = "gs"
	GCS_FILE_PREFIX string = "gs://"

	// File upload prefix for dump and session load.
	UPLOAD_FILE_DIR string = "upload-file"
	// Rule types
	GlobalDataTypeChange = "global_datatype_change"
	AddIndex             = "add_index"
	EditColumnMaxLength  = "edit_column_max_length"
	AddShardIdPrimaryKey = "add_shard_id_primary_key"
	//bulk migration type
	BULK_MIGRATION = "bulk"
	//dataflow migration type
	DATAFLOW_MIGRATION = "dataflow"
	//DMS migration type
	DMS_MIGRATION = "dms"

	SESSION_FILE = "sessionFile"

	//Default shardId
	DEFAULT_SHARD_ID string = "smt-default"
	//Metadata database name
	METADATA_DB string = "spannermigrationtool_metadata"
	//Migration types
	MINIMAL_DOWNTIME_MIGRATION = "minimal_downtime"
	//Job Resource Types
	DATAFLOW_RESOURCE       string = "dataflow"
	PUBSUB_RESOURCE         string = "pubsub"
	PUBSUB_TOPIC_RESOURCE   string = "pubsub_topic"
	PUBSUB_SUB_RESOURCE     string = "pubsub_sub"
	MONITORING_RESOURCE     string = "monitoring"
	AGG_MONITORING_RESOURCE string = "aggregated_monitoring"
	DATASTREAM_RESOURCE     string = "datastream"
	GCS_RESOURCE            string = "gcs"
	// Metadata table names
	SMT_JOB_TABLE      string = "SMT_JOB"
	SMT_RESOURCE_TABLE string = "SMT_RESOURCE"
	// Auto Generated Keys
	UUID           string = "UUID"
	SEQUENCE       string = "Sequence"
	AUTO_INCREMENT string = "Auto Increment"
	// Default gcs path of the Dataflow template.
	DEFAULT_TEMPLATE_PATH string = "gs://dataflow-templates/latest/flex/Cloud_Datastream_to_Spanner"

	//FK Actions
	NO_ACTION   string = "NO ACTION"
	CASCADE     string = "CASCADE"
	SET_DEFAULT string = "SET DEFAULT"
	SET_NULL    string = "SET NULL"
	RESTRICT    string = "RESTRICT"
)

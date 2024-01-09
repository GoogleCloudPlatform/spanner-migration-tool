// Copyright 2022 Google LLC
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

package helpers

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	database "cloud.google.com/go/spanner/admin/database/apiv1"
	spanneracc "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/spanner"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
)

const (
	DUMP_MODE              = "dumpFile"
	DIRECT_CONNECT_MODE    = "directConnect"
	SESSION_FILE_MODE      = "sessionFile"
	SCHEMA_ONLY            = "Schema"
	DATA_ONLY              = "Data"
	LOW_DOWNTIME_MIGRATION = "lowdt"
	POSTGRESQL_DIALECT     = "PostgreSQL"
	GOOGLE_SQL_DIALECT     = "Google Standard SQL"
)

var TABLE_STATEMENTS = []string{
	`CREATE TABLE IF NOT EXISTS SchemaConversionSession (
		VersionId STRING(36) NOT NULL,
		PreviousVersionId ARRAY<STRING(36)>,
		SessionName STRING(50) NOT NULL,
		EditorName STRING(100) NOT NULL,
		DatabaseType STRING(50) NOT NULL,
		DatabaseName STRING(50) NOT NULL,
		Dialect STRING(50) NOT NULL,
		Notes ARRAY<STRING(MAX)> NOT NULL,
		Tags ARRAY<STRING(20)>,
		SchemaChanges STRING(MAX),
		SchemaConversionObject JSON NOT NULL,
		CreateTimestamp TIMESTAMP NOT NULL,
	  ) PRIMARY KEY(VersionId)`,
	`CREATE TABLE IF NOT EXISTS SMT_JOB (
		JobId STRING(100) NOT NULL,
		JobName STRING(100) NOT NULL,
		JobType STRING(100) NOT NULL,
		JobStateData JSON,
		JobData JSON,
		Dialect STRING(50) NOT NULL,
		SpannerDatabaseName STRING(100) NOT NULL,
		CreatedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
		UpdatedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
	  ) PRIMARY KEY(JobId)`,
	`CREATE TABLE IF NOT EXISTS SMT_JOB_HISTORY (
		JobId STRING(100) NOT NULL,
		Version INT64 NOT NULL,
		JobName STRING(100) NOT NULL,
		JobType STRING(100) NOT NULL,
		JobStateData JSON,
		JobData JSON,
		Dialect STRING(50) NOT NULL,
		SpannerDatabaseName STRING(100) NOT NULL,
		CreatedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
	  ) PRIMARY KEY(JobId, Version)`,
	`CREATE TABLE IF NOT EXISTS SMT_RESOURCE (
		ResourceId STRING(100) NOT NULL,
		JobId STRING(100) NOT NULL,
		ExternalId STRING(100),
		ResourceName STRING(100) NOT NULL,
		ResourceType STRING(100) NOT NULL,
		ResourceStateData JSON,
		ResourceData JSON,
		CreatedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
		UpdatedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
	  ) PRIMARY KEY(ResourceId)`,
	`CREATE TABLE IF NOT EXISTS SMT_RESOURCE_HISTORY (
		ResourceId STRING(100) NOT NULL,
		Version INT64 NOT NULL,
		JobId STRING(100) NOT NULL,
		ExternalId STRING(100),
		ResourceName STRING(100) NOT NULL,
		ResourceType STRING(100) NOT NULL,
		ResourceStateData JSON,
		ResourceData JSON,
		CreatedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
	) PRIMARY KEY(ResourceId, Version)`,
}

func GetSpannerUri(projectId string, instanceId string) string {
	return fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectId, instanceId, constants.METADATA_DB)
}

// Creates the schema for the internal metadata database
func createDatabase(ctx context.Context, uri string, dbExists bool) error {

	// Spanner uri will be in this format 'projects/project-id/instances/spanner-instance-id/databases/db-name'
	matches := regexp.MustCompile("^(.*)/databases/(.*)$").FindStringSubmatch(uri)
	spInstance := matches[1]
	dbName := matches[2]

	adminClient, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		return err
	}
	defer adminClient.Close()
	fmt.Println("Creating/Updating database to store session metadata...")
	if dbExists {
		op, err := adminClient.UpdateDatabaseDdl(ctx, &adminpb.UpdateDatabaseDdlRequest{
			Database:   uri,
			Statements: TABLE_STATEMENTS,
		})
		if err != nil {
			return err
		}
		if err := op.Wait(ctx); err != nil {
			return err
		}
		fmt.Printf("Updated database [%s]\n", matches[2])
	} else {
		op, err := adminClient.CreateDatabase(ctx, &adminpb.CreateDatabaseRequest{
			Parent:          spInstance,
			CreateStatement: "CREATE DATABASE`" + dbName + "`",
			ExtraStatements: TABLE_STATEMENTS,
		})
		if err != nil {
			return err
		}
		if _, err := op.Wait(ctx); err != nil {
			return err
		}
		fmt.Printf("Created database [%s]\n", matches[2])
	}
	return nil
}

func CheckOrCreateMetadataDb(projectId string, instanceId string) bool {
	uri := GetSpannerUri(projectId, instanceId)
	if uri == "" {
		fmt.Println("Invalid spanner uri")
		return false
	}

	ctx := context.Background()
	adminClient, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		fmt.Println(err)
		return false
	}
	defer adminClient.Close()

	dbExists, err := spanneracc.CheckExistingDb(ctx, uri)
	if err != nil {
		fmt.Println(err)
		return false
	}
	err = createDatabase(ctx, uri, dbExists)
	if err != nil {
		fmt.Println(err)
		return false
	}
	return true
}

func GetSourceDatabaseFromDriver(driver string) (string, error) {
	switch driver {
	case constants.MYSQLDUMP, constants.MYSQL:
		return constants.MYSQL, nil
	case constants.PGDUMP, constants.POSTGRES:
		return constants.POSTGRES, nil
	case constants.ORACLE, constants.SQLSERVER:
		return driver, nil
	default:
		return "", fmt.Errorf("unsupported driver type: %v", driver)
	}
}

func GetDialectDisplayStringFromDialect(dialect string) string {
	if strings.ToLower(dialect) == constants.DIALECT_POSTGRESQL {
		return POSTGRESQL_DIALECT
	}
	return GOOGLE_SQL_DIALECT
}

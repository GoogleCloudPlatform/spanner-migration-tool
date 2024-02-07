// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package spanneraccessor

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	spanneradmin "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/clients/spanner/admin"
	spannerclient "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/clients/spanner/client"
	spinstanceadmin "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/clients/spanner/instanceadmin"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/metrics"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/utils"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
	"google.golang.org/api/iterator"
	adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

// The SpannerAccessor provides methods that internally use a spanner client (can be adminClient/databaseclient/instanceclient etc).
// Methods should only contain generic logic here that can be used by multiple workflows.
type SpannerAccessor interface {
	// Fetch the dialect of the spanner database.
	GetDatabaseDialect(ctx context.Context, adminClient spanneradmin.AdminClient, dbURI string) (string, error)
	// CheckExistingDb checks whether the database with dbURI exists or not.
	// If API call doesn't respond then user is informed after every 5 minutes on command line.
	CheckExistingDb(ctx context.Context, adminClient spanneradmin.AdminClient, dbURI string) (bool, error)
	// Create a database with no schema.
	CreateEmptyDatabase(ctx context.Context, adminClient spanneradmin.AdminClient, dbURI string) error
	// Fetch the leader of the Spanner instance.
	GetSpannerLeaderLocation(ctx context.Context, instanceClient spinstanceadmin.InstanceAdminClient, instanceURI string) (string, error)
	// Check if a change stream already exists.
	CheckIfChangeStreamExists(ctx context.Context, changeStreamName, dbURI string) (bool, error)
	// Validate that change stream option 'VALUE_CAPTURE_TYPE' is 'NEW_ROW'.
	ValidateChangeStreamOptions(ctx context.Context, changeStreamName, dbURI string) error
	// Create a change stream with default options.
	CreateChangeStream(ctx context.Context, adminClient spanneradmin.AdminClient, changeStreamName, dbURI string) error
	// Create new Database using conv
	CreateDatabase(ctx context.Context, adminClient spanneradmin.AdminClient, dbURI string, conv *internal.Conv, out *os.File, driver string, migrationType string) error 
	// Update Database using conv
	UpdateDatabase(ctx context.Context, adminClient spanneradmin.AdminClient, dbURI string, conv *internal.Conv, out *os.File, driver string) error
}

// This implements the SpannerAccessor interface. This is the primary implementation that should be used in all places other than tests.
type SpannerAccessorImpl struct{}

func (sp *SpannerAccessorImpl) GetDatabaseDialect(ctx context.Context, adminClient spanneradmin.AdminClient, dbURI string) (string, error) {
	result, err := adminClient.GetDatabase(ctx, &databasepb.GetDatabaseRequest{Name: dbURI})
	if err != nil {
		return "", fmt.Errorf("cannot connect to database: %v", err)
	}
	return strings.ToLower(result.DatabaseDialect.String()), nil
}

func (sp *SpannerAccessorImpl) CheckExistingDb(ctx context.Context, adminClient spanneradmin.AdminClient, dbURI string) (bool, error) {
	gotResponse := make(chan bool)
	var err error
	go func() {
		_, err = adminClient.GetDatabase(ctx, &databasepb.GetDatabaseRequest{Name: dbURI})
		gotResponse <- true
	}()
	for {
		select {
		case <-time.After(5 * time.Minute):
			fmt.Println("WARNING! API call not responding: make sure that spanner api endpoint is configured properly")
		case <-gotResponse:
			if err != nil {
				if utils.ContainsAny(strings.ToLower(err.Error()), []string{"database not found"}) {
					return false, nil
				}
				return false, fmt.Errorf("can't get database info: %s", err)
			}
			return true, nil
		}
	}
}

func (sp *SpannerAccessorImpl) CreateEmptyDatabase(ctx context.Context, adminClient spanneradmin.AdminClient, dbURI string) error {
	project, instance, dbName := utils.ParseDbURI(dbURI)
	req := &databasepb.CreateDatabaseRequest{
		Parent:          fmt.Sprintf("projects/%s/instances/%s", project, instance),
		CreateStatement: "CREATE DATABASE `" + dbName + "`",
	}
	op, err := adminClient.CreateDatabase(ctx, req)
	if err != nil {
		return fmt.Errorf("can't build CreateDatabaseRequest: %w", utils.AnalyzeError(err, dbURI))
	}
	if _, err := op.Wait(ctx); err != nil {
		return fmt.Errorf("createDatabase call failed: %w", utils.AnalyzeError(err, dbURI))
	}
	return nil
}

func (sp *SpannerAccessorImpl) GetSpannerLeaderLocation(ctx context.Context, instanceClient spinstanceadmin.InstanceAdminClient, instanceURI string) (string, error) {
	instanceInfo, err := instanceClient.GetInstance(ctx, &instancepb.GetInstanceRequest{Name: instanceURI})
	if err != nil {
		return "", err
	}
	instanceConfig, err := instanceClient.GetInstanceConfig(ctx, &instancepb.GetInstanceConfigRequest{Name: instanceInfo.Config})
	if err != nil {
		return "", err

	}
	for _, replica := range instanceConfig.Replicas {
		if replica.DefaultLeaderLocation {
			return replica.Location, nil
		}
	}
	return "", fmt.Errorf("no leader found for spanner instance %s while trying fetch location", instanceURI)
}

// Consider using a CreateChangestream operation and check for alreadyExists error. That uses adminClient which can be unit tested.
func (sp *SpannerAccessorImpl) CheckIfChangeStreamExists(ctx context.Context, changeStreamName, dbURI string) (bool, error) {
	spClient, err := spannerclient.GetOrCreateClient(ctx, dbURI)
	if err != nil {
		return false, err
	}
	stmt := spanner.Statement{
		SQL: `SELECT CHANGE_STREAM_NAME FROM information_schema.change_streams`,
	}
	iter := spClient.Single().Query(ctx, stmt)
	defer iter.Stop()
	var cs_name string
	csExists := false
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return false, fmt.Errorf("couldn't read row from change_streams table: %w", err)
		}
		err = row.Columns(&cs_name)
		if err != nil {
			return false, fmt.Errorf("can't scan row from change_streams table: %v", err)
		}
		if cs_name == changeStreamName {
			csExists = true
			break
		}
	}
	return csExists, nil
}

func (sp *SpannerAccessorImpl) ValidateChangeStreamOptions(ctx context.Context, changeStreamName, dbURI string) error {
	spClient, err := spannerclient.GetOrCreateClient(ctx, dbURI)
	if err != nil {
		return err
	}
	// Validate if change stream options are set correctly.
	stmt := spanner.Statement{
		SQL: `SELECT option_value FROM information_schema.change_stream_options
		WHERE change_stream_name = @p1 AND option_name = 'value_capture_type'`,
		Params: map[string]interface{}{
			"p1": changeStreamName,
		},
	}
	iter := spClient.Single().Query(ctx, stmt)
	defer iter.Stop()
	var option_value string
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("couldn't read row from change_stream_options table: %w", err)
		}
		err = row.Columns(&option_value)
		if err != nil {
			return fmt.Errorf("can't scan row from change_stream_options table: %v", err)
		}
		if option_value != "NEW_ROW" {
			return fmt.Errorf("VALUE_CAPTURE_TYPE for changestream %s is not NEW_ROW. Please update the changestream option or create a new one", changeStreamName)
		}
	}
	return nil
}

func (sp *SpannerAccessorImpl) CreateChangeStream(ctx context.Context, adminClient spanneradmin.AdminClient, changeStreamName, dbURI string) error {
	op, err := adminClient.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
		Database: dbURI,
		// TODO: create change stream for only the tables present in Spanner.
		Statements: []string{fmt.Sprintf("CREATE CHANGE STREAM %s FOR ALL OPTIONS (value_capture_type = 'NEW_ROW', retention_period = '7d')", changeStreamName)},
	})
	if err != nil {
		return fmt.Errorf("cannot submit request create change stream request: %v", err)
	}
	if err := op.Wait(ctx); err != nil {
		return fmt.Errorf("could not update database ddl: %v", err)
	} else {
		fmt.Println("Successfully created changestream", changeStreamName)
	}
	return nil
}

// CreateDatabase returns a newly create Spanner DB.
// It automatically determines an appropriate project, selects a
// Spanner instance to use, generates a new Spanner DB name,
// and call into the Spanner admin interface to create the new DB.
func (sp *SpannerAccessorImpl) CreateDatabase(ctx context.Context, adminClient spanneradmin.AdminClient, dbURI string, conv *internal.Conv, out *os.File, driver string, migrationType string) error {
	project, instance, dbName := utils.ParseDbURI(dbURI)
	fmt.Fprintf(out, "Creating new database %s in instance %s with default permissions ... \n", dbName, instance)
	// The schema we send to Spanner excludes comments (since Cloud
	// Spanner DDL doesn't accept them), and protects table and col names
	// using backticks (to avoid any issues with Spanner reserved words).
	// Foreign Keys are set to false since we create them post data migration.
	req := &adminpb.CreateDatabaseRequest{
		Parent: fmt.Sprintf("projects/%s/instances/%s", project, instance),
	}
	if conv.SpDialect == constants.DIALECT_POSTGRESQL {
		// PostgreSQL dialect doesn't support:
		// a) backticks around the database name, and
		// b) DDL statements as part of a CreateDatabase operation (so schema
		// must be set using a separate UpdateDatabase operation).
		req.CreateStatement = "CREATE DATABASE \"" + dbName + "\""
		req.DatabaseDialect = adminpb.DatabaseDialect_POSTGRESQL
	} else {
		req.CreateStatement = "CREATE DATABASE `" + dbName + "`"
		if migrationType == constants.DATAFLOW_MIGRATION {
			req.ExtraStatements = conv.SpSchema.GetDDL(ddl.Config{Comments: false, ProtectIds: true, Tables: true, ForeignKeys: true, SpDialect: conv.SpDialect, Source: driver})
		} else {
			req.ExtraStatements = conv.SpSchema.GetDDL(ddl.Config{Comments: false, ProtectIds: true, Tables: true, ForeignKeys: false, SpDialect: conv.SpDialect, Source: driver})
		}

	}

	op, err := adminClient.CreateDatabase(ctx, req)
	if err != nil {
		return fmt.Errorf("can't build CreateDatabaseRequest: %w", utils.AnalyzeError(err, dbURI))
	}
	if _, err := op.Wait(ctx); err != nil {
		return fmt.Errorf("createDatabase call failed: %w", utils.AnalyzeError(err, dbURI))
	}
	fmt.Fprintf(out, "Created database successfully.\n")

	if conv.SpDialect == constants.DIALECT_POSTGRESQL {
		// Update schema separately for PG databases.
		return sp.UpdateDatabase(ctx, adminClient, dbURI, conv, out, driver)
	}
	return nil
}

// UpdateDatabase updates an existing spanner database.
func (sp *SpannerAccessorImpl) UpdateDatabase(ctx context.Context, adminClient spanneradmin.AdminClient, dbURI string, conv *internal.Conv, out *os.File, driver string) error {
	fmt.Fprintf(out, "Updating schema for %s with default permissions ... \n", dbURI)
	// The schema we send to Spanner excludes comments (since Cloud
	// Spanner DDL doesn't accept them), and protects table and col names
	// using backticks (to avoid any issues with Spanner reserved words).
	// Foreign Keys are set to false since we create them post data migration.
	schema := conv.SpSchema.GetDDL(ddl.Config{Comments: false, ProtectIds: true, Tables: true, ForeignKeys: false, SpDialect: conv.SpDialect, Source: driver})
	req := &adminpb.UpdateDatabaseDdlRequest{
		Database:   dbURI,
		Statements: schema,
	}
	// Update queries for postgres as target db return response after more
	// than 1 min for large schemas, therefore, timeout is specified as 5 minutes
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	op, err := adminClient.UpdateDatabaseDdl(ctx, req)
	if err != nil {
		return fmt.Errorf("can't build UpdateDatabaseDdlRequest: %w", utils.AnalyzeError(err, dbURI))
	}
	if err := op.Wait(ctx); err != nil {
		return fmt.Errorf("UpdateDatabaseDdl call failed: %w", utils.AnalyzeError(err, dbURI))
	}
	fmt.Fprintf(out, "Updated schema successfully.\n")
	return nil
}

// CreatesOrUpdatesDatabase updates an existing Spanner database or creates a new one if one does not exist.
func (sp *SpannerAccessorImpl) CreateOrUpdateDatabase(ctx context.Context, adminClient spanneradmin.AdminClient, dbURI, driver string, conv *internal.Conv, out *os.File, migrationType string) error {
	dbExists, err := sp.VerifyDb(ctx, adminClient, dbURI)
	if err != nil {
		return err
	}
	if !conv.Audit.SkipMetricsPopulation {
		// Adding migration metadata to the outgoing context.
		migrationData := metrics.GetMigrationData(conv, driver, constants.SchemaConv)
		serializedMigrationData, _ := proto.Marshal(migrationData)
		migrationMetadataValue := base64.StdEncoding.EncodeToString(serializedMigrationData)
		ctx = metadata.AppendToOutgoingContext(ctx, constants.MigrationMetadataKey, migrationMetadataValue)
	}
	if dbExists {
		if conv.SpDialect != constants.DIALECT_POSTGRESQL && migrationType == constants.DATAFLOW_MIGRATION {
			return fmt.Errorf("spanner migration tool does not support minimal downtime schema/schema-and-data migrations to an existing database")
		}
		err := sp.UpdateDatabase(ctx, adminClient, dbURI, conv, out, driver)
		if err != nil {
			return fmt.Errorf("can't update database schema: %v", err)
		}
	} else {
		err := sp.CreateDatabase(ctx, adminClient, dbURI, conv, out, driver, migrationType)
		if err != nil {
			return fmt.Errorf("can't create database: %v", err)
		}
	}
	return nil
}

// VerifyDb checks whether the db exists and if it does, verifies if the schema is what we currently support.
func (sp *SpannerAccessorImpl) VerifyDb(ctx context.Context, adminClient spanneradmin.AdminClient, dbURI string) (dbExists bool, err error) {
	dbExists, err = sp.CheckExistingDb(ctx, adminClient, dbURI)
	if err != nil {
		return dbExists, err
	}
	if dbExists {
		err = sp.ValidateDDL(ctx, adminClient, dbURI)
	}
	return dbExists, err
}

// ValidateDDL verifies if an existing DB's ddl follows what is supported by Spanner migration tool. Currently,
// we only support empty schema when db already exists.
func(sp *SpannerAccessorImpl)  ValidateDDL(ctx context.Context, adminClient spanneradmin.AdminClient, dbURI string) error {
	dbDdl, err := adminClient.GetDatabaseDdl(ctx, &adminpb.GetDatabaseDdlRequest{Database: dbURI})
	if err != nil {
		return fmt.Errorf("can't fetch database ddl: %v", err)
	}
	if len(dbDdl.Statements) != 0 {
		return fmt.Errorf("spanner migration tool supports writing to existing databases only if they have an empty schema")
	}
	return nil
}
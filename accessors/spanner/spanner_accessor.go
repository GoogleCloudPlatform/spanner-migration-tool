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
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	spanneradmin "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/clients/spanner/admin"
	spannerclient "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/clients/spanner/client"
	spinstanceadmin "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/clients/spanner/instanceadmin"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/utils"
	"google.golang.org/api/iterator"
)

type SpannerAccessor interface {
	GetDatabaseDialect(ctx context.Context, adminClient spanneradmin.AdminClient, dbURI string) (string, error)
	CheckExistingDb(ctx context.Context, adminClient spanneradmin.AdminClient, dbURI string) (bool, error)
	CreateEmptyDatabase(ctx context.Context, adminClient spanneradmin.AdminClient, dbURI string) error
	GetSpannerLeaderLocation(ctx context.Context, instanceClient spinstanceadmin.InstanceAdminClient, instanceURI string) (string, error)
	CheckIfChangeStreamExists(ctx context.Context, changeStreamName, dbURI string) (bool, error)
	ValidateChangeStreamOptions(ctx context.Context, changeStreamName, dbURI string) error
	CreateChangeStream(ctx context.Context, adminClient spanneradmin.AdminClient, changeStreamName, dbURI string) error
}

type SpannerAccessorImpl struct{}

func (sp *SpannerAccessorImpl) GetDatabaseDialect(ctx context.Context, adminClient spanneradmin.AdminClient, dbURI string) (string, error) {
	result, err := adminClient.GetDatabase(ctx, &databasepb.GetDatabaseRequest{Name: dbURI})
	if err != nil {
		return "", fmt.Errorf("cannot connect to database: %v", err)
	}
	return strings.ToLower(result.DatabaseDialect.String()), nil
}

// CheckExistingDb checks whether the database with dbURI exists or not.
// If API call doesn't respond then user is informed after every 5 minutes on command line.
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

/* Copyright 2025 Google LLC
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
// limitations under the License.*/

package cmd

import (
	"context"
	"flag"
	"fmt"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	spanneraccessor "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/spanner"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/import_data"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/common"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/csv"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/spanner"
	"github.com/google/subcommands"
	"go.uber.org/zap"
)

type ImportDataCmd struct {
	instanceId        string
	databaseName      string
	tableName         string
	sourceUri         string
	sourceFormat      string
	schemaUri         string
	csvLineDelimiter  string
	csvFieldDelimiter string
	project           string
}

func (cmd *ImportDataCmd) SetFlags(set *flag.FlagSet) {
	set.StringVar(&cmd.instanceId, "instance-id", "", "Spanner instance Id")
	set.StringVar(&cmd.databaseName, "database-name", "", "Spanner database name")
	set.StringVar(&cmd.tableName, "table-name", "", "Spanner table name. Optional. If not specified, source-uri name will be used")
	set.StringVar(&cmd.sourceUri, "source-uri", "", "URI of the file to import")
	set.StringVar(&cmd.sourceFormat, "source-format", "", "Format of the file to import. Valid values {csv, mysqldump}")
	set.StringVar(&cmd.schemaUri, "schema-uri", "", "URI of the file with schema for the csv to import. Only used for csv format.")
	set.StringVar(&cmd.csvLineDelimiter, "csv-line-delimiter", "", "Token to be used as line delimiter for csv format. Defaults to '\\n'. Only used for csv format.")
	set.StringVar(&cmd.csvFieldDelimiter, "csv-field-delimiter", "", "Token to be used as field delimiter for csv format. Defaults to ','. Only used for csv format.")
	set.StringVar(&cmd.project, "project", "", "Project id for all resources related to this import")
}

func (cmd *ImportDataCmd) Execute(ctx context.Context, f *flag.FlagSet, args ...interface{}) subcommands.ExitStatus {
	logger.Log.Debug(fmt.Sprintf("instanceId %s, dbName %s, schemaUri %s\n", cmd.instanceId, cmd.databaseName, cmd.schemaUri))

	err := validateInputLocal(cmd)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("Input validation failed. Reason %v", err))
		return subcommands.ExitFailure
	}

	err = validateInputRemote(ctx, cmd)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("Input validation failed. Reason %v", err))
		return subcommands.ExitFailure
	}

	dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", cmd.project, cmd.instanceId, cmd.databaseName)

	switch cmd.sourceFormat {
	case constants.CSV:
		//TODO: handle POSTGRESQL
		dialect := constants.DIALECT_GOOGLESQL
		err := cmd.handleCsv(ctx, dbURI, dialect)
		if err != nil {
			logger.Log.Error(fmt.Sprintf("Unable to handle Csv %v", err))
			return subcommands.ExitFailure
		}
		return subcommands.ExitSuccess
	case constants.MYSQLDUMP:
		err := cmd.handleDatabaseDumpFile(ctx, dbURI, constants.MYSQLDUMP, constants.DIALECT_GOOGLESQL)
		if err != nil {
			logger.Log.Error(fmt.Sprintf("Unable to handle MYSQL Dump %v. Please reachout to the support team.", err))
			return subcommands.ExitFailure
		}
		return subcommands.ExitSuccess
	default:
		logger.Log.Warn(fmt.Sprintf("format %s not supported yet", cmd.sourceFormat))
	}
	return subcommands.ExitFailure
}

func validateInputRemote(ctx context.Context, input *ImportDataCmd) error {
	if !isSpannerAccessible(ctx, input.project, input.instanceId, input.databaseName) {
		return fmt.Errorf("spanner instanceId: %v, databaseName: %v not accessible. please check the input and access permissions and try again", input.instanceId, input.databaseName)
	}

	if !isUriAccessible(input.sourceUri) {
		return fmt.Errorf("sourceUri:%v not accessible. Please check the input and access permissions and try again", input.sourceUri)
	}
	if input.sourceFormat == constants.CSV && !isUriAccessible(input.schemaUri) {
		return fmt.Errorf("schemaUri:%v not accessible. Please check the input and access permissions and try again", input.schemaUri)
	}
	return nil
}

/*
1. instance Id is mandatory and accessible
2. database name is mandatory and accessible
3. source uri is mandatory and accessible
4. source format is valid
5. If CSV, schema URI is mandatory and accessible
*/
func validateInputLocal(input *ImportDataCmd) error {

	var err error
	if len(input.instanceId) == 0 {
		return fmt.Errorf("Please specify instanceId using the --instance-id parameter. Received instanceId: %v", input.instanceId)
	}

	if len(input.databaseName) == 0 {
		return fmt.Errorf("Please specify databaseName using the --database-name parameter. Received  databaseName: %v", input.databaseName)
	}

	if len(input.sourceUri) == 0 {
		return fmt.Errorf("Please specify sourceUri using the --source-uri parameter. Received  sourceUri: %v", input.sourceUri)
	}

	if len(input.sourceFormat) == 0 {
		return fmt.Errorf("Please specify sourceFormat using the --source-format parameter. Received  sourceFormat: %v", input.sourceFormat)
	}

	if input.sourceFormat == constants.CSV && len(input.schemaUri) == 0 {
		return fmt.Errorf("Please specify schemaUri using the --schema-uri parameter. Received  schemaUri: %v", input.sourceFormat)
	}

	return err
}

func isSpannerAccessible(ctx context.Context, projectID, instanceId, databaseName string) bool {
	_, err := spanneraccessor.NewSpannerAccessorClientImplWithSpannerClient(ctx, getDBUri(projectID, instanceId, databaseName))
	if err != nil {
		logger.Log.Error(fmt.Sprintf("Unable to instantiate spanner client %v", err))
		return false
	}
	return true
}

func isUriAccessible(uri string) bool {
	parsedURI, err := url.Parse(uri)

	if err != nil {
		logger.Log.Error(fmt.Sprintf("Invalid URI: %s\n", uri))
		return false
	}

	switch parsedURI.Scheme {
	case "file":
		localPath := parsedURI.Path
		_, err := os.Stat(localPath)
		return err == nil
	case "gs":
		ctx := context.Background()
		client, err := storage.NewClient(ctx)
		if err != nil {
			logger.Log.Error(fmt.Sprintf("Error creating GCS client: %v\n", err))
			return false
		}
		defer client.Close()

		bucket := parsedURI.Host
		object := parsedURI.Path[1:] // Remove the leading slash

		_, err = client.Bucket(bucket).Object(object).Attrs(ctx)
		if err != nil {
			logger.Log.Error(fmt.Sprintf("Error checking GCS object %s: %v\n", uri, err))
			return false
		}

		return true
	case "": // Likely a local file path without a scheme
		_, err := os.Stat(uri)
		return err == nil
	default:
		logger.Log.Error(fmt.Sprintf("Unsupported URI scheme: %s\n", uri))
		return false
	}
}

func (cmd *ImportDataCmd) handleCsv(ctx context.Context, infoSchema *spanner.InfoSchemaImpl) error {
	//TODO: handle POSTGRESQL
	dialect := constants.DIALECT_GOOGLESQL

	cmd.tableName = handleTableNameDefaults(cmd.tableName, cmd.sourceUri)
	dbURI := getDBUri(cmd.project, cmd.instanceId, cmd.databaseName)
	sp, err := spanneraccessor.NewSpannerAccessorClientImplWithSpannerClient(ctx, dbURI)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("Unable to instantiate spanner client %v", err))
		return err
	}

	startTime := time.Now()
	csvSchema := import_data.CsvSchemaImpl{ProjectId: cmd.project, InstanceId: cmd.instanceId,
		TableName: cmd.tableName, DbName: cmd.databaseName, SchemaUri: cmd.schemaUri}
	err = csvSchema.CreateSchema(ctx, dialect, sp)

	endTime1 := time.Now()
	elapsedTime := endTime1.Sub(startTime)
	logger.Log.Info(fmt.Sprintf("Schema creation took %f secs", elapsedTime.Seconds()))
	if err != nil {
		return err
	}

	csvData := import_file.CsvDataImpl{ProjectId: cmd.project, InstanceId: cmd.instanceId,
		TableName: cmd.tableName, DbName: cmd.databaseName, SourceUri: cmd.sourceUri, CsvFieldDelimiter: cmd.csvFieldDelimiter}
	err = csvData.ImportData(ctx, infoSchema, dialect, internal.MakeConv(), &common.InfoSchemaImpl{}, &csv.CsvImpl{})

	endTime2 := time.Now()
	elapsedTime = endTime2.Sub(endTime1)
	logger.Log.Info(fmt.Sprintf("Data import took %f secs", elapsedTime.Seconds()))
	return err

}

func getDBUri(projectId, instanceId, databaseName string) string {
	return fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectId, instanceId, databaseName)
}

/*
Handle table name defaults, if they are not passed. Assumes sourceUri file name as table name
This method does not handle validation. It is supposed to be called only after calling validateInputLocal method
*/
func handleTableNameDefaults(tableName, sourceUri string) string {
	if len(tableName) != 0 {
		return tableName
	}

	parsedURL, _ := url.Parse(sourceUri)
	path := parsedURL.Path

	if strings.HasPrefix(path, "/") && len(path) > 1 {
		path = path[1:] // Remove leading slash if present
	}
	return filepath.Base(path)
}

func init() {
	logger.Log, _ = zap.NewDevelopment()
}

func (cmd *ImportDataCmd) Name() string {
	return "import"
}

// Synopsis returns summary of operation.
func (cmd *ImportDataCmd) Synopsis() string {
	return "Import data from supported source files to spanner"
}

// Usage returns usage info of the command.
func (cmd *ImportDataCmd) Usage() string {
	return fmt.Sprintf(`%v import --instance-id=i1 --database-name=db1 --source-format=csv --source-uri=uri1 --schema-uri=uri2 ...

Import data from supported source files to spanner
`, path.Base(os.Args[0]))

}

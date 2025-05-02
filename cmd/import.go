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
	"github.com/GoogleCloudPlatform/spanner-migration-tool/import_file"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/spanner"
	"time"

	spanneraccessor "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/spanner"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
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
	set.StringVar(&cmd.tableName, "table-name", "", "Spanner table name")
	set.StringVar(&cmd.sourceUri, "source-uri", "", "URI of the file to import")
	set.StringVar(&cmd.sourceFormat, "source-format", "", "Format of the file to import. Valid values {csv, mysqldump}")
	set.StringVar(&cmd.schemaUri, "schema-uri", "", "URI of the file with schema for the csv to import. Only used for csv format.")
	set.StringVar(&cmd.csvLineDelimiter, "csv-line-delimiter", "", "Token to be used as line delimiter for csv format. Defaults to '\\n'. Only used for csv format.")
	set.StringVar(&cmd.csvFieldDelimiter, "csv-field-delimiter", "", "Token to be used as field delimiter for csv format. Defaults to ','. Only used for csv format.")
	set.StringVar(&cmd.project, "project", "", "Project id for all resources related to this import")
}

func (cmd *ImportDataCmd) Execute(ctx context.Context, f *flag.FlagSet, args ...interface{}) subcommands.ExitStatus {
	logger.Log.Debug(fmt.Sprintf("instanceId %s, dbName %s, schemaUri %s\n", cmd.instanceId, cmd.databaseName, cmd.schemaUri))

	dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", cmd.project, cmd.instanceId, cmd.databaseName)
	sp, err := spanneraccessor.NewSpannerAccessorClientImplWithSpannerClient(ctx, dbURI)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("Unable to instantiate spanner client %v", err))
		return subcommands.ExitFailure
	}

	switch cmd.sourceFormat {
	case constants.CSV:
		//TODO: handle POSTGRESQL
		dialect := constants.DIALECT_GOOGLESQL
		err := cmd.handleCsv(ctx, dbURI, dialect, sp)
		if err != nil {
			logger.Log.Error(fmt.Sprintf("Unable to handle Csv %v", err))
			return subcommands.ExitFailure
		}
		return subcommands.ExitSuccess
	case constants.MYSQLDUMP:
		err := cmd.handleDatabaseDumpFile(ctx, dbURI, constants.MYSQLDUMP, constants.DIALECT_GOOGLESQL, sp)
		if err != nil {
			logger.Log.Error(fmt.Sprintf("Unable to handle MYSQL Dump %v", err))
			return subcommands.ExitFailure
		}
		return subcommands.ExitSuccess
	default:
		logger.Log.Warn(fmt.Sprintf("format %s not supported yet", cmd.sourceFormat))
	}
	return subcommands.ExitFailure
}

func (cmd *ImportDataCmd) handleCsv(ctx context.Context, dbURI string, dialect string, sp *spanneraccessor.SpannerAccessorImpl) error {
	infoSchema, err := spanner.NewInfoSchemaImplWithSpannerClient(ctx, dbURI, dialect)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("Unable to read Spanner schema %v", err))
		return err
	}

	startTime := time.Now()
	csvSchema := import_file.CsvSchemaImpl{ProjectId: cmd.project, InstanceId: cmd.instanceId,
		TableName: cmd.tableName, DbName: cmd.databaseName, SchemaUri: cmd.schemaUri, CsvFieldDelimiter: cmd.csvFieldDelimiter}
	err = csvSchema.CreateSchema(ctx, dialect, sp)

	endTime1 := time.Now()
	elapsedTime := endTime1.Sub(startTime)
	fmt.Println("Schema creation took ", elapsedTime.Seconds(), "  secs")
	if err != nil {
		return err
	}

	csvData := import_file.CsvDataImpl{ProjectId: cmd.project, InstanceId: cmd.instanceId,
		TableName: cmd.tableName, DbName: cmd.databaseName, SourceUri: cmd.sourceUri, CsvFieldDelimiter: cmd.csvFieldDelimiter}
	err = csvData.ImportData(ctx, infoSchema, dialect)

	endTime2 := time.Now()
	elapsedTime = endTime2.Sub(endTime1)
	fmt.Println("Data import took ", elapsedTime.Seconds(), "  secs")
	return err

}

func (cmd *ImportDataCmd) handleDatabaseDumpFile(ctx context.Context, dbUri, driver string, dialect string, spannerAccessor spanneraccessor.SpannerAccessor) error {

	importDump, err := import_file.NewImportFromDump(cmd.project, cmd.instanceId, cmd.databaseName, cmd.sourceUri, driver, spannerAccessor)
	if err != nil {
		return err
	}

	defer importDump.Finalize()

	schemaStartTime := time.Now()
	conv, err := importDump.CreateSchema(dialect)
	if err != nil {
		return fmt.Errorf(fmt.Sprintf("can't create schema: %v\n", err))
	}

	err = spannerAccessor.CreateOrUpdateDatabase(ctx, dbUri, driver, conv, driver)
	spannerAccessor.Refresh(ctx, dbUri)

	schemaEndTime := time.Now()
	elapsedTime := schemaEndTime.Sub(schemaStartTime)
	logger.Log.Info(fmt.Sprintf("Schema creation took %f secs", elapsedTime.Seconds()))
	if err != nil {
		return err
	}

	err = importDump.ImportData(conv)

	dataEndTime := time.Now()
	elapsedTime = dataEndTime.Sub(schemaEndTime)
	logger.Log.Info(fmt.Sprintf("Data import took %f secs", elapsedTime.Seconds()))
	return err

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
	//TODO implement me
	return fmt.Sprintf("test usage")
}

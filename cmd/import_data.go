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

	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/import_data"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/spanner"
	"github.com/google/subcommands"
	"go.uber.org/zap"
)

type ImportDataCmd struct {
	instanceId        string
	dbName            string
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
	set.StringVar(&cmd.dbName, "db-name", "", "Spanner database name")
	set.StringVar(&cmd.tableName, "table-name", "", "Spanner table name")
	set.StringVar(&cmd.sourceUri, "source-uri", "", "URI of the file to import")
	set.StringVar(&cmd.sourceFormat, "format", "", "Format of the file to import. Valid values {csv}")
	set.StringVar(&cmd.schemaUri, "schema-uri", "", "URI of the file with schema for the csv to import. Only used for csv format.")
	set.StringVar(&cmd.csvLineDelimiter, "csv-line-delimiter", "", "Token to be used as line delimiter for csv format. Defaults to '\\n'. Only used for csv format.")
	set.StringVar(&cmd.csvFieldDelimiter, "csv-field-delimiter", "", "Token to be used as field delimiter for csv format. Defaults to ','. Only used for csv format.")
	set.StringVar(&cmd.project, "project", "", "Project id for all resources related to this import")
}

func (cmd *ImportDataCmd) Execute(ctx context.Context, f *flag.FlagSet, args ...interface{}) subcommands.ExitStatus {
	logger.Log.Debug(fmt.Sprintf("instanceId %s, dbName %s, schemaUri %s\n", cmd.instanceId, cmd.dbName, cmd.schemaUri))

	dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", cmd.project, cmd.instanceId, cmd.dbName)
	infoSchema, err := spanner.NewInfoSchemaImplWithSpannerClient(ctx, dbURI, constants.DIALECT_GOOGLESQL)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("Unable to read Spanner schema %v", err))
		return subcommands.ExitFailure
	}

	switch cmd.sourceFormat {
	case constants.CSV:
		err := cmd.handleCsv(ctx, infoSchema)
		if err != nil {
			logger.Log.Error(fmt.Sprintf("Unable to handle Csv %v", err))
			return subcommands.ExitFailure
		}
		return subcommands.ExitSuccess
	default:
		logger.Log.Warn(fmt.Sprintf("format %s not supported yet", cmd.sourceFormat))
	}
	return subcommands.ExitFailure
}

func (cmd *ImportDataCmd) handleCsv(ctx context.Context, infoSchema *spanner.InfoSchemaImpl) error {
	//TODO: handle POSTGRESQL
	dialect := constants.DIALECT_GOOGLESQL

	csvSchema := import_data.CsvSchemaImpl{}
	csvSchema.ProjectId = cmd.project
	csvSchema.InstanceId = cmd.instanceId
	csvSchema.TableName = cmd.tableName
	csvSchema.DbName = cmd.dbName
	csvSchema.SchemaUri = cmd.schemaUri
	csvSchema.CsvFieldDelimiter = cmd.csvFieldDelimiter
	err := csvSchema.CreateSchema(ctx, dialect)
	if err != nil {
		return err
	}

	csvData := import_data.CsvDataImpl{}
	csvData.ProjectId = cmd.project
	csvData.InstanceId = cmd.instanceId
	csvData.TableName = cmd.tableName
	csvData.DbName = cmd.dbName
	csvData.SourceUri = cmd.sourceUri
	csvData.CsvFieldDelimiter = cmd.csvFieldDelimiter
	return csvData.ImportData(ctx, infoSchema, dialect)

}

func init() {
	logger.Log = zap.NewNop()
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

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
	"sync/atomic"

	sp "cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/parse"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/utils"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/proto/migration"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/csv"
	"go.uber.org/zap"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/writer"
	"github.com/google/subcommands"
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
	fmt.Printf("instanceId %s, dbName %s, schemaUri %s\n", cmd.instanceId, cmd.dbName, cmd.schemaUri)

	switch cmd.sourceFormat {
	case constants.CSV:
		importCsv(ctx, cmd, &csv.CsvImpl{})
	default:
		fmt.Printf("format %s not supported yet", cmd.sourceFormat)
	}

	return 0
}

func importCsv(ctx context.Context, cmd *ImportDataCmd, csv csv.CsvInterface) subcommands.ExitStatus {
	// TODO: start with single table imports

	// get connection to spanner
	adminClient, client, err := GetSpannerClient(ctx, cmd.project, cmd.instanceId, cmd.dbName)
	if err != nil {
		err = fmt.Errorf("can't create database client: %v", err)
		return subcommands.ExitFailure
	}
	defer adminClient.Close()
	defer client.Close()

	//TODO: uncomment and implement
	// createSchema(cmd.schemaUri)

	// TODO: Response code -  error /success contract between gcloud and SMT

	// TODO: get CSV locally. start with unchunked and later figure out chunking for larger sizes

	conv := getConvObject(cmd)
	batchWriter := getBatchWriterWithConfig(client, conv)
	err = utils.ReadSpannerSchema(ctx, conv, client)
	if err != nil {
		fmt.Errorf("Unable to read Spanner schema %v", err)
		return subcommands.ExitFailure
	}

	tableId, err := internal.GetTableIdFromSpName(conv.SpSchema, cmd.tableName)
	if err != nil {
		fmt.Errorf("Table %s not found in Spanner", cmd.tableName)
		return subcommands.ExitFailure
	}
	columnNames := []string{}
	for _, v := range conv.SpSchema[tableId].ColIds {
		columnNames = append(columnNames, conv.SpSchema[tableId].ColDefs[v].Name)
	}

	err = csv.ProcessSingleCSV(conv, cmd.tableName, columnNames,
		conv.SpSchema[tableId].ColDefs, cmd.sourceUri, "", rune(cmd.csvFieldDelimiter[0]))
	if err != nil {
		return subcommands.ExitFailure
	}
	batchWriter.Flush()

	return subcommands.ExitSuccess
}

func createSchema(schemaUri string) {
	// TODO: create table, find a place for it. create table if not exists, validate schema matches
	parseSchema()
	//TODO: implement me

}

func init() {
	logger.Log = zap.NewNop()
}
func getConvObject(cmd *ImportDataCmd) *internal.Conv {
	conv := internal.MakeConv()
	conv.Audit.MigrationType = migration.MigrationData_DATA_ONLY.Enum()
	conv.Audit.SkipMetricsPopulation = true
	conv.Audit.DryRun = false

	conv.SpDialect = constants.DIALECT_GOOGLESQL //TODO: handle POSTGRESQL
	conv.SpProjectId = cmd.project
	conv.SpInstanceId = cmd.instanceId

	return conv
}

func parseSchema() map[string]ddl.ColumnDef {
	// TODO: implement me
	return make(map[string]ddl.ColumnDef)
}

func getBatchWriterWithConfig(client *sp.Client, conv *internal.Conv) *writer.BatchWriter {
	// TODO: review these limits
	config := writer.BatchWriterConfig{
		BytesLimit: 100 * 1000 * 1000,
		WriteLimit: 2000,
		RetryLimit: 1000,
		Verbose:    internal.Verbose(),
	}

	rows := int64(0)
	config.Write = func(m []*sp.Mutation) error {
		ctx := context.Background()
		_, err := client.Apply(ctx, m)
		if err != nil {
			return err
		}
		atomic.AddInt64(&rows, int64(len(m)))
		return nil
	}
	batchWriter := writer.NewBatchWriter(config)
	conv.SetDataMode()
	conv.SetDataSink(
		func(table string, cols []string, vals []interface{}) {
			batchWriter.AddRow(table, cols, vals)
		})
	conv.DataFlush = func() {
		batchWriter.Flush()
	}
	return batchWriter
}

func GetSpannerClient(ctx context.Context, project string, instance string,
	dbName string) (*database.DatabaseAdminClient, *sp.Client, error) {

	dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", project, instance, dbName)
	adminClient, err := utils.NewDatabaseAdminClient(ctx)
	if err != nil {
		err = fmt.Errorf("can't create admin client: %v", parse.AnalyzeError(err, dbURI))
		return nil, nil, err
	}
	client, err := utils.GetClient(ctx, dbURI)
	if err != nil {
		err = fmt.Errorf("can't create client for db %s: %v", dbURI, err)
		return adminClient, nil, err
	}
	return adminClient, client, nil
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

package import_data

import (
	"context"
	"fmt"
	"sync/atomic"

	sp "cloud.google.com/go/spanner"
	spannerclient "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/clients/spanner/client"
	spanneraccessor "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/spanner"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/proto/migration"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/csv"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/spanner"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/writer"
	"github.com/google/subcommands"
	"go.uber.org/zap"
)

type SourceCsv interface {
	Import(ctx context.Context) subcommands.ExitStatus
}

type SourceCsvImpl struct {
	ProjectId         string
	InstanceId        string
	DbName            string
	TableName         string
	SourceUri         string
	CsvFieldDelimiter string
}

func (source *SourceCsvImpl) Import(ctx context.Context) subcommands.ExitStatus {
	// TODO: start with single table imports

	//TODO: uncomment and implement
	// createSchema(cmd.schemaUri)

	// TODO: Response code -  error /success contract between gcloud and SMT

	// TODO: get CSV locally. start with unchunked and later figure out chunking for larger sizes

	conv := getConvObject(source.ProjectId, source.InstanceId)
	dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", source.ProjectId, source.InstanceId, source.DbName)
	infoSchema, err := spanner.NewInfoSchemaImplWithSpannerClient(ctx, dbURI, constants.DIALECT_GOOGLESQL)
	batchWriter := getBatchWriterWithConfig(infoSchema.SpannerClient, conv)

	err = infoSchema.PopulateSpannerSchema(ctx, conv)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("Unable to read Spanner schema %v", err))
		return subcommands.ExitFailure
	}

	tableId, err := internal.GetTableIdFromSpName(conv.SpSchema, source.TableName)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("Table %s not found in Spanner", source.TableName))
		return subcommands.ExitFailure
	}
	columnNames := []string{}
	for _, v := range conv.SpSchema[tableId].ColIds {
		columnNames = append(columnNames, conv.SpSchema[tableId].ColDefs[v].Name)
	}

	csv := csv.CsvImpl{}
	err = csv.ProcessSingleCSV(conv, source.TableName, columnNames,
		conv.SpSchema[tableId].ColDefs, source.SourceUri, "", rune(source.CsvFieldDelimiter[0]))
	if err != nil {
		return subcommands.ExitFailure
	}
	batchWriter.Flush()

	return subcommands.ExitSuccess
}

func getConvObject(projectId, instanceId string) *internal.Conv {
	conv := internal.MakeConv()
	conv.Audit.MigrationType = migration.MigrationData_DATA_ONLY.Enum()
	conv.Audit.SkipMetricsPopulation = true
	conv.Audit.DryRun = false

	conv.SpDialect = constants.DIALECT_GOOGLESQL //TODO: handle POSTGRESQL
	conv.SpProjectId = projectId
	conv.SpInstanceId = instanceId

	return conv
}

func getBatchWriterWithConfig(spannerClient spannerclient.SpannerClient, conv *internal.Conv) *writer.BatchWriter {
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
		_, err := spannerClient.Apply(ctx, m)
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

func createSchema(schemaUri string) {
	// TODO: create table, find a place for it. create table if not exists, validate schema matches
	//parseSchema()
	// parse schema from schemaURI

	//TODO: implement me
	// check if table exists
	//dbExists, err = sp.CheckExistingDb(ctx, dbURI)
	// if exists, verify table schema is same as passed

	// if not exists create table with passed schema
}

func init() {
	logger.Log = zap.NewNop()
}

func parseSchema(spAccess *spanneraccessor.SpannerAccessorImpl) map[string]ddl.ColumnDef {
	// TODO: implement me
	return make(map[string]ddl.ColumnDef)
}

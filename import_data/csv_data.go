package import_data

import (
	"context"
	"fmt"
	"sync/atomic"

	sp "cloud.google.com/go/spanner"
	spannerclient "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/clients/spanner/client"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/proto/migration"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/csv"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/spanner"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/writer"
	"go.uber.org/zap"
)

type CsvData interface {
	ImportData(ctx context.Context, infoSchema *spanner.InfoSchemaImpl, dialect string) error
}

type CsvDataImpl struct {
	ProjectId         string
	InstanceId        string
	DbName            string
	TableName         string
	SourceUri         string
	CsvFieldDelimiter string
}

func (source *CsvDataImpl) ImportData(ctx context.Context, infoSchema *spanner.InfoSchemaImpl, dialect string) error {
	// TODO: start with single table imports
	// TODO: Response code -  error /success contract between gcloud and SMT
	// TODO: get CSV locally. start with unchunked and later figure out chunking for larger sizes

	conv := getConvObject(source.ProjectId, source.InstanceId, dialect)
	batchWriter := getBatchWriterWithConfig(infoSchema.SpannerClient, conv)

	err := infoSchema.PopulateSpannerSchema(ctx, conv)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("Unable to read Spanner schema %v", err))
		return err
	}

	tableId, err := internal.GetTableIdFromSpName(conv.SpSchema, source.TableName)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("Table %s not found in Spanner", source.TableName))
		return err
	}
	columnNames := []string{}
	for _, v := range conv.SpSchema[tableId].ColIds {
		columnNames = append(columnNames, conv.SpSchema[tableId].ColDefs[v].Name)
	}

	csv := csv.CsvImpl{}
	err = csv.ProcessSingleCSV(conv, source.TableName, columnNames,
		conv.SpSchema[tableId].ColDefs, source.SourceUri, "", rune(source.CsvFieldDelimiter[0]))
	if err != nil {
		return err
	}
	batchWriter.Flush()
	return err
}

func getConvObject(projectId, instanceId, dialect string) *internal.Conv {
	conv := internal.MakeConv()
	conv.Audit.MigrationType = migration.MigrationData_DATA_ONLY.Enum()
	conv.Audit.SkipMetricsPopulation = true
	conv.Audit.DryRun = false

	conv.SpDialect = dialect
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

func init() {
	logger.Log = zap.NewNop()
}

package import_file

import (
	"bufio"
	"context"
	"fmt"
	spanneraccessor "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/spanner"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/expressions_api"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/common"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/mysql"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/postgres"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/writer"
	"go.uber.org/zap"
	"os"
)

var NewSpannerAccessor = func(ctx context.Context, dbURI string) (spanneraccessor.SpannerAccessor, error) {
	return spanneraccessor.NewSpannerAccessorClientImplWithSpannerClient(ctx, dbURI)
}

type ImportFromDump interface {
	CreateSchema(ctx context.Context, dialect string) (*internal.Conv, error)
	ImportData(ctx context.Context, conv *internal.Conv) error
	Close()
}

type ImportFromDumpImpl struct {
	ProjectId       string
	InstanceId      string
	DatabaseName    string
	DumpUri         string
	dbUri           string
	dumpReader      *os.File
	SourceFormat    string
	SpannerAccessor spanneraccessor.SpannerAccessor
	schemaToSpanner common.SchemaToSpannerInterface
	dbDumpProcessor common.DbDump
}

func NewImportFromDump(
	ctx context.Context,
	projectId string,
	instanceId string,
	databaseName string,
	dumpUri string,
	sourceFormat string,
	dbURI string) (ImportFromDump, error) {
	dbDump, err := getDbDump(sourceFormat)
	if err != nil {
		return nil, err
	}
	// TODO: handle GCS
	dumpReader, err := os.Open(dumpUri)
	if err != nil {
		if err != nil {
			return nil, fmt.Errorf(fmt.Sprintf("can't read dump file: %s due to: %v", dumpUri, err))
		}
	}
	spannerAccessor, err := NewSpannerAccessor(ctx, dbURI)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("Unable to instantiate spanner client %v", err))
		return nil, fmt.Errorf("unable to instantiate spanner client %v", err)
	}

	schemaToSpanner := &common.SchemaToSpannerImpl{
		ExpressionVerificationAccessor: &expressions_api.ExpressionVerificationAccessorImpl{SpannerAccessor: spannerAccessor},
	}

	return &ImportFromDumpImpl{
		projectId,
		instanceId,
		databaseName,
		dumpUri,
		dbURI,
		dumpReader,
		sourceFormat,
		spannerAccessor,
		schemaToSpanner,
		dbDump,
	}, nil
}

// CreateSchema Process database dump file. Convert schema to spanner DDL. Update the provided database with the schema.
func (source *ImportFromDumpImpl) CreateSchema(ctx context.Context, dialect string) (*internal.Conv, error) {

	r := internal.NewReader(bufio.NewReader(source.dumpReader), nil)
	conv := internal.MakeConv()
	conv.SpDialect = dialect
	conv.Source = source.SourceFormat
	conv.SpProjectId = source.ProjectId
	conv.SpInstanceId = source.InstanceId
	conv.SetSchemaMode() // Build schema and ignore data in dump.
	conv.SetDataSink(nil)
	if err := source.dbDumpProcessor.ProcessDump(conv, r); err != nil {
		logger.Log.Error("Failed to parse the dump file:", zap.Error(err))
		return nil, fmt.Errorf("failed to process source schema: %v", err)
	}

	if err := common.ConvertSchemaToSpannerDDL(conv, source.dbDumpProcessor, source.schemaToSpanner); err != nil {
		logger.Log.Error("Failed to convert schema to spanner DDL:", zap.Error(err))
		return nil, fmt.Errorf("failed to convert schema to spanner DDL: %v", err)

	}

	// TODO: Only update database
	err := source.SpannerAccessor.CreateOrUpdateDatabase(ctx, source.dbUri, source.SourceFormat, conv, source.SourceFormat)
	if err != nil {
		return nil, fmt.Errorf("can't create or update database: %v", err)
	}
	source.SpannerAccessor.Refresh(ctx, source.dbUri)

	return conv, nil
}

// ImportData process database dump file. Convert insert statement to spanner mutation. Load data into spanner.
func (source *ImportFromDumpImpl) ImportData(ctx context.Context, conv *internal.Conv) error {
	dumpReader, err := ResetReader(source.dumpReader, source.DumpUri)
	if err != nil {
		return fmt.Errorf("can't read dump file: %s due to: %v", source.DumpUri, err)
	}
	source.dumpReader = dumpReader
	logger.Log.Info(fmt.Sprintf("Importing %d rows.", conv.Rows()))
	r := internal.NewReader(bufio.NewReader(source.dumpReader), nil)
	batchWriter := writer.GetBatchWriterWithConfig(ctx, source.SpannerAccessor.GetSpannerClient(), conv)

	if err := source.dbDumpProcessor.ProcessDump(conv, r); err != nil {
		return err
	}
	batchWriter.Flush()

	return nil
}

func getDbDump(sourceFormat string) (common.DbDump, error) {
	switch sourceFormat {
	case constants.MYSQLDUMP:
		return mysql.DbDumpImpl{}, nil
	case constants.PGDUMP:
		return postgres.DbDumpImpl{}, nil
	default:
		return nil, fmt.Errorf("process dump for sourceFormat %s not supported", sourceFormat)
	}
}

func (source *ImportFromDumpImpl) Close() {
	source.dumpReader.Close()
}

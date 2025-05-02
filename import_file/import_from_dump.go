package import_file

import (
	"bufio"
	"fmt"
	spanneraccessor "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/spanner"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/expressions_api"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/common"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/mysql"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/postgres"
	"go.uber.org/zap"
	"os"
)

type ImportFromDump interface {
	CreateSchema(dialect string) (*internal.Conv, error)
	ImportData(conv *internal.Conv) error
	Finalize() error
}

type ImportFromDumpImpl struct {
	ProjectId       string
	InstanceId      string
	DatabaseName    string
	DumpUri         string
	dumpReader      *os.File
	SourceFormat    string
	SpannerAccessor spanneraccessor.SpannerAccessor
	schemaToSpanner common.SchemaToSpannerInterface
	dbDumpProcessor common.DbDump
}

func NewImportFromDump(
	projectId string,
	instanceId string,
	databaseName string,
	dumpUri string,
	sourceFormat string,
	spannerAccessor spanneraccessor.SpannerAccessor) (ImportFromDump, error) {
	dbDump, err := getDbDump(sourceFormat)
	if err != nil {
		return nil, err
	}
	// TODO: handle GCS
	dumpReader, err := os.Open(dumpUri)
	if err != nil {
		if err != nil {
			return nil, fmt.Errorf(fmt.Sprintf("can't read dump file: %s due to: %v\n", dumpUri, err))
		}
	}

	schemaToSpanner := &common.SchemaToSpannerImpl{
		ExpressionVerificationAccessor: &expressions_api.ExpressionVerificationAccessorImpl{SpannerAccessor: spannerAccessor},
	}

	return &ImportFromDumpImpl{
		projectId,
		instanceId,
		databaseName,
		dumpUri,
		dumpReader,
		sourceFormat,
		spannerAccessor,
		schemaToSpanner,
		dbDump,
	}, nil
}

func (source *ImportFromDumpImpl) CreateSchema(dialect string) (*internal.Conv, error) {

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
		return nil, err
	}

	if err := common.ConvertSchemaToSpannerDDL(conv, source.dbDumpProcessor, source.schemaToSpanner); err != nil {
		logger.Log.Error("Failed to convert schema to spanner DDL:", zap.Error(err))
		return nil, err

	}
	return conv, nil
}

func (source *ImportFromDumpImpl) ImportData(conv *internal.Conv) error {
	dumpReader, err := ResetReader(source.dumpReader, source.DumpUri)
	if err != nil {
		return err
	}
	source.dumpReader = dumpReader
	logger.Log.Info(fmt.Sprintf("Importing %d rows.", conv.Rows()))
	r := internal.NewReader(bufio.NewReader(source.dumpReader), nil)
	batchWriter := getBatchWriterWithConfig(source.SpannerAccessor.GetSpannerClient(), conv)

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

func (source *ImportFromDumpImpl) Finalize() error {
	return source.dumpReader.Close()
}

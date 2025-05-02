package import_file

import (
	"bufio"
	"fmt"
	spannerclient "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/clients/spanner/client"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/conversion"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"go.uber.org/zap"
	"os"
)

type ImportFromDump interface {
	CreateSchema(dialect string, processDump conversion.ProcessDumpByDialectInterface) (*internal.Conv, error)
	ImportData(conv *internal.Conv, processDump conversion.ProcessDumpByDialectInterface, client spannerclient.SpannerClient) error
	Finalize() error
}

type ImportFromDumpImpl struct {
	ProjectId  string
	InstanceId string
	DbName     string
	DumpUri    string
	dumpReader *os.File
	Driver     string
}

func (source *ImportFromDumpImpl) CreateSchema(dialect string, processDump conversion.ProcessDumpByDialectInterface) (*internal.Conv, error) {
	// TODO: handle GCS
	dumpReader, err := os.Open(source.DumpUri)
	if err != nil {
		if err != nil {
			return nil, fmt.Errorf(fmt.Sprintf("can't read dump file: %s due to: %v\n", source.DumpUri, err))
		}
	}

	source.dumpReader = dumpReader

	r := internal.NewReader(bufio.NewReader(source.dumpReader), nil)
	conv := internal.MakeConv()
	conv.SpDialect = dialect
	conv.Source = source.Driver
	conv.SpProjectId = source.ProjectId
	conv.SpInstanceId = source.InstanceId
	conv.SetSchemaMode() // Build schema and ignore data in dump.
	conv.SetDataSink(nil)
	err = processDump.ProcessDump(source.Driver, conv, r)
	if err != nil {
		logger.Log.Error("Failed to parse the dump file:", zap.Error(err))
		return nil, fmt.Errorf("failed to parse the dump file")
	}
	return conv, nil
}

func (source *ImportFromDumpImpl) ImportData(conv *internal.Conv, processDump conversion.ProcessDumpByDialectInterface, client spannerclient.SpannerClient) error {
	dumpReader, err := ResetReader(source.dumpReader, source.DumpUri)
	if err != nil {
		return err
	}
	source.dumpReader = dumpReader
	logger.Log.Info(fmt.Sprintf("Importing %d rows.", conv.Rows()))
	r := internal.NewReader(bufio.NewReader(source.dumpReader), nil)
	batchWriter := getBatchWriterWithConfig(client, conv)
	err = processDump.ProcessDump(source.Driver, conv, r)
	batchWriter.Flush()
	if err != nil {
		logger.Log.Error("Failed to parse the dump file:", zap.Error(err))
		return fmt.Errorf("failed to parse the dump file")
	}

	return nil
}

func (source *ImportFromDumpImpl) Finalize() error {
	return source.dumpReader.Close()
}

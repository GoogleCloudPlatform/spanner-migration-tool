package file_reader

import (
	"context"
	"fmt"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"io"
	"os"
)

type LocalFileReaderImpl struct {
	uri  string
	file *os.File
}

func (reader *LocalFileReaderImpl) ResetReader(ctx context.Context) (io.Reader, error) {
	if reader.file != nil {
		_, err := reader.file.Seek(0, 0)
		if err == nil {
			return reader.file, nil
		}
		reader.file.Close()
	}
	return reader.CreateReader(ctx)

}

func (reader *LocalFileReaderImpl) CreateReader(_ context.Context) (io.Reader, error) {
	f, err := os.Open(reader.uri)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("readFile: unable to open file: %s. Error: %q", reader.uri, err))
		return nil, err
	}
	reader.file = f
	return f, nil
}

func (reader *LocalFileReaderImpl) Close() {
	if reader.file != nil {
		reader.file.Close()
	}
}

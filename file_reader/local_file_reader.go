package file_reader

import (
	"context"
	"fmt"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"io"
	"os"
)

type LocalFileReaderImpl struct {
	uri        string
	fileHandle *os.File
}

func NewLocalFileReader(uri string) (*LocalFileReaderImpl, error) {
	_, err := os.Stat(uri)
	if err != nil {
		return nil, err
	}
	return &LocalFileReaderImpl{uri: uri}, nil
}

func (reader *LocalFileReaderImpl) ResetReader(ctx context.Context) (io.Reader, error) {
	if reader.fileHandle != nil {
		_, err := reader.fileHandle.Seek(0, 0)
		if err == nil {
			return reader.fileHandle, nil
		}
		reader.fileHandle.Close()
	}
	return reader.CreateReader(ctx)

}

func (reader *LocalFileReaderImpl) CreateReader(_ context.Context) (io.Reader, error) {
	f, err := os.Open(reader.uri)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("readFile: unable to open fileHandle: %s. Error: %q", reader.uri, err))
		return nil, err
	}
	reader.fileHandle = f
	return f, nil
}

func (reader *LocalFileReaderImpl) Close() {
	if reader.fileHandle != nil {
		reader.fileHandle.Close()
	}
}

func (reader *LocalFileReaderImpl) ReadAll(_ context.Context) ([]byte, error) {
	if reader.fileHandle == nil {
		_, err := reader.CreateReader(context.Background())
		if err != nil {
			return nil, err
		}
	}
	return io.ReadAll(reader.fileHandle)
}

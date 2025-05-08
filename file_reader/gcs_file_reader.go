package file_reader

import (
	"cloud.google.com/go/storage"
	"context"
	"fmt"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"io"
)

type GcsFileReaderImpl struct {
	uri           string
	bucket        string
	gcsFilePath   string
	storageClient *storage.Client
	storageReader *storage.Reader
}

func (reader *GcsFileReaderImpl) ResetReader(ctx context.Context) (io.Reader, error) {
	if reader.storageReader != nil {
		reader.storageReader.Close()
	}

	return reader.CreateReader(ctx)
}

func (reader *GcsFileReaderImpl) CreateReader(ctx context.Context) (io.Reader, error) {

	rc, err := reader.storageClient.Bucket(reader.bucket).Object(reader.gcsFilePath).NewReader(ctx)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("readFile: unable to open file from bucket %q, file %q: %v", reader.bucket, reader.gcsFilePath, err))
		return nil, err
	}
	reader.storageReader = rc
	return rc, nil
}

func (reader *GcsFileReaderImpl) Close() {
	if reader.storageReader != nil {
		reader.storageReader.Close()
	}
	if reader.storageClient != nil {
		reader.storageClient.Close()
	}

}

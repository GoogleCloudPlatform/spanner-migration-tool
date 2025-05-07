package import_file

import (
	"cloud.google.com/go/storage"
	"context"
	"fmt"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"google.golang.org/api/option"
	"io"
	"net/url"
	"os"
)

var GoogleStorageNewClient = func(ctx context.Context, opts ...option.ClientOption) (*storage.Client, error) {
	return storage.NewClient(ctx, opts...)
}

type FileReader interface {
	// ResetReader reset the reader to the beginning of the file. If seek is not possible,
	// then the file is closed and opened again.
	ResetReader(ctx context.Context) (io.Reader, error)
	// CreateReader Create an io.reader for the file.
	CreateReader(ctx context.Context) (io.Reader, error)
	Close()
}

type LocalFileReaderImpl struct {
	uri  string
	file *os.File
}

type GcsFileReaderImpl struct {
	uri           string
	bucket        string
	gcsFilePath   string
	storageClient *storage.Client
	storageReader *storage.Reader
}

func NewFileReader(ctx context.Context, uri string) (FileReader, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}

	if u.Scheme == constants.GCS_SCHEME {
		storageClient, err := GoogleStorageNewClient(ctx)
		if err != nil {
			return nil, err
		}
		return &GcsFileReaderImpl{
			uri:           uri,
			bucket:        u.Host,
			gcsFilePath:   u.Path[1:], // removes "/" from beginning of path
			storageClient: storageClient,
		}, nil
	} else {
		file, err := os.Open(uri)
		if err != nil {
			return nil, err
		}
		return &LocalFileReaderImpl{uri: uri, file: file}, nil
	}
}

func (reader *GcsFileReaderImpl) ResetReader(ctx context.Context) (io.Reader, error) {
	if reader.storageReader != nil {
		reader.storageReader.Close()
	}

	return reader.CreateReader(ctx)
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

func (reader *LocalFileReaderImpl) Close() {
	if reader.file != nil {
		reader.file.Close()
	}
}

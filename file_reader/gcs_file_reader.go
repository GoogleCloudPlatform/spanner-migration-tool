package file_reader

import (
	"cloud.google.com/go/storage"
	"context"
	"fmt"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/clients"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"google.golang.org/api/option"
	"io"
)

var GoogleStorageNewClient = func(ctx context.Context, opts ...option.ClientOption) (*storage.Client, error) {
	return storage.NewClient(ctx, opts...)
}

type GcsFileReaderImpl struct {
	uri           string
	bucket        string
	gcsFilePath   string
	storageClient *storage.Client
	storageReader *storage.Reader
}

func NewGcsFileReader(ctx context.Context, uri, host, path string) (*GcsFileReaderImpl, error) {
	fmt.Printf("uri: %v, host: %v, path: %v\n", uri, host, path)
	clientOptions := clients.FetchStorageClientOptions()
	storageClient, err := GoogleStorageNewClient(ctx, clientOptions...)
	if err != nil {
		return nil, err
	}
	err = validateObjectExists(ctx, storageClient, host, path[1:])
	if err != nil {
		storageClient.Close()
		return nil, err
	}
	return &GcsFileReaderImpl{
		uri:           uri,
		bucket:        host,
		gcsFilePath:   path[1:], // removes "/" from beginning of path
		storageClient: storageClient,
	}, nil
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
		logger.Log.Error(fmt.Sprintf("readFile: unable to open fileHandle from bucket %q, fileHandle %q: %v", reader.bucket, reader.gcsFilePath, err))
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

func (reader *GcsFileReaderImpl) ReadAll(ctx context.Context) ([]byte, error) {
	if reader.storageReader == nil {
		_, err := reader.CreateReader(ctx)
		if err != nil {
			return nil, err
		}
	}
	return io.ReadAll(reader.storageReader)
}

func validateObjectExists(ctx context.Context, client *storage.Client, bucket, object string) error {
	_, err := client.Bucket(bucket).Object(object).Attrs(ctx)
	return err
}

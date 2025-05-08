package file_reader

import (
	"cloud.google.com/go/storage"
	"context"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
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

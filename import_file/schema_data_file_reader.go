package import_data

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

type SchemeDataReader interface {
	ResetReader(ctx context.Context) (io.Reader, error)
	CreateReader(ctx context.Context) (io.Reader, error)
	Close()
}

type SchemeDataReaderImpl struct {
	DumpUri       string
	isGCS         bool
	bucket        string
	gcsFilePath   string
	file          *os.File
	storageClient *storage.Client
	storageReader *storage.Reader
}

func NewSchemeDataReader(ctx context.Context, dumpUri string) (SchemeDataReader, error) {
	u, err := url.Parse(dumpUri)
	if err != nil {
		return nil, err
	}
	schemeDataReader := &SchemeDataReaderImpl{
		DumpUri: dumpUri,
	}
	if u.Scheme == constants.GCS_SCHEME {
		schemeDataReader.bucket = u.Host
		schemeDataReader.gcsFilePath = u.Path[1:] // removes "/" from beginning of path
		schemeDataReader.storageClient, err = GoogleStorageNewClient(ctx)
		if err != nil {
			return nil, err
		}
		schemeDataReader.isGCS = true
	} else {
		schemeDataReader.file, err = os.Open(dumpUri)
		if err != nil {
			return nil, err
		}
		schemeDataReader.isGCS = false
	}
	return schemeDataReader, nil
}

func (reader *SchemeDataReaderImpl) CreateReader(ctx context.Context) (io.Reader, error) {
	if reader.isGCS {
		return reader.createGCSReader(ctx)
	}
	return reader.createFileReader()
}

func (reader *SchemeDataReaderImpl) ResetReader(ctx context.Context) (io.Reader, error) {
	if reader.isGCS {
		return reader.resetGcsReader(ctx)
	}
	return reader.resetFileReader()
}

func (reader *SchemeDataReaderImpl) resetGcsReader(ctx context.Context) (io.Reader, error) {

	if reader.storageReader != nil {
		reader.storageReader.Close()
	}

	return reader.createGCSReader(ctx)
}

func (reader *SchemeDataReaderImpl) resetFileReader() (io.Reader, error) {
	if reader.file != nil {
		_, err := reader.file.Seek(0, 0)
		if err == nil {
			return reader.file, nil
		}
		reader.file.Close()
	}
	return reader.createFileReader()

}

func (reader *SchemeDataReaderImpl) createFileReader() (io.Reader, error) {
	f, err := os.Open(reader.DumpUri)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("readFile: unable to open file: %s. Error: %q", reader.DumpUri, err))
		return nil, err
	}
	reader.file = f
	return f, nil
}

func (reader *SchemeDataReaderImpl) createGCSReader(ctx context.Context) (*storage.Reader, error) {

	rc, err := reader.storageClient.Bucket(reader.bucket).Object(reader.gcsFilePath).NewReader(ctx)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("readFile: unable to open file from bucket %q, file %q: %v", reader.bucket, reader.gcsFilePath, err))
		return nil, err
	}
	reader.storageReader = rc
	return rc, nil
}

func (reader *SchemeDataReaderImpl) Close() {
	if reader.storageReader != nil {
		reader.storageReader.Close()
	}
	if reader.storageClient != nil {
		reader.storageClient.Close()
	}
	if reader.file != nil {
		reader.file.Close()
	}
}

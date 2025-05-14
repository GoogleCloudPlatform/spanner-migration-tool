package file_reader

import (
	"context"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"io"
	"net/url"
)

var NewFileReader = newFileReader

type FileReader interface {
	// ResetReader reset the reader to the beginning of the fileHandle. If seek is not possible,
	// then the fileHandle is closed and opened again.
	ResetReader(ctx context.Context) (io.Reader, error)
	// CreateReader Create an io.reader for the fileHandle.
	CreateReader(ctx context.Context) (io.Reader, error)
	// ReadAll return all the bytes in the file.
	ReadAll(ctx context.Context) ([]byte, error)
	Close()
}

func newFileReader(ctx context.Context, uri string) (FileReader, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}

	if u.Scheme == constants.GCS_SCHEME {
		return NewGcsFileReader(ctx, uri, u.Host, u.Path)
	} else {
		return NewLocalFileReader(uri)
	}
}

type MockFileReader struct {
	ReadAllFn      func(ctx context.Context) ([]byte, error)
	CloseFn        func()
	CreateReaderFn func(ctx context.Context) (io.Reader, error)
	ResetReaderFn  func(ctx context.Context) (io.Reader, error)
}

func (reader *MockFileReader) ReadAll(ctx context.Context) ([]byte, error) {
	if reader.ReadAllFn != nil {
		return reader.ReadAllFn(ctx)
	}
	return nil, nil
}

func (reader *MockFileReader) Close() {
	if reader.CloseFn != nil {
		reader.CloseFn()
	}
}

func (reader *MockFileReader) CreateReader(ctx context.Context) (io.Reader, error) {
	if reader.CreateReaderFn != nil {
		return reader.CreateReaderFn(ctx)
	}
	return nil, nil

}

func (reader *MockFileReader) ResetReader(ctx context.Context) (io.Reader, error) {
	if reader.ResetReaderFn != nil {
		return reader.ResetReaderFn(ctx)
	}
	return nil, nil
}

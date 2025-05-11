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

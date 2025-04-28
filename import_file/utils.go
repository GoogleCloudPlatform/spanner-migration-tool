package import_file

import (
	"cloud.google.com/go/storage"
	"context"
	"fmt"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"os"
)

func ResetReaderFile(f *os.File, fileUri string) (*os.File, error) {
	_, err := f.Seek(0, 0)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("can't reset reader: %v\n", err))
		f.Close()
		return os.Open(fileUri)
	}
	return f, err

}

func CreateDumpFile(fileUri string) (*os.File, error) {
	f, err := os.Open(fileUri)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("readFile: unable to open file: %s. Error: %q", fileUri, err))
		return nil, err
	}
	return f, nil
}

func ResetGCSReader(ctx context.Context, client *storage.Client, bucketName, filePath string) (*storage.Reader, error) {
	return client.Bucket(bucketName).Object(filePath[1:]).NewReader(ctx)
}

func CreateGCSReader(ctx context.Context, bucketName, filePath string) (*storage.Reader, *storage.Client, error) {

	client, err := storage.NewClient(ctx)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("Failed to create GCS client for bucket %q", err))
		return nil, nil, err
	}

	rc, err := client.Bucket(bucketName).Object(filePath[1:]).NewReader(ctx)
	if err != nil {
		defer client.Close()
		logger.Log.Error(fmt.Sprintf("readFile: unable to open file from bucket %q, file %q: %v", bucketName, filePath, err))
		return nil, nil, err
	}
	return rc, client, nil
}

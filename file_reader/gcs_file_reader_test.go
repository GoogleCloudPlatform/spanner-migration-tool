package file_reader

import (
	"cloud.google.com/go/httpreplay"
	"cloud.google.com/go/storage"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"google.golang.org/api/option"
	"log"
	"os"
	"strings"
	"testing"
	"time"
)

var (
	replayFilename = "../test_data/gcs_unit_test.replay"
	// Any changes in `GcsFileReaderImpl` will require new recording.
	// In the recorded file, ensure that `X-Goog-User-Project` is not present.
	record = flag.Bool("record", false, "If true, rpc interaction with GCS will be recorded.")

	bucketName   = flag.String("bucketName", "smt-test-ut", "SMT test bucket name.")
	fileName     = flag.String("fileName", "smt-ut-file.sql", "SMT test file name.")
	testFileName = flag.String("testFileName", "/test_file.txt", "SMT test file name.")

	newTestClient func(ctx context.Context, opts ...option.ClientOption) (*storage.Client, error)
)

func initTestSetup() func() error {
	if *record {
		now := time.Now().UTC()
		nowBytes, err := json.Marshal(now)
		if err != nil {
			log.Fatal(err)
		}
		recorder, err := httpreplay.NewRecorder(replayFilename, nowBytes)
		if err != nil {
			log.Fatalf("could not record: %v", err)
		}
		newTestClient = func(ctx context.Context, opts ...option.ClientOption) (*storage.Client, error) {
			fmt.Printf("ctx: %v", ctx)
			hc, err := recorder.Client(ctx)
			if err != nil {
				return nil, err
			}
			return storage.NewClient(ctx, option.WithHTTPClient(hc), option.WithoutAuthentication())
		}
		return func() error {
			err2 := recorder.Close()
			return err2
		}
	} else {
		httpreplay.DebugHeaders()
		replayer, err := httpreplay.NewReplayer(replayFilename)
		if err != nil {
			log.Fatal(err)
		}
		var t time.Time
		if err := json.Unmarshal(replayer.Initial(), &t); err != nil {
			log.Fatal(err)
		}
		newTestClient = func(ctx context.Context, _ ...option.ClientOption) (*storage.Client, error) {
			hc, err := replayer.Client(ctx) // no creds needed
			if err != nil {
				return nil, err
			}
			return storage.NewClient(ctx, option.WithHTTPClient(hc), option.WithoutAuthentication())
		}
		return func() error {
			err2 := replayer.Close()
			return err2
		}
	}
}

func TestMain(m *testing.M) {
	logger.Log = zap.NewNop()
	cleanup := initTestSetup()
	defer cleanup()
	exit := m.Run()
	if err := cleanup(); err != nil {
		// Don't fail the test if cleanup fails.
		log.Printf("Post-test cleanup failed: %v", err)
	}
	os.Exit(exit)
}

func TestNewGCSFileReader(t *testing.T) {
	tests := []struct {
		name           string
		dumpUri        string
		expectedBucket string
		expectedPath   string
		wantErr        bool
		expectedError  string
	}{
		{
			name:           "GCS file",
			dumpUri:        fmt.Sprintf("gs://%s/%s", *bucketName, *fileName),
			expectedBucket: *bucketName,
			expectedPath:   *fileName,
			wantErr:        false,
		},
		{
			name:          "GCS client creation error",
			dumpUri:       "gs://test-bucket/file.sql",
			wantErr:       true,
			expectedError: "does not have storage.objects.get access to the Google Cloud Storage object",
		},
	}
	originalGoogleStorageNewClient := GoogleStorageNewClient

	defer func() { GoogleStorageNewClient = originalGoogleStorageNewClient }()
	GoogleStorageNewClient = newTestClient

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			reader, err := NewFileReader(context.Background(), tt.dumpUri)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.expectedError != "" {
					assert.Contains(t, err.Error(), tt.expectedError)
				}
				assert.Nil(t, reader)
				return
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, reader)
				assert.IsType(t, &GcsFileReaderImpl{}, reader)
				impl, ok := reader.(*GcsFileReaderImpl)
				assert.True(t, ok)

				assert.NotNil(t, impl.storageClient)
				assert.Equal(t, tt.expectedBucket, impl.bucket)
				assert.Equal(t, tt.expectedPath, impl.gcsFilePath)
				impl.Close() // Ensure resources are cleaned up
			}
		})
	}
}

func TestGCSFileReaderImpl_CreateReader(t *testing.T) {
	tests := []struct {
		name    string
		dumpUri string
		path    string
		wantErr bool
	}{
		{
			name:    "GCS file",
			dumpUri: fmt.Sprintf("gs://%s/%s", *bucketName, *fileName),
			path:    *fileName,
			wantErr: false,
		},
		{
			name:    "GCS file error",
			dumpUri: fmt.Sprintf("gs://%s/nonexistent_file.sql", *bucketName),
			path:    "nonexistent_file.sql",
			wantErr: true,
		},
	}
	originalGoogleStorageNewClient := GoogleStorageNewClient
	defer func() { GoogleStorageNewClient = originalGoogleStorageNewClient }()
	GoogleStorageNewClient = newTestClient

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			storageClient, err := GoogleStorageNewClient(context.Background())
			if err != nil {
				t.Fatalf("Failed to create FileReader: %v", err)
			}

			reader := &GcsFileReaderImpl{
				uri:           tt.dumpUri,
				bucket:        *bucketName,
				gcsFilePath:   tt.path,
				storageClient: storageClient,
			}

			defer reader.Close()

			r, err := reader.CreateReader(context.Background())
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, r)
				assert.IsType(t, &storage.Reader{}, r)
			}
		})
	}
}

func TestGCSFileReaderImpl_ResetReader(t *testing.T) {
	validUri := fmt.Sprintf("gs://%s/%s", *bucketName, *fileName)
	invalidUri := "gs://test-bucket/nonexistent_file.sql"
	tests := []struct {
		name      string
		wantErr   bool
		nullCheck bool
	}{
		{
			name:      "GCS file",
			wantErr:   false,
			nullCheck: false,
		},
		{
			name:      "GCS file Nil",
			wantErr:   false,
			nullCheck: true,
		},
		{
			name:      "GCS file error",
			wantErr:   true,
			nullCheck: false,
		},
		{
			name:      "GCS file error nil",
			wantErr:   true,
			nullCheck: true,
		},
	}
	originalGoogleStorageNewClient := GoogleStorageNewClient
	defer func() { GoogleStorageNewClient = originalGoogleStorageNewClient }()

	GoogleStorageNewClient = newTestClient
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var reader FileReader
			var err error

			reader, err = NewFileReader(context.Background(), validUri)
			if err != nil {
				t.Fatalf("Failed to create FileReader: %v", err)
			}
			defer reader.Close()
			if tt.wantErr {
				reader.(*GcsFileReaderImpl).uri = invalidUri
				reader.(*GcsFileReaderImpl).bucket = "bucketName"
				reader.(*GcsFileReaderImpl).gcsFilePath = "filePath"
			}
			if tt.nullCheck {
				reader.(*GcsFileReaderImpl).storageReader = nil
			}

			r, err := reader.ResetReader(context.Background())
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, r)
				assert.IsType(t, &storage.Reader{}, r)
			}
		})
	}
}

func TestGCSFileReaderImpl_Close(t *testing.T) {
	originalGoogleStorageNewClient := GoogleStorageNewClient
	defer func() { GoogleStorageNewClient = originalGoogleStorageNewClient }()

	t.Run("GCS File", func(t *testing.T) {
		GoogleStorageNewClient = newTestClient
		validURI := fmt.Sprintf("gs://%s/%s", *bucketName, *fileName)

		reader, err := NewFileReader(context.Background(), validURI)
		if err != nil {
			t.Fatalf("Failed to create FileReader: %v", err)
		}

		impl, ok := reader.(*GcsFileReaderImpl)
		assert.True(t, ok)
		assert.NotNil(t, impl.storageClient)

		reader.Close()
	})
}

func TestGcsFileReaderImpl_ReadAll(t *testing.T) {
	tests := []struct {
		name     string
		content  string
		uri      string
		host     string
		path     string
		expected []byte
		wantErr  bool
	}{
		{
			name:     "read all content",
			content:  "This is a test file content.",
			uri:      fmt.Sprintf("gs://%s%s", *bucketName, *testFileName),
			host:     *bucketName,
			path:     *testFileName,
			expected: []byte("This is a test file content."),
			wantErr:  false,
		},
		{
			name:     "file not found",
			uri:      fmt.Sprintf("gs://%s/nonexistent_file.txt", *bucketName),
			host:     "test-bucket",
			path:     "/nonexistent_file.txt",
			expected: nil,
			wantErr:  true,
		},
		{
			name:     "bucket not found",
			uri:      fmt.Sprintf("gs://nonexistent-bucket/%s", *testFileName),
			host:     "nonexistent-bucket",
			path:     *testFileName,
			expected: nil,
			wantErr:  true,
		},
	}

	originalGoogleStorageNewClient := GoogleStorageNewClient
	defer func() { GoogleStorageNewClient = originalGoogleStorageNewClient }()

	GoogleStorageNewClient = newTestClient
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			reader, err := NewGcsFileReader(context.Background(), tt.uri, tt.host, tt.path)
			if (err != nil) != tt.wantErr && !strings.Contains(tt.name, "or it may not exist") {
				t.Fatalf("NewGcsFileReader() error = %v, wantErr %v", err, tt.wantErr)
			}
			if reader == nil && !tt.wantErr {
				t.Fatalf("Reader is nil when no error was expected")
			}
			if reader != nil {
				defer reader.Close()
			}
			if strings.Contains(tt.name, "file not found") || strings.Contains(tt.name, "bucket not found") {
				assert.Error(t, err)
				return
			}

			actual, err := reader.ReadAll(context.Background())
			if (err != nil) != tt.wantErr {
				t.Errorf("GcsFileReaderImpl.ReadAll() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr && !assert.Equal(t, tt.expected, actual) {
				t.Errorf("GcsFileReaderImpl.ReadAll() = %v, expected %v", actual, tt.expected)
			}
		})
	}
}

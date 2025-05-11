package file_reader

import (
	"context"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestFileReaderImpl_CreateReaderFile(t *testing.T) {
	tests := []struct {
		name    string
		uri     string
		wantErr bool
	}{
		{
			name:    "Local file",
			uri:     "test_file.sql",
			wantErr: false,
		},
		{
			name:    "Local file error",
			uri:     "nonexistent_file.sql",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var err error

			if !tt.wantErr {
				// Create a dummy file for testing local file reading.
				tmpFile, err := os.CreateTemp("", "test_file_*.sql")
				if err != nil {
					t.Fatalf("Failed to create temp file: %v", err)
				}
				defer os.Remove(tmpFile.Name())
				tt.uri = tmpFile.Name()
			}

			reader := &LocalFileReaderImpl{
				uri: tt.uri,
			}
			if err != nil {
				t.Fatalf("Failed to create FileReader: %v", err)
			}
			defer reader.Close()

			r, err := reader.CreateReader(context.Background())
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, r)
				assert.IsType(t, &os.File{}, r)
			}
		})
	}
}

func TestFileReaderImpl_ResetReaderFileSuccess(t *testing.T) {
	tests := []struct {
		name    string
		dumpUri string
		isGCS   bool
		seek    bool
	}{
		{
			name:    "Local file",
			dumpUri: "test_file.sql",
			seek:    false,
		},
		{
			name:    "Local file error",
			dumpUri: "test_file.sql",
			seek:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a dummy file for testing local file reading.
			tmpFile, err := os.CreateTemp("", "test_file_*.sql")
			if err != nil {
				t.Fatalf("Failed to create temp file: %v", err)
			}
			defer os.Remove(tmpFile.Name())
			reader, err := NewFileReader(context.Background(), tmpFile.Name())
			if err != nil {
				t.Fatalf("Failed to create FileReader: %v", err)
			}
			defer reader.Close()
			if tt.seek {
				reader.(*LocalFileReaderImpl).fileHandle.Close()
			}

			r, err := reader.ResetReader(context.Background())
			assert.NoError(t, err)
			assert.NotNil(t, r)
			assert.IsType(t, &os.File{}, r)
		})
	}
}

func TestFileReaderImpl_ResetReaderFileError(t *testing.T) {
	tests := []struct {
		name    string
		dumpUri string
		isGCS   bool
		seek    bool
	}{
		{
			name:    "Local file",
			dumpUri: "test_nonexistent_file.sql",
			seek:    false,
		},
		{
			name:    "Local file error",
			dumpUri: "test_nonexistent_file.sql",
			seek:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a dummy file for testing local file reading.
			tmpFile, err := os.CreateTemp("", "test_file_*.sql")
			if err != nil {
				t.Fatalf("Failed to create temp file: %v", err)
			}
			defer os.Remove(tmpFile.Name())
			reader, err := NewFileReader(context.Background(), tmpFile.Name())
			if err != nil {
				t.Fatalf("Failed to create FileReader: %v", err)
			}
			defer reader.Close()
			reader.(*LocalFileReaderImpl).uri = tt.dumpUri
			if tt.seek {
				reader.(*LocalFileReaderImpl).fileHandle.Close()
			} else {
				reader.(*LocalFileReaderImpl).fileHandle = nil
			}

			_, err = reader.ResetReader(context.Background())
			assert.Error(t, err)
		})
	}
}

func TestFileReaderImpl_Close(t *testing.T) {
	originalGoogleStorageNewClient := GoogleStorageNewClient
	defer func() { GoogleStorageNewClient = originalGoogleStorageNewClient }()

	t.Run("Local File", func(t *testing.T) {
		tmpFile, err := os.CreateTemp("", "test_file_*.sql")
		if err != nil {
			t.Fatalf("Failed to create temp file: %v", err)
		}
		defer os.Remove(tmpFile.Name())

		reader, err := NewFileReader(context.Background(), tmpFile.Name())
		if err != nil {
			t.Fatalf("Failed to create FileReader: %v", err)
		}

		impl, ok := reader.(*LocalFileReaderImpl)
		assert.True(t, ok)
		assert.Nil(t, impl.fileHandle)

		reader.Close()
	})
}

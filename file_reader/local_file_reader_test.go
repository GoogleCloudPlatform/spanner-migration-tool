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

	t.Run("local file seek failed", func(t *testing.T) {
		tmpFile, err := os.CreateTemp("", "test_file_*.sql")
		if err != nil {
			t.Fatalf("Failed to create temp file: %v", err)
		}
		defer os.Remove(tmpFile.Name())
		reader, err := NewFileReader(context.Background(), tmpFile.Name())
		reader.(*LocalFileReaderImpl).fileHandle.Close()

		_, err = reader.ResetReader(context.Background())
		assert.NoError(t, err)
	})
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

func TestLocalFileReaderImpl_ReadAll(t *testing.T) {
	tests := []struct {
		name     string
		content  string
		uri      string
		expected []byte
		wantErr  bool
	}{
		{
			name:     "read all content",
			content:  "This is a test file content.",
			expected: []byte("This is a test file content."),
			wantErr:  false,
		},
		{
			name:     "empty file",
			content:  "",
			expected: []byte(""),
			wantErr:  false,
		},
		{
			name:     "file not found",
			uri:      "nonexistent_file.txt",
			expected: nil,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var reader *LocalFileReaderImpl
			var err error

			if tt.uri == "" {
				// Create a temporary file with content for testing.
				tmpFile, err := os.CreateTemp("", "test_file_*.txt")
				if err != nil {
					t.Fatalf("Failed to create temp file: %v", err)
				}
				defer os.Remove(tmpFile.Name())

				if _, err := tmpFile.WriteString(tt.content); err != nil {
					t.Fatalf("Failed to write to temp file: %v", err)
				}

				if _, err := tmpFile.Seek(0, 0); err != nil {
					t.Fatalf("Failed to seek to beginning of temp file: %v", err)
				}

				reader, err = NewLocalFileReader(tmpFile.Name())
				if err != nil {
					t.Fatalf("Failed to create FileReader: %v", err)
				}
			} else {
				reader = &LocalFileReaderImpl{uri: tt.uri}
			}

			defer func() {
				if reader != nil {
					reader.Close()
				}
			}()

			actual, err := reader.ReadAll(context.Background())
			if (err != nil) != tt.wantErr {
				t.Errorf("LocalFileReaderImpl.ReadAll() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr && !assert.Equal(t, tt.expected, actual) {
				t.Errorf("LocalFileReaderImpl.ReadAll() = %v, expected %v", actual, tt.expected)
			}
		})
	}
}

package file_reader

import (
	"context"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestFileReaderFile(t *testing.T) {
	tests := []struct {
		name          string
		dumpUri       string
		wantErr       bool
		expectedError string
	}{
		{
			name:    "Local file",
			dumpUri: "local_file.sql",
			wantErr: false,
		},
		{
			name:          "Invalid URI",
			dumpUri:       "://invalid-uri",
			wantErr:       true,
			expectedError: "missing protocol scheme",
		},
		{
			name:          "Local file open error",
			dumpUri:       "nonexistent_file.sql",
			wantErr:       true,
			expectedError: "open nonexistent_file.sql: no such file or directory",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !tt.wantErr {
				// Create a dummy file for testing local file reading.
				tmpFile, _ := os.CreateTemp("", "local_file.sql")
				tt.dumpUri = tmpFile.Name()
				defer tmpFile.Close()
			}

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
				assert.IsType(t, &LocalFileReaderImpl{}, reader)
				impl, ok := reader.(*LocalFileReaderImpl)
				assert.True(t, ok)

				assert.NotNil(t, impl.fileHandle)
				impl.fileHandle.Close()
				impl.Close() // Ensure resources are cleaned up
			}
		})
	}
}

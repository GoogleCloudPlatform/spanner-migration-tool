package import_file

import (
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"os"
	"testing"
)

func Test_getBatchWriterWithConfig(t *testing.T) {
	spannerClient := getSpannerClientMock(getDefaultRowIteratoMock())
	conv := internal.MakeConv()
	bw := getBatchWriterWithConfig(spannerClient, conv)

	if bw == nil {
		t.Errorf("getBatchWriterWithConfig() returned nil")
	}
}

func TestResetReader(t *testing.T) {
	// Create a test file
	tmpfile, err := os.CreateTemp("", "testfile.txt")
	if err != nil {
		t.Fatalf("Failed to create temporary file: %v", err)
	}
	defer os.Remove(tmpfile.Name()) // Clean up after test

	_, err = tmpfile.WriteString("Test content")
	if err != nil {
		t.Fatalf("Failed to write to temporary file: %v", err)
	}
	tmpfile.Close()

	// Open the file
	file, err := os.Open(tmpfile.Name())
	if err != nil {
		t.Fatalf("Failed to open temporary file: %v", err)
	}
	defer file.Close()

	// Read some bytes to change the offset
	buffer := make([]byte, 4)
	_, err = file.Read(buffer)
	if err != nil {
		t.Fatalf("Failed to read from file: %v", err)
	}

	// Check if offset is changed
	if offset, _ := file.Seek(0, 1); offset != 4 {
		t.Fatalf("Expected offset to be 4, got %d", offset)
	}

	// Reset the reader
	resetFile, err := ResetReader(file, tmpfile.Name())
	if err != nil {
		t.Fatalf("Failed to reset reader: %v", err)
	}
	defer resetFile.Close()

	// Check if the offset is reset to 0
	if offset, _ := resetFile.Seek(0, 1); offset != 0 {
		t.Fatalf("Expected offset to be 0 after reset, got %d", offset)
	}

}

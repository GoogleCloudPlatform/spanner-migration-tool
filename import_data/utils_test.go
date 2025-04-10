package import_data

import (
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
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

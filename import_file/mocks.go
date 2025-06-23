package import_file

import (
	"context"
	spanneraccessor "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/spanner"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/common"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/csv"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/spanner"
)

// MockCsvSchema for testing.
type MockCsvSchema struct {
	CreateSchemaFn func(ctx context.Context, dialect string, sp spanneraccessor.SpannerAccessor) error
}

func (m *MockCsvSchema) CreateSchema(ctx context.Context, dialect string, sp spanneraccessor.SpannerAccessor) error {
	if m.CreateSchemaFn != nil {
		return m.CreateSchemaFn(ctx, dialect, sp)
	}
	return nil
}

// MockCsvData for testing.
type MockCsvData struct {
	ImportDataFn func(ctx context.Context, spannerInfoSchema *spanner.InfoSchemaImpl, dialect string, conv *internal.Conv, commonInfoSchema common.InfoSchemaInterface, csv csv.CsvInterface) error
}

func (m *MockCsvData) ImportData(ctx context.Context, spannerInfoSchema *spanner.InfoSchemaImpl, dialect string, conv *internal.Conv, commonInfoSchema common.InfoSchemaInterface, csv csv.CsvInterface) error {
	if m.ImportDataFn != nil {
		return m.ImportDataFn(ctx, spannerInfoSchema, dialect, conv, commonInfoSchema, csv)
	}
	return nil
}

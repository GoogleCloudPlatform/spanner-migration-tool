package import_data

import (
	"context"
	"testing"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/proto/migration"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/spanner"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
)

func TestCsvDataImpl_ImportData(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name       string
		source     CsvDataImpl
		infoSchema *spanner.InfoSchemaImpl
		dialect    string
		wantErr    bool
	}{
		{
			name: "table not found error",
			source: CsvDataImpl{
				ProjectId:         "test-project",
				InstanceId:        "test-instance",
				DbName:            "test-db",
				TableName:         "nonexistent-table",
				SourceUri:         "test-uri",
				CsvFieldDelimiter: ",",
			},
			infoSchema: &spanner.InfoSchemaImpl{
				SpannerClient: getSpannerClientMock(getDefaultRowIteratoMock()),
			},
			dialect: "googleSQL",
			wantErr: true,
		},
		// add more cases here
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spannerClient := getSpannerClientMock(getDefaultRowIteratoMock())
			tt.infoSchema.SpannerClient = spannerClient

			conv := getConvObject(tt.source.ProjectId, tt.source.InstanceId, tt.dialect)
			{
			}
			conv.SpSchema = map[string]ddl.CreateTable{
				"test-table": {
					ColIds: []string{"col1", "col2"},
					ColDefs: map[string]ddl.ColumnDef{
						"col1": {Name: "col1"},
						"col2": {Name: "col2"},
					},
				},
			}
			if tt.source.TableName == "nonexistent-table" {
				conv.SpSchema = map[string]ddl.CreateTable{}
			}

			if err := tt.source.ImportData(ctx, tt.infoSchema, tt.dialect); (err != nil) != tt.wantErr {
				t.Errorf("CsvDataImpl.ImportData() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_getConvObject(t *testing.T) {
	project := "test-project"
	instance := "test-instance"
	dialect := "googleSQL"
	conv := getConvObject(project, instance, dialect)

	if conv.Audit.MigrationType.Enum().String() != migration.MigrationData_DATA_ONLY.Enum().String() {
		t.Errorf("getConvObject() MigrationType = %v, want %v", conv.Audit.MigrationType, migration.MigrationData_DATA_ONLY.Enum())
	}
	if conv.Audit.SkipMetricsPopulation != true {
		t.Errorf("getConvObject() SkipMetricsPopulation = %v, want %v", conv.Audit.SkipMetricsPopulation, true)
	}
	if conv.Audit.DryRun != false {
		t.Errorf("getConvObject() DryRun = %v, want %v", conv.Audit.DryRun, false)
	}
	if conv.SpDialect != dialect {
		t.Errorf("getConvObject() SpDialect = %v, want %v", conv.SpDialect, dialect)
	}
	if conv.SpProjectId != project {
		t.Errorf("getConvObject() SpProjectId = %v, want %v", conv.SpProjectId, project)
	}
	if conv.SpInstanceId != instance {
		t.Errorf("getConvObject() SpInstanceId = %v, want %v", conv.SpInstanceId, instance)
	}
}

func Test_getBatchWriterWithConfig(t *testing.T) {
	spannerClient := getSpannerClientMock(getDefaultRowIteratoMock())
	conv := internal.MakeConv()
	bw := getBatchWriterWithConfig(spannerClient, conv)

	if bw == nil {
		t.Errorf("getBatchWriterWithConfig() returned nil")
	}
}

package import_data

import (
	"context"
	"errors"
	"testing"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/utils"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/profiles"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/proto/migration"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/schema"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/common"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/csv"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/spanner"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
)

func TestCsvDataImpl_ImportData(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name              string
		source            CsvDataImpl
		spannerInfoSchema *spanner.InfoSchemaImpl
		commonInfoSchema  common.InfoSchemaInterface
		csvHandler        csv.CsvInterface
		dialect           string
		wantErr           bool
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
			spannerInfoSchema: &spanner.InfoSchemaImpl{
				SpannerClient: getSpannerClientMock(getDefaultRowIteratoMock()),
			},
			commonInfoSchema: getCommonInfoSchemaMock(0),
			csvHandler:       getCsvInterfaceMock(nil),
			dialect:          "googleSQL",
			wantErr:          true,
		},
		{
			name: "csv processing error",
			source: CsvDataImpl{
				ProjectId:         "test-project",
				InstanceId:        "test-instance",
				DbName:            "test-db",
				TableName:         "test-table",
				SourceUri:         "test-uri",
				CsvFieldDelimiter: ",",
			},
			spannerInfoSchema: &spanner.InfoSchemaImpl{
				SpannerClient: getSpannerClientMock(getDefaultRowIteratoMock()),
			},
			commonInfoSchema: getCommonInfoSchemaMock(1),
			csvHandler:       getCsvInterfaceMock(errors.New("test error")),
			dialect:          "googleSQL",
			wantErr:          true,
		},
		{
			name: "success case",
			source: CsvDataImpl{
				ProjectId:         "test-project",
				InstanceId:        "test-instance",
				DbName:            "test-db",
				TableName:         "test-table",
				SourceUri:         "test-uri",
				CsvFieldDelimiter: ",",
			},
			spannerInfoSchema: &spanner.InfoSchemaImpl{
				SpannerClient: getSpannerClientMock(getDefaultRowIteratoMock()),
			},
			commonInfoSchema: getCommonInfoSchemaMock(1),
			csvHandler:       getCsvInterfaceMock(nil),
			dialect:          "googleSQL",
			wantErr:          false,
		},
		// add more cases here
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spannerClient := getSpannerClientMock(getDefaultRowIteratoMock())
			tt.spannerInfoSchema.SpannerClient = spannerClient

			conv := internal.MakeConv()
			conv.SpSchema = map[string]ddl.CreateTable{
				"test-table": {
					ColIds: []string{"col1", "col2"},
					ColDefs: map[string]ddl.ColumnDef{
						"col1": {Name: "col1"},
						"col2": {Name: "col2"},
					},
					Name: "test-table",
				},
			}
			if tt.source.TableName == "nonexistent-table" {
				conv.SpSchema = map[string]ddl.CreateTable{}
			}

			if err := tt.source.ImportData(ctx, tt.spannerInfoSchema, tt.dialect, conv, tt.commonInfoSchema, tt.csvHandler); (err != nil) != tt.wantErr {
				t.Errorf("CsvDataImpl.ImportData() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func getCsvInterfaceMock(singleProcessError error) *MockCsvInterface {
	return &MockCsvInterface{
		MockProcessSingleCSV: func(conv *internal.Conv, tableName string, columnNames []string, colDefs map[string]ddl.ColumnDef, filePath string, nullStr string, delimiter rune) error {
			return singleProcessError
		},
	}
}

func getCommonInfoSchemaMock(tableCount int) *MockInfoSchemaInterface {
	return &MockInfoSchemaInterface{
		MockGenerateSrcSchema: func(conv *internal.Conv, infoSchema common.InfoSchema, numWorkers int) (int, error) {
			return tableCount, nil
		},
		MockProcessData: func(conv *internal.Conv, infoSchema common.InfoSchema, additionalAttributes internal.AdditionalDataAttributes) {
		},
		MockSetRowStats: func(conv *internal.Conv, infoSchema common.InfoSchema) {},
		MockProcessTable: func(conv *internal.Conv, table common.SchemaAndName, infoSchema common.InfoSchema) (schema.Table, error) {
			return schema.Table{}, nil
		},
		MockGetIncludedSrcTablesFromConv: func(conv *internal.Conv) (schemaToTablesMap map[string]internal.SchemaDetails, err error) {
			return map[string]internal.SchemaDetails{}, nil
		},
	}
}

type MockInfoSchemaInterface struct {
	MockGenerateSrcSchema            func(conv *internal.Conv, infoSchema common.InfoSchema, numWorkers int) (int, error)
	MockProcessData                  func(conv *internal.Conv, infoSchema common.InfoSchema, additionalAttributes internal.AdditionalDataAttributes)
	MockSetRowStats                  func(conv *internal.Conv, infoSchema common.InfoSchema)
	MockProcessTable                 func(conv *internal.Conv, table common.SchemaAndName, infoSchema common.InfoSchema) (schema.Table, error)
	MockGetIncludedSrcTablesFromConv func(conv *internal.Conv) (schemaToTablesMap map[string]internal.SchemaDetails, err error)
}

type MockCsvInterface struct {
	MockGetCSVFiles      func(conv *internal.Conv, sourceProfile profiles.SourceProfile) (tables []utils.ManifestTable, err error)
	MockSetRowStats      func(conv *internal.Conv, tables []utils.ManifestTable, delimiter rune) error
	MockProcessCSV       func(conv *internal.Conv, tables []utils.ManifestTable, nullStr string, delimiter rune) error
	MockProcessSingleCSV func(conv *internal.Conv, tableName string, columnNames []string, colDefs map[string]ddl.ColumnDef, filePath string, nullStr string, delimiter rune) error
}

func (m MockCsvInterface) GetCSVFiles(conv *internal.Conv, sourceProfile profiles.SourceProfile) (tables []utils.ManifestTable, err error) {
	return m.MockGetCSVFiles(conv, sourceProfile)
}

func (m MockCsvInterface) SetRowStats(conv *internal.Conv, tables []utils.ManifestTable, delimiter rune) error {
	return m.MockSetRowStats(conv, tables, delimiter)
}

func (m MockCsvInterface) ProcessCSV(conv *internal.Conv, tables []utils.ManifestTable, nullStr string, delimiter rune) error {
	return m.MockProcessCSV(conv, tables, nullStr, delimiter)
}

func (m MockCsvInterface) ProcessSingleCSV(conv *internal.Conv, tableName string, columnNames []string, colDefs map[string]ddl.ColumnDef, filePath string, nullStr string, delimiter rune) error {
	return m.MockProcessSingleCSV(conv, tableName, columnNames, colDefs, filePath, nullStr, delimiter)
}

func (m *MockInfoSchemaInterface) ProcessData(conv *internal.Conv, infoSchema common.InfoSchema, additionalAttributes internal.AdditionalDataAttributes) {
}

func (m *MockInfoSchemaInterface) SetRowStats(conv *internal.Conv, infoSchema common.InfoSchema) {
}

func (m *MockInfoSchemaInterface) ProcessTable(conv *internal.Conv, table common.SchemaAndName, infoSchema common.InfoSchema) (schema.Table, error) {
	return m.MockProcessTable(conv, table, infoSchema)
}

func (m *MockInfoSchemaInterface) GetIncludedSrcTablesFromConv(conv *internal.Conv) (schemaToTablesMap map[string]internal.SchemaDetails, err error) {
	return m.MockGetIncludedSrcTablesFromConv(conv)
}

func (m *MockInfoSchemaInterface) GenerateSrcSchema(conv *internal.Conv, infoSchema common.InfoSchema, numWorkers int) (int, error) {
	return m.MockGenerateSrcSchema(conv, infoSchema, numWorkers)
}

func Test_getConvObject(t *testing.T) {
	project := "test-project"
	instance := "test-instance"
	dialect := "googleSQL"
	conv := getConvObject(project, instance, dialect, internal.MakeConv())

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

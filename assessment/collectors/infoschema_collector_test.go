package assessment

import (
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	sources "github.com/GoogleCloudPlatform/spanner-migration-tool/assessment/sources"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/assessment/sources/mysql"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/assessment/utils"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/profiles"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/schema"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"
)

func init() {
	logger.Log = zap.NewNop()
}

func TestGetInfoSchema(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	sourceProfileMySQL := profiles.SourceProfile{
		Driver: constants.MYSQL,
		Conn: profiles.SourceProfileConnection{
			Mysql: profiles.SourceProfileConnectionMySQL{
				Db: "test_db",
			},
		},
	}
	infoSchemaMySQL, err := getInfoSchema(db, sourceProfileMySQL)
	assert.NoError(t, err)
	assert.NotNil(t, infoSchemaMySQL)
	assert.IsType(t, mysql.InfoSchemaImpl{}, infoSchemaMySQL)

	sourceProfileUnsupported := profiles.SourceProfile{
		Driver: "unsupported",
	}
	infoSchemaUnsupported, err := getInfoSchema(db, sourceProfileUnsupported)
	assert.Error(t, err)
	assert.Nil(t, infoSchemaUnsupported)
	assert.Contains(t, err.Error(), "driver unsupported not supported")
}

func TestInfoSchemaCollector_IsEmpty(t *testing.T) {
	tests := []struct {
		name      string
		collector InfoSchemaCollector
		want      bool
	}{
		{
			name:      "empty collector",
			collector: InfoSchemaCollector{},
			want:      true,
		},
		{
			name: "collector with conv",
			collector: InfoSchemaCollector{
				conv: &internal.Conv{},
			},
			want: false,
		},
		{
			name: "collector with indexes",
			collector: InfoSchemaCollector{
				indexes: []utils.IndexAssessmentInfo{{}},
			},
			want: false,
		},
		{
			name: "collector with tables",
			collector: InfoSchemaCollector{
				tables: map[string]utils.TableAssessmentInfo{"t1": {}},
			},
			want: false,
		},
		{
			name: "collector with storedProcedures",
			collector: InfoSchemaCollector{
				storedProcedures: []utils.StoredProcedureAssessmentInfo{{}},
			},
			want: false,
		},
		{
			name: "collector with triggers",
			collector: InfoSchemaCollector{
				triggers: []utils.TriggerAssessmentInfo{{}},
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.collector.IsEmpty(); got != tt.want {
				t.Errorf("IsEmpty() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestInfoSchemaCollector_ListTriggers(t *testing.T) {
	mockConv := &internal.Conv{
		SrcSchema: map[string]schema.Table{
			"table1": {
				Id:   "table1",
				Name: "table1",
			},
		},
	}
	collector := InfoSchemaCollector{
		conv: mockConv,
		triggers: []utils.TriggerAssessmentInfo{
			{Name: "trigger1", Operation: "INSERT", TargetTable: "table1"},
			{Name: "trigger2", Operation: "UPDATE", TargetTable: "table1"},
		},
	}
	triggers := collector.ListTriggers()
	assert.Len(t, triggers, 2)
	for _, trigger := range triggers {
		assert.Contains(t, []string{"trigger1", "trigger2"}, trigger.Name)
		assert.Equal(t, "table1", trigger.TargetTable)
		assert.Equal(t, "table1", trigger.TargetTableId)
	}
}

func TestInfoSchemaCollector_ListFunctions(t *testing.T) {
	mockConv := &internal.Conv{}
	collector := InfoSchemaCollector{
		conv: mockConv,
		functions: []utils.FunctionAssessmentInfo{
			{Name: "function1", Definition: "CREATE FUNCTION function1() RETURNS INT RETURN 1;"},
			{Name: "function2", Definition: "CREATE FUNCTION function2() RETURNS INT RETURN 2;"},
		},
	}
	functions := collector.ListFunctions()
	assert.Len(t, functions, 2)
	for _, function := range functions {
		assert.Contains(t, []string{"function1", "function2"}, function.Name)
		assert.Equal(t, 1, function.LinesOfCode)
	}
}

func TestInfoSchemaCollector_ListViews(t *testing.T) {
	mockConv := &internal.Conv{
		UsedNames: make(map[string]bool),
	}
	collector := InfoSchemaCollector{
		conv: mockConv,
		views: []utils.ViewAssessmentInfo{
			{Name: "view1", Definition: "SELECT * FROM table1"},
			{Name: "view2", Definition: "SELECT col1, col2 FROM table2 WHERE col3 > 10"},
		},
	}
	views := collector.ListViews()
	assert.Len(t, views, 2)
	for _, view := range views {
		assert.Contains(t, []string{"view1", "view2"}, view.SrcName)
		assert.Equal(t, "NON-MATERIALIZED", view.SrcViewType)
		assert.NotEmpty(t, view.SpName)
	}
}

func TestInfoSchemaCollector_ListStoredProcedures(t *testing.T) {
	mockConv := &internal.Conv{}
	collector := InfoSchemaCollector{
		conv: mockConv,
		storedProcedures: []utils.StoredProcedureAssessmentInfo{
			{Name: "procedure1", Definition: "CREATE PROCEDURE procedure1() BEGIN SELECT 1; END;"},
			{Name: "procedure2", Definition: "CREATE PROCEDURE procedure2() BEGIN SELECT 2; END;"},
		},
	}
	sps := collector.ListStoredProcedures()
	assert.Len(t, sps, 2)
	for _, sp := range sps {
		assert.Contains(t, []string{"procedure1", "procedure2"}, sp.Name)
		assert.Equal(t, 2, sp.LinesOfCode)
	}
}

func TestInfoSchemaCollector_ListSpannerSequences(t *testing.T) {
	mockConv := &internal.Conv{
		SpSequences: map[string]ddl.Sequence{
			"seq1": {Name: "seq1"},
			"seq2": {Name: "seq2"},
		},
	}
	collector := InfoSchemaCollector{
		conv: mockConv,
	}
	sequences := collector.ListSpannerSequences()
	assert.Len(t, sequences, 2)
	if _, ok := sequences["seq1"]; !ok {
		t.Errorf("expected sequence seq1")
	}
	if _, ok := sequences["seq2"]; !ok {
		t.Errorf("expected sequence seq2")
	}
}

func TestInfoSchemaCollector_ListColumnDefinitions(t *testing.T) {
	mockConv := &internal.Conv{
		SrcSchema: map[string]schema.Table{
			"table1": {
				Id:   "table1",
				Name: "table1",
				PrimaryKeys: []schema.Key{
					{ColId: "col1", Order: 1},
				},
				ForeignKeys: []schema.ForeignKey{
					{Name: "fk1", ColIds: []string{"col2"}},
				},
				ColDefs: map[string]schema.Column{
					"col1": {Id: "col1", Name: "column1", Type: schema.Type{Name: "INT"}, NotNull: true},
					"col2": {Id: "col2", Name: "column2", Type: schema.Type{Name: "VARCHAR", Mods: []int64{255}}},
					"col3": {Id: "col3", Name: "column3", Type: schema.Type{Name: "TIMESTAMP"}, DefaultValue: ddl.DefaultValue{IsPresent: true, Value: ddl.Expression{Statement: "CURRENT_TIMESTAMP"}}},
				},
			},
		},
		SpSchema: map[string]ddl.CreateTable{
			"table1": {
				Id:   "table1",
				Name: "table1",
				PrimaryKeys: []ddl.IndexKey{
					{ColId: "col_a", Order: 1},
				},
				ForeignKeys: []ddl.Foreignkey{
					{Name: "fk_a", ColIds: []string{"col_b"}},
				},
				ColDefs: map[string]ddl.ColumnDef{
					"col_a": {Id: "col_a", Name: "column_a", T: ddl.Type{Name: "INT"}, NotNull: true},
					"col_b": {Id: "col_b", Name: "column_b", T: ddl.Type{Name: "STRING", Len: 255}},
					"col_c": {Id: "col_c", Name: "column_c", T: ddl.Type{Name: "TIMESTAMP"}, DefaultValue: ddl.DefaultValue{Value: ddl.Expression{Statement: "CURRENT_TIMESTAMP"}}},
				},
			},
		},
	}
	collector := InfoSchemaCollector{
		conv: mockConv,
		tables: map[string]utils.TableAssessmentInfo{
			"table1": {
				ColumnAssessmentInfos: map[string]utils.ColumnAssessmentInfo[any]{
					"col1": {IsUnsigned: false},
					"col2": {IsUnsigned: true, MaxColumnSize: 255},
					"col3": {IsOnUpdateTimestampSet: true},
				},
			},
		},
	}
	srcCols, spCols := collector.ListColumnDefinitions()

	assert.Len(t, srcCols, 3)
	col1Src, ok := srcCols["col1"]
	assert.True(t, ok)
	assert.Equal(t, "column1", col1Src.Name)
	assert.Equal(t, 1, col1Src.PrimaryKeyOrder)
	assert.Empty(t, col1Src.ForeignKey)
	assert.False(t, col1Src.IsUnsigned)
	assert.False(t, col1Src.IsOnInsertTimestampSet)

	col2Src, ok := srcCols["col2"]
	assert.True(t, ok)
	assert.Equal(t, "column2", col2Src.Name)
	assert.Equal(t, -1, col2Src.PrimaryKeyOrder)
	assert.Equal(t, []string{"fk1"}, col2Src.ForeignKey)
	assert.True(t, col2Src.IsUnsigned)
	assert.Equal(t, int64(255), col2Src.MaxColumnSize)

	col3Src, ok := srcCols["col3"]
	assert.True(t, ok)
	assert.True(t, col3Src.IsOnInsertTimestampSet)

	assert.Len(t, spCols, 3)
	colASp, ok := spCols["col_a"]
	assert.True(t, ok)
	assert.Equal(t, "column_a", colASp.Name)
	assert.Equal(t, 1, colASp.PrimaryKeyOrder)
	assert.Empty(t, colASp.ForeignKey)

	colBSp, ok := spCols["col_b"]
	assert.True(t, ok)
	assert.Equal(t, "column_b", colBSp.Name)
	assert.Equal(t, -1, colBSp.PrimaryKeyOrder)
	assert.Equal(t, []string{"fk_a"}, colBSp.ForeignKey)
	assert.Equal(t, int64(255), colBSp.Len)

	colCSp, ok := spCols["col_c"]
	assert.True(t, ok)
	assert.Equal(t, "column_c", colCSp.Name)
	assert.Equal(t, "CURRENT_TIMESTAMP", colCSp.DefaultValue.Value.Statement)
}

func TestInfoSchemaCollector_ListIndexes(t *testing.T) {
	mockConv := &internal.Conv{
		SrcSchema: map[string]schema.Table{
			"table1": {
				Id:   "table1",
				Name: "table1",
			},
		},
		SpSchema: map[string]ddl.CreateTable{
			"table1": {
				Id:   "table1",
				Name: "table1",
				Indexes: []ddl.CreateIndex{
					{Id: "index1", Name: "index1"},
				},
			},
		},
		UsedNames: make(map[string]bool),
	}
	collector := InfoSchemaCollector{
		conv: mockConv,
		indexes: []utils.IndexAssessmentInfo{
			{
				IndexDef: schema.Index{
					Id:     "index1",
					Name:   "secondary_index",
					Unique: true,
					Keys: []schema.Key{
						{ColId: "col1", Order: 1},
					},
				},
				TableId: "table1",
				Ty:      "BTREE",
			},
		},
	}
	srcIndexes, spIndexes := collector.ListIndexes()

	assert.Len(t, srcIndexes, 1)
	srcIndexDetails, ok := srcIndexes["index1"]
	assert.True(t, ok)
	assert.Equal(t, "secondary_index", srcIndexDetails.Name)
	assert.Equal(t, "table1", srcIndexDetails.TableId)
	assert.Equal(t, "BTREE", srcIndexDetails.Type)
	assert.True(t, srcIndexDetails.IsUnique)

	assert.Len(t, spIndexes, 1)
	spIndexDetails, ok := spIndexes["index1"]
	assert.True(t, ok)
	assert.Equal(t, "secondary_index", spIndexDetails.Name)
	assert.Equal(t, "table1", spIndexDetails.TableId)
	assert.True(t, spIndexDetails.IsUnique)
}

func TestGetSpannerIndex(t *testing.T) {
	spSchema := ddl.CreateTable{
		Indexes: []ddl.CreateIndex{
			{Id: "index1", Name: "index_a"},
			{Id: "index2", Name: "index_b"},
		},
	}
	index := getSpannerIndex("index1", spSchema)
	assert.Equal(t, "index_a", index.Name)

	emptyIndex := getSpannerIndex("index3", spSchema)
	assert.Equal(t, ddl.CreateIndex{}, emptyIndex)
}

func TestInfoSchemaCollector_ListTables(t *testing.T) {
	mockConv := &internal.Conv{
		SrcSchema: map[string]schema.Table{
			"table1": {
				Id:   "table1",
				Name: "table1",
				CheckConstraints: []schema.CheckConstraint{
					{Id: "ck1"},
				},
				ForeignKeys: []schema.ForeignKey{
					{Id: "fk1", ReferTableId: "table2"},
				},
			},
			"table2": {
				Id:   "table2",
				Name: "table2",
				PrimaryKeys: []schema.Key{
					{ColId: "col1", Order: 1},
				},
				ColDefs: map[string]schema.Column{
					"col1": {Id: "col1", Name: "pk_col"},
				},
			},
		},
		SpSchema: map[string]ddl.CreateTable{
			"table1": {
				Name: "table1",
				Id:   "table1",
				CheckConstraints: []ddl.CheckConstraint{
					{Id: "ck1"},
				},
				ForeignKeys: []ddl.Foreignkey{
					{Id: "fk1", ReferTableId: "table2", ColIds: []string{"col_a"}, ReferColumnIds: []string{"col1"}},
				},
				PrimaryKeys: []ddl.IndexKey{{ColId: "col_a", Order: 1}},
				ColDefs: map[string]ddl.ColumnDef{
					"col_a": {Name: "pk_col"},
				},
			},
			"table2": {
				Name:        "table2",
				Id:          "table2",
				PrimaryKeys: []ddl.IndexKey{{ColId: "col1", Order: 1}},
				ColDefs: map[string]ddl.ColumnDef{
					"col1": {Name: "pk_col"},
				},
			},
		},
	}
	collector := InfoSchemaCollector{
		conv: mockConv,
		tables: map[string]utils.TableAssessmentInfo{
			"table1": {
				Charset:   "utf8",
				Collation: "utf8_general_ci",
				ColumnAssessmentInfos: map[string]utils.ColumnAssessmentInfo[any]{
					"col1": {
						ColumnDef: schema.Column{
							Id: "col1",
							DefaultValue: ddl.DefaultValue{
								IsPresent: true,
								Value: ddl.Expression{
									Statement: "CURRENT_TIMESTAMP",
								},
							},
						},
						IsUnsigned: false,
					},
				},
			},
		},
	}
	srcTables, spTables := collector.ListTables()

	assert.Len(t, srcTables, 1)
	srcTableDetails, ok := srcTables["table1"]
	assert.True(t, ok)
	assert.Equal(t, "table1", srcTableDetails.Name)
	assert.Equal(t, "utf8", srcTableDetails.Charset)
	assert.Equal(t, "utf8_general_ci", srcTableDetails.Collation)
	assert.Len(t, srcTableDetails.CheckConstraints, 1)
	assert.Len(t, srcTableDetails.SourceForeignKey, 1)

	assert.Len(t, spTables, 1)
	spTableDetails, ok := spTables["table1"]
	assert.True(t, ok)
	assert.Equal(t, "table1", spTableDetails.Name)
	assert.Len(t, spTableDetails.CheckConstraints, 1)
	assert.Len(t, spTableDetails.SpannerForeignKey, 1)
}

type MockInfoSchema struct {
	mock.Mock
}

func (m *MockInfoSchema) GetTableInfo(conv *internal.Conv) (map[string]utils.TableAssessmentInfo, error) {
	args := m.Called(conv)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(map[string]utils.TableAssessmentInfo), args.Error(1)
}

func (m *MockInfoSchema) GetIndexInfo(tableName string, index schema.Index) (utils.IndexAssessmentInfo, error) {
	args := m.Called(tableName, index)
	if args.Get(0) == nil {
		return utils.IndexAssessmentInfo{}, args.Error(1)
	}
	return args.Get(0).(utils.IndexAssessmentInfo), args.Error(1)
}

func (m *MockInfoSchema) GetTriggerInfo() ([]utils.TriggerAssessmentInfo, error) {
	args := m.Called()
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]utils.TriggerAssessmentInfo), args.Error(1)
}

func (m *MockInfoSchema) GetStoredProcedureInfo() ([]utils.StoredProcedureAssessmentInfo, error) {
	args := m.Called()
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]utils.StoredProcedureAssessmentInfo), args.Error(1)
}

func (m *MockInfoSchema) GetFunctionInfo() ([]utils.FunctionAssessmentInfo, error) {
	args := m.Called()
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]utils.FunctionAssessmentInfo), args.Error(1)
}

func (m *MockInfoSchema) GetViewInfo() ([]utils.ViewAssessmentInfo, error) {
	args := m.Called()
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]utils.ViewAssessmentInfo), args.Error(1)
}

type MockConnectionConfigProvider struct {
	mock.Mock
}

func (m *MockConnectionConfigProvider) GetConnectionConfig(sourceProfile profiles.SourceProfile) (interface{}, error) {
	args := m.Called(sourceProfile)
	return args.String(0), args.Error(1)
}

type MockDBConnector struct {
	mock.Mock
}

func (m *MockDBConnector) Connect(driver string, cfg interface{}) (*sql.DB, error) {
	args := m.Called(driver, cfg)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*sql.DB), args.Error(1)
}

func TestCreateInfoSchemaCollector(t *testing.T) {

	dummyDb, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Error creating dummy sqlmock DB: %v", err)
	}
	defer dummyDb.Close()

	dbName := "test_db"
	tableName := "table1"
	tableId := "table1_id"
	indexName := "idx1"
	indexId := "idx1_id"
	schemaIndex := schema.Index{Name: indexName, Id: indexId}

	mockConv := &internal.Conv{
		SrcSchema: map[string]schema.Table{
			tableId: {
				Name:    tableName,
				Id:      tableId,
				ColDefs: map[string]schema.Column{"col1": {Name: "col1", Id: "col1_id"}},
				Indexes: []schema.Index{schemaIndex},
			},
		},
	}
	sourceProfile := profiles.SourceProfile{
		Driver: constants.MYSQL,
		Conn:   profiles.SourceProfileConnection{Mysql: profiles.SourceProfileConnectionMySQL{Db: dbName}},
	}

	expectedTables := map[string]utils.TableAssessmentInfo{tableId: {Name: tableName}}
	expectedIndexInfoItem := utils.IndexAssessmentInfo{Name: indexName, TableId: tableId}
	expectedIndexesResult := []utils.IndexAssessmentInfo{expectedIndexInfoItem}

	expectedTriggers := []utils.TriggerAssessmentInfo{{Name: "trigger1"}}
	expectedSps := []utils.StoredProcedureAssessmentInfo{{Name: "sp1"}}
	expectedFuncs := []utils.FunctionAssessmentInfo{{Name: "func1"}}
	expectedViews := []utils.ViewAssessmentInfo{{Name: "view1"}}

	t.Run("All InfoSchema methods mocked successfully", func(t *testing.T) {
		mockCfgProvider := new(MockConnectionConfigProvider)
		mockDbConnector := new(MockDBConnector)
		mockIS := new(MockInfoSchema)

		mockCfgProvider.On("GetConnectionConfig", sourceProfile).Return("dummy_cfg", nil).Once()
		mockDbConnector.On("Connect", sourceProfile.Driver, "dummy_cfg").Return(dummyDb, nil).Once()

		mockIS.On("GetTableInfo", mockConv).Return(expectedTables, nil).Once()
		mockIS.On("GetIndexInfo", tableName, schemaIndex).Return(expectedIndexInfoItem, nil).Once()
		mockIS.On("GetTriggerInfo").Return(expectedTriggers, nil).Once()
		mockIS.On("GetStoredProcedureInfo").Return(expectedSps, nil).Once()
		mockIS.On("GetFunctionInfo").Return(expectedFuncs, nil).Once()
		mockIS.On("GetViewInfo").Return(expectedViews, nil).Once()

		collector, err := GetInfoSchemaCollector(mockConv, sourceProfile, mockDbConnector, mockCfgProvider, func(db *sql.DB, sp profiles.SourceProfile) (sources.InfoSchema, error) {
			return mockIS, nil
		})

		assert.NoError(t, err)
		assert.NotNil(t, collector)
		assert.Equal(t, expectedTables, collector.tables)
		assert.Equal(t, expectedIndexesResult, collector.indexes)
		assert.Equal(t, expectedTriggers, collector.triggers)
		assert.Equal(t, expectedSps, collector.storedProcedures)
		assert.Equal(t, expectedFuncs, collector.functions)
		assert.Equal(t, expectedViews, collector.views)
		assert.Equal(t, mockConv, collector.conv)

		mockCfgProvider.AssertExpectations(t)
		mockDbConnector.AssertExpectations(t)
		mockIS.AssertExpectations(t)
	})

	t.Run("ErrorFromConfigProvider", func(t *testing.T) {
		mockCfgProvider := new(MockConnectionConfigProvider)
		mockDbConnector := new(MockDBConnector)
		mockIS := new(MockInfoSchema)

		expectedErr := errors.New("config provider error")
		mockCfgProvider.On("GetConnectionConfig", sourceProfile).Return("", expectedErr).Once()

		collector, err := GetInfoSchemaCollector(mockConv, sourceProfile, mockDbConnector, mockCfgProvider, func(db *sql.DB, sp profiles.SourceProfile) (sources.InfoSchema, error) {
			return mockIS, nil
		})

		assert.Error(t, err)
		assert.Equal(t, expectedErr, err)
		assert.True(t, collector.IsEmpty(), "Collector should be empty on early error")

		mockCfgProvider.AssertExpectations(t)
		mockDbConnector.AssertNotCalled(t, "Connect", mock.Anything, mock.Anything)
	})

	t.Run("ErrorFromDBConnector", func(t *testing.T) {
		mockCfgProvider := new(MockConnectionConfigProvider)
		mockDbConnector := new(MockDBConnector)
		mockIS := new(MockInfoSchema)

		expectedErr := errors.New("db connect error")
		mockCfgProvider.On("GetConnectionConfig", sourceProfile).Return("dummy_cfg", nil).Once()
		mockDbConnector.On("Connect", sourceProfile.Driver, "dummy_cfg").Return(nil, expectedErr).Once()

		collector, err := GetInfoSchemaCollector(mockConv, sourceProfile, mockDbConnector, mockCfgProvider, func(db *sql.DB, sp profiles.SourceProfile) (sources.InfoSchema, error) {
			return mockIS, nil
		})

		assert.Error(t, err)
		assert.Equal(t, expectedErr, err)
		assert.True(t, collector.IsEmpty())

		mockCfgProvider.AssertExpectations(t)
		mockDbConnector.AssertExpectations(t)
	})

	t.Run("ErrorFromActualGetInfoSchemaCall", func(t *testing.T) {
		mockCfgProvider := new(MockConnectionConfigProvider)
		mockDbConnector := new(MockDBConnector)

		mockCfgProvider.On("GetConnectionConfig", sourceProfile).Return("dummy_cfg", nil).Once()
		mockDbConnector.On("Connect", sourceProfile.Driver, "dummy_cfg").Return(dummyDb, nil).Once()

		collector, err := GetInfoSchemaCollector(mockConv, sourceProfile, mockDbConnector, mockCfgProvider, func(db *sql.DB, sp profiles.SourceProfile) (sources.InfoSchema, error) {
			return nil, fmt.Errorf("error getting infoschema")
		})

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "error getting info schema")
		assert.True(t, collector.IsEmpty())

		mockCfgProvider.AssertExpectations(t)
		mockDbConnector.AssertExpectations(t)
	})

	t.Run("MultipleErrorsAccumulatedFromInfoSchemaMethods", func(t *testing.T) {
		mockCfgProvider := new(MockConnectionConfigProvider)
		mockDbConnector := new(MockDBConnector)
		mockIS := new(MockInfoSchema)

		errTables := errors.New("tables error")
		errGetIndexInfo := errors.New("get index info error")
		errTriggers := errors.New("triggers error")

		mockCfgProvider.On("GetConnectionConfig", sourceProfile).Return("dummy_cfg", nil).Once()
		mockDbConnector.On("Connect", sourceProfile.Driver, "dummy_cfg").Return(dummyDb, nil).Once()

		mockIS.On("GetTableInfo", mockConv).Return(nil, errTables).Once()
		mockIS.On("GetIndexInfo", tableName, schemaIndex).Return(utils.IndexAssessmentInfo{}, errGetIndexInfo).Once()
		mockIS.On("GetTriggerInfo").Return(nil, errTriggers).Once()
		mockIS.On("GetStoredProcedureInfo").Return(nil, errors.New("get stored procedure error")).Once()
		mockIS.On("GetFunctionInfo").Return(nil, errors.New("some func error")).Once()
		mockIS.On("GetViewInfo").Return(nil, errors.New("get view error")).Once()

		collector, errResult := GetInfoSchemaCollector(mockConv, sourceProfile, mockDbConnector, mockCfgProvider, func(db *sql.DB, sp profiles.SourceProfile) (sources.InfoSchema, error) {
			return mockIS, nil
		})

		assert.Error(t, errResult)
		fullErrorMsg := errResult.Error()
		assert.Contains(t, fullErrorMsg, "Error while scanning tables: tables error")
		assert.Contains(t, fullErrorMsg, "Error while scanning indexes: get index info error")
		assert.Contains(t, fullErrorMsg, "Error while scanning triggers: triggers error")
		assert.Contains(t, fullErrorMsg, "Error while scanning functions: some func error")
		assert.Contains(t, fullErrorMsg, "Error while scanning stored procedures: get stored procedure error")
		assert.Contains(t, fullErrorMsg, "Error while scanning views: get view error")

		assert.Nil(t, collector.tables)
		assert.Nil(t, collector.indexes)
		assert.Nil(t, collector.triggers)
		assert.Nil(t, collector.storedProcedures)
		assert.Nil(t, collector.functions)
		assert.Nil(t, collector.views)

		mockCfgProvider.AssertExpectations(t)
		mockDbConnector.AssertExpectations(t)
		mockIS.AssertExpectations(t)
	})

}

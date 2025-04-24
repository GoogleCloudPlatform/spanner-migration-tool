package assessment

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/assessment/sources/mysql"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/assessment/utils"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/profiles"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/schema"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

type MockInfoSchema struct {
	tables           map[string]utils.TableAssessmentInfo
	indexes          []utils.IndexAssessmentInfo
	triggers         []utils.TriggerAssessmentInfo
	storedProcedures []utils.StoredProcedureAssessmentInfo
	functions        []utils.FunctionAssessmentInfo
	views            []utils.ViewAssessmentInfo
	err              error
}

func init() {
	logger.Log = zap.NewNop()
}

type MockConnectionConfigProvider struct {
	config interface{}
	err    error
}

func (m MockConnectionConfigProvider) GetConnectionConfig(sourceProfile profiles.SourceProfile) (interface{}, error) {
	return m.config, m.err
}

type MockDBConnector struct {
	db  *sql.DB
	err error
}

func (m MockDBConnector) Connect(driver string, connectionConfig interface{}) (*sql.DB, error) {
	return m.db, m.err
}

func TestCreateInfoSchemaCollector(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	dbName := "test_db"
	tableName := "table1"
	columnName := "column1"
	indexName := "index1"
	triggerName := "trigger1"
	procedureName := "procedure1"
	functionName := "function1"
	viewName := "view1"

	mock.ExpectQuery(`SELECT TABLE_COLLATION, SUBSTRING_INDEX\(TABLE_COLLATION, '_', 1\) as CHARACTER_SET\s+FROM INFORMATION_SCHEMA.TABLES\s+WHERE TABLE_SCHEMA = \? AND TABLE_NAME = \?`).
		WithArgs(dbName, tableName).
		WillReturnRows(sqlmock.NewRows([]string{"TABLE_COLLATION", "CHARACTER_SET"}).AddRow("utf8_general_ci", "utf8"))

	mock.ExpectQuery(`SELECT c.column_type, c.extra, c.generation_expression\s+FROM information_schema.COLUMNS c\s+where table_schema = \? and table_name = \? and column_name = \? ORDER BY c.ordinal_position`).
		WithArgs(dbName, tableName, columnName).
		WillReturnRows(sqlmock.NewRows([]string{"column_type", "extra", "generation_expression"}).AddRow("INT", "auto_increment", ""))

	mock.ExpectQuery(`SELECT DISTINCT INDEX_NAME,COLUMN_NAME,SEQ_IN_INDEX,COLLATION,NON_UNIQUE,INDEX_TYPE\s+FROM INFORMATION_SCHEMA.STATISTICS\s+WHERE TABLE_SCHEMA = \?\s+AND TABLE_NAME = \?\s+AND INDEX_NAME = \?\s+ORDER BY INDEX_NAME, SEQ_IN_INDEX`).
		WithArgs(dbName, tableName, indexName).
		WillReturnRows(sqlmock.NewRows([]string{"INDEX_NAME", "COLUMN_NAME", "SEQ_IN_INDEX", "COLLATION", "NON_UNIQUE", "INDEX_TYPE"}).AddRow(indexName, columnName, 1, "utf8_general_ci", 1, "BTREE"))

	mock.ExpectQuery(`SELECT DISTINCT TRIGGER_NAME,EVENT_OBJECT_TABLE,ACTION_STATEMENT,ACTION_TIMING,EVENT_MANIPULATION\s+FROM INFORMATION_SCHEMA.TRIGGERS\s+WHERE EVENT_OBJECT_SCHEMA = \?`).
		WithArgs(dbName).
		WillReturnRows(sqlmock.NewRows([]string{"TRIGGER_NAME", "EVENT_OBJECT_TABLE", "ACTION_STATEMENT", "ACTION_TIMING", "EVENT_MANIPULATION"}).AddRow(triggerName, tableName, "INSERT", "BEFORE", "INSERT"))

	mock.ExpectQuery(`SELECT DISTINCT ROUTINE_NAME,ROUTINE_DEFINITION,IS_DETERMINISTIC\s+FROM INFORMATION_SCHEMA.ROUTINES\s+WHERE ROUTINE_TYPE='PROCEDURE' AND ROUTINE_SCHEMA = \?`).
		WithArgs(dbName).
		WillReturnRows(sqlmock.NewRows([]string{"ROUTINE_NAME", "ROUTINE_DEFINITION", "IS_DETERMINISTIC"}).AddRow(procedureName, "CREATE PROCEDURE procedure1() BEGIN END", "YES"))

	mock.ExpectQuery(`SELECT DISTINCT ROUTINE_NAME,ROUTINE_DEFINITION,IS_DETERMINISTIC, DTD_IDENTIFIER\s+FROM INFORMATION_SCHEMA.ROUTINES\s+WHERE ROUTINE_TYPE='FUNCTION' AND ROUTINE_SCHEMA = \?`).
		WithArgs(dbName).
		WillReturnRows(sqlmock.NewRows([]string{"ROUTINE_NAME", "ROUTINE_DEFINITION", "IS_DETERMINISTIC", "DTD_IDENTIFIER"}).AddRow(functionName, "CREATE FUNCTION function1() RETURNS INT RETURN 1", "NO", "INT"))

	mock.ExpectQuery(`SELECT DISTINCT TABLE_NAME,VIEW_DEFINITION,CHECK_OPTION, IS_UPDATABLE\s+FROM INFORMATION_SCHEMA.VIEWS\s+WHERE TABLE_SCHEMA = \?`).
		WithArgs(dbName).
		WillReturnRows(sqlmock.NewRows([]string{"TABLE_NAME", "VIEW_DEFINITION", "CHECK_OPTION", "IS_UPDATABLE"}).AddRow(viewName, "SELECT * FROM table1", "NONE", "NO"))

	mockConv := &internal.Conv{
		SrcSchema: map[string]schema.Table{
			tableName: {
				Name: tableName,
				Id:   tableName,
				ColDefs: map[string]schema.Column{
					columnName: {
						Name: columnName,
						Id:   columnName,
						Type: schema.Type{Name: "INT"},
						AutoGen: ddl.AutoGenCol{
							Name: "sample_auto",
						},
					},
				},
				Indexes: []schema.Index{{
					Name: indexName,
				},
				},
			},
		},
	}
	sourceProfile := profiles.SourceProfile{
		Driver: constants.MYSQL,
		Conn: profiles.SourceProfileConnection{
			Mysql: profiles.SourceProfileConnectionMySQL{
				Db: dbName,
			},
		},
	}
	configProvider := MockConnectionConfigProvider{config: "test_config", err: nil}
	dbConnector := MockDBConnector{db: db, err: nil}

	collector, err := CreateInfoSchemaCollector(mockConv, sourceProfile, dbConnector, configProvider)

	if err != nil {
		t.Errorf("error was not expected while creating the collector: %s", err)
	}

	if collector.IsEmpty() {
		t.Errorf("collector should not be empty")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}

	assert.NotNil(t, collector.tables, "tables map should not be nil")
	assert.Len(t, collector.tables, 1, "should have 1 table")
	tableInfo, ok := collector.tables[tableName]
	assert.True(t, ok, "table table1 should be present")
	assert.Equal(t, "utf8_general_ci", tableInfo.Collation, "table collation should be utf8_general_ci")
	assert.Equal(t, "utf8", tableInfo.Charset, "table charset should be utf8")

	assert.NotNil(t, tableInfo.ColumnAssessmentInfos, "ColumnAssessmentInfos map should not be nil")
	assert.Len(t, tableInfo.ColumnAssessmentInfos, 1, "should have 1 column")
	columnInfo, ok := tableInfo.ColumnAssessmentInfos[columnName]
	assert.True(t, ok, "column column1 should be present")
	assert.Equal(t, "sample_auto", columnInfo.ColumnDef.AutoGen.Name, "column should be auto increment")

	assert.NotNil(t, collector.indexes, "indexes array should not be nil")
	assert.Len(t, collector.indexes, 1, "should have 1 index")
	assert.Equal(t, indexName, collector.indexes[0].Name, "index name should be index1")
	assert.Equal(t, "BTREE", collector.indexes[0].Ty, "index type should be BTREE")

	assert.NotNil(t, collector.triggers, "triggers array should not be nil")
	assert.Len(t, collector.triggers, 1, "should have 1 trigger")
	assert.Equal(t, triggerName, collector.triggers[0].Name, "trigger name should be trigger1")
	assert.Equal(t, tableName, collector.triggers[0].TargetTable, "trigger target table should be table1")

	assert.NotNil(t, collector.storedProcedures, "stored procedures array should not be nil")
	assert.Len(t, collector.storedProcedures, 1, "should have 1 stored procedure")
	assert.Equal(t, procedureName, collector.storedProcedures[0].Name, "stored procedure name should be procedure1")
	assert.Equal(t, true, collector.storedProcedures[0].IsDeterministic, "stored procedure should be deterministic")

	assert.NotNil(t, collector.functions, "functions array should not be nil")
	assert.Len(t, collector.functions, 1, "should have 1 function")
	assert.Equal(t, functionName, collector.functions[0].Name, "function name should be function1")
	assert.Equal(t, false, collector.functions[0].IsDeterministic, "function should not be deterministic")
	assert.Equal(t, "INT", collector.functions[0].Datatype, "function datatype should be INT")

	assert.NotNil(t, collector.views, "views array should not be nil")
	assert.Len(t, collector.views, 1, "should have 1 view")
	assert.Equal(t, viewName, collector.views[0].Name, "view name should be view1")
	assert.Equal(t, false, collector.views[0].IsUpdatable, "view should not be updatable")
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

func TestCreateInfoSchemaCollector_ErrorPaths(t *testing.T) {
	baseConv := &internal.Conv{
		SrcSchema: map[string]schema.Table{
			"table1": {
				Name: "table1",
				Id:   "table1",
				ColDefs: map[string]schema.Column{
					"column1": {
						Name:    "column1",
						Id:      "column1",
						Type:    schema.Type{Name: "INT"},
						AutoGen: ddl.AutoGenCol{Name: "sample_auto"},
					},
				},
				Indexes: []schema.Index{
					{Name: "index1", Id: "idx_table1_index1"},
				},
			},
		},
	}
	baseSourceProfile := profiles.SourceProfile{
		Driver: constants.MYSQL,
		Conn: profiles.SourceProfileConnection{
			Mysql: profiles.SourceProfileConnectionMySQL{
				Db: "test_db",
			},
		},
	}

	dbName := baseSourceProfile.Conn.Mysql.Db
	tableName := "table1"
	columnName := "column1"
	indexName := "index1"
	tests := []struct {
		name                  string
		mockConv              *internal.Conv
		sourceProfile         profiles.SourceProfile
		connProviderConfig    interface{}
		connProviderErr       error
		dbConnectorConnectErr error
		mockSQLSetupFn        func(mock sqlmock.Sqlmock)
		wantError             bool
		errorMsgContains      string
	}{
		{
			name:                  "GetConnectionConfig fails",
			mockConv:              baseConv,
			sourceProfile:         baseSourceProfile,
			connProviderConfig:    nil,
			connProviderErr:       fmt.Errorf("config error"),
			dbConnectorConnectErr: nil,
			mockSQLSetupFn:        nil,
			wantError:             true,
			errorMsgContains:      "config error",
		},
		{
			name:                  "Connect fails",
			mockConv:              baseConv,
			sourceProfile:         baseSourceProfile,
			connProviderConfig:    "test_config",
			connProviderErr:       nil,
			dbConnectorConnectErr: fmt.Errorf("connection error"),
			mockSQLSetupFn:        nil,
			wantError:             true,
			errorMsgContains:      "connection error",
		},
		{
			name:     "getInfoSchema fails (unsupported driver)",
			mockConv: baseConv,
			sourceProfile: profiles.SourceProfile{
				Driver: "unsupported_driver",
				Conn:   baseSourceProfile.Conn,
			},
			connProviderConfig:    "test_config",
			connProviderErr:       nil,
			dbConnectorConnectErr: nil,
			mockSQLSetupFn:        func(mock sqlmock.Sqlmock) {},
			wantError:             true,
			errorMsgContains:      "error getting info schema: driver unsupported_driver not supported",
		},
		{
			name:                  "GetTableInfo fails",
			mockConv:              baseConv,
			sourceProfile:         baseSourceProfile,
			connProviderConfig:    "test_config",
			connProviderErr:       nil,
			dbConnectorConnectErr: nil,
			mockSQLSetupFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`SELECT TABLE_COLLATION, SUBSTRING_INDEX\(TABLE_COLLATION, '_', 1\) as CHARACTER_SET`).
					WithArgs(dbName, tableName).
					WillReturnError(fmt.Errorf("db query error for table info"))
			},
			wantError:        true,
			errorMsgContains: "Error while scanning tables: couldn't get schema for table table1: db query error for table info",
		},
		{
			name:                  "GetColumnInfo (within GetTableInfo) fails",
			mockConv:              baseConv,
			sourceProfile:         baseSourceProfile,
			connProviderConfig:    "test_config",
			connProviderErr:       nil,
			dbConnectorConnectErr: nil,
			mockSQLSetupFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`SELECT TABLE_COLLATION, SUBSTRING_INDEX\(TABLE_COLLATION, '_', 1\) as CHARACTER_SET`).
					WithArgs(dbName, tableName).
					WillReturnRows(sqlmock.NewRows([]string{"TABLE_COLLATION", "CHARACTER_SET"}).AddRow("utf8mb4_general_ci", "utf8mb4"))
				mock.ExpectQuery(`SELECT c.column_type, c.extra, c.generation_expression`).
					WithArgs(dbName, tableName, columnName).
					WillReturnError(fmt.Errorf("db query error for column info"))
			},
			wantError:        true,
			errorMsgContains: "Error while scanning tables: couldn't get schema for column table1.column1: db query error for column info",
		},
		{
			name:                  "GetIndexInfo fails",
			mockConv:              baseConv,
			sourceProfile:         baseSourceProfile,
			connProviderConfig:    "test_config",
			connProviderErr:       nil,
			dbConnectorConnectErr: nil,
			mockSQLSetupFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`SELECT TABLE_COLLATION, SUBSTRING_INDEX\(TABLE_COLLATION, '_', 1\) as CHARACTER_SET`).
					WithArgs(dbName, tableName).
					WillReturnRows(sqlmock.NewRows([]string{"TABLE_COLLATION", "CHARACTER_SET"}).AddRow("utf8mb4_general_ci", "utf8mb4"))
				mock.ExpectQuery(`SELECT c.column_type, c.extra, c.generation_expression`).
					WithArgs(dbName, tableName, columnName).
					WillReturnRows(sqlmock.NewRows([]string{"column_type", "extra", "generation_expression"}).AddRow("INT", "", ""))

				mock.ExpectQuery(`SELECT DISTINCT INDEX_NAME,COLUMN_NAME,SEQ_IN_INDEX,COLLATION,NON_UNIQUE,INDEX_TYPE`).
					WithArgs(dbName, tableName, indexName).
					WillReturnError(fmt.Errorf("db query error for index info"))
			},
			wantError:        true,
			errorMsgContains: "Error while scanning indexes: couldn't get index for index name table1.index1: db query error for index info",
		},
		{
			name:                  "GetTriggerInfo fails",
			mockConv:              baseConv,
			sourceProfile:         baseSourceProfile,
			connProviderConfig:    "test_config",
			connProviderErr:       nil,
			dbConnectorConnectErr: nil,
			mockSQLSetupFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`SELECT TABLE_COLLATION, SUBSTRING_INDEX\(TABLE_COLLATION, '_', 1\) as CHARACTER_SET`).
					WithArgs(dbName, tableName).
					WillReturnRows(sqlmock.NewRows([]string{"TABLE_COLLATION", "CHARACTER_SET"}).AddRow("utf8mb4_general_ci", "utf8mb4"))
				mock.ExpectQuery(`SELECT c.column_type, c.extra, c.generation_expression`).
					WithArgs(dbName, tableName, columnName).
					WillReturnRows(sqlmock.NewRows([]string{"column_type", "extra", "generation_expression"}).AddRow("INT", "", ""))
				mock.ExpectQuery(`SELECT DISTINCT INDEX_NAME,COLUMN_NAME,SEQ_IN_INDEX,COLLATION,NON_UNIQUE,INDEX_TYPE`).
					WithArgs(dbName, tableName, indexName).
					WillReturnRows(sqlmock.NewRows([]string{"INDEX_NAME", "COLUMN_NAME", "SEQ_IN_INDEX", "COLLATION", "NON_UNIQUE", "INDEX_TYPE"}).AddRow(indexName, columnName, 1, "A", 0, "BTREE"))

				mock.ExpectQuery(`SELECT DISTINCT TRIGGER_NAME,EVENT_OBJECT_TABLE,ACTION_STATEMENT,ACTION_TIMING,EVENT_MANIPULATION`).
					WithArgs(dbName).
					WillReturnError(fmt.Errorf("db query error for trigger info"))
			},
			wantError:        true,
			errorMsgContains: "Error while scanning triggers: db query error for trigger info",
		},
		{
			name:                  "GetStoredProcedureInfo fails",
			mockConv:              baseConv,
			sourceProfile:         baseSourceProfile,
			connProviderConfig:    "test_config",
			connProviderErr:       nil,
			dbConnectorConnectErr: nil,
			mockSQLSetupFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`SELECT TABLE_COLLATION, SUBSTRING_INDEX\(TABLE_COLLATION, '_', 1\) as CHARACTER_SET`).
					WithArgs(dbName, tableName).
					WillReturnRows(sqlmock.NewRows([]string{"TABLE_COLLATION", "CHARACTER_SET"}).AddRow("utf8mb4_general_ci", "utf8mb4"))
				mock.ExpectQuery(`SELECT c.column_type, c.extra, c.generation_expression`).
					WithArgs(dbName, tableName, columnName).
					WillReturnRows(sqlmock.NewRows([]string{"column_type", "extra", "generation_expression"}).AddRow("INT", "", ""))
				mock.ExpectQuery(`SELECT DISTINCT INDEX_NAME,COLUMN_NAME,SEQ_IN_INDEX,COLLATION,NON_UNIQUE,INDEX_TYPE`).
					WithArgs(dbName, tableName, indexName).
					WillReturnRows(sqlmock.NewRows([]string{"INDEX_NAME", "COLUMN_NAME", "SEQ_IN_INDEX", "COLLATION", "NON_UNIQUE", "INDEX_TYPE"}).AddRow(indexName, columnName, 1, "A", 0, "BTREE"))
				mock.ExpectQuery(`SELECT DISTINCT TRIGGER_NAME,EVENT_OBJECT_TABLE,ACTION_STATEMENT,ACTION_TIMING,EVENT_MANIPULATION`).
					WithArgs(dbName).
					WillReturnRows(sqlmock.NewRows([]string{"TRIGGER_NAME", "EVENT_OBJECT_TABLE", "ACTION_STATEMENT", "ACTION_TIMING", "EVENT_MANIPULATION"})) // No triggers

				mock.ExpectQuery(`SELECT DISTINCT ROUTINE_NAME,ROUTINE_DEFINITION,IS_DETERMINISTIC`).
					WithArgs(dbName).
					WillReturnError(fmt.Errorf("db query error for sproc info"))
			},
			wantError:        true,
			errorMsgContains: "Error while scanning stored procedures: db query error for sproc info",
		},
		{
			name:                  "GetFunctionInfo fails",
			mockConv:              baseConv,
			sourceProfile:         baseSourceProfile,
			connProviderConfig:    "test_config",
			connProviderErr:       nil,
			dbConnectorConnectErr: nil,
			mockSQLSetupFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`SELECT TABLE_COLLATION, SUBSTRING_INDEX\(TABLE_COLLATION, '_', 1\) as CHARACTER_SET`).
					WithArgs(dbName, tableName).
					WillReturnRows(sqlmock.NewRows([]string{"TABLE_COLLATION", "CHARACTER_SET"}).AddRow("utf8mb4_general_ci", "utf8mb4"))
				mock.ExpectQuery(`SELECT c.column_type, c.extra, c.generation_expression`).
					WithArgs(dbName, tableName, columnName).
					WillReturnRows(sqlmock.NewRows([]string{"column_type", "extra", "generation_expression"}).AddRow("INT", "", ""))
				mock.ExpectQuery(`SELECT DISTINCT INDEX_NAME,COLUMN_NAME,SEQ_IN_INDEX,COLLATION,NON_UNIQUE,INDEX_TYPE`).
					WithArgs(dbName, tableName, indexName).
					WillReturnRows(sqlmock.NewRows([]string{"INDEX_NAME", "COLUMN_NAME", "SEQ_IN_INDEX", "COLLATION", "NON_UNIQUE", "INDEX_TYPE"}).AddRow(indexName, columnName, 1, "A", 0, "BTREE"))
				mock.ExpectQuery(`SELECT DISTINCT TRIGGER_NAME,EVENT_OBJECT_TABLE,ACTION_STATEMENT,ACTION_TIMING,EVENT_MANIPULATION`).
					WithArgs(dbName).
					WillReturnRows(sqlmock.NewRows([]string{"TRIGGER_NAME", "EVENT_OBJECT_TABLE", "ACTION_STATEMENT", "ACTION_TIMING", "EVENT_MANIPULATION"}))
				mock.ExpectQuery(`SELECT DISTINCT ROUTINE_NAME,ROUTINE_DEFINITION,IS_DETERMINISTIC`).
					WithArgs(dbName).
					WillReturnRows(sqlmock.NewRows([]string{"ROUTINE_NAME", "ROUTINE_DEFINITION", "IS_DETERMINISTIC"})) // No sprocs

				mock.ExpectQuery(`SELECT DISTINCT ROUTINE_NAME,ROUTINE_DEFINITION,IS_DETERMINISTIC, DTD_IDENTIFIER`).
					WithArgs(dbName).
					WillReturnError(fmt.Errorf("db query error for func info"))
			},
			wantError:        true,
			errorMsgContains: "Error while scanning functions: db query error for func info",
		},
		{
			name:                  "GetViewInfo fails",
			mockConv:              baseConv,
			sourceProfile:         baseSourceProfile,
			connProviderConfig:    "test_config",
			connProviderErr:       nil,
			dbConnectorConnectErr: nil,
			mockSQLSetupFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`SELECT TABLE_COLLATION, SUBSTRING_INDEX\(TABLE_COLLATION, '_', 1\) as CHARACTER_SET`).
					WithArgs(dbName, tableName).
					WillReturnRows(sqlmock.NewRows([]string{"TABLE_COLLATION", "CHARACTER_SET"}).AddRow("utf8mb4_general_ci", "utf8mb4"))
				mock.ExpectQuery(`SELECT c.column_type, c.extra, c.generation_expression`).
					WithArgs(dbName, tableName, columnName).
					WillReturnRows(sqlmock.NewRows([]string{"column_type", "extra", "generation_expression"}).AddRow("INT", "", ""))
				mock.ExpectQuery(`SELECT DISTINCT INDEX_NAME,COLUMN_NAME,SEQ_IN_INDEX,COLLATION,NON_UNIQUE,INDEX_TYPE`).
					WithArgs(dbName, tableName, indexName).
					WillReturnRows(sqlmock.NewRows([]string{"INDEX_NAME", "COLUMN_NAME", "SEQ_IN_INDEX", "COLLATION", "NON_UNIQUE", "INDEX_TYPE"}).AddRow(indexName, columnName, 1, "A", 0, "BTREE"))
				mock.ExpectQuery(`SELECT DISTINCT TRIGGER_NAME,EVENT_OBJECT_TABLE,ACTION_STATEMENT,ACTION_TIMING,EVENT_MANIPULATION`).
					WithArgs(dbName).
					WillReturnRows(sqlmock.NewRows([]string{"TRIGGER_NAME", "EVENT_OBJECT_TABLE", "ACTION_STATEMENT", "ACTION_TIMING", "EVENT_MANIPULATION"}))
				mock.ExpectQuery(`SELECT DISTINCT ROUTINE_NAME,ROUTINE_DEFINITION,IS_DETERMINISTIC`).
					WithArgs(dbName).
					WillReturnRows(sqlmock.NewRows([]string{"ROUTINE_NAME", "ROUTINE_DEFINITION", "IS_DETERMINISTIC"}))
				mock.ExpectQuery(`SELECT DISTINCT ROUTINE_NAME,ROUTINE_DEFINITION,IS_DETERMINISTIC, DTD_IDENTIFIER`).
					WithArgs(dbName).
					WillReturnRows(sqlmock.NewRows([]string{"ROUTINE_NAME", "ROUTINE_DEFINITION", "IS_DETERMINISTIC", "DTD_IDENTIFIER"})) // No functions

				mock.ExpectQuery(`SELECT DISTINCT TABLE_NAME,VIEW_DEFINITION,CHECK_OPTION, IS_UPDATABLE`).
					WithArgs(dbName).
					WillReturnError(fmt.Errorf("db query error for view info"))
			},
			wantError:        true,
			errorMsgContains: "Error while scanning views: db query error for view info",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockConfigProvider := MockConnectionConfigProvider{
				config: tt.connProviderConfig,
				err:    tt.connProviderErr,
			}

			var db *sql.DB
			var mock sqlmock.Sqlmock
			var sqlmockErr error
			if tt.dbConnectorConnectErr == nil && tt.connProviderErr == nil {
				db, mock, sqlmockErr = sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
				if sqlmockErr != nil {
					t.Fatalf("an error '%s' was not expected when opening a stub database connection", sqlmockErr)
				}
				if tt.mockSQLSetupFn != nil {
					tt.mockSQLSetupFn(mock)
				}
			}

			mockDBConnector := MockDBConnector{
				db:  db,
				err: tt.dbConnectorConnectErr,
			}

			logger.Log = zap.NewNop()

			collector, err := CreateInfoSchemaCollector(tt.mockConv, tt.sourceProfile, mockDBConnector, mockConfigProvider)

			if tt.wantError {
				assert.Error(t, err, "Expected an error")
				if tt.errorMsgContains != "" {
					assert.Contains(t, err.Error(), tt.errorMsgContains, "Error message mismatch")
				}
			} else {
				assert.NoError(t, err, "Did not expect an error")
				assert.NotNil(t, collector, "Expected a non-nil collector")
			}

			if mock != nil {
				if tt.connProviderErr == nil && tt.dbConnectorConnectErr == nil {
					if mockExpectationsErr := mock.ExpectationsWereMet(); mockExpectationsErr != nil {
						t.Errorf("there were unfulfilled sqlmock expectations: %s", mockExpectationsErr)
					}
				}
				db.Close()
			}
		})
	}
}

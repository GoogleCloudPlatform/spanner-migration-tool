// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"sort"
	"strings"

	sp "cloud.google.com/go/spanner"
	_ "github.com/go-sql-driver/mysql" // The driver should be used via the database/sql package.
	_ "github.com/lib/pq"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/profiles"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/schema"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/common"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/streaming"
)

var collationRegex = regexp.MustCompile(constants.DB_COLLATION_REGEX)

// InfoSchemaImpl is MySQL specific implementation for InfoSchema.
type InfoSchemaImpl struct {
	DbName             string
	Db                 *sql.DB
	MigrationProjectId string
	SourceProfile      profiles.SourceProfile
	TargetProfile      profiles.TargetProfile
}

// GetToDdl implement the common.InfoSchema interface.
func (isi InfoSchemaImpl) GetToDdl() common.ToDdl {
	return ToDdlImpl{}
}

// GetTableName returns table name.
func (isi InfoSchemaImpl) GetTableName(dbName string, tableName string) string {
	return tableName
}

// GetRowsFromTable returns a sql Rows object for a table.
func (isi InfoSchemaImpl) GetRowsFromTable(conv *internal.Conv, tableId string) (interface{}, error) {
	srcSchema := conv.SrcSchema[tableId]
	srcCols := []string{}

	for _, srcColId := range srcSchema.ColIds {
		srcCols = append(srcCols, conv.SrcSchema[tableId].ColDefs[srcColId].Name)
	}
	if len(srcCols) == 0 {
		conv.Unexpected(fmt.Sprintf("Couldn't get source columns for table %s ", srcSchema.Name))
		return nil, nil
	}
	// MySQL schema and name can be arbitrary strings.
	// Ideally we would pass schema/name as a query parameter,
	// but MySQL doesn't support this. So we quote it instead.
	colNameList := buildColNameList(srcSchema, srcCols)
	q := fmt.Sprintf("SELECT %s FROM `%s`.`%s`;", colNameList, isi.DbName, srcSchema.Name)
	rows, err := isi.Db.Query(q)
	return rows, err
}

// Building list of column names to support mysql spatial datatypes instead of
// using 'SELECT *' because spatial columns will be fetched using ST_AsText(colName).
func buildColNameList(srcSchema schema.Table, srcColName []string) string {
	var srcColTypes []string
	var colList, colTmpName string
	for _, colName := range srcColName {
		// To handle cases where column name is reserved keyword or having space between words.
		colTmpName = "`" + colName + "`"
		srcColTypes = append(srcColTypes, srcSchema.ColDefs[colName].Type.Name)
		for _, spatial := range MysqlSpatialDataTypes {
			if strings.Contains(strings.ToLower(srcSchema.ColDefs[colName].Type.Name), spatial) {
				colTmpName = "ST_AsText" + "(" + colTmpName + ")" + colTmpName
				break
			}
		}
		colList = colList + colTmpName + ","
	}
	return colList[:len(colList)-1]
}

// ProcessData performs data conversion for source database.
func (isi InfoSchemaImpl) ProcessData(conv *internal.Conv, tableId string, srcSchema schema.Table, commonColIds []string, spSchema ddl.CreateTable, additionalAttributes internal.AdditionalDataAttributes) error {
	srcTableName := conv.SrcSchema[tableId].Name
	rowsInterface, err := isi.GetRowsFromTable(conv, tableId)
	if err != nil {
		conv.Unexpected(fmt.Sprintf("Couldn't get data for table %s : err = %s", srcTableName, err))
		return err
	}
	rows := rowsInterface.(*sql.Rows)
	defer rows.Close()
	srcCols, _ := rows.Columns()
	v, scanArgs := buildVals(len(srcCols))
	colNameIdMap := internal.GetSrcColNameIdMap(conv.SrcSchema[tableId])
	for rows.Next() {
		// get RawBytes from data.
		err := rows.Scan(scanArgs...)
		if err != nil {
			conv.Unexpected(fmt.Sprintf("Couldn't process sql data row: %s", err))
			// Scan failed, so we don't have any data to add to bad rows.
			conv.StatsAddBadRow(srcTableName, conv.DataMode())
			continue
		}
		values := valsToStrings(v)

		newValues, err := common.PrepareValues(conv, tableId, colNameIdMap, commonColIds, srcCols, values)
		if err != nil {
			conv.Unexpected(fmt.Sprintf("Error while converting data: %s\n", err))
			conv.StatsAddBadRow(srcTableName, conv.DataMode())
			conv.CollectBadRow(srcTableName, srcCols, values)
			continue
		}

		ProcessDataRow(conv, tableId, commonColIds, srcSchema, spSchema, newValues, additionalAttributes)
	}
	return nil
}

// GetRowCount with number of rows in each table.
func (isi InfoSchemaImpl) GetRowCount(table common.SchemaAndName) (int64, error) {
	// MySQL schema and name can be arbitrary strings.
	// Ideally we would pass schema/name as a query parameter,
	// but MySQL doesn't support this. So we quote it instead.
	q := fmt.Sprintf("SELECT COUNT(*) FROM `%s`.`%s`;", table.Schema, table.Name)
	rows, err := isi.Db.Query(q)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	var count int64
	if rows.Next() {
		err := rows.Scan(&count)
		return count, err
	}
	return 0, nil // Check if 0 is ok to return
}

// GetTables return list of tables in the selected database.
// Note that sql.DB already effectively has the dbName
// embedded within it (dbName is part of the DSN passed to sql.Open),
// but unfortunately there is no way to extract it from sql.DB.
func (isi InfoSchemaImpl) GetTables() ([]common.SchemaAndName, error) {
	// In MySQL, schema is the same as database name.
	q := "SELECT table_name FROM information_schema.tables where table_type = 'BASE TABLE' and table_schema=?"
	rows, err := isi.Db.Query(q, isi.DbName)
	if err != nil {
		return nil, fmt.Errorf("couldn't get tables: %w", err)
	}
	defer rows.Close()
	var tableName string
	var tables []common.SchemaAndName
	for rows.Next() {
		rows.Scan(&tableName)
		tables = append(tables, common.SchemaAndName{Schema: isi.DbName, Name: tableName})
	}
	return tables, nil
}

// GetColumns returns a list of Column objects and names// ProcessColumns
func (isi InfoSchemaImpl) GetColumns(conv *internal.Conv, table common.SchemaAndName, constraints map[string][]string, primaryKeys []string) (map[string]schema.Column, []string, error) {
	q := `SELECT c.column_name, c.data_type, c.column_type, c.is_nullable, c.column_default, c.character_maximum_length, c.numeric_precision, c.numeric_scale, c.extra
              FROM information_schema.COLUMNS c
              where table_schema = ? and table_name = ? ORDER BY c.ordinal_position;`
	cols, err := isi.Db.Query(q, table.Schema, table.Name)
	if err != nil {
		return nil, nil, fmt.Errorf("couldn't get schema for table %s.%s: %s", table.Schema, table.Name, err)
	}
	defer cols.Close()
	colDefs := make(map[string]schema.Column)
	var colIds []string
	var colName, dataType, isNullable, columnType string
	var colDefault, colExtra sql.NullString
	var charMaxLen, numericPrecision, numericScale sql.NullInt64
	var colAutoGen ddl.AutoGenCol
	for cols.Next() {
		err := cols.Scan(&colName, &dataType, &columnType, &isNullable, &colDefault, &charMaxLen, &numericPrecision, &numericScale, &colExtra)
		if err != nil {
			conv.Unexpected(fmt.Sprintf("Can't scan: %v", err))
			continue
		}
		ignored := schema.Ignored{}
		ignored.Default = colDefault.Valid
		colId := internal.GenerateColumnId()
		if colExtra.String == "auto_increment" {
			sequence := createSequence(conv)
			colAutoGen = ddl.AutoGenCol{
				Name:           sequence.Name,
				GenerationType: constants.AUTO_INCREMENT,
			}
			sequence.ColumnsUsingSeq = map[string][]string{
				table.Id: {colId},
			}
			conv.ConvLock.Lock()
			conv.SrcSequences[sequence.Id] = sequence
			conv.ConvLock.Unlock()
		} else {
			colAutoGen = ddl.AutoGenCol{}
		}

		defaultVal := ddl.DefaultValue{
			IsPresent: colDefault.Valid,
			Value:     ddl.Expression{},
		}
		if colDefault.Valid {
			ty := dataType
			if conv.SpDialect == constants.DIALECT_POSTGRESQL {
				ty = ddl.GetPGType(ddl.Type{Name: ty})
			}
			defaultVal.Value = ddl.Expression{
				ExpressionId: internal.GenerateExpressionId(),
				Statement:    common.SanitizeDefaultValue(colDefault.String, ty, colExtra.String == constants.DEFAULT_GENERATED),
			}
		}

		c := schema.Column{
			Id:           colId,
			Name:         colName,
			Type:         toType(dataType, columnType, charMaxLen, numericPrecision, numericScale),
			NotNull:      common.ToNotNull(conv, isNullable),
			Ignored:      ignored,
			AutoGen:      colAutoGen,
			DefaultValue: defaultVal,
		}
		colDefs[colId] = c
		colIds = append(colIds, colId)
	}
	return colDefs, colIds, nil
}

// GetConstraints returns a list of primary keys and by-column map of
// other constraints.  Note: we need to preserve ordinal order of
// columns in primary key constraints.
// Note that foreign key constraints are handled in getForeignKeys.
func (isi InfoSchemaImpl) GetConstraints(conv *internal.Conv, table common.SchemaAndName) ([]string, []schema.CheckConstraint, map[string][]string, error) {
	finalQuery, err := isi.getConstraintsDQL()
	if err != nil {
		return nil, nil, nil, err
	}
	rows, err := isi.Db.Query(finalQuery, table.Schema, table.Name)
	if err != nil {
		return nil, nil, nil, err
	}
	defer rows.Close()

	var primaryKeys []string
	var checkKeys []schema.CheckConstraint
	m := make(map[string][]string)

	for rows.Next() {
		if err := isi.processRow(rows, conv, &primaryKeys, &checkKeys, m); err != nil {
			conv.Unexpected(fmt.Sprintf("Can't scan constrants. error: %v", err))
			continue
		}
	}

	return primaryKeys, checkKeys, m, nil
}

// getConstraintsDQL returns the appropriate SQL query based on the existence of CHECK_CONSTRAINTS.
func (isi InfoSchemaImpl) getConstraintsDQL() (string, error) {
	var tableExistsCount int
	// check if CHECK_CONSTRAINTS table exists.
	checkQuery := `SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE (TABLE_SCHEMA = 'information_schema' OR TABLE_SCHEMA = 'INFORMATION_SCHEMA') AND TABLE_NAME = 'CHECK_CONSTRAINTS';`
	err := isi.Db.QueryRow(checkQuery).Scan(&tableExistsCount)
	if err != nil {
		return "", err
	}

	// mysql version 8.0.16 and above has CHECK_CONSTRAINTS table.
	if tableExistsCount > 0 {
		return `SELECT DISTINCT COALESCE(k.COLUMN_NAME,'') AS COLUMN_NAME,t.CONSTRAINT_NAME, t.CONSTRAINT_TYPE, COALESCE(c.CHECK_CLAUSE, '') AS CHECK_CLAUSE, COALESCE(k.ORDINAL_POSITION, 0) AS ORDINAL_POSITION
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS t
            LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS k
            ON t.CONSTRAINT_NAME = k.CONSTRAINT_NAME 
            AND t.CONSTRAINT_SCHEMA = k.CONSTRAINT_SCHEMA 
            AND t.TABLE_NAME = k.TABLE_NAME
            LEFT JOIN INFORMATION_SCHEMA.CHECK_CONSTRAINTS AS c
            ON t.CONSTRAINT_NAME = c.CONSTRAINT_NAME
	    AND t.TABLE_SCHEMA = c.CONSTRAINT_SCHEMA
            WHERE t.TABLE_SCHEMA = ? 
            AND t.TABLE_NAME = ? 
            ORDER BY COALESCE(k.ORDINAL_POSITION, 0);`, nil
	}
	return `SELECT k.COLUMN_NAME, t.CONSTRAINT_TYPE
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS t
            INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS k
            ON t.CONSTRAINT_NAME = k.CONSTRAINT_NAME 
            AND t.CONSTRAINT_SCHEMA = k.CONSTRAINT_SCHEMA 
            AND t.TABLE_NAME = k.TABLE_NAME
            WHERE t.TABLE_SCHEMA = ?
            AND t.TABLE_NAME = ?
            ORDER BY k.ORDINAL_POSITION;`, nil
}

// processRow handles scanning and processing of a database row for GetConstraints.
func (isi InfoSchemaImpl) processRow(
	rows *sql.Rows, conv *internal.Conv, primaryKeys *[]string,
	checkKeys *[]schema.CheckConstraint, m map[string][]string,
) error {
	var col, constraintType, checkClause, constraintName, ordinal_position string
	var err error
	cols, err := rows.Columns()
	if err != nil {
		conv.Unexpected(fmt.Sprintf("Failed to get columns: %v", err))
		return err
	}

	switch len(cols) {
	case 2:
		err = rows.Scan(&col, &constraintType)
	case 5:
		err = rows.Scan(&col, &constraintName, &constraintType, &checkClause, &ordinal_position)
	default:
		conv.Unexpected(fmt.Sprintf("unexpected number of columns: %d", len(cols)))
		return fmt.Errorf("unexpected number of columns: %d", len(cols))
	}
	if err != nil {
		return err
	}

	if col == "" && constraintType == "" {
		conv.Unexpected("Got empty column or constraint type")
		return nil
	}

	switch constraintType {
	case "PRIMARY KEY":
		*primaryKeys = append(*primaryKeys, col)

	// Case added to handle check constraints
	case "CHECK":
		checkClause = collationRegex.ReplaceAllString(checkClause, "")
		checkClause = checkAndAddParentheses(checkClause)
		*checkKeys = append(*checkKeys, schema.CheckConstraint{Name: constraintName, Expr: checkClause, ExprId: internal.GenerateExpressionId(), Id: internal.GenerateCheckConstrainstId()})
	default:
		m[col] = append(m[col], constraintType)
	}
	return nil
}

// checkAndAddParentheses this method will check parentheses  if found it will return same string
// or add the parentheses then return the string
func checkAndAddParentheses(checkClause string) string {
	if strings.HasPrefix(checkClause, "(") && strings.HasSuffix(checkClause, ")") {
		return checkClause
	} else {
		return `(` + checkClause + `)`
	}
}

// GetForeignKeys return list all the foreign keys constraints.
// MySQL supports cross-database foreign key constraints. We ignore
// them because the Spanner migration tool works database at a time (a specific run
// of the Spanner migration tool focuses on a specific database) and so we can't handle
// them effectively.
func (isi InfoSchemaImpl) GetForeignKeys(conv *internal.Conv, table common.SchemaAndName) (foreignKeys []schema.ForeignKey, err error) {
	q := `SELECT k.REFERENCED_TABLE_NAME,
			k.COLUMN_NAME,
			k.REFERENCED_COLUMN_NAME,
			k.CONSTRAINT_NAME,
			r.DELETE_RULE,
			r.UPDATE_RULE
		FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS AS r
		INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS k
			ON r.CONSTRAINT_NAME = k.CONSTRAINT_NAME
			AND r.CONSTRAINT_SCHEMA = k.CONSTRAINT_SCHEMA
			AND r.TABLE_NAME = k.TABLE_NAME
			AND r.REFERENCED_TABLE_NAME = k.REFERENCED_TABLE_NAME
			AND k.REFERENCED_TABLE_SCHEMA = k.TABLE_SCHEMA
		WHERE k.TABLE_SCHEMA = ?
			AND k.TABLE_NAME = ?
		ORDER BY
			k.REFERENCED_TABLE_NAME,
			k.ORDINAL_POSITION;` //TODO(khajanchi): Add a UT for the change of removing column name from order by clause
	rows, err := isi.Db.Query(q, table.Schema, table.Name)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var col, refCol, refTable, fKeyName, OnDelete, OnUpdate string
	fKeys := make(map[string]common.FkConstraint)
	var keyNames []string

	for rows.Next() {
		err := rows.Scan(&refTable, &col, &refCol, &fKeyName, &OnDelete, &OnUpdate)
		if err != nil {
			conv.Unexpected(fmt.Sprintf("Can't scan: %v", err))
			continue
		}
		if _, found := fKeys[fKeyName]; found {
			fk := fKeys[fKeyName]
			fk.Cols = append(fk.Cols, col)
			fk.Refcols = append(fk.Refcols, refCol)
			fKeys[fKeyName] = fk
			fk.OnDelete = OnDelete
			fk.OnUpdate = OnUpdate
			continue
		}
		fKeys[fKeyName] = common.FkConstraint{Name: fKeyName, Table: refTable, Refcols: []string{refCol}, Cols: []string{col}, OnDelete: OnDelete, OnUpdate: OnUpdate}
		keyNames = append(keyNames, fKeyName)
	}
	sort.Strings(keyNames)
	for _, k := range keyNames {
		foreignKeys = append(foreignKeys,
			schema.ForeignKey{
				Id:               internal.GenerateForeignkeyId(),
				Name:             fKeys[k].Name,
				ColumnNames:      fKeys[k].Cols,
				ReferTableName:   fKeys[k].Table,
				ReferColumnNames: fKeys[k].Refcols,
				OnDelete:         fKeys[k].OnDelete,
				OnUpdate:         fKeys[k].OnUpdate,
			})
	}
	return foreignKeys, nil
}

// GetIndexes return a list of all indexes for the specified table.
func (isi InfoSchemaImpl) GetIndexes(conv *internal.Conv, table common.SchemaAndName, colNameIdMap map[string]string) ([]schema.Index, error) {
	q := `SELECT DISTINCT INDEX_NAME,COLUMN_NAME,SEQ_IN_INDEX,COLLATION,NON_UNIQUE
		FROM INFORMATION_SCHEMA.STATISTICS 
		WHERE TABLE_SCHEMA = ?
			AND TABLE_NAME = ?
			AND INDEX_NAME != 'PRIMARY' 
		ORDER BY INDEX_NAME, SEQ_IN_INDEX;`
	rows, err := isi.Db.Query(q, table.Schema, table.Name)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var name, column, sequence, nonUnique string
	var collation sql.NullString
	indexMap := make(map[string]schema.Index)
	var indexNames []string
	var indexes []schema.Index
	for rows.Next() {
		if err := rows.Scan(&name, &column, &sequence, &collation, &nonUnique); err != nil {
			conv.Unexpected(fmt.Sprintf("Can't scan: %v", err))
			continue
		}
		if _, found := indexMap[name]; !found {
			indexNames = append(indexNames, name)
			indexMap[name] = schema.Index{
				Id:     internal.GenerateIndexesId(),
				Name:   name,
				Unique: (nonUnique == "0"),
			}
		}
		index := indexMap[name]
		index.Keys = append(index.Keys, schema.Key{
			ColId: colNameIdMap[column],
			Desc:  (collation.Valid && collation.String == "D"),
		})
		indexMap[name] = index
	}
	for _, k := range indexNames {
		indexes = append(indexes, indexMap[k])
	}
	return indexes, nil
}

// StartChangeDataCapture is used for automatic triggering of Datastream job when
// performing a streaming migration.
func (isi InfoSchemaImpl) StartChangeDataCapture(ctx context.Context, conv *internal.Conv) (map[string]interface{}, error) {
	mp := make(map[string]interface{})
	var (
		schemaDetails map[string]internal.SchemaDetails
		err           error
	)
	commonInfoSchema := common.InfoSchemaImpl{}
	schemaDetails, err = commonInfoSchema.GetIncludedSrcTablesFromConv(conv)
	streamingCfg, err := streaming.ReadStreamingConfig(isi.SourceProfile.Conn.Mysql.StreamingConfig, isi.TargetProfile.Conn.Sp.Dbname, schemaDetails)
	if err != nil {
		return nil, fmt.Errorf("error reading streaming config: %v", err)
	}
	pubsubCfg, err := streaming.CreatePubsubResources(ctx, isi.MigrationProjectId, streamingCfg.DatastreamCfg.DestinationConnectionConfig, isi.SourceProfile.Conn.Mysql.Db, constants.REGULAR_GCS)
	if err != nil {
		return nil, fmt.Errorf("error creating pubsub resources: %v", err)
	}
	streamingCfg.PubsubCfg = *pubsubCfg
	dlqPubsubCfg, err := streaming.CreatePubsubResources(ctx, isi.MigrationProjectId, streamingCfg.DatastreamCfg.DestinationConnectionConfig, isi.SourceProfile.Conn.Mysql.Db, constants.DLQ_GCS)
	if err != nil {
		return nil, fmt.Errorf("error creating pubsub resources: %v", err)
	}
	streamingCfg.DlqPubsubCfg = *dlqPubsubCfg
	streamingCfg, err = streaming.StartDatastream(ctx, isi.MigrationProjectId, streamingCfg, isi.SourceProfile, isi.TargetProfile, schemaDetails)
	if err != nil {
		err = fmt.Errorf("error starting datastream: %v", err)
		return nil, err
	}
	mp["streamingCfg"] = streamingCfg
	return mp, err
}

// StartStreamingMigration is used for automatic triggering of Dataflow job when
// performing a streaming migration.
func (isi InfoSchemaImpl) StartStreamingMigration(ctx context.Context, migrationProjectId string, client *sp.Client, conv *internal.Conv, streamingInfo map[string]interface{}) (internal.DataflowOutput, error) {
	streamingCfg, _ := streamingInfo["streamingCfg"].(streaming.StreamingCfg)

	dfOutput, err := streaming.StartDataflow(ctx, migrationProjectId, isi.TargetProfile, streamingCfg, conv)
	if err != nil {
		err = fmt.Errorf("error starting dataflow: %v", err)
		return internal.DataflowOutput{}, err
	}
	return dfOutput, nil
}

func toType(dataType string, columnType string, charLen sql.NullInt64, numericPrecision, numericScale sql.NullInt64) schema.Type {
	switch {
	case dataType == "set":
		return schema.Type{Name: dataType, ArrayBounds: []int64{-1}}
	case charLen.Valid:
		return schema.Type{Name: dataType, Mods: []int64{charLen.Int64}}
	// We only want to parse the length for tinyints when it is present, in the form tinyint(12). columnType can also be just 'tinyint',
	// in which case we skip this parsing.
	case dataType == "tinyint" && len(columnType) > len("tinyint"):
		var length int64
		_, err := fmt.Sscanf(columnType, "tinyint(%d)", &length)
		if err != nil {
			return schema.Type{Name: dataType}
		}
		return schema.Type{Name: dataType, Mods: []int64{length}}
	case numericPrecision.Valid && numericScale.Valid && numericScale.Int64 != 0:
		return schema.Type{Name: dataType, Mods: []int64{numericPrecision.Int64, numericScale.Int64}}
	case numericPrecision.Valid:
		return schema.Type{Name: dataType, Mods: []int64{numericPrecision.Int64}}
	default:
		return schema.Type{Name: dataType}
	}
}

// buildVals constructs []sql.RawBytes value containers to scan row
// results into.  Returns both the underlying containers (as a slice)
// as well as an interface{} of pointers to containers to pass to
// rows.Scan.
func buildVals(n int) (v []sql.RawBytes, iv []interface{}) {
	v = make([]sql.RawBytes, n)
	// rows.Scan wants '[]interface{}' as an argument, so we must copy the
	// references into such a slice.
	iv = make([]interface{}, len(v))
	for i := range v {
		iv[i] = &v[i]
	}
	return v, iv
}

func valsToStrings(vals []sql.RawBytes) []string {
	toString := func(val sql.RawBytes) string {
		if val == nil {
			return "NULL"
		}
		return string(val)
	}
	var s []string
	for _, v := range vals {
		s = append(s, toString(v))
	}
	return s
}

func createSequence(conv *internal.Conv) ddl.Sequence {
	id := internal.GenerateSequenceId()
	sequenceName := "Sequence" + id[1:]
	sequence := ddl.Sequence{
		Id:           id,
		Name:         sequenceName,
		SequenceKind: "BIT REVERSED SEQUENCE",
	}
	conv.ConvLock.Lock()
	defer conv.ConvLock.Unlock()
	srcSequences := conv.SrcSequences
	srcSequences[id] = sequence
	return sequence
}

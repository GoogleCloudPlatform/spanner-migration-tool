// Copyright 2025 Google LLC
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

package mysql_test

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"cloud.google.com/go/spanner"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/conversion"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
	"github.com/stretchr/testify/assert"
	"google.golang.org/api/iterator"
)

var expectedColumnDefsSpannerDialect = map[string]string {
	"bit_to_bool": "BOOL", "bit1_to_bool": "BOOL", "bitn_to_bytesmax": "BYTES(MAX)", "bitn_to_int64": "INT64", "bit_to_string": "STRING(MAX)", "bool_to_bool": "BOOL", "bool_to_string": "STRING(MAX)", "boolean_to_bool": "BOOL", "boolean_to_string": "STRING(MAX)", "tinyint1_to_bool": "BOOL", "tinyint_to_int64": "INT64", "tinyint_to_string": "STRING(MAX)", "smallint_to_int64": "INT64", "smallint_to_string": "STRING(MAX)", "mediumint_to_int64": "INT64", "mediumint_to_string": "STRING(MAX)", "int_to_int64": "INT64", "int_to_string": "STRING(MAX)", "integer_to_int64": "INT64", "integer_to_string": "STRING(MAX)", "bigint_to_int64": "INT64", "bigint_to_string": "STRING(MAX)", "decimal_to_numeric": "NUMERIC", "decimal_to_string": "STRING(MAX)", "dec_to_numeric": "NUMERIC", "dec_to_string": "STRING(MAX)", "numeric_to_numeric": "NUMERIC", "numeric_to_string": "STRING(MAX)", "large_decimal_to_numeric": "NUMERIC", "large_decimal_to_string": "STRING(MAX)", "large_dec_to_numeric": "NUMERIC", "large_dec_to_string": "STRING(MAX)", "large_numeric_to_numeric": "NUMERIC", "large_numeric_to_string": "STRING(MAX)", "float_to_float32": "FLOAT32", "float_to_string": "STRING(MAX)", "double_to_float64": "FLOAT64", "double_to_string": "STRING(MAX)", "double_precision_to_float64": "FLOAT64", "double_precision_to_string": "STRING(MAX)", "real_to_float64": "FLOAT64", "real_to_string": "STRING(MAX)", "date_to_date": "DATE", "date_to_string": "STRING(MAX)", "datetime_to_timestamp": "TIMESTAMP", "datetime_to_string": "STRING(MAX)", "timestamp_to_timestamp": "TIMESTAMP", "timestamp_to_string": "STRING(MAX)", "time_to_string": "STRING(MAX)", "year_to_string": "STRING(MAX)", "char_to_string1": "STRING(1)", "charn_to_stringn": "STRING(100)", "varcharn_to_stringn": "STRING(100)", "binary_to_bytesmax": "BYTES(MAX)", "binaryn_to_bytesmax": "BYTES(MAX)", "binary_to_string": "STRING(MAX)", "varbinaryn_to_bytesmax": "BYTES(MAX)", "varbinaryn_to_string": "STRING(MAX)", "tinyblob_to_bytes": "BYTES(255)", "tinyblob_to_string": "STRING(MAX)", "tinytext_to_string": "STRING(MAX)", "blob_to_bytes": "BYTES(65535)", "blobn_to_bytesn": "BYTES(500)", "blob_to_string": "STRING(MAX)", "text_to_string": "STRING(MAX)", "textn_to_string": "STRING(MAX)", "mediumblob_to_bytes": "BYTES(10485760)", "mediumblob_to_string": "STRING(MAX)", "mediumtext_to_string": "STRING(MAX)", "longblob_to_bytes": "BYTES(10485760)", "longblob_to_string": "STRING(MAX)", "longtext_to_string": "STRING(MAX)", "enum_to_string": "STRING(MAX)", "set_to_array": "ARRAY", "set_to_string": "STRING(MAX)", "json_to_json": "JSON", "json_to_string": "STRING(MAX)", "geom_to_string": "STRING(MAX)", "pnt_to_string": "STRING(MAX)", "linestr_to_string": "STRING(MAX)", "poly_to_string": "STRING(MAX)", "multipnt_to_string": "STRING(MAX)", "multilinestr_to_string": "STRING(MAX)", "multipoly_to_string": "STRING(MAX)", "geomcoll_to_string": "STRING(MAX)",
}

var expectedColumnDefsPostgresDialect = map[string]string {
	"bit_to_bool": "boolean", "bit1_to_bool": "boolean", "bitn_to_bytesmax": "bytea", "bitn_to_int64": "bigint", "bit_to_string": "character varying", "bool_to_bool": "boolean", "bool_to_string": "character varying", "boolean_to_bool": "boolean", "boolean_to_string": "character varying", "tinyint1_to_bool": "boolean", "tinyint_to_int64": "bigint", "tinyint_to_string": "character varying", "smallint_to_int64": "bigint", "smallint_to_string": "character varying", "mediumint_to_int64": "bigint", "mediumint_to_string": "character varying", "int_to_int64": "bigint", "int_to_string": "character varying", "integer_to_int64": "bigint", "integer_to_string": "character varying", "bigint_to_int64": "bigint", "bigint_to_string": "character varying", "decimal_to_numeric": "numeric", "decimal_to_string": "character varying", "dec_to_numeric": "numeric", "dec_to_string": "character varying", "numeric_to_numeric": "numeric", "numeric_to_string": "character varying", "large_decimal_to_numeric": "numeric", "large_decimal_to_string": "character varying", "large_dec_to_numeric": "numeric", "large_dec_to_string": "character varying", "large_numeric_to_numeric": "numeric", "large_numeric_to_string": "character varying", "float_to_float32": "real", "float_to_string": "character varying", "double_to_float64": "double precision", "double_to_string": "character varying", "double_precision_to_float64": "double precision", "double_precision_to_string": "character varying", "real_to_float64": "double precision", "real_to_string": "character varying", "date_to_date": "date", "date_to_string": "character varying", "datetime_to_timestamp": "timestamp with time zone", "datetime_to_string": "character varying", "timestamp_to_timestamp": "timestamp with time zone", "timestamp_to_string": "character varying", "time_to_string": "character varying", "year_to_string": "character varying", "char_to_string1": "character varying (1)", "charn_to_stringn": "character varying (100)", "varcharn_to_stringn": "character varying (100)", "binary_to_bytesmax": "bytea", "binaryn_to_bytesmax": "bytea", "binary_to_string": "character varying", "varbinaryn_to_bytesmax": "bytea", "varbinaryn_to_string": "character varying", "tinyblob_to_bytes": "bytea", "tinyblob_to_string": "character varying", "tinytext_to_string": "character varying", "blob_to_bytes": "bytea", "blobn_to_bytesn": "bytea", "blob_to_string": "character varying", "text_to_string": "character varying", "textn_to_string": "character varying", "mediumblob_to_bytes": "bytea", "mediumblob_to_string": "character varying", "mediumtext_to_string": "character varying", "longblob_to_bytes": "bytea", "longblob_to_string": "character varying", "longtext_to_string": "character varying", "enum_to_string": "character varying", "set_to_array": "array", "set_to_string": "character varying", "json_to_json": "jsonb", "json_to_string": "character varying", "geom_to_string": "character varying", "pnt_to_string": "character varying", "linestr_to_string": "character varying", "poly_to_string": "character varying", "multipnt_to_string": "character varying", "multilinestr_to_string": "character varying", "multipoly_to_string": "character varying", "geomcoll_to_string": "character varying",
}

func TestE2E_MySQLDUMP_CheckDataTypes(t *testing.T) {
	onlyRunForEndToEndTest(t)

	dialects := []string{constants.DIALECT_GOOGLESQL, constants.DIALECT_POSTGRESQL}

	tmpdir := prepareIntegrationTest(t)
	defer os.RemoveAll(tmpdir)

	for _, dialect := range dialects {
		t.Run(dialect, func(t *testing.T) {
			runMySQLDumpDataTypeTest(t, tmpdir, dialect)
		})
	}
}

func TestE2E_MySQL_CheckDataTypes(t *testing.T) {
	onlyRunForEndToEndTest(t)

	dialects := []string{constants.DIALECT_GOOGLESQL, constants.DIALECT_POSTGRESQL}

	tmpdir := prepareIntegrationTest(t)
	defer os.RemoveAll(tmpdir)

	for _, dialect := range dialects {
		t.Run(dialect, func(t *testing.T) {
			runMySQLDataTypeTest(t, tmpdir, dialect)
		})
	}
}

func runMySQLDumpDataTypeTest(t *testing.T, tmpdir string, dialect string) {
	dbName := "mysql-dump-data-types"
	if dialect == constants.DIALECT_POSTGRESQL {
		dbName += "-pg"
	}
	dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, dbName)
	defer dropDatabase(t, dbURI)

	filePrefix := filepath.Join(tmpdir, dbName)
	dumpFilePath := "../../test_data/mysql_data_types_dump.test.out"

	createInitialSessionFile(filePrefix, dbName, dialect, dumpFilePath)
	sessionFileName := filePrefix + ".session.json"
	updateSessionFileWithDataTypeMappings(sessionFileName)

	args := fmt.Sprintf("schema -prefix %s -source=mysql -target-profile='instance=%s,dbName=%s,project=%s,dialect=%s' -session=%s < %s", filePrefix, instanceID, dbName, projectID, dialect, sessionFileName, dumpFilePath)
	_, err := RunCommandReturningStdOut(args, projectID)

	assert.NoError(t, err)

	checkColumnDataTypes(t, dbURI, dialect)
}

func runMySQLDataTypeTest(t *testing.T, tmpdir string, dialect string) {
	dbName := "mysql-data-types"
	if dialect == constants.DIALECT_POSTGRESQL {
		dbName += "-pg"
	}
	dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, dbName)
	defer dropDatabase(t, dbURI)

	filePrefix := filepath.Join(tmpdir, dbName)

	host, user, srcDb, password := os.Getenv("MYSQLHOST"), os.Getenv("MYSQLUSER"), os.Getenv("MYSQLDB_DATA_TYPES"), os.Getenv("MYSQLPWD")

	createInitialSessionFileFromDB(filePrefix, dbName, dialect, host, user, srcDb, password)
	sessionFileName := filePrefix + ".session.json"
	updateSessionFileWithDataTypeMappings(sessionFileName)

	args := fmt.Sprintf("schema -prefix=%s -source=mysql -source-profile='host=%s,user=%s,dbName=%s,password=%s' -target-profile='instance=%s,dbName=%s,project=%s,dialect=%s' -session=%s", filePrefix, host, user, srcDb, password, instanceID, dbName, projectID, dialect, sessionFileName)
	_, err := RunCommandReturningStdOut(args, projectID)

	assert.NoError(t, err)

	checkColumnDataTypes(t, dbURI, dialect)
}

func createInitialSessionFileFromDB(filePrefix, dbName, dialect, host, user, srcDb, password string) {
	args := fmt.Sprintf("schema -dry-run -prefix=%s -source=mysql -source-profile='host=%s,user=%s,dbName=%s,password=%s' -target-profile='instance=%s,dbName=%s,project=%s,dialect=%s'", filePrefix, host, user, srcDb, password, instanceID, dbName, projectID, dialect)
	_, err := RunCommandReturningStdOut(args, projectID)
	if err != nil {
		log.Fatal(err)
	}
}

func checkColumnDataTypes(t *testing.T, dbURI string, dialect string) {
	client, err := spanner.NewClient(ctx, dbURI)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	var query spanner.Statement
	if dialect == constants.DIALECT_GOOGLESQL {
		query = spanner.Statement{SQL: `SELECT COLUMN_NAME, SPANNER_TYPE, NULL FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'AllDataTypes'`}
	} else {
		query = spanner.Statement{SQL: `SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'alldatatypes'`}
	}
	iter := client.Single().Query(ctx, query)
	defer iter.Stop()
	actualColumnDefs := make(map[string]string)
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		var columnName, columnType string
		var columnLength int64
		row.Columns(&columnName, &columnType, &columnLength)
		if columnLength != 0 {
			columnType = fmt.Sprintf("%s (%d)", columnType, columnLength)
		}
		actualColumnDefs[columnName] = columnType
	}

	delete(actualColumnDefs, "row_id")
	fmt.Println(actualColumnDefs)

	var expectedColumnDefs map[string]string
	if dialect == constants.DIALECT_GOOGLESQL {
		expectedColumnDefs = expectedColumnDefsSpannerDialect
	} else {
		expectedColumnDefs = expectedColumnDefsPostgresDialect
	}

	assert.Equal(t, expectedColumnDefs, actualColumnDefs)
}

func updateSessionFileWithDataTypeMappings(sessionFileName string) {
	conv := internal.MakeConv()
	err := conversion.ReadSessionFile(conv, sessionFileName)
	if err != nil {
		log.Fatal(err)
	}

	// We update the Conv object with the type mapping overrides and then update the session file with this new Conv
	updateConvWithDataTypeMappings(conv)
	conversion.WriteSessionFile(conv, sessionFileName, os.Stdout)
}

func updateConvWithDataTypeMappings(conv *internal.Conv) {
	tableId, err := internal.GetTableIdFromSpName(conv.SpSchema, "AllDataTypes")
	if err != nil {
		log.Fatal(err)
	}

	table := conv.SpSchema[tableId]
	for colId, colDef := range table.ColDefs {
		if strings.HasSuffix(colDef.Name, "_to_string") && colDef.T.Name != ddl.String {
			colDef.T = ddl.Type {
				Name: ddl.String,
				Len: ddl.MaxLength,
				IsArray: false,
			}
			table.ColDefs[colId] = colDef
		} else if colDef.Name == "bitn_to_int64" {
			colDef.T = ddl.Type {
				Name: ddl.Int64,
			}
			table.ColDefs[colId] = colDef
		}
	}
	conv.SpSchema[tableId] = table
}

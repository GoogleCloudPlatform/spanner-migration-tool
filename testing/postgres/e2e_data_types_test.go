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

package postgres_test

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
	"bit_to_bytes": "BYTES(MAX)", "bit_to_bool_array": "ARRAY<BOOL>", "bit_to_string": "STRING(MAX)", "bit_n_to_bytes": "BYTES(MAX)", "bit_n_to_bool_array": "ARRAY<BOOL>", "bit_n_to_string": "STRING(MAX)", "bit_varying_to_bytes": "BYTES(MAX)", "bit_varying_to_bool_array": "ARRAY<BOOL>", "bit_varying_to_string": "STRING(MAX)", "bit_varying_n_to_bytes": "BYTES(MAX)", "bit_varying_n_to_bool_array": "ARRAY<BOOL>", "bit_varying_n_to_string": "STRING(MAX)", "varbit_to_bytes": "BYTES(MAX)", "varbit_to_bool_array": "ARRAY<BOOL>", "varbit_to_string": "STRING(MAX)", "varbit_n_to_bytes": "BYTES(MAX)", "varbit_n_to_bool_array": "ARRAY<BOOL>", "varbit_n_to_string": "STRING(MAX)", "bool_to_bool": "BOOL", "bool_to_string": "STRING(MAX)", "bool_array_to_bool_array": "ARRAY<BOOL>", "bool_array_to_string": "STRING(MAX)", "boolean_to_bool": "BOOL", "boolean_to_string": "STRING(MAX)", "smallint_to_int64": "INT64", "smallint_to_string": "STRING(MAX)", "smallint_array_to_int64_array": "ARRAY<INT64>", "smallint_array_to_string": "STRING(MAX)", "int2_to_int64": "INT64", "int2_to_string": "STRING(MAX)", "int_to_int64": "INT64", "int_to_string": "STRING(MAX)", "int_array_to_int64_array": "ARRAY<INT64>", "int_array_to_string": "STRING(MAX)", "int4_to_int64": "INT64", "int4_to_string": "STRING(MAX)", "integer_to_int64": "INT64", "integer_to_string": "STRING(MAX)", "bigint_to_int64": "INT64", "bigint_to_string": "STRING(MAX)", "bigint_array_to_int64_array": "ARRAY<INT64>", "bigint_array_to_string": "STRING(MAX)", "int8_to_int64": "INT64", "int8_to_string": "STRING(MAX)", "smallserial_to_int64": "INT64", "smallserial_to_string": "STRING(MAX)", "serial2_to_int64": "INT64", "serial2_to_string": "STRING(MAX)", "serial_to_int64": "INT64", "serial_to_string": "STRING(MAX)", "serial4_to_int64": "INT64", "serial4_to_string": "STRING(MAX)", "bigserial_to_int64": "INT64", "bigserial_to_string": "STRING(MAX)", "serial8_to_int64": "INT64", "serial8_to_string": "STRING(MAX)", "decimal_to_numeric": "NUMERIC", "decimal_to_string": "STRING(MAX)", "numeric_to_numeric": "NUMERIC", "numeric_to_string": "STRING(MAX)", "large_decimal_to_numeric": "NUMERIC", "large_decimal_to_string": "STRING(MAX)", "large_numeric_to_numeric": "NUMERIC", "large_numeric_to_string": "STRING(MAX)", "float4_to_float32": "FLOAT32", "float4_to_float64": "FLOAT64", "float4_to_string": "STRING(MAX)", "real_to_float32": "FLOAT32", "real_to_float64": "FLOAT64", "real_to_string": "STRING(MAX)", "real_array_to_float32_array": "ARRAY<FLOAT32>", "real_array_to_string": "STRING(MAX)", "double_precision_to_float64": "FLOAT64", "double_precision_to_string": "STRING(MAX)", "float_to_float64": "FLOAT64", "float_to_string": "STRING(MAX)", "float_array_to_float64_array": "ARRAY<FLOAT64>", "float_array_to_string": "STRING(MAX)", "float8_to_float64": "FLOAT64", "float8_to_string": "STRING(MAX)", "date_to_date": "DATE", "date_to_string": "STRING(MAX)", "timestamp_to_timestamp": "TIMESTAMP", "timestamp_to_string": "STRING(MAX)", "timestamp_with_timezone_to_timestamp": "TIMESTAMP", "timestamp_with_timezone_to_string": "STRING(MAX)", "timestamptz_to_timestamp": "TIMESTAMP", "timestamptz_to_string": "STRING(MAX)", "time_to_string": "STRING(MAX)", "time_with_timezone_to_string": "STRING(MAX)", "timetz_to_string": "STRING(MAX)", "char_to_string": "STRING(1)", "char_n_to_string": "STRING(255)", "character_to_string": "STRING(1)", "character_n_to_string": "STRING(255)", "varchar_to_string": "STRING(MAX)", "varchar_n_to_string": "STRING(255)", "character_varying_to_string": "STRING(MAX)", "character_varying_n_to_string": "STRING(255)", "text_to_string": "STRING(MAX)", "bytea_to_bytes": "BYTES(MAX)", "bytea_to_string": "STRING(MAX)", "json_to_json": "JSON", "json_to_string": "STRING(MAX)", "jsonb_to_json": "JSON", "jsonb_to_string": "STRING(MAX)", "interval_to_int64": "INT64", "interval_to_string": "STRING(MAX)", "box_to_string": "STRING(MAX)", "box_to_float64_array": "ARRAY<FLOAT64>", "circle_to_string": "STRING(MAX)", "circle_to_float64_array": "ARRAY<FLOAT64>", "line_to_string": "STRING(MAX)", "line_to_float64_array": "ARRAY<FLOAT64>", "lseg_to_string": "STRING(MAX)", "lseg_to_float64_array": "ARRAY<FLOAT64>", "path_to_string": "STRING(MAX)", "path_to_float64_array": "ARRAY<FLOAT64>", "point_to_string": "STRING(MAX)", "point_to_float64_array": "ARRAY<FLOAT64>", "polygon_to_string": "STRING(MAX)", "polygon_to_float64_array": "ARRAY<FLOAT64>", "money_to_int64": "INT64", "money_to_string": "STRING(MAX)", "uuid_to_bytes": "BYTES(MAX)", "uuid_to_string": "STRING(MAX)", "xml_to_string": "STRING(MAX)", "cidr_to_string": "STRING(MAX)", "inet_to_string": "STRING(MAX)", "macaddr_to_string": "STRING(MAX)",
}

var expectedColumnDefsPostgresDialect = map[string]string {
	"bit_to_bytes": "bytea", "bit_to_bool_array": "boolean[]", "bit_to_string": "character varying", "bit_n_to_bytes": "bytea", "bit_n_to_bool_array": "boolean[]", "bit_n_to_string": "character varying", "bit_varying_to_bytes": "bytea", "bit_varying_to_bool_array": "boolean[]", "bit_varying_to_string": "character varying", "bit_varying_n_to_bytes": "bytea", "bit_varying_n_to_bool_array": "boolean[]", "bit_varying_n_to_string": "character varying", "varbit_to_bytes": "bytea", "varbit_to_bool_array": "boolean[]", "varbit_to_string": "character varying", "varbit_n_to_bytes": "bytea", "varbit_n_to_bool_array": "boolean[]", "varbit_n_to_string": "character varying", "bool_to_bool": "boolean", "bool_to_string": "character varying", "bool_array_to_bool_array": "boolean[]", "bool_array_to_string": "character varying", "boolean_to_bool": "boolean", "boolean_to_string": "character varying", "smallint_to_int64": "bigint", "smallint_to_string": "character varying", "smallint_array_to_int64_array": "bigint[]", "smallint_array_to_string": "character varying", "int2_to_int64": "bigint", "int2_to_string": "character varying", "int_to_int64": "bigint", "int_to_string": "character varying", "int_array_to_int64_array": "bigint[]", "int_array_to_string": "character varying", "int4_to_int64": "bigint", "int4_to_string": "character varying", "integer_to_int64": "bigint", "integer_to_string": "character varying", "bigint_to_int64": "bigint", "bigint_to_string": "character varying", "bigint_array_to_int64_array": "bigint[]", "bigint_array_to_string": "character varying", "int8_to_int64": "bigint", "int8_to_string": "character varying", "smallserial_to_int64": "bigint", "smallserial_to_string": "character varying", "serial2_to_int64": "bigint", "serial2_to_string": "character varying", "serial_to_int64": "bigint", "serial_to_string": "character varying", "serial4_to_int64": "bigint", "serial4_to_string": "character varying", "bigserial_to_int64": "bigint", "bigserial_to_string": "character varying", "serial8_to_int64": "bigint", "serial8_to_string": "character varying", "decimal_to_numeric": "numeric", "decimal_to_string": "character varying", "numeric_to_numeric": "numeric", "numeric_to_string": "character varying", "large_decimal_to_numeric": "numeric", "large_decimal_to_string": "character varying", "large_numeric_to_numeric": "numeric", "large_numeric_to_string": "character varying", "float4_to_float32": "real", "float4_to_float64": "double precision", "float4_to_string": "character varying", "real_to_float32": "real", "real_to_float64": "double precision", "real_to_string": "character varying", "real_array_to_float32_array": "real[]", "real_array_to_string": "character varying", "double_precision_to_float64": "double precision", "double_precision_to_string": "character varying", "float_to_float64": "double precision", "float_to_string": "character varying", "float_array_to_float64_array": "ARRAY<double precision>", "float_array_to_string": "character varying", "float8_to_float64": "double precision", "float8_to_string": "character varying", "date_to_date": "date", "date_to_string": "character varying", "timestamp_to_timestamp": "timestamp with time zone", "timestamp_to_string": "character varying", "timestamp_with_timezone_to_timestamp": "timestamp with time zone", "timestamp_with_timezone_to_string": "character varying", "timestamptz_to_timestamp": "timestamp with time zone", "timestamptz_to_string": "character varying", "time_to_string": "character varying", "time_with_timezone_to_string": "character varying", "timetz_to_string": "character varying", "char_to_string": "character varying (1)", "char_n_to_string": "character varying (255)", "character_to_string": "character varying (1)", "character_n_to_string": "character varying (255)", "varchar_to_string": "character varying", "varchar_n_to_string": "character varying (255)", "character_varying_to_string": "character varying", "character_varying_n_to_string": "character varying (255)", "text_to_string": "character varying", "bytea_to_bytes": "bytea", "bytea_to_string": "character varying", "json_to_json": "jsonb", "json_to_string": "character varying", "jsonb_to_json": "jsonb", "jsonb_to_string": "character varying", "interval_to_int64": "bigint", "interval_to_string": "character varying", "box_to_string": "character varying", "box_to_float64_array": "double precision[]", "circle_to_string": "character varying", "circle_to_float64_array": "double precision[]", "line_to_string": "character varying", "line_to_float64_array": "double precision[]", "lseg_to_string": "character varying", "lseg_to_float64_array": "double precision[]", "path_to_string": "character varying", "path_to_float64_array": "double precision[]", "point_to_string": "character varying", "point_to_float64_array": "double precision[]", "polygon_to_string": "character varying", "polygon_to_float64_array": "double precision[]", "money_to_int64": "bigint", "money_to_string": "character varying", "uuid_to_bytes": "bytea", "uuid_to_string": "character varying", "xml_to_string": "character varying", "cidr_to_string": "character varying", "inet_to_string": "character varying", "macaddr_to_string": "character varying",
}

func TestE2E_PGDUMP_CheckDataTypes(t *testing.T) {
	onlyRunForEndToEndTest(t)

	dialects := []string{constants.DIALECT_GOOGLESQL, constants.DIALECT_POSTGRESQL}

	tmpdir := prepareIntegrationTest(t)
	defer os.RemoveAll(tmpdir)

	for _, dialect := range dialects {
		t.Run(dialect, func(t *testing.T) {
			runPgDumpDataTypeTest(t, tmpdir, dialect)
		})
	}
}

func TestE2E_PostgreSQL_CheckDataTypes(t *testing.T) {
	onlyRunForEndToEndTest(t)

	dialects := []string{constants.DIALECT_GOOGLESQL, constants.DIALECT_POSTGRESQL}

	tmpdir := prepareIntegrationTest(t)
	defer os.RemoveAll(tmpdir)

	for _, dialect := range dialects {
		t.Run(dialect, func(t *testing.T) {
			runPostgreSQLDataTypeTest(t, tmpdir, dialect)
		})
	}
}

func runPgDumpDataTypeTest(t *testing.T, tmpdir string, dialect string) {
	dbName := "pg-dump-data-types"
	if dialect == constants.DIALECT_POSTGRESQL {
		dbName += "-pg"
	}
	dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, dbName)
	defer dropDatabase(t, dbURI)

	filePrefix := filepath.Join(tmpdir, dbName)
	dumpFilePath := "../../test_data/postgres_data_types_dump.test.out"

	createInitialSessionFile(filePrefix, dbName, dialect, dumpFilePath)
	sessionFileName := filePrefix + ".session.json"
	updateSessionFileWithDataTypeMappings(sessionFileName)

	args := fmt.Sprintf("schema -prefix %s -source=postgres -target-profile='instance=%s,dbName=%s,project=%s,dialect=%s' -session=%s < %s", filePrefix, instanceID, dbName, projectID, dialect, sessionFileName, dumpFilePath)
	_, err := RunCommandReturningStdOut(args, projectID)

	assert.NoError(t, err)

	checkColumnDataTypes(t, dbURI, dialect)
}

func runPostgreSQLDataTypeTest(t *testing.T, tmpdir string, dialect string) {
	dbName := "postgresql-data-types"
	if dialect == constants.DIALECT_POSTGRESQL {
		dbName += "-pg"
	}
	dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, dbName)
	defer dropDatabase(t, dbURI)

	filePrefix := filepath.Join(tmpdir, dbName)

	host, port, user, srcDb, password := os.Getenv("PGHOST"), os.Getenv("PGPORT"), os.Getenv("PGUSER"), os.Getenv("PGDB_DATA_TYPES"), os.Getenv("PGPASSWORD")

	createInitialSessionFileFromDB(filePrefix, dbName, dialect, host, port, user, srcDb, password)
	sessionFileName := filePrefix + ".session.json"
	updateSessionFileWithDataTypeMappings(sessionFileName)

	args := fmt.Sprintf("schema -prefix=%s -source=postgres -source-profile='host=%s,port=%s,user=%s,dbName=%s,password=%s' -target-profile='instance=%s,dbName=%s,project=%s,dialect=%s' -session=%s", filePrefix, host, port, user, srcDb, password, instanceID, dbName, projectID, dialect, sessionFileName)
	_, err := RunCommandReturningStdOut(args, projectID)

	assert.NoError(t, err)

	checkColumnDataTypes(t, dbURI, dialect)
}

func createInitialSessionFileFromDB(filePrefix, dbName, dialect, host, port, user, srcDb, password string) {
	args := fmt.Sprintf("schema -dry-run -prefix=%s -source=postgres -source-profile='host=%s,port=%s,user=%s,dbName=%s,password=%s' -target-profile='instance=%s,dbName=%s,project=%s,dialect=%s'", filePrefix, host, port, user, srcDb, password, instanceID, dbName, projectID, dialect)
	fmt.Println(args)
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
		query = spanner.Statement{SQL: `SELECT COLUMN_NAME, SPANNER_TYPE, NULL FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'all_data_types'`}
	} else {
		query = spanner.Statement{SQL: `SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'all_data_types'`}
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
	tableId, err := internal.GetTableIdFromSpName(conv.SpSchema, "all_data_types")
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
			colDef.AutoGen = ddl.AutoGenCol{}
			table.ColDefs[colId] = colDef
		} else if colDef.Name == "float4_to_float64" || colDef.Name == "real_to_float64" {
			colDef.T = ddl.Type {
				Name: ddl.Float64,
			}
			table.ColDefs[colId] = colDef
		}
	}
	conv.SpSchema[tableId] = table
}

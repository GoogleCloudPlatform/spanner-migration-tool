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

package mysql_test

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	spanneraccessor "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/spanner"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/conversion"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/stretchr/testify/assert"
	"google.golang.org/api/iterator"

	"cloud.google.com/go/spanner"
)

type TableLimitTestCase struct {
	name string

	dialect string
	ddls []string

	expectError bool
	expectErrorMessageContains string
	expectedTableLevelSchemaIssues map[string][]internal.SchemaIssue

	expectedNumberOfTablesCreated int64
	expectedNumberOfColumnsPerTable map[string]int64
	expectedNumberOfPrimaryKeyColumnsPerTable map[string]int64
}

type IndexLimitTestCase struct {
	name string

	dialect string
	ddls []string

	expectError bool
	expectErrorMessageContains string

	expectedTotalNumberOfIndexes int64
	expectedNumberOfIndexesPerTable map[string]int64
}

func TestE2E_CheckTableLimits(t *testing.T) {
	onlyRunForEndToEndTest(t)

	testCases := []TableLimitTestCase {
		{
			name: "Spanner dialect with more than 5000 tables",

			dialect: constants.DIALECT_GOOGLESQL,
			ddls: generateCreateTableDdls(5001),

			expectError: true,
			expectErrorMessageContains: "too many tables",
		},
		{
			name: "Postgres dialect with more than 5000 tables",

			dialect: constants.DIALECT_POSTGRESQL,
			ddls: generateCreateTableDdls(5001),

			expectError: true,
			expectErrorMessageContains: "too many tables",
		},
		{
			name: "Spanner dialect with exactly 5000 tables",

			dialect: constants.DIALECT_GOOGLESQL,
			ddls: generateCreateTableDdls(5000),

			expectedNumberOfTablesCreated: 5000,
		},
		{
			name: "Postgres dialect with exactly 5000 tables",

			dialect: constants.DIALECT_POSTGRESQL,
			ddls: generateCreateTableDdls(5000),

			expectedNumberOfTablesCreated: 5000,
		},
		{
			name: "Spanner dialect with table name longer than 128 chars",

			dialect: constants.DIALECT_GOOGLESQL,
			ddls: []string{generateCreateTableDdlWithName(strings.Repeat("t", 129))},

			expectError: true,
			expectErrorMessageContains: "table name not valid",
		},
		{
			name: "Postgres dialect with table name longer than 128 chars",

			dialect: constants.DIALECT_POSTGRESQL,
			ddls: []string{generateCreateTableDdlWithName(strings.Repeat("t", 129))},

			expectError: true,
			expectErrorMessageContains: "table name not valid",
		},
		{
			name: "Spanner dialect with table name exactly 128 chars",

			dialect: constants.DIALECT_GOOGLESQL,
			ddls: []string{generateCreateTableDdlWithName(strings.Repeat("t", 128))},

			expectedNumberOfTablesCreated: 1,
		},
		{
			name: "Postgres dialect with table name exactly 128 chars",

			dialect: constants.DIALECT_POSTGRESQL,
			ddls: []string{generateCreateTableDdlWithName(strings.Repeat("t", 128))},

			expectedNumberOfTablesCreated: 1,
		},
		{
			name: "Spanner dialect with table name exactly 1 char",

			dialect: constants.DIALECT_GOOGLESQL,
			ddls: []string{generateCreateTableDdlWithName(strings.Repeat("t", 1))},

			expectedNumberOfTablesCreated: 1,
		},
		{
			name: "Postgres dialect with table name exactly 1 char",

			dialect: constants.DIALECT_POSTGRESQL,
			ddls: []string{generateCreateTableDdlWithName(strings.Repeat("t", 1))},

			expectedNumberOfTablesCreated: 1,
		},
		{
			name: "Spanner dialect with table with more than 1024 columns",

			dialect: constants.DIALECT_GOOGLESQL,
			ddls: []string{generateCreateTableDdlWithColumns("t1", 1025)},

			expectError: true,
			expectErrorMessageContains: "too many columns",
		},
		{
			name: "Postgres dialect with table with more than 1024 columns",

			dialect: constants.DIALECT_POSTGRESQL,
			ddls: []string{generateCreateTableDdlWithColumns("t1", 1025)},

			expectError: true,
			expectErrorMessageContains: "too many columns",
		},
		{
			name: "Spanner dialect with table with exactly 1024 columns",

			dialect: constants.DIALECT_GOOGLESQL,
			ddls: []string{generateCreateTableDdlWithColumns("t1", 1024)},

			expectedNumberOfTablesCreated: 1,
			expectedNumberOfColumnsPerTable: map[string]int64{"t1": 1024},
		},
		{
			name: "Postgres dialect with table with exactly 1024 columns",

			dialect: constants.DIALECT_POSTGRESQL,
			ddls: []string{generateCreateTableDdlWithColumns("t1", 1024)},

			expectedNumberOfTablesCreated: 1,
			expectedNumberOfColumnsPerTable: map[string]int64{"t1": 1024},
		},
		{
			name: "Spanner dialect with table with column name longer than 128 chars",

			dialect: constants.DIALECT_GOOGLESQL,
			ddls: []string{generateCreateTableDdlWithColumnNames("t1", []string{"c1", strings.Repeat("c", 129)})},

			expectError: true,
			expectErrorMessageContains: "Column name not valid",
		},
		{
			name: "Postgres dialect with table with column name longer than 128 chars",

			dialect: constants.DIALECT_POSTGRESQL,
			ddls: []string{generateCreateTableDdlWithColumnNames("t1", []string{"c1", strings.Repeat("c", 129)})},

			expectError: true,
			expectErrorMessageContains: "Column name not valid",
		},
		{
			name: "Spanner dialect with table with column name exactly 128 chars",

			dialect: constants.DIALECT_GOOGLESQL,
			ddls: []string{generateCreateTableDdlWithColumnNames("t1", []string{"c1", strings.Repeat("c", 128)})},

			expectedNumberOfTablesCreated: 1,
			expectedNumberOfColumnsPerTable: map[string]int64{"t1": 2},
		},
		{
			name: "Postgres dialect with table with column name exactly 128 chars",

			dialect: constants.DIALECT_POSTGRESQL,
			ddls: []string{generateCreateTableDdlWithColumnNames("t1", []string{"c1", strings.Repeat("c", 128)})},

			expectedNumberOfTablesCreated: 1,
			expectedNumberOfColumnsPerTable: map[string]int64{"t1": 2},
		},
		{
			name: "Spanner dialect with table with column name exactly 1 char",

			dialect: constants.DIALECT_GOOGLESQL,
			ddls: []string{generateCreateTableDdlWithColumnNames("t1", []string{"c1", strings.Repeat("c", 1)})},

			expectedNumberOfTablesCreated: 1,
			expectedNumberOfColumnsPerTable: map[string]int64{"t1": 2},
		},
		{
			name: "Postgres dialect with table with column name exactly 1 char",

			dialect: constants.DIALECT_POSTGRESQL,
			ddls: []string{generateCreateTableDdlWithColumnNames("t1", []string{"c1", strings.Repeat("c", 1)})},

			expectedNumberOfTablesCreated: 1,
			expectedNumberOfColumnsPerTable: map[string]int64{"t1": 2},
		},
		{
			name: "Spanner dialect with table with primary key with more than 16 columns",

			dialect: constants.DIALECT_GOOGLESQL,
			ddls: []string{generateCreateTableDdlWithPrimaryKeys("t1", 17)},

			expectError: true,
			expectErrorMessageContains: "too many keys",
		},
		{
			name: "Postgres dialect with table with primary key with more than 16 columns",

			dialect: constants.DIALECT_POSTGRESQL,
			ddls: []string{generateCreateTableDdlWithPrimaryKeys("t1", 17)},

			expectError: true,
			expectErrorMessageContains: "too many keys",
		},
		{
			name: "Spanner dialect with table with primary key with exactly 16 columns",

			dialect: constants.DIALECT_GOOGLESQL,
			ddls: []string{generateCreateTableDdlWithPrimaryKeys("t1", 16)},

			expectedNumberOfTablesCreated: 1,
			expectedNumberOfPrimaryKeyColumnsPerTable: map[string]int64{"t1": 16},
		},
		{
			name: "Postgres dialect with table with primary key with exactly 16 columns",

			dialect: constants.DIALECT_POSTGRESQL,
			ddls: []string{generateCreateTableDdlWithPrimaryKeys("t1", 16)},

			expectedNumberOfTablesCreated: 1,
			expectedNumberOfPrimaryKeyColumnsPerTable: map[string]int64{"t1": 16},
		},
		{
			name: "Spanner dialect with table with primary key with size larger than 8KiB",

			dialect: constants.DIALECT_GOOGLESQL,
			ddls: []string{generateCreateTableDdl("t1", map[string]string{"c1": "binary(4096)", "c2": "binary(4096)", "c3": "binary(1)"}, []string{"c1", "c2", "c3"})},

			expectError: true,
			expectErrorMessageContains: "exceeds the maximum",
		},
		{
			name: "Postgres dialect with table with primary key with size larger than 8KiB",

			dialect: constants.DIALECT_POSTGRESQL,
			ddls: []string{generateCreateTableDdl("t1", map[string]string{"c1": "binary(4096)", "c2": "binary(4096)", "c3": "binary(1)"}, []string{"c1", "c2", "c3"})},

			expectError: true,
			expectErrorMessageContains: "exceeds the maximum",
		},
		{
			name: "Spanner dialect with table with primary key with size exactly 8KiB",

			dialect: constants.DIALECT_GOOGLESQL,
			ddls: []string{generateCreateTableDdl("t1", map[string]string{"c1": "binary(4096)", "c2": "binary(4096)", "c3": "binary(1)"}, []string{"c1", "c2"})},

			expectedNumberOfTablesCreated: 1,
			expectedNumberOfPrimaryKeyColumnsPerTable: map[string]int64{"t1": 2},
		},
		{
			name: "Postgres dialect with table with primary key with size exactly 8KiB",

			dialect: constants.DIALECT_POSTGRESQL,
			ddls: []string{generateCreateTableDdl("t1", map[string]string{"c1": "binary(4096)", "c2": "binary(4096)", "c3": "binary(1)"}, []string{"c1", "c2"})},

			expectedNumberOfTablesCreated: 1,
			expectedNumberOfPrimaryKeyColumnsPerTable: map[string]int64{"t1": 2},
		},
		{
			name: "Spanner dialect with table with non-key columns with size larger than 1600MiB",

			dialect: constants.DIALECT_GOOGLESQL,
			ddls: []string{generateCreateTableDdlLargeTable("t1", 160, 1)},

			expectError: true,
			expectedTableLevelSchemaIssues: map[string][]internal.SchemaIssue{
				"t1": []internal.SchemaIssue{internal.RowLimitExceeded},
			},
		},
		{
			name: "Postgres dialect with table with non-key columns with size larger than 1600MiB",

			dialect: constants.DIALECT_POSTGRESQL,
			ddls: []string{generateCreateTableDdlLargeTable("t1", 160, 1)},

			expectError: true,
			expectedTableLevelSchemaIssues: map[string][]internal.SchemaIssue{
				"t1": []internal.SchemaIssue{internal.RowLimitExceeded},
			},
		},
		{
			name: "Spanner dialect with table with non-key columns with size exactly 1600MiB",

			dialect: constants.DIALECT_GOOGLESQL,
			ddls: []string{generateCreateTableDdlLargeTable("t1", 160, 0)},

			expectedNumberOfTablesCreated: 1,
		},
		{
			name: "Postgres dialect with table with non-key columns with size exactly 1600MiB",

			dialect: constants.DIALECT_POSTGRESQL,
			ddls: []string{generateCreateTableDdlLargeTable("t1", 160, 0)},

			expectedNumberOfTablesCreated: 1,
		},
	}

	tmpdir := prepareIntegrationTest(t)
	defer os.RemoveAll(tmpdir)

	for idx, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			runTableLimitTestCase(t, tmpdir, tc, idx)
		})
	}
}

func TestE2E_CheckIndexLimits(t *testing.T) {
	onlyRunForEndToEndTest(t)

	testCases := []IndexLimitTestCase {
		{
			name: "Spanner dialect with more than 10000 indexes",

			dialect: constants.DIALECT_GOOGLESQL,
			ddls: generateCreateIndexDdls(10001),

			expectError: true,
			expectErrorMessageContains: "too many indices",
		},
		{
			name: "Postgres dialect with more than 10000 indexes",

			dialect: constants.DIALECT_POSTGRESQL,
			ddls: generateCreateIndexDdls(10001),

			expectError: true,
			expectErrorMessageContains: "too many indices",
		},
		{
			name: "Spanner dialect with exactly 10000 indexes",

			dialect: constants.DIALECT_GOOGLESQL,
			ddls: generateCreateIndexDdls(10000),

			expectedTotalNumberOfIndexes: 10000,
		},
		{
			name: "Postgres dialect with exactly 10000 indexes",

			dialect: constants.DIALECT_POSTGRESQL,
			ddls: generateCreateIndexDdls(10000),

			expectedTotalNumberOfIndexes: 10000,
		},
		{
			name: "Spanner dialect with table with more than 128 indexes",

			dialect: constants.DIALECT_GOOGLESQL,
			ddls: generateCreateTableDdlWithIndexDdls("t1", 129),

			expectError: true,
			expectErrorMessageContains: "too many indices",
		},
		{
			name: "Postgres dialect with table with more than 128 indexes",

			dialect: constants.DIALECT_POSTGRESQL,
			ddls: generateCreateTableDdlWithIndexDdls("t1", 129),

			expectError: true,
			expectErrorMessageContains: "too many indices",
		},
		{
			name: "Spanner dialect with table with exactly 128 indexes",

			dialect: constants.DIALECT_GOOGLESQL,
			ddls: generateCreateTableDdlWithIndexDdls("t1", 128),

			expectedNumberOfIndexesPerTable: map[string]int64{"t1": 128},
		},
		{
			name: "Postgres dialect with table with exactly 128 indexes",

			dialect: constants.DIALECT_POSTGRESQL,
			ddls: generateCreateTableDdlWithIndexDdls("t1", 128),

			expectedNumberOfIndexesPerTable: map[string]int64{"t1": 128},
		},
	}

	tmpdir := prepareIntegrationTest(t)
	defer os.RemoveAll(tmpdir)

	for idx, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			runIndexLimitTestCase(t, tmpdir, tc, idx)
		})
	}
}

func runTableLimitTestCase(t *testing.T, tmpdir string, tc TableLimitTestCase, index int) {
	dbName := "mysql-table-limits"
	dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, dbName)
	defer dropDatabase(t, dbURI)

	filePrefix := filepath.Join(tmpdir, dbName + strconv.Itoa(index))
	dumpFilePath := filepath.Join(tmpdir, dbName + strconv.Itoa(index) + "_dump.sql")

	writeDumpFile(t, dumpFilePath, tc.ddls)

	args := fmt.Sprintf("schema -prefix %s -source=mysql -target-profile='instance=%s,dbName=%s,project=%s,dialect=%s' < %s", filePrefix, instanceID, dbName, projectID, tc.dialect, dumpFilePath)
	stdout, err := RunCommandReturningStdOut(args, projectID)

	if tc.expectError {
		assert.Error(t, err)

		output := stdout
		if err != nil {
			output += err.Error()
		}

		assert.Contains(t, output, tc.expectErrorMessageContains)
		checkSchemaIssues(t, filePrefix, tc.expectedTableLevelSchemaIssues)
		checkDatabaseNotCreatedOrEmpty(t, dbURI, tc.dialect)
	} else {
		assert.NoError(t, err)
		checkSchemaIssues(t, filePrefix, tc.expectedTableLevelSchemaIssues)
		checkDatabaseSchema(t, dbURI, tc)
	}
}

func runIndexLimitTestCase(t *testing.T, tmpdir string, tc IndexLimitTestCase, index int) {
	dbName := "mysql-index-limits"
	dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, dbName)
	defer dropDatabase(t, dbURI)

	filePrefix := filepath.Join(tmpdir, dbName + strconv.Itoa(index))
	dumpFilePath := filepath.Join(tmpdir, dbName + strconv.Itoa(index) + "_dump.sql")

	writeDumpFile(t, dumpFilePath, tc.ddls)

	args := fmt.Sprintf("schema -prefix %s -source=mysql -target-profile='instance=%s,dbName=%s,project=%s,dialect=%s' < %s", filePrefix, instanceID, dbName, projectID, tc.dialect, dumpFilePath)
	stdout, err := RunCommandReturningStdOut(args, projectID)

	if tc.expectError {
		assert.Error(t, err)

		output := stdout
		if err != nil {
			output += err.Error()
		}

		assert.Contains(t, output, tc.expectErrorMessageContains)
		checkDatabaseNotCreatedOrEmpty(t, dbURI, tc.dialect)
	} else {
		assert.NoError(t, err)
		checkDatabaseIndexes(t, dbURI, tc)
	}
}

func checkSchemaIssues(t *testing.T, filePrefix string, expectedTableLevelSchemaIssues map[string][]internal.SchemaIssue) {
	conv := internal.MakeConv()
	err := conversion.ReadSessionFile(conv, filePrefix + ".session.json")
	if err != nil {
		t.Fatal(err)
	}
	for table, issues := range expectedTableLevelSchemaIssues {
		assert.Equal(t, conv.SchemaIssues[table].TableLevelIssues, issues)
	}
}

func checkDatabaseNotCreatedOrEmpty(t *testing.T, dbURI, dialect string) {
	sp, err := spanneraccessor.NewSpannerAccessorClientImpl(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(ctx)
	dbExists, err := sp.CheckExistingDb(ctx, dbURI)
	if err != nil {
		log.Fatal(err)
	}
	// The Postgres dialect creates the DB and adds tables in two separate calls, so the DB will exist but it
	// should be empty
	if dialect == constants.DIALECT_POSTGRESQL {
		assert.True(t, dbExists)

		client, err := spanner.NewClient(ctx, dbURI)
		if err != nil {
			log.Fatal(err)
		}
		defer client.Close()

		checkNumberOfTables(t, client, 0)
	} else {
		assert.False(t, dbExists)
	}
}

func checkDatabaseSchema(t *testing.T, dbURI string, tc TableLimitTestCase) {
	client, err := spanner.NewClient(ctx, dbURI)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	checkNumberOfTables(t, client, tc.expectedNumberOfTablesCreated)
	checkNumberOfColumns(t, client, tc.expectedNumberOfColumnsPerTable)
	checkNumberOfPrimaryKeyColumns(t, client, tc.expectedNumberOfPrimaryKeyColumnsPerTable)
}

func checkNumberOfTables(t *testing.T, client *spanner.Client, expectedNumberOfTablesCreated int64) {
	query := spanner.Statement{SQL: `SELECT count(1) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA', 'SPANNER_SYS') AND TABLE_TYPE = 'BASE TABLE'`}
	iter := client.Single().Query(ctx, query)
	defer iter.Stop()
	var numberOfTablesCreated int64
	row, err := iter.Next()
	if err != nil {
		t.Fatal(err)
	}
	row.Columns(&numberOfTablesCreated)

	assert.Equal(t, expectedNumberOfTablesCreated, numberOfTablesCreated)
}

func checkNumberOfColumns(t *testing.T, client *spanner.Client, expectedNumberOfColumnsPerTable map[string]int64) {
	if len(expectedNumberOfColumnsPerTable) == 0 {
		return
	}

	tableNames := make([]string, 0, len(expectedNumberOfColumnsPerTable))
	for table := range expectedNumberOfColumnsPerTable {
		tableNames = append(tableNames, table)
	}

	query := spanner.Statement{
		SQL: fmt.Sprintf("SELECT TABLE_NAME, count(1) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME IN ('%s') GROUP BY TABLE_NAME", strings.Join(tableNames, "', '")),
	}
	iter := client.Single().Query(ctx, query)
	defer iter.Stop()
	var tableName string
	var numberOfColumns int64
	actualNumberOfColumnsPerTable := make(map[string]int64)
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		row.Columns(&tableName, &numberOfColumns)
		actualNumberOfColumnsPerTable[tableName] = numberOfColumns
	}

	assert.Equal(t, expectedNumberOfColumnsPerTable, actualNumberOfColumnsPerTable)
}

func checkNumberOfPrimaryKeyColumns(t *testing.T, client *spanner.Client, expectedNumberOfPrimaryKeyColumnsPerTable map[string]int64) {
	if len(expectedNumberOfPrimaryKeyColumnsPerTable) == 0 {
		return
	}

	tableNames := make([]string, 0, len(expectedNumberOfPrimaryKeyColumnsPerTable))
	for table := range expectedNumberOfPrimaryKeyColumnsPerTable {
		tableNames = append(tableNames, table)
	}

	var query spanner.Statement
	query = spanner.Statement{
		SQL: fmt.Sprintf("SELECT TABLE_NAME, count(1) FROM INFORMATION_SCHEMA.INDEX_COLUMNS WHERE TABLE_NAME IN ('%s') AND INDEX_TYPE = 'PRIMARY_KEY' GROUP BY TABLE_NAME", strings.Join(tableNames, "', '")),
	}
	iter := client.Single().Query(ctx, query)
	defer iter.Stop()
	var tableName string
	var numberOfPrimaryKeyColumns int64
	actualNumberOfPrimaryKeyColumnsPerTable := make(map[string]int64)
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		row.Columns(&tableName, &numberOfPrimaryKeyColumns)
		actualNumberOfPrimaryKeyColumnsPerTable[tableName] = numberOfPrimaryKeyColumns
	}

	assert.Equal(t, expectedNumberOfPrimaryKeyColumnsPerTable, actualNumberOfPrimaryKeyColumnsPerTable)
}

func checkDatabaseIndexes(t *testing.T, dbURI string, tc IndexLimitTestCase) {
	client, err := spanner.NewClient(ctx, dbURI)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	if tc.expectedTotalNumberOfIndexes != 0 {
		checkTotalNumberOfIndexes(t, client, tc.expectedTotalNumberOfIndexes)
	}
	checkNumberOfIndexesPerTable(t, client, tc.expectedNumberOfIndexesPerTable)
}

func checkTotalNumberOfIndexes(t *testing.T, client *spanner.Client, expectedTotalNumberOfIndexes int64) {
	query := spanner.Statement{SQL: `SELECT count(1) FROM INFORMATION_SCHEMA.INDEXES WHERE TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA', 'SPANNER_SYS') AND INDEX_TYPE != 'PRIMARY_KEY'`}
	iter := client.Single().Query(ctx, query)
	defer iter.Stop()
	var totalNumberOfIndexes int64
	row, err := iter.Next()
	if err != nil {
		t.Fatal(err)
	}
	row.Columns(&totalNumberOfIndexes)

	assert.Equal(t, expectedTotalNumberOfIndexes, totalNumberOfIndexes)
}

func checkNumberOfIndexesPerTable(t *testing.T, client *spanner.Client, expectedNumberOfIndexesPerTable map[string]int64) {
	if len(expectedNumberOfIndexesPerTable) == 0 {
		return
	}

	tableNames := make([]string, 0, len(expectedNumberOfIndexesPerTable))
	for table := range expectedNumberOfIndexesPerTable {
		tableNames = append(tableNames, table)
	}

	var query spanner.Statement
	query = spanner.Statement{
		SQL: fmt.Sprintf("SELECT TABLE_NAME, count(1) FROM INFORMATION_SCHEMA.INDEXES WHERE TABLE_NAME IN ('%s') AND INDEX_TYPE != 'PRIMARY_KEY' GROUP BY TABLE_NAME", strings.Join(tableNames, "', '")),
	}
	iter := client.Single().Query(ctx, query)
	defer iter.Stop()
	var tableName string
	var numberOfIndexes int64
	actualNumberOfIndexesPerTable := make(map[string]int64)
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		row.Columns(&tableName, &numberOfIndexes)
		actualNumberOfIndexesPerTable[tableName] = numberOfIndexes
	}

	assert.Equal(t, expectedNumberOfIndexesPerTable, actualNumberOfIndexesPerTable)
}

func generateCreateTableDdls(numTables int) []string {
	tableDdls := make([]string, 0)
	for i := 1; i <= numTables; i++ {
		tableName := fmt.Sprintf("Table%d", i)
		tableDdls = append(tableDdls, generateCreateTableDdlWithName(tableName))
	}
	return tableDdls
}

func generateCreateTableDdlWithName(tableName string) string {
	return generateCreateTableDdlWithColumns(tableName, 1)
}

func generateCreateTableDdlWithColumns(tableName string, numColumns int) string {
	columnNames := make([]string, 0, numColumns)
	for i := 1; i <= numColumns; i++ {
		columnNames = append(columnNames, fmt.Sprintf("c%d", i))
	}
	return generateCreateTableDdlWithColumnNames(tableName, columnNames)
}

func generateCreateTableDdlWithColumnNames(tableName string, columnNames []string) string {
	columns := make(map[string]string, len(columnNames))
	for _, columnName := range columnNames {
		columns[columnName] = "int"
	}
	return generateCreateTableDdl(tableName, columns, columnNames[:1])
}

func generateCreateTableDdlWithPrimaryKeys(tableName string, numPrimaryKeyColumns int) string {
	columns := make(map[string]string, numPrimaryKeyColumns)
	primaryKeyColumns := make([]string, 0, numPrimaryKeyColumns)
	for i := 1; i <= numPrimaryKeyColumns; i++ {
		columnName := fmt.Sprintf("c%d", i)
		columns[columnName] = "int"
		primaryKeyColumns = append(primaryKeyColumns, columnName)
	}
	return generateCreateTableDdl(tableName, columns, primaryKeyColumns)
}

func generateCreateTableDdlLargeTable(tableName string, numberOfLargeColumns, numberOfSmallColumns int) string {
	columns := make(map[string]string, numberOfLargeColumns + numberOfSmallColumns + 1)
	columns["c1"] = "int"
	for i := 1; i <= numberOfLargeColumns; i++ {
		columnName := fmt.Sprintf("l%d", i)
		columns[columnName] = "binary(10485760)"
	}
	for i := 1; i <= numberOfSmallColumns; i++ {
		columnName := fmt.Sprintf("s%d", i)
		columns[columnName] = "binary(1)"
	}

	return generateCreateTableDdl(tableName, columns, []string{"c1"})
}

func generateCreateTableDdl(tableName string, columns map[string]string, primaryKeyColumns []string) string {
	colDdls := make([]string, 0, len(columns))
	for columnName, columnType := range columns {
		colDdls = append(colDdls, fmt.Sprintf("%s %s", columnName, columnType))
	}
	return fmt.Sprintf("CREATE TABLE %s (\n%s,\nPRIMARY KEY (%s));", tableName, strings.Join(colDdls, ",\n"), strings.Join(primaryKeyColumns, ", "))
}

func generateCreateIndexDdls(numIndexes int) []string {
	// Determine how many tables we need, assuming 100 indexes per table (plus a potential extra table for any remaining
	// indexes if numIndexes is not exactly divisible by 100)
	numIndexesPerTable := 100
	numTables := numIndexes / numIndexesPerTable
	extraIndexes := numIndexes % numIndexesPerTable

	tableDef := make(map[string]int)
	for i := 1; i <= numTables; i++ {
		tableName := fmt.Sprintf("t%d", i)
		tableDef[tableName] = numIndexesPerTable
	}
	if extraIndexes > 0 {
		tableName := fmt.Sprintf("t%d", numTables + 1)
		tableDef[tableName] = extraIndexes
	}

	ddls := make([]string, 0, len(tableDef) + numIndexes)
	for tableName, numCols := range tableDef {
		ddls = append(ddls, generateCreateTableDdlWithIndexDdls(tableName, numCols)...)
	}

	return ddls
}

func generateCreateTableDdlWithIndexDdls(tableName string, numIndexes int) []string {
	columns := make(map[string]string, numIndexes + 1)
	columns["p1"] = "bigint"
	for i := 1; i <= numIndexes; i++ {
		columnName := fmt.Sprintf("c%d", i)
		columns[columnName] = "bigint"
	}

	ddls := make([]string, 0, numIndexes + 1)
	ddls = append(ddls, generateCreateTableDdl(tableName, columns, []string{"p1"}))

	for i := 1; i <= numIndexes; i++ {
		columnName := fmt.Sprintf("c%d", i)
		indexName := fmt.Sprintf("%s_%s_idx", tableName, columnName)
		ddls = append(ddls, generateCreateIndexDdl(indexName, tableName, []string{columnName}))
	}

	return ddls
}

func generateCreateIndexDdl(indexName, tableName string, columns []string) string {
	return fmt.Sprintf("CREATE INDEX %s ON %s (%s);", indexName, tableName, strings.Join(columns, ", "))
}

func writeDumpFile(t *testing.T, dumpFilePath string, ddls []string) {
	writeDumpErr := os.WriteFile(dumpFilePath, []byte(strings.Join(ddls, "\n")), os.FileMode(0644))
	if writeDumpErr != nil {
		t.Fatal(writeDumpErr)
	}
}

func onlyRunForEndToEndTest(t *testing.T) {
	if os.Getenv("SPANNER_MIGRATION_TOOL_RUN_E2E_TESTS") == "" || os.Getenv("SPANNER_EMULATOR_HOST") != "" {
		t.Skip("Skipping end-to-end tests. To run end-to-end tests, set SPANNER_MIGRATION_TOOL_RUN_E2E_TESTS env var to true, unset SPANNER_EMULATOR_HOST env var and ensure SPANNER_MIGRATION_TOOL_TESTS_GCLOUD_INSTANCE_ID and SPANNER_MIGRATION_TOOL_TESTS_GCLOUD_PROJECT_ID env vars are set.")
	}
}

func RunCommandReturningStdOut(args string, projectID string) (string, error) {
	cmd := exec.Command("bash", "-c", fmt.Sprintf("go run github.com/GoogleCloudPlatform/spanner-migration-tool %v", args))
	var out, stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("GCLOUD_PROJECT=%s", projectID),
	)
	if err := cmd.Run(); err != nil {
		fmt.Printf("stdout: %q\n", out.String())
		fmt.Printf("stderr: %q\n", stderr.String())
		return out.String(), err
	}
	return out.String(), nil
}

// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package mysql

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
)

// mockSpec defines a single mock expectation for a database query.
type mockSpec struct {
	query string
	args  []driver.Value
	cols  []string
	rows  [][]driver.Value
	err   error
}

// mkMockDB creates a *sql.DB instance with the specified mock expectations.
func mkMockDB(t *testing.T, ms []mockSpec) *sql.DB {
	db, mock, err := sqlmock.New()
	assert.Nil(t, err)
	for _, m := range ms {
		queryExpectation := mock.ExpectQuery(m.query)
		if len(m.args) > 0 {
			queryExpectation = queryExpectation.WithArgs(m.args...)
		}

		if m.err != nil {
			queryExpectation.WillReturnError(m.err)
		} else {
			rows := sqlmock.NewRows(m.cols)
			for _, r := range m.rows {
				rows.AddRow(r...)
			}
			queryExpectation.WillReturnRows(rows)
		}
	}
	return db
}

// TestGetAllQueries_Success tests the GetAllQueries method when database returns valid data.
func TestGetAllQueries_Success(t *testing.T) {
	ms := []mockSpec{
		{
			query: `SELECT\s+DIGEST_TEXT,\s+SUM\(COUNT_STAR\)\s+AS\s+total_count\s+FROM\s+performance_schema\.events_statements_summary_by_digest\s+WHERE\s+SCHEMA_NAME\s+=\s+\?\s+AND\s+DIGEST_TEXT\s+NOT\s+LIKE\s+'COMMIT%'\s+AND\s+DIGEST_TEXT\s+NOT\s+LIKE\s+'ROLLBACK%'\s+AND\s+DIGEST_TEXT\s+NOT\s+LIKE\s+'SET%'\s+AND\s+DIGEST_TEXT\s+NOT\s+LIKE\s+'SHOW%'\s+AND\s+DIGEST_TEXT\s+NOT\s+LIKE\s+'PREPARE%'\s+AND\s+DIGEST_TEXT\s+NOT\s+LIKE\s+'EXECUTE%stmt%'\s+GROUP\s+BY\s+DIGEST_TEXT\s+ORDER\s+BY\s+total_count\s+DESC;`,
			args:  []driver.Value{"test_db"},
			cols:  []string{"DIGEST_TEXT", "total_count"},
			rows: [][]driver.Value{
				{"SELECT * FROM users", 100},
				{"INSERT INTO products VALUES (?)", 50},
			},
		},
	}
	db := mkMockDB(t, ms)
	defer db.Close()

	psi := PerformanceSchemaImpl{
		Db:     db,
		DbName: "test_db",
	}

	queries, err := psi.GetAllQueries()

	assert.NoError(t, err)
	assert.Len(t, queries, 2)
	assert.Equal(t, "SELECT * FROM users", queries[0].Query)
	assert.Equal(t, "test_db", queries[0].Db.DatabaseName)
	assert.Equal(t, 100, queries[0].Count)
	assert.Equal(t, "INSERT INTO products VALUES (?)", queries[1].Query)
	assert.Equal(t, "test_db", queries[1].Db.DatabaseName)
	assert.Equal(t, 50, queries[1].Count)
}

// TestGetAllQueries_DbQueryError tests GetAllQueries when the database query fails.
func TestGetAllQueries_DbQueryError(t *testing.T) {
	ms := []mockSpec{
		{
			query: `SELECT\s+DIGEST_TEXT,\s+SUM\(COUNT_STAR\)\s+AS\s+total_count\s+FROM\s+performance_schema\.events_statements_summary_by_digest\s+WHERE\s+SCHEMA_NAME\s+=\s+\?\s+AND\s+DIGEST_TEXT\s+NOT\s+LIKE\s+'COMMIT%'\s+AND\s+DIGEST_TEXT\s+NOT\s+LIKE\s+'ROLLBACK%'\s+AND\s+DIGEST_TEXT\s+NOT\s+LIKE\s+'SET%'\s+AND\s+DIGEST_TEXT\s+NOT\s+LIKE\s+'SHOW%'\s+AND\s+DIGEST_TEXT\s+NOT\s+LIKE\s+'PREPARE%'\s+AND\s+DIGEST_TEXT\s+NOT\s+LIKE\s+'EXECUTE%stmt%'\s+GROUP\s+BY\s+DIGEST_TEXT\s+ORDER\s+BY\s+total_count\s+DESC;`,
			args:  []driver.Value{"test_db"},
			err:   errors.New("database connection error"),
		},
	}
	db := mkMockDB(t, ms)
	defer db.Close()

	psi := PerformanceSchemaImpl{
		Db:     db,
		DbName: "test_db",
	}

	queries, err := psi.GetAllQueries()

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "couldn't read events_statements_summary_by_digest from performance schema : database connection error")
	assert.Nil(t, queries)
}

// TestGetAllQueries_ScanError tests GetAllQueries when scanning rows fails for some records.
func TestGetAllQueries_ScanError(t *testing.T) {
	ms := []mockSpec{
		{
			query: `SELECT\s+DIGEST_TEXT,\s+SUM\(COUNT_STAR\)\s+AS\s+total_count\s+FROM\s+performance_schema\.events_statements_summary_by_digest\s+WHERE\s+SCHEMA_NAME\s+=\s+\?\s+AND\s+DIGEST_TEXT\s+NOT\s+LIKE\s+'COMMIT%'\s+AND\s+DIGEST_TEXT\s+NOT\s+LIKE\s+'ROLLBACK%'\s+AND\s+DIGEST_TEXT\s+NOT\s+LIKE\s+'SET%'\s+AND\s+DIGEST_TEXT\s+NOT\s+LIKE\s+'SHOW%'\s+AND\s+DIGEST_TEXT\s+NOT\s+LIKE\s+'PREPARE%'\s+AND\s+DIGEST_TEXT\s+NOT\s+LIKE\s+'EXECUTE%stmt%'\s+GROUP\s+BY\s+DIGEST_TEXT\s+ORDER\s+BY\s+total_count\s+DESC;`,
			args:  []driver.Value{"test_db"},
			cols:  []string{"DIGEST_TEXT", "total_count"},
			rows: [][]driver.Value{
				{"SELECT * FROM users", 100},
				{"INSERT INTO products VALUES (?)", "not_an_int"}, // This will cause a scan error
				{"UPDATE inventory", 200},
			},
		},
	}
	db := mkMockDB(t, ms)
	defer db.Close()

	psi := PerformanceSchemaImpl{
		Db:     db,
		DbName: "test_db",
	}

	queries, err := psi.GetAllQueries()

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Can't scan")
	assert.Len(t, queries, 2) // One successful row, one failed, but the working ones are returned.
	assert.Equal(t, "SELECT * FROM users", queries[0].Query)
	assert.Equal(t, 100, queries[0].Count)
	assert.Equal(t, "UPDATE inventory", queries[1].Query)
	assert.Equal(t, 200, queries[1].Count)
}

// TestGetAllQueries_NoRows tests GetAllQueries when no rows are returned.
func TestGetAllQueries_NoRows(t *testing.T) {
	ms := []mockSpec{
		{
			query: `SELECT\s+DIGEST_TEXT,\s+SUM\(COUNT_STAR\)\s+AS\s+total_count\s+FROM\s+performance_schema\.events_statements_summary_by_digest\s+WHERE\s+SCHEMA_NAME\s+=\s+\?\s+AND\s+DIGEST_TEXT\s+NOT\s+LIKE\s+'COMMIT%'\s+AND\s+DIGEST_TEXT\s+NOT\s+LIKE\s+'ROLLBACK%'\s+AND\s+DIGEST_TEXT\s+NOT\s+LIKE\s+'SET%'\s+AND\s+DIGEST_TEXT\s+NOT\s+LIKE\s+'SHOW%'\s+AND\s+DIGEST_TEXT\s+NOT\s+LIKE\s+'PREPARE%'\s+AND\s+DIGEST_TEXT\s+NOT\s+LIKE\s+'EXECUTE%stmt%'\s+GROUP\s+BY\s+DIGEST_TEXT\s+ORDER\s+BY\s+total_count\s+DESC;`,
			args:  []driver.Value{"test_db"},
			cols:  []string{"DIGEST_TEXT", "total_count"},
			rows:  [][]driver.Value{}, // No rows
		},
	}
	db := mkMockDB(t, ms)
	defer db.Close()

	psi := PerformanceSchemaImpl{
		Db:     db,
		DbName: "test_db",
	}

	queries, err := psi.GetAllQueries()

	assert.NoError(t, err)
	assert.Len(t, queries, 0)
	assert.Nil(t, queries)
}

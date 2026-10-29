package import_file

import (
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	pg_query "github.com/pganalyze/pg_query_go/v6"
	"testing"
)

func TestPostgresCSVSchemaParses(t *testing.T) {
	columns := []ColumnDefinition{
		{Name: "id", Type: "bigint", NotNull: true, PkOrder: 1},
		{Name: "label", Type: "text"},
	}
	statement := getCreateTableStmt("test_table", columns, constants.DIALECT_POSTGRESQL)
	if _, err := pg_query.Parse(statement); err != nil {
		t.Fatalf("generated PostgreSQL DDL is not parseable: %v; statement: %s", err, statement)
	}
}

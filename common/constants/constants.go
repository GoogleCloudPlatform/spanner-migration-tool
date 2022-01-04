// Package constants contains constants used across multiple other packages.
// All string constants have a lower_case value and thus string matching is
// performend against other lower_case strings.
package constants

const (
	// PGDUMP is the driver name for pg_dump.
	PGDUMP string = "pg_dump"

	// POSTGRES is the driver name for PostgreSQL.
	POSTGRES string = "postgres"

	// MYSQLDUMP is the driver name for mysqldump.
	MYSQLDUMP string = "mysqldump"

	// MYSQL is the driver name for MySQL.
	MYSQL string = "mysql"

	// DYNAMODB is the driver name for AWS DynamoDB.
	// This is an experimental driver; implementation in progress.
	DYNAMODB string = "dynamodb"

	// ORACLE is the driver name for Oracle.
	// This is an experimental driver; implementation in progress.
	ORACLE string = "oracle"

	// Target db for which schema is being generated.
	TargetSpanner              string = "spanner"
	TargetExperimentalPostgres string = "experimental_postgres"

	// Supported dialects for Cloud Spanner database.
	DIALECT_POSTGRESQL string = "postgresql"
	DIALECT_GOOGLESQL  string = "google_standard_sql"
)

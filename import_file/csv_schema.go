package import_file

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"

	spanneraccessor "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/spanner"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/parse"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
)

type CsvSchema interface {
	CreateSchema(ctx context.Context, dialect string, sp spanneraccessor.SpannerAccessor) error
}

type CsvSchemaImpl struct {
	ProjectId  string
	InstanceId string
	DbName     string
	TableName  string
	SchemaUri  string
}

// ColumnDefinition represents the definition of a Spanner table column.
type ColumnDefinition struct {
	Name    string `json:"name"`
	Type    string `json:"type"` // e.g., "INT64", "STRING(MAX)", "TIMESTAMP", "DATE"
	NotNull bool   `json:"notNull"`
	PkOrder int    `json:"primaryKeyOrder"` // defines the order in the PK for the table, 0 means absence.
}

type PrimaryKey struct {
	Name    string
	PkOrder int // defines the order in the PK for the table, 0 means absence.
}

func (source *CsvSchemaImpl) CreateSchema(ctx context.Context, dialect string, sp spanneraccessor.SpannerAccessor) error {

	dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", source.ProjectId, source.InstanceId, source.DbName)
	colDef, err := parseSchema(source.SchemaUri)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("Unable to parse schema URI %v", err))
		return err
	}

	dbExists, err := sp.TableExists(ctx, source.TableName)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("Unable to check existing schema %v", err))
		return err
	}

	if dbExists {
		logger.Log.Error(fmt.Sprintf("table %s exists ", source.TableName))
		// if exists, verify table schema is same as passed
		// TODO: validate schema matches
		return nil
	}

	ddl := getCreateTableStmt(source.TableName, colDef, dialect)

	stmts := []string{ddl}
	req := &adminpb.UpdateDatabaseDdlRequest{
		Database:   dbURI,
		Statements: stmts,
	}
	op, err := sp.GetSpannerAdminClient().UpdateDatabaseDdl(ctx, req)
	if err != nil {
		return fmt.Errorf("can't build UpdateDatabaseDdlRequest: %w", parse.AnalyzeError(err, dbURI))
	}
	if err := op.Wait(ctx); err != nil {
		return fmt.Errorf("UpdateDatabaseDdl call failed: %w", parse.AnalyzeError(err, dbURI))
	}

	logger.Log.Info(fmt.Sprintf("Created table %v successfully\n", source.TableName))
	return nil
}

func parseSchema(schemaUri string) ([]ColumnDefinition, error) {

	schemaFile, err := os.ReadFile(schemaUri)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("Error reading schema file: %v", err))
		return nil, err
	}

	var schema []ColumnDefinition
	err = json.Unmarshal(schemaFile, &schema)
	if err != nil {
		fmt.Println("Error parsing schema file:", err)
		return nil, err
	}

	var colDefs []ColumnDefinition
	for _, column := range schema {

		colDef := ColumnDefinition{column.Name, column.Type, column.NotNull, column.PkOrder}
		colDefs = append(colDefs, colDef)
	}
	return colDefs, nil
}

func getCreateTableStmt(tableName string, colDef []ColumnDefinition, dialect string) string {
	var col, pk string
	pks := []PrimaryKey{}

	for _, cd := range colDef {
		s := printColumnDef(cd)
		if len(col) > 0 {
			s = "," + s
		}
		col = col + s
		if cd.PkOrder != 0 {
			pks = append(pks, PrimaryKey{cd.Name, cd.PkOrder})
		}
	}

	sort.Slice(pks, func(i, j int) bool {
		return pks[i].PkOrder < pks[j].PkOrder
	})

	for _, p := range pks {
		s := quote(p.Name)
		if len(pk) > 0 {
			s = "," + s
		}
		pk = pk + s
	}

	var stmt string
	if dialect == constants.DIALECT_POSTGRESQL {
		stmt = fmt.Sprintf("CREATE TABLE %s (\n%s PRIMARY KEY (%s)\n)", quote(tableName), col, pk)
	}
	stmt = fmt.Sprintf("CREATE TABLE %s (\n%s) PRIMARY KEY (%s)", quote(tableName), col, pk)
	logger.Log.Debug(fmt.Sprintf("create table cmd %s ==", stmt))
	return stmt
}

func printColumnDef(c ColumnDefinition) string {
	s := fmt.Sprintf("%s %s", quote(c.Name), c.Type)
	if c.NotNull {
		s += " NOT NULL "
	}
	return s
}

func quote(s string) string {
	return "`" + s + "`"
}

func StringToBool(s string) bool {
	s = strings.ToLower(strings.TrimSpace(s)) // Normalize the string
	if s == "" {
		return false
	}

	boolVal, err := strconv.ParseBool(s)
	if err == nil {
		return boolVal
	}
	return false
}

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

// Package ddl provides a go representation of Spanner DDL
// as well as helpers for building and manipulating Spanner DDL.
// We only implement enough DDL types to meet the needs of HarbourBridge.
//
// Definitions are from
// https://cloud.google.com/spanner/docs/data-definition-language.
package ddl

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/cloudspannerecosystem/harbourbridge/common/constants"
)

const (
	// Types supported by Spanner with google_standard_sql (default) dialect.
	// Bool represent BOOL type.
	Bool string = "BOOL"
	// Bytes represent BYTES type.
	Bytes string = "BYTES"
	// Date represent DATE type.
	Date string = "DATE"
	// Float64 represent FLOAT64 type.
	Float64 string = "FLOAT64"
	// Int64 represent INT64 type.
	Int64 string = "INT64"
	// String represent STRING type.
	String string = "STRING"
	// Timestamp represent TIMESTAMP type.
	Timestamp string = "TIMESTAMP"
	// Numeric represent NUMERIC type.
	Numeric string = "NUMERIC"
	// Json represent JSON type.
	JSON string = "JSON"
	// MaxLength is a sentinel for Type's Len field, representing the MAX value.
	MaxLength = math.MaxInt64
	// StringMaxLength represents maximum allowed STRING length.
	StringMaxLength = 2621440

	// Types specific to Spanner with postgresql dialect, when they differ from
	// Spanner with google_standard_sql.
	// PGBytea represent BYTEA type, which is BYTES type in PG.
	PGBytea string = "BYTEA"
	// PGFloat8 represent FLOAT8 type, which is double type in PG.
	PGFloat8 string = "FLOAT8"
	// PGInt8 respresent INT8, which is INT type in PG.
	PGInt8 string = "INT8"
	// PGVarchar represent VARCHAR, which is STRING type in PG.
	PGVarchar string = "VARCHAR"
	// PGTimestamptz represent TIMESTAMPTZ, which is TIMESTAMP type in PG.
	PGTimestamptz string = "TIMESTAMPTZ"
	// PGMaxLength represents sentinel for Type's Len field in PG.
	PGMaxLength = 2621440
)

// Type represents the type of a column.
//     type:
//        { BOOL | INT64 | FLOAT64 | STRING( length ) | BYTES( length ) | DATE | TIMESTAMP | NUMERIC }
type Type struct {
	Name string
	// Len encodes the following Spanner DDL definition:
	//     length:
	//        { int64_value | MAX }
	Len int64
	// IsArray represents if Type is an array_type or not
	// When false, column has type T; when true, it is an array of type T.
	IsArray bool
}

// PrintColumnDefType unparses the type encoded in a ColumnDef.
func (ty Type) PrintColumnDefType() string {
	str := ty.Name
	if ty.Name == String || ty.Name == Bytes {
		str += "("
		if ty.Len == MaxLength {
			str += "MAX"
		} else {
			str += strconv.FormatInt(ty.Len, 10)
		}
		str += ")"
	}
	if ty.IsArray {
		str = "ARRAY<" + str + ">"
	}
	return str
}

func (ty Type) PGPrintColumnDefType() string {
	var str string
	switch ty.Name {
	case Bytes:
		str = PGBytea
	case Float64:
		str = PGFloat8
	case Int64:
		str = PGInt8
	case String:
		str = PGVarchar
	case Timestamp:
		str = PGTimestamptz
	default:
		str = ty.Name
	}
	// PG doesn't support array types, and we don't expect to receive a type
	// with IsArray set to true. In the unlikely event, set to string type.
	if ty.IsArray {
		str = PGVarchar
		ty.Len = PGMaxLength
	}
	// PG doesn't support variable length Bytea and thus doesn't support
	// setting length (or max length) for the Bytes.
	if ty.Name == String || ty.IsArray {
		str += "("
		if ty.Len == MaxLength || ty.Len == PGMaxLength {
			str += fmt.Sprintf("%v", PGMaxLength)
		} else {
			str += strconv.FormatInt(ty.Len, 10)
		}
		str += ")"
	}
	return str
}

// ColumnDef encodes the following DDL definition:
//     column_def:
//       column_name type [NOT NULL] [options_def]
type ColumnDef struct {
	Name    string
	T       Type
	NotNull bool
	Comment string
}

// Config controls how AST nodes are printed (aka unparsed).
type Config struct {
	Comments    bool // If true, print comments.
	ProtectIds  bool // If true, table and col names are quoted using backticks (avoids reserved-word issue).
	Tables      bool // If true, print tables
	ForeignKeys bool // If true, print foreign key constraints.
	TargetDb    string
}

func (c Config) quote(s string) string {
	if c.ProtectIds {
		if c.TargetDb == constants.TargetExperimentalPostgres {
			return "\"" + s + "\""
		} else {
			return "`" + s + "`"
		}
	}
	return s
}

// PrintColumnDef unparses ColumnDef and returns it as well as any ColumnDef
// comment. These are returned as separate strings to support formatting
// needs of PrintCreateTable.
func (cd ColumnDef) PrintColumnDef(c Config) (string, string) {
	var s string
	if c.TargetDb == constants.TargetExperimentalPostgres {
		s = fmt.Sprintf("%s %s", c.quote(cd.Name), cd.T.PGPrintColumnDefType())
	} else {
		s = fmt.Sprintf("%s %s", c.quote(cd.Name), cd.T.PrintColumnDefType())
	}
	if cd.NotNull {
		s += " NOT NULL"
	}
	return s, cd.Comment
}

// IndexKey encodes the following DDL definition:
//     primary_key:
//       PRIMARY KEY ( [key_part, ...] )
//     key_part:
//        column_name [{ ASC | DESC }]
type IndexKey struct {
	Col  string
	Desc bool // Default order is ascending i.e. Desc = false.
}

// PrintIndexKey unparses the index keys.
func (pk IndexKey) PrintIndexKey(c Config) string {
	col := c.quote(pk.Col)
	if pk.Desc {
		return fmt.Sprintf("%s DESC", col)
	}
	// Don't print out ASC -- that's the default.
	return col
}

// Foreignkey encodes the following DDL definition:
//    [ CONSTRAINT constraint_name ]
// 	  FOREIGN KEY ( column_name [, ... ] ) REFERENCES ref_table ( ref_column [, ... ] ) }
type Foreignkey struct {
	Name         string
	Columns      []string
	ReferTable   string
	ReferColumns []string
}

// PrintForeignKey unparses the foreign keys.
func (k Foreignkey) PrintForeignKey(c Config) string {
	var cols, referCols []string
	for i, col := range k.Columns {
		cols = append(cols, c.quote(col))
		referCols = append(referCols, c.quote(k.ReferColumns[i]))
	}
	var s string
	if k.Name != "" {
		s = fmt.Sprintf("CONSTRAINT %s ", c.quote(k.Name))
	}
	return s + fmt.Sprintf("FOREIGN KEY (%s) REFERENCES %s (%s)", strings.Join(cols, ", "), c.quote(k.ReferTable), strings.Join(referCols, ", "))
}

// CreateTable encodes the following DDL definition:
//     create_table: CREATE TABLE table_name ([column_def, ...] ) primary_key [, cluster]
type CreateTable struct {
	Name     string
	ColNames []string             // Provides names and order of columns
	ColDefs  map[string]ColumnDef // Provides definition of columns (a map for simpler/faster lookup during type processing)
	Pks      []IndexKey
	Fks      []Foreignkey
	Indexes  []CreateIndex
	Parent   string //if not empty, this table will be interleaved
	Comment  string
}

// PrintCreateTable unparses a CREATE TABLE statement.
func (ct CreateTable) PrintCreateTable(config Config) string {
	var col []string
	var colComment []string
	var keys []string
	for _, cn := range ct.ColNames {
		s, c := ct.ColDefs[cn].PrintColumnDef(config)
		s = "\t" + s + ","
		col = append(col, s)
		colComment = append(colComment, c)
	}

	n := maxStringLength(col)
	var cols string
	for i, c := range col {
		cols += c
		if config.Comments && len(colComment[i]) > 0 {
			cols += strings.Repeat(" ", n-len(c)) + " -- " + colComment[i]
		}
		cols += "\n"
	}

	for _, p := range ct.Pks {
		keys = append(keys, p.PrintIndexKey(config))
	}
	var tableComment string
	if config.Comments && len(ct.Comment) > 0 {
		tableComment = "--\n-- " + ct.Comment + "\n--\n"
	}

	var interleave string
	if ct.Parent != "" {
		if config.TargetDb == constants.TargetExperimentalPostgres {
			// PG spanner only supports PRIMARY KEY() inside the CREATE TABLE()
			// and thus INTERLEAVE follows immediately after closing brace.
			interleave = " INTERLEAVE IN PARENT " + config.quote(ct.Parent)
		} else {
			interleave = ",\nINTERLEAVE IN PARENT " + config.quote(ct.Parent)
		}
	}

	if config.TargetDb == constants.TargetExperimentalPostgres {
		return fmt.Sprintf("%sCREATE TABLE %s (\n%s\tPRIMARY KEY (%s)\n)%s", tableComment, config.quote(ct.Name), cols, strings.Join(keys, ", "), interleave)
	}
	return fmt.Sprintf("%sCREATE TABLE %s (\n%s) PRIMARY KEY (%s)%s", tableComment, config.quote(ct.Name), cols, strings.Join(keys, ", "), interleave)
}

// CreateIndex encodes the following DDL definition:
//     create index: CREATE [UNIQUE] [NULL_FILTERED] INDEX index_name ON table_name ( key_part [, ...] ) [ storing_clause ] [ , interleave_clause ]
type CreateIndex struct {
	Name   string
	Table  string
	Unique bool
	Keys   []IndexKey
	// We have no requirements for null-filtered option and
	// storing/interleaving clauses yet, so we omit them for now.
}

// PrintCreateIndex unparses a CREATE INDEX statement.
func (ci CreateIndex) PrintCreateIndex(c Config) string {
	var keys []string
	for _, p := range ci.Keys {
		keys = append(keys, p.PrintIndexKey(c))
	}
	var unique string
	if ci.Unique {
		unique = "UNIQUE "
	}
	return fmt.Sprintf("CREATE %sINDEX %s ON %s (%s)", unique, c.quote(ci.Name), c.quote(ci.Table), strings.Join(keys, ", "))
}

// PrintForeignKeyAlterTable unparses the foreign keys using ALTER TABLE.
func (k Foreignkey) PrintForeignKeyAlterTable(c Config, tableName string) string {
	var cols, referCols []string
	for i, col := range k.Columns {
		cols = append(cols, c.quote(col))
		referCols = append(referCols, c.quote(k.ReferColumns[i]))
	}
	var s string
	if k.Name != "" {
		s = fmt.Sprintf("CONSTRAINT %s ", c.quote(k.Name))
	}
	return fmt.Sprintf("ALTER TABLE %s ADD %sFOREIGN KEY (%s) REFERENCES %s (%s)", c.quote(tableName), s, strings.Join(cols, ", "), c.quote(k.ReferTable), strings.Join(referCols, ", "))
}

// Schema stores a map of table names and Tables.
type Schema map[string]CreateTable

// NewSchema creates a new Schema object.
func NewSchema() Schema {
	return make(map[string]CreateTable)
}

// Tables are ordered in alphabetical order with one exception: interleaved
// tables appear after the definition of their parent table.
// TODO: Move this method to mapping.go and preserve the table names in sorted
// order in conv so that we don't need to order the table names multiple times.
func OrderTables(s Schema) []string {
	var tableNames, sortedTableNames []string
	for t := range s {
		tableNames = append(tableNames, t)
	}
	sort.Strings(tableNames)
	tableQueue := tableNames
	tableAdded := make(map[string]bool)
	for len(tableQueue) > 0 {
		tableName := tableQueue[0]
		table := s[tableName]
		tableQueue = tableQueue[1:]

		// Add table t if either:
		// a) t is not interleaved in another table, or
		// b) t is interleaved in another table and that table has already been added to the list.
		if table.Parent == "" || tableAdded[table.Parent] {
			sortedTableNames = append(sortedTableNames, tableName)
			tableAdded[tableName] = true
		} else {
			// We can't add table t now because its parent hasn't been added.
			// Add it at end of tables and we'll try again later.
			// We might need multiple iterations to add chains of interleaved tables,
			// but we will always make progress because interleaved tables can't
			// have cycles. In principle this could be O(n^2), but in practice chains
			// of interleaved tables are small.
			tableQueue = append(tableQueue, tableName)
		}
	}
	return sortedTableNames
}

// GetDDL returns the string representation of Spanner schema represented by Schema struct.
// Tables are printed in alphabetical order with one exception: interleaved
// tables are potentially out of order since they must appear after the
// definition of their parent table.
func (s Schema) GetDDL(c Config) []string {
	var ddl []string
	sortedTableNames := OrderTables(s)

	if c.Tables {
		for _, tableName := range sortedTableNames {
			ddl = append(ddl, s[tableName].PrintCreateTable(c))
			for _, index := range s[tableName].Indexes {
				ddl = append(ddl, index.PrintCreateIndex(c))
			}
		}
	}
	// Append foreign key constraints to DDL.
	// We always use alter table statements for foreign key constraints.
	// The alternative of putting foreign key constraints in-line as part of create
	// table statements is tricky because of table order (need to define tables
	// before they are referenced by foreign key constraints) and the possibility
	// of circular foreign keys definitions. We opt for simplicity.
	if c.ForeignKeys {
		for _, t := range sortedTableNames {
			for _, fk := range s[t].Fks {
				ddl = append(ddl, fk.PrintForeignKeyAlterTable(c, t))
			}
		}
	}
	return ddl
}

// CheckInterleaved checks if schema contains interleaved tables.
func (s Schema) CheckInterleaved() bool {
	for _, table := range s {
		if table.Parent != "" {
			return true
		}
	}
	return false
}

func maxStringLength(s []string) int {
	n := 0
	for _, x := range s {
		if len(x) > n {
			n = len(x)
		}
	}
	return n
}

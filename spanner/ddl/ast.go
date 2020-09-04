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
	"strings"
)

const (
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
	// MaxLength represent "MAX" length of column type.
	MaxLength string = "MAX"
)

// PrintLength unparses length of a Type.
func PrintLength(m interface{}) string {
	return fmt.Sprintf("%v", m)
}

// Type represents the type of a column.
//     type:
//        { BOOL | INT64 | FLOAT64 | STRING( length ) | BYTES( length ) | DATE | TIMESTAMP }
type Type struct {
	Name string
	// Len encodes the following Spanner DDL definition:
	//     length:
	//        { int64_value | MAX }
	Len interface{}
}

// PrintType unparses Type of a column.
func (ty Type) PrintType() string {
	if ty.Len == nil {
		return ty.Name
	}
	return fmt.Sprintf("%s(%s)", ty.Name, PrintLength(ty.Len))
}

// ColumnDef encodes the following DDL definition:
//     column_def:
//       column_name type [NOT NULL] [options_def]
type ColumnDef struct {
	Name    string
	T       Type
	IsArray bool // When false, this column has type T; when true, it is an array of type T.
	NotNull bool
	Comment string
}

// Config controls how AST nodes are printed (aka unparsed).
type Config struct {
	Comments   bool // If true, print comments.
	ProtectIds bool // If true, table and col names are quoted using backticks (avoids reserved-word issue).
}

func (c Config) quote(s string) string {
	if c.ProtectIds {
		return "`" + s + "`"
	}
	return s
}

// PrintColumnDef unparses ColumnDef and returns it as well as any ColumnDef
// comment. These are returned as separate strings to support formatting
// needs of PrintCreateTable.
func (cd ColumnDef) PrintColumnDef(c Config) (string, string) {
	s := fmt.Sprintf("%s %s", c.quote(cd.Name), cd.PrintColumnDefType())
	if cd.NotNull {
		s += " NOT NULL"
	}
	return s, cd.Comment
}

// PrintColumnDefType unparses the type encoded in a ColumnDef.
func (cd ColumnDef) PrintColumnDefType() string {
	t := cd.T.PrintType()
	if cd.IsArray {
		return fmt.Sprintf("ARRAY<%s>", t)
	}
	return t
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

// CreateTable encodes the following DDL definition:
//     create_table: CREATE TABLE table_name ([column_def, ...] ) primary_key [, cluster]
type CreateTable struct {
	Name     string
	ColNames []string             // Provides names and order of columns
	ColDefs  map[string]ColumnDef // Provides definition of columns (a map for simpler/faster lookup during type processing)
	Pks      []IndexKey
	Comment  string
}

// PrintCreateTable unparses a CREATE TABLE statement.
func (ct CreateTable) PrintCreateTable(config Config) string {
	var col []string
	var colComment []string
	var keys []string
	for i, cn := range ct.ColNames {
		s, c := ct.ColDefs[cn].PrintColumnDef(config)
		s = "\n    " + s
		if i < len(ct.ColNames)-1 {
			s += ","
		} else {
			s += " "
		}
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
	}
	for _, p := range ct.Pks {
		keys = append(keys, p.PrintIndexKey(config))
	}
	var tableComment string
	if config.Comments && len(ct.Comment) > 0 {
		tableComment = "--\n-- " + ct.Comment + "\n--\n"
	}
	return fmt.Sprintf("%sCREATE TABLE %s (%s\n) PRIMARY KEY (%s)", tableComment, config.quote(ct.Name), cols, strings.Join(keys, ", "))
}

// CreateIndex encodes the following DDL definition:
//     create index: CREATE [UNIQUE] [NULL_FILTERED] INDEX index_name ON table_name ( key_part [, ...] ) [ storing_clause ] [ , interleave_clause ]
type CreateIndex struct {
	Name  string
	Table string
	Keys  []IndexKey
	// We have no requirements for unique and null-filtered options and
	// storing/interleaving clauses yet, so we omit them for now.
}

// PrintCreateIndex unparses a CREATE INDEX statement.
func (ci CreateIndex) PrintCreateIndex(c Config) string {
	var keys []string
	for _, p := range ci.Keys {
		keys = append(keys, p.PrintIndexKey(c))
	}
	return fmt.Sprintf("CREATE INDEX %s ON %s (%s)", c.quote(ci.Name), c.quote(ci.Table), strings.Join(keys, ", "))
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

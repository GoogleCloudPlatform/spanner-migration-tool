// Copyright 2019 Google LLC
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
// Before defining each type, we give the snippet from this definition.
//
// We use go interface types to preserve the structural constraints of
// Spanner DDL. For example, suppose ScalarType could be either
// an INT or a STRING with a length specifier:
//    ScalarType := INT | STRING( length )
// then we use a go interface type to encode ScalarType:
//    type ScalarType interface {
//        PrScalarType() string  // To print ScalarTypes types.
//    }
// and struct types for each case:
//    type Int struct { }
//    type String struct { length int64 }
// and finally, we define functions:
//    func (i Int) PrScalarType() { ... }
//    func (s String) PrScalarType() { .. }
//
// The net result is that the only way to build a ScalarType
// is using Int or String.
package ddl

import (
	"fmt"
	"strings"
)

// Length encodes the following Spanner DDL definition:
//     length:
//        { int64_value | MAX }
type Length interface {
	PrLength() string
}

// Int64Length wraps an integer length specifier.
type Int64Length struct{ Value int64 }

// MaxLength represents "MAX".
type MaxLength struct{}

// PrLength unparses Int64Length.
func (i Int64Length) PrLength() string { return fmt.Sprintf("%d", i.Value) }

// PrLength unparses MaxLength.
func (m MaxLength) PrLength() string { return fmt.Sprintf("MAX") }

// ScalarType encodes the following DDL definition:
//     scalar_type:
//        { BOOL | INT64 | FLOAT64 | STRING( length ) | BYTES( length ) | DATE | TIMESTAMP }
type ScalarType interface {
	PrScalarType() string
}

// Bool encodes DDL BOOL.
type Bool struct{}

// Bytes encodes DDL BYTES( length ).
type Bytes struct{ Len Length }

// Date encodes DDL DATE.
type Date struct{}

// Float64 encodes DDL FLOAT64.
type Float64 struct{}

// Int64 encodes DDL INT64.
type Int64 struct{}

// String encodes DDL STRING.
type String struct{ Len Length }

// Timestamp encodes DDL TIMESTAMP.
type Timestamp struct{}

// PrScalarType unparses Bool.
func (b Bool) PrScalarType() string { return "BOOL" }

// PrScalarType unparses Bytes.
func (b Bytes) PrScalarType() string { return fmt.Sprintf("BYTES(%s)", b.Len.PrLength()) }

// PrScalarType unparses Date.
func (d Date) PrScalarType() string { return "DATE" }

// PrScalarType unparses Float64
func (g Float64) PrScalarType() string { return "FLOAT64" }

// PrScalarType unparses Int64
func (i Int64) PrScalarType() string { return "INT64" }

// PrScalarType unparses String
func (s String) PrScalarType() string { return fmt.Sprintf("STRING(%s)", s.Len.PrLength()) }

// PrScalarType unparses Timestamp
func (t Timestamp) PrScalarType() string { return "TIMESTAMP" }

// ColumnDef encodes the following DDL definition:
//     column_def:
//       column_name {scalar_type | array_type} [NOT NULL] [options_def]
type ColumnDef struct {
	Name    string
	T       ScalarType
	IsArray bool // When false, this column has type T; when true, it is an array of type T.
	NotNull bool
	Comment string
}

// PrColumnDef unparses ColumnDef and returns it as well as any ColumnDef
// comment. These are returned as separate strings to support formatting
// needs of PrCreateTable.
func (cd ColumnDef) PrColumnDef(c Config) (string, string) {
	s := fmt.Sprintf("%s %s", backtick(c)(cd.Name), cd.PrColumnDefType())
	if cd.NotNull {
		s += " NOT NULL"
	}
	return s, cd.Comment
}

// PrColumnDefType unparses the type encoded in a ColumnDef.
func (cd ColumnDef) PrColumnDefType() string {
	t := cd.T.PrScalarType()
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

// PrIndexKey unparses the index keys.
func (pk IndexKey) PrIndexKey(c Config) string {
	col := backtick(c)(pk.Col)
	if pk.Desc {
		return fmt.Sprintf("%s DESC", col)
	}
	// Don't print out ASC -- that's the default.
	return col
}

// CreateTable encodes the following DDL definition:
//     create_table: CREATE TABLE table_name ([column_def, ...] ) primary_key [, cluster]
type CreateTable struct {
	Name    string
	Cols    []string             // Provides names and order of columns
	Cds     map[string]ColumnDef // Provides definition of columns (a map for simpler/faster lookup during type processing)
	Pks     []IndexKey
	Comment string
}

// Config controls unparsing.
type Config struct {
	Comments   bool // If true, print comments.
	ProtectIds bool // If true, table and col names are enclosed with backticks (avoids reserved-word issue).
}

// PrCreateTable unparses a CREATE TABLE statement.
func (ct CreateTable) PrCreateTable(config Config) string {
	bt := backtick(config)
	var col []string
	var colComment []string
	var keys []string
	for i, cn := range ct.Cols {
		s, c := ct.Cds[cn].PrColumnDef(config)
		s = "\n    " + s
		if i < len(ct.Cols)-1 {
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
		keys = append(keys, p.PrIndexKey(config))
	}
	var tableComment string
	if config.Comments && len(ct.Comment) > 0 {
		tableComment = "--\n-- " + ct.Comment + "\n--\n"
	}
	return fmt.Sprintf("%sCREATE TABLE %s (%s\n) PRIMARY KEY (%s)", tableComment, bt(ct.Name), cols, strings.Join(keys, ", "))
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

// PrCreateIndex unparses a CREATE INDEX statement.
func (ci CreateIndex) PrCreateIndex(c Config) string {
	bt := backtick(c)
	var keys []string
	for _, p := range ci.Keys {
		keys = append(keys, p.PrIndexKey(c))
	}
	return fmt.Sprintf("CREATE INDEX %s ON %s (%s)", bt(ci.Name), bt(ci.Table), strings.Join(keys, ", "))
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

func backtick(config Config) func(s string) string {
	if config.ProtectIds {
		return func(s string) string { return "`" + s + "`" }
	}
	return func(s string) string { return s }
}

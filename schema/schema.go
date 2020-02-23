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

// Package schema provides a generic representation of database
// schemas. Note that our goal is not to faithfully represent all
// aspects of the schema, but just the relevant components for
// conversion to Spanner and reporting on the quality of the
// conversion (this motivates us to keep partial information about
// some features we will report on but not use in the conversion
// e.g. default values, check constraints).
//
// The current version supports PostgreSQL. Expect it to grow as we
// support other databases. We might eventually support the Spanner
// schema, and potentially get rid of the ddl package.
package schema

// Table represents a database table.
type Table struct {
	Name        string
	ColNames    []string          // List of column names (for predictable iteration order e.g. printing).
	ColDef      map[string]Column // Details of columns.
	PrimaryKeys []Key
	Indexes     []Index
}

type Key struct {
	Column string
	Desc   bool // By default, order is ASC. Set to true to specifiy DESC.
}

type Index struct {
	Name string
	Keys []Key
}

// Column represents a database column.
type Column struct {
	Name    string
	Type    Type
	NotNull bool
	Unique  bool
	// 	ForeignKey []ForeignKey
	Ignored Ignored
}

type Type struct {
	Id          string
	Mods        []int64 // List of modifiers (aka type parameters e.g. varchar(8) or numeric(6, 4).
	ArrayBounds []int64 // Empty for scalar types.
}

// TODO: not used yet.
type ForeignKey struct {
	Table  string
	Column string
	// TODO: Add what happens on delete e.g. CASCADE, RESTRICT.
}

// Ignored column properties/constraints. We drop the details, but retain
// presence/absence for reporting purposes.
type Ignored struct {
	Check      bool
	Identity   bool
	Default    bool
	Exclusion  bool
	ForeignKey bool
}

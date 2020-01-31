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

package postgres

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unicode"

	nodes "github.com/lfittl/pg_query_go/nodes"

	"harbourbridge/internal"
	"harbourbridge/spanner/ddl"
)

type copyOrInsert struct {
	stmt    stmtType
	spTable string   // Spanner table name.
	pgTable string   // Postgres table name.
	cols    []string // Spanner cols names.
	vals    []string // Empty for COPY-FROM.
}

type stmtType int

const (
	copyFrom stmtType = iota
	insert
)

// processStatements extracts schema information and data from PostgreSQL
// statements, updating Conv with new schema information, and returning
// copyOrInsert if a COPY-FROM or INSERT statement is encountered.
// Note that the actual parsing/processing of COPY-FROM data blocks is
// handled elsewhere (see process.go).
func processStatements(conv *Conv, statements []nodes.Node) *copyOrInsert {
	// Typically we'll have only one statement, but we handle the general case.
	for i, node := range statements {
		switch n := node.(type) {
		// Unwrap RawStatement.
		case nodes.RawStmt:
			node = n.Stmt
		}
		switch n := node.(type) {
		case nodes.AlterTableStmt:
			if conv.schemaMode() {
				processAlterTableStmt(conv, n)
			}
		case nodes.CopyStmt:
			if i != len(statements)-1 {
				conv.unexpected("CopyFrom is not the last statement in batch: ignoring following statements")
				conv.errorInStatement([]nodes.Node{node})
			}
			return processCopyStmt(conv, n)
		case nodes.CreateStmt:
			if conv.schemaMode() {
				processCreateStmt(conv, n)
			}
		case nodes.InsertStmt:
			return processInsertStmt(conv, n)
		case nodes.VariableSetStmt:
			if conv.schemaMode() {
				processVariableSetStmt(conv, n)
			}
		default:
			conv.skipStatement([]nodes.Node{node})
		}
	}
	return nil
}

func processAlterTableStmt(conv *Conv, n nodes.AlterTableStmt) {
	if n.Relation == nil {
		logStmtError(conv, n, fmt.Errorf("relation is nil"))
		return
	}
	pgTable, spTable, err := getTableName(conv, *n.Relation)
	if err != nil {
		logStmtError(conv, n, fmt.Errorf("can't get table name: %w", err))
		return
	}
	if _, ok := conv.spSchema[spTable]; ok {
		for _, i := range n.Cmds.Items {
			switch a := i.(type) {
			case nodes.AlterTableCmd:
				switch {
				case a.Subtype == nodes.AT_SetNotNull && a.Name != nil:
					c := constraint{ct: nodes.CONSTR_NOTNULL, keys: []string{*a.Name}}
					updateSchema(conv, spTable, pgTable, []constraint{c}, "ALTER TABLE")
					conv.schemaStatement([]nodes.Node{n, a})
				case a.Subtype == nodes.AT_AddConstraint && a.Def != nil:
					switch d := a.Def.(type) {
					case nodes.Constraint:
						updateSchema(conv, spTable, pgTable, extractConstraints(conv, n, pgTable, []nodes.Node{d}), "ALTER TABLE")
						conv.schemaStatement([]nodes.Node{n, a, d})
					default:
						conv.skipStatement([]nodes.Node{n, a, d})
					}
				default:
					conv.skipStatement([]nodes.Node{n, a})
				}
			default:
				conv.skipStatement([]nodes.Node{n, a})
			}
		}
	} else {
		// In PostgreSQL, AlterTable statements can be applied to views,
		// sequences and indexes in addition to tables. Since we only
		// track tables created by "CREATE TABLE", this lookup can fail.
		// For debugging purposes we log the lookup failure if we're
		// in verbose mode, but otherwise  we just skip these statements.
		conv.skipStatement([]nodes.Node{n})
		internal.VerbosePrintf("Processing %v statement: table %s not found", reflect.TypeOf(n), spTable)
	}
}

func processCreateStmt(conv *Conv, n nodes.CreateStmt) {
	var cols []string
	cds := make(map[string]ddl.ColumnDef)
	pgCols := make(map[string]pgColDef)
	if n.Relation == nil {
		logStmtError(conv, n, fmt.Errorf("relation is nil"))
		return
	}
	pgTable, spTable, err := getTableName(conv, *n.Relation)
	if err != nil {
		logStmtError(conv, n, fmt.Errorf("can't get table name: %w", err))
		return
	}
	var constraints []constraint
	for _, te := range n.TableElts.Items {
		switch i := te.(type) {
		case nodes.ColumnDef:
			col, cd, pgCol, cdConstraints, err := processColumnDef(conv, i, pgTable)
			if err != nil {
				logStmtError(conv, n, err)
				return
			}
			cols = append(cols, col)
			cds[col] = cd
			pgCols[col] = pgCol
			constraints = append(constraints, cdConstraints...)
		case nodes.Constraint:
			// Note: there should be at most one Constraint nodes in
			// n.TableElts.Items. We don't check this. We just keep
			// collecting constraints.
			constraints = extractConstraints(conv, n, pgTable, []nodes.Node{i})
		default:
			conv.unexpected(fmt.Sprintf("Found %s node while processing CreateStmt TableElts", prNodeType(i)))
		}
	}
	conv.schemaStatement([]nodes.Node{n})
	conv.spSchema[spTable] = ddl.CreateTable{spTable, cols, cds, nil, mkTableComment(pgTable)}
	conv.pgSchema[pgTable] = pgTableDef{pgCols}
	// Note: constraints contains all info about primary keys and not-null keys.
	updateSchema(conv, spTable, pgTable, constraints, "CREATE TABLE")
}

func processColumnDef(conv *Conv, n nodes.ColumnDef, pgTable string) (string, ddl.ColumnDef, pgColDef, []constraint, error) {
	mods := getTypeMods(conv, n.TypeName.Typmods)
	if n.Colname == nil {
		return "", ddl.ColumnDef{}, pgColDef{}, nil, fmt.Errorf("colname is nil")
	}
	pgCol := *n.Colname
	spCol, err := GetSpannerCol(conv, pgTable, pgCol, false)
	if err != nil {
		return "", ddl.ColumnDef{}, pgColDef{}, nil, fmt.Errorf("can't get Spanner col: %w", err)
	}
	tid, err := getTypeID(n.TypeName.Names.Items)
	if err != nil {
		return "", ddl.ColumnDef{}, pgColDef{}, nil, fmt.Errorf("can't get type id for %s: %w", spCol, err)
	}
	t, issues := getSpannerType(conv, tid, mods)

	// Treatment of arrays: if the PostgreSQL type has a single array parameter
	// we map it to a Spanner array. The array bounds themselves are ignored
	// since Spanner doesn't support them (and PostgreSQL ignores them anyway:
	// https://www.postgresql.org/docs/9.1/arrays.html).
	// Spanner does not support multi-dimensional arrays -- we map those
	// to STRING(MAX).
	a := getArrayBounds(conv, n.TypeName.ArrayBounds)
	if len(a) > 1 {
		t = ddl.String{ddl.MaxLength{}}
		issues = append(issues, multiDimensionalArray)
	}
	constraints := analyzeColDefConstraints(conv, n, pgTable, n.Constraints.Items, pgCol)
	pgSchema := pgColDef{id: tid, mods: mods, array: a, issues: issues}
	cd := ddl.ColumnDef{
		Name:    spCol,
		T:       t,
		IsArray: len(a) == 1,
		Comment: mkColComment(pgCol, printType(pgSchema)),
	}
	return spCol, cd, pgSchema, constraints, nil
}

func processInsertStmt(conv *Conv, n nodes.InsertStmt) *copyOrInsert {
	if n.Relation == nil {
		logStmtError(conv, n, fmt.Errorf("relation is nil"))
		return nil
	}
	pgTable, spTable, err := getTableName(conv, *n.Relation)
	if err != nil {
		logStmtError(conv, n, fmt.Errorf("can't get table name: %w", err))
		return nil
	}
	conv.statsAddRow(spTable, conv.schemaMode())
	cols, err := getCols(conv, pgTable, n.Cols.Items)
	if err != nil {
		logStmtError(conv, n, fmt.Errorf("can't get col name: %w", err))
		conv.statsAddBadRow(spTable, conv.schemaMode())
		return nil
	}
	var values []string
	switch sel := n.SelectStmt.(type) {
	case nodes.SelectStmt:
		values = getVals(conv, sel.ValuesLists, n)
		conv.dataStatement([]nodes.Node{n})
		if conv.dataMode() {
			return &copyOrInsert{stmt: insert, spTable: spTable, pgTable: pgTable, cols: cols, vals: values}
		}
	default:
		conv.unexpected(fmt.Sprintf("Found %s node while processing InsertStmt SelectStmt", prNodeType(sel)))
	}
	return nil
}

func processCopyStmt(conv *Conv, n nodes.CopyStmt) *copyOrInsert {
	// Always return a copyOrInsert{stmt: copyFrom, ...} even if we
	// encounter errors. Otherwise we won't be able to parse
	// the data portion of the COPY-FROM statement, and we'll
	// likely get stuck at this point in the pg_dump file.
	pgTable := "BOGUS_COPY_FROM_TABLE"
	spTable := "BOGUS_COPY_FROM_TABLE"
	var err error
	if n.Relation != nil {
		pgTable, spTable, err = getTableName(conv, *n.Relation)
		if err != nil {
			conv.unexpected(fmt.Sprintf("Processing %v statement: %s", reflect.TypeOf(n), err))
		}
	} else {
		logStmtError(conv, n, fmt.Errorf("relation is nil"))
	}
	var cols []string
	for _, a := range n.Attlist.Items {
		s, err := getString(a)
		if err == nil {
			s, err = GetSpannerCol(conv, pgTable, s, true)
		}
		if err != nil {
			conv.unexpected(fmt.Sprintf("Processing %v statement Attlist: %s", reflect.TypeOf(n), err))
			s = "BOGUS_COPY_FROM_COLUMN"
		}
		cols = append(cols, s)
	}
	conv.dataStatement([]nodes.Node{n})
	return &copyOrInsert{stmt: copyFrom, spTable: spTable, pgTable: pgTable, cols: cols}
}

func processVariableSetStmt(conv *Conv, n nodes.VariableSetStmt) {
	if n.Name != nil && *n.Name == "timezone" {
		if len(n.Args.Items) == 1 {
			switch c := n.Args.Items[0].(type) {
			case nodes.A_Const:
				tz, err := getString(c.Val)
				if err != nil {
					logStmtError(conv, n, fmt.Errorf("can't get Arg: %w", err))
					return
				}
				loc, err := time.LoadLocation(tz)
				if err != nil {
					logStmtError(conv, n, err)
					return
				}
				conv.SetLocation(loc)
			default:
				logStmtError(conv, n, fmt.Errorf("found %s node in Arg", reflect.TypeOf(c)))
				return
			}
		}
	}
}

// getSpannerType determines the Spanner type for the scalar postgres type
// defined by id and mods. This is the core postgres-to-Spanner type mapping.
// getSpannerType returns the Spanner type and the schema issues encountered
// during conversion.
func getSpannerType(conv *Conv, id string, mods []int64) (ddl.ScalarType, []schemaIssue) {
	maxExpectedMods := func(n int) {
		if len(mods) > n {
			conv.unexpected(fmt.Sprintf("Found %d mods while processing type id=%s", len(mods), id))
		}
	}
	switch id {
	case "bool":
		maxExpectedMods(0)
		return ddl.Bool{}, nil
	case "bigserial":
		maxExpectedMods(0)
		return ddl.Int64{}, []schemaIssue{serial}
	case "bpchar": // Note: Postgres internal name for char is bpchar (aka blank padded char).
		maxExpectedMods(1)
		if len(mods) > 0 {
			return ddl.String{ddl.Int64Length{mods[0]}}, nil
		}
		// Note: bpchar without length specifier is equivalent to bpchar(1)
		return ddl.String{ddl.Int64Length{1}}, nil
	case "bytea":
		maxExpectedMods(0)
		return ddl.Bytes{ddl.MaxLength{}}, nil
	case "date":
		maxExpectedMods(0)
		return ddl.Date{}, nil
	case "float8":
		maxExpectedMods(0)
		return ddl.Float64{}, nil
	case "float4":
		maxExpectedMods(0)
		return ddl.Float64{}, []schemaIssue{widened}
	case "int8":
		maxExpectedMods(0)
		return ddl.Int64{}, nil
	case "int4":
		maxExpectedMods(0)
		return ddl.Int64{}, []schemaIssue{widened}
	case "int2":
		maxExpectedMods(0)
		return ddl.Int64{}, []schemaIssue{widened}
	case "numeric": // Map all numeric types to float64.
		maxExpectedMods(2)
		if len(mods) > 0 && mods[0] <= 15 {
			// float64 can represent this numeric type faithfully.
			// Note: int64 has 53 bits for mantissa, which is ~15.96
			// decimal digits.
			return ddl.Float64{}, []schemaIssue{numericThatFits}
		}
		return ddl.Float64{}, []schemaIssue{numeric}
	case "serial":
		maxExpectedMods(0)
		return ddl.Int64{}, []schemaIssue{serial}
	case "text":
		maxExpectedMods(0)
		return ddl.String{ddl.MaxLength{}}, nil
	case "timestamptz":
		maxExpectedMods(1)
		return ddl.Timestamp{}, nil
	case "timestamp":
		maxExpectedMods(1)
		// Map timestamp without timezone to Spanner timestamp.
		return ddl.Timestamp{}, []schemaIssue{timestamp}
	case "varchar":
		maxExpectedMods(1)
		if len(mods) > 0 {
			return ddl.String{ddl.Int64Length{mods[0]}}, nil
		}
		return ddl.String{ddl.MaxLength{}}, nil
	}
	return ddl.String{ddl.MaxLength{}}, []schemaIssue{noGoodType}
}

func getTypeMods(conv *Conv, t nodes.List) (l []int64) {
	for _, x := range t.Items {
		switch t1 := x.(type) {
		case nodes.A_Const:
			switch t2 := t1.Val.(type) {
			case nodes.Integer:
				l = append(l, t2.Ival)
			default:
				conv.unexpected(fmt.Sprintf("Found %s node while processing Typmods", prNodeType(t1.Val)))
			}
		default:
			conv.unexpected(fmt.Sprintf("Found %s node while processing Typmods", prNodeType(x)))
		}
	}
	return l
}

func getArrayBounds(conv *Conv, t nodes.List) (l []int64) {
	for _, x := range t.Items {
		switch t := x.(type) {
		case nodes.Integer:
			// 'Ival' provides the array bound (-1 for an array where bound is not specified).
			l = append(l, t.Ival)
		default:
			conv.unexpected(fmt.Sprintf("Found %s node while processing ArrayBounds", prNodeType(x)))
		}
	}
	return l
}

func getTypeID(l []nodes.Node) (string, error) {
	// The pg_query library generates a pg_catalog schema prefix for most
	// types, but not for all. Typically "aliases" don't have the prefix.
	// For example, "boolean" is parsed to ["pg_catalog", "bool"], but "bool" is
	// parsed to ["bool"]. However the exact rules are unclear e.g. "date"
	// is parsed to just ["date"].
	// For simplicity, we strip off the pg_catalog prefix.
	var ids []string
	for _, t := range l {
		s, err := getString(t)
		if err != nil {
			return "", err
		}
		ids = append(ids, s)
	}
	if len(ids) > 1 && ids[0] == "pg_catalog" {
		ids = ids[1:]
	}
	return strings.Join(ids, "."), nil
}

func printType(pgCol pgColDef) string {
	s := pgCol.id
	if len(pgCol.mods) > 0 {
		var l []string
		for _, x := range pgCol.mods {
			l = append(l, strconv.FormatInt(x, 10))
		}
		s = fmt.Sprintf("%s(%s)", s, strings.Join(l, ","))
	}
	if len(pgCol.array) > 0 {
		l := []string{s}
		for _, x := range pgCol.array {
			if x == -1 {
				l = append(l, "[]")
			} else {
				l = append(l, fmt.Sprintf("[%d]", x))
			}
		}
		s = strings.Join(l, "")
	}
	return s
}

// getTableName extracts the table name from RangeVar n, and returns
// the raw extracted name (the PostgreSQL table name) plus the corresponding
// Spanner table name.
func getTableName(conv *Conv, n nodes.RangeVar) (string, string, error) {
	// RangeVar is used to represent table names. It consists of three components:
	//  Catalogname: database name; either not specified or the current database
	//  Schemaname: schemas are PostgreSql namepaces; often unspecified; defaults to "public"
	//  Relname: name of the table
	// We build a table name from these three components as follows:
	// a) nil components are dropped.
	// b) if more than one component is specified, they are joined using "."
	//    (Note that Spanner doesn't allow "." in table names, so this
	//    will eventually get re-mapped when we construct the Spanner table name).
	// c) Schemaname is dropped if it is "public".
	// d) return error if Relname is nil or "".
	var l []string
	if n.Catalogname != nil {
		l = append(l, *n.Catalogname)
	}
	if n.Schemaname != nil && *n.Schemaname != "public" { // Don't include "public".
		l = append(l, *n.Schemaname)
	}
	if n.Relname == nil && *n.Relname == "" {
		return "", "", fmt.Errorf("relname is empty: can't build table name")
	}
	l = append(l, *n.Relname)
	pgTable := strings.Join(l, ".")
	spTable, err := GetSpannerTable(conv, pgTable)
	if err != nil {
		return "", "", err
	}
	return pgTable, spTable, nil
}

type constraint struct {
	ct   nodes.ConstrType
	keys []string
}

// extractConstraints traverses a list of nodes (expecting them to be
// Constraint nodes), and collects the contraints they represent as
// a list of constraint-type/key-list pairs.
func extractConstraints(conv *Conv, n nodes.Node, pgTable string, l []nodes.Node) (cs []constraint) {
	for _, i := range l {
		switch d := i.(type) {
		case nodes.Constraint:
			var keys []string
			for _, j := range d.Keys.Items {
				k, err := getString(j)
				if err == nil {
					keys = append(keys, k)
				}
				if err != nil {
					conv.unexpected(fmt.Sprintf("Processing %v statement: error processing constraints: %s", reflect.TypeOf(n), err.Error()))
					conv.errorInStatement([]nodes.Node{n, d})
				}
			}
			cs = append(cs, constraint{d.Contype, keys})
		default:
			conv.unexpected(fmt.Sprintf("Processing %v statement: found %s node while processing constraints\n", reflect.TypeOf(n), reflect.TypeOf(d)))
		}
	}
	return cs
}

// analyzeColDefConstraints is like extractConstraints, but is specifially for
// ColDef constraints. These constraints don't specify a key since they
// are constraints for the column defined by ColDef.
func analyzeColDefConstraints(conv *Conv, n nodes.Node, pgTable string, l []nodes.Node, pgCol string) (cs []constraint) {
	// Do generic constraint processing and then set the keys of each constraint
	// to {pgCol}.
	for _, c := range extractConstraints(conv, n, pgTable, l) {
		if len(c.keys) != 0 {
			conv.unexpected("ColumnDef constraint has keys")
		}
		c.keys = []string{pgCol}
		cs = append(cs, c)
	}
	return cs
}

// getConstraint finds the constraint-type ct in the constraint list cs.
// It returns the keys from the constraint (if found) and a boolean to
// indicate if the constraint was found or not.
func getConstraint(ct nodes.ConstrType, cs []constraint) ([]string, bool) {
	for _, c := range cs {
		if c.ct == ct {
			return c.keys, true
		}
	}
	return []string{}, false
}

// updateSchema updates the schema for spTable based on the given constraints.
// 's' is the statement type being processed, and is used for debug messages.
func updateSchema(conv *Conv, spTable, pgTable string, cs []constraint, s string) {
	if len(cs) == 0 {
		return
	}
	if keys, found := getConstraint(nodes.CONSTR_PRIMARY, cs); found {
		ct := conv.spSchema[spTable]
		checkEmpty(conv, ct.Pks, s)
		ct.Pks = toDdlPkeys(conv, pgTable, keys) // Drop any previous primary keys.
		// In Spanner, primary key columns are usually annotated with NOT NULL,
		// but this can be omitted to allow NULL values in key columns.
		// In PostgreSQL, the primary key constraint is a combination of
		// NOT NULL and UNIQUE i.e. primary keys must be NOT NULL.
		// We preserve PostgreSQL semantics and enforce NOT NULL.
		ct.Cds = setNotNull(ct.Pks, ct.Cds)
		conv.spSchema[spTable] = ct
	}
	if keys, found := getConstraint(nodes.CONSTR_NOTNULL, cs); found {
		ct := conv.spSchema[spTable]
		ct.Cds = setNotNull(toDdlPkeys(conv, pgTable, keys), ct.Cds)
		conv.spSchema[spTable] = ct
	}
	for _, c := range cs {
		for _, col := range c.keys {
			pgCol := conv.pgSchema[pgTable].cols[col]
			switch c.ct {
			case nodes.CONSTR_DEFAULT:
				pgCol.issues = append(pgCol.issues, defaultValue)
			case nodes.CONSTR_FOREIGN:
				pgCol.issues = append(pgCol.issues, foreignKey)
			}
			conv.pgSchema[pgTable].cols[col] = pgCol
		}
	}
}

// toDdlPkeys converts a string list of PostgreSQL primary keys to
// Spanner ddl primary keys.
func toDdlPkeys(conv *Conv, pgTable string, s []string) (l []ddl.IndexKey) {
	for _, k := range s {
		col, err := GetSpannerCol(conv, pgTable, k, true)
		if err == nil {
			// PostgreSQL primary keys have no notation of ascending/descending.
			// We map them all into Spanner ascending primarary keys (the default).
			l = append(l, ddl.IndexKey{Col: col})
		} else {
			conv.unexpected(fmt.Sprintf("Can't get Spanner col: %s", err.Error()))
		}
	}
	return l
}

// getCols extracts and returns the columns for an InsertStatement.
func getCols(conv *Conv, pgTable string, l []nodes.Node) (cols []string, err error) {
	for _, n := range l {
		switch r := n.(type) {
		case nodes.ResTarget:
			if r.Name != nil {
				col, err := GetSpannerCol(conv, pgTable, *r.Name, true)
				if err != nil {
					return nil, fmt.Errorf("can't get col name: %w", err)
				}
				cols = append(cols, col)
			}
		default:
			return nil, fmt.Errorf("expecting ResTarget node but got %v node while processing Cols", reflect.TypeOf(r))
		}
	}
	return cols, nil
}

// getVals extracts and returns the values for an InsertStatement.
func getVals(conv *Conv, l [][]nodes.Node, n nodes.InsertStmt) (values []string) {
	for _, vl := range l {
		for _, v := range vl {
			switch c := v.(type) {
			case nodes.A_Const:
				switch st := c.Val.(type) {
				case nodes.String:
					values = append(values, st.Str)
				case nodes.Integer:
					// For uniformity, convert to string and handle everything in
					// dataConversion(). If performance of insert statements becomes a
					// high priority (it isn't right now), then consider preserving int64
					// here to avoid the int64 -> string -> int64 conversions.
					values = append(values, strconv.FormatInt(st.Ival, 10))
				default:
					conv.unexpected(fmt.Sprintf("Processing %v statement: found %s node for A_Const Val", reflect.TypeOf(n), reflect.TypeOf(c.Val)))
				}
			default:
				conv.unexpected(fmt.Sprintf("Processing %v statement: found %s node in ValuesList", reflect.TypeOf(n), reflect.TypeOf(v)))
			}
		}
	}
	return values
}

func logStmtError(conv *Conv, n nodes.Node, err error) {
	conv.unexpected(fmt.Sprintf("Processing %v statement: %s", reflect.TypeOf(n), err))
	conv.errorInStatement([]nodes.Node{n})
}

func getString(node nodes.Node) (string, error) {
	switch n := node.(type) {
	case nodes.String:
		return n.Str, nil
	default:
		return "", fmt.Errorf("node %v is a not String node", reflect.TypeOf(node))
	}
}

// checkEmpty verifies that pkeys is empty and generates a warning if it isn't.
// PostgreSQL explicitly forbids multiple primary keys.
func checkEmpty(conv *Conv, pkeys []ddl.IndexKey, s string) {
	if len(pkeys) != 0 {
		conv.unexpected(fmt.Sprintf("%s statement is adding a second primary key", s))
	}
}

func mkColComment(name, t string) string {
	if needQuote(name) {
		name = strconv.Quote(name)
	}
	return "From PostgreSQL: " + name + " " + t
}

func mkTableComment(name string) string {
	if needQuote(name) {
		name = strconv.Quote(name)
	}
	return "Spanner schema for PostgreSQL table " + name
}

// needQuote returns true if we need to quote this name before
// printing it as part of schema comments.
func needQuote(s string) bool {
	for _, r := range s {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || unicode.IsPunct(r) {
			continue
		}
		return true
	}
	return false
}

func setNotNull(pkeys []ddl.IndexKey, cds map[string]ddl.ColumnDef) map[string]ddl.ColumnDef {
	for _, pk := range pkeys {
		cd := cds[pk.Col]
		cd.NotNull = true
		cds[pk.Col] = cd
	}
	return cds
}

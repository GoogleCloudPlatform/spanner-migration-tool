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

	pg_query "github.com/lfittl/pg_query_go"
	nodes "github.com/lfittl/pg_query_go/nodes"

	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/schema"
)

type copyOrInsert struct {
	stmt  stmtType
	table string
	cols  []string
	vals  []string // Empty for COPY-FROM.
}

type stmtType int

const (
	copyFrom stmtType = iota
	insert
)

// ProcessPgDump reads pg_dump data from r and does schema or data conversion,
// depending on whether conv is configured for schema mode or data mode.
// In schema mode, ProcessPgDump incrementally builds a schema (updating conv).
// In data mode, ProcessPgDump uses this schema to convert PostgreSQL data
// and writes it to Spanner, using the data sink specified in conv.
func ProcessPgDump(conv *internal.Conv, r *internal.Reader) error {
	for {
		startLine := r.LineNumber
		startOffset := r.Offset
		b, stmts, err := readAndParseChunk(conv, r)
		if err != nil {
			return err
		}
		ci := processStatements(conv, stmts)
		internal.VerbosePrintf("Parsed SQL command at line=%d/fpos=%d: %d stmts (%d lines, %d bytes) ci=%v\n", startLine, startOffset, len(stmts), r.LineNumber-startLine, len(b), ci != nil)
		if ci != nil {
			switch ci.stmt {
			case copyFrom:
				processCopyBlock(conv, ci.table, ci.cols, r)
			case insert:
				// Handle INSERT statements where columns are not
				// specified i.e. an insert for all table columns.
				if len(ci.cols) == 0 {
					ProcessDataRow(conv, ci.table, conv.SrcSchema[ci.table].ColNames, ci.vals)
				} else {
					ProcessDataRow(conv, ci.table, ci.cols, ci.vals)
				}
			}
		}
		if r.EOF {
			break
		}
	}
	if conv.SchemaMode() {
		schemaToDDL(conv)
		conv.AddPrimaryKeys()
	}

	return nil
}

// readAndParseChunk parses a chunk of pg_dump data, returning the bytes read,
// the parsed AST (nil if nothing read), and whether we've hit end-of-file.
func readAndParseChunk(conv *internal.Conv, r *internal.Reader) ([]byte, []nodes.Node, error) {
	var l [][]byte
	for {
		b := r.ReadLine()
		l = append(l, b)
		// If we see a semicolon or eof, we're likely to have a command, so try to parse it.
		// Note: we could just parse every iteration, but that would mean more attempts at parsing.
		if strings.Contains(string(b), ";") || r.EOF {
			n := 0
			for i := range l {
				n += len(l[i])
			}
			s := make([]byte, n)
			n = 0
			for i := range l {
				n += copy(s[n:], l[i])
			}
			tree, err := pg_query.Parse(string(s))
			if err == nil {
				return s, tree.Statements, nil
			}
			// Likely causes of failing to parse:
			// a) complex statements with embedded semicolons e.g. 'CREATE FUNCTION'
			// b) a semicolon embedded in a multi-line comment, or
			// c) a semicolon embedded a string constant or column/table name.
			// We deal with this case by reading another line and trying again.
			conv.Stats.Reparsed++
		}
		if r.EOF {
			return nil, nil, fmt.Errorf("Error parsing last %d line(s) of input", len(l))
		}
	}
}

func processCopyBlock(conv *internal.Conv, srcTable string, srcCols []string, r *internal.Reader) {
	internal.VerbosePrintf("Parsing COPY-FROM stdin block starting at line=%d/fpos=%d\n", r.LineNumber, r.Offset)
	for {
		b := r.ReadLine()
		if string(b) == "\\.\n" || string(b) == "\\.\r\n" {
			internal.VerbosePrintf("Parsed COPY-FROM stdin block ending at line=%d/fpos=%d\n", r.LineNumber, r.Offset)
			return
		}
		if r.EOF {
			conv.Unexpected("Reached eof while parsing copy-block")
			return
		}
		conv.StatsAddRow(srcTable, conv.SchemaMode())
		// We have to read the copy-block data so that we can process the remaining
		// pg_dump content. However, if we don't want the data, stop here.
		// In particular, avoid the strings.Split and ProcessDataRow calls below, which
		// will be expensive for huge datasets.
		if !conv.DataMode() {
			continue
		}
		// pg_dump escapes backslash in copy-block statements. For example:
		// a) a\"b becomes a\\"b in COPY-BLOCK (but 'a\"b' in INSERT-INTO)
		// b) {"a\"b"} becomes {"a\\"b"} in COPY-BLOCK (but '{"a\"b"}' in INSERT-INTO)
		// Note: a'b and {a'b} are unchanged in COPY-BLOCK and INSERT-INTO.
		s := strings.ReplaceAll(string(b), `\\`, `\`)
		// COPY-FROM blocks use tabs to separate data items. Note that space within data
		// items is significant e.g. if a table row contains data items "a ", " b "
		// it will be shown in the COPY-FROM block as "a \t b ".
		ProcessDataRow(conv, srcTable, srcCols, strings.Split(strings.Trim(s, "\r\n"), "\t"))
	}
}

// processStatements extracts schema information and data from PostgreSQL
// statements, updating Conv with new schema information, and returning
// copyOrInsert if a COPY-FROM or INSERT statement is encountered.
// Note that the actual parsing/processing of COPY-FROM data blocks is
// handled elsewhere (see process.go).
func processStatements(conv *internal.Conv, statements []nodes.Node) *copyOrInsert {
	// Typically we'll have only one statement, but we handle the general case.
	for i, node := range statements {
		switch n := node.(type) {
		// Unwrap RawStatement.
		case nodes.RawStmt:
			node = n.Stmt
		}
		switch n := node.(type) {
		case nodes.AlterTableStmt:
			if conv.SchemaMode() {
				processAlterTableStmt(conv, n)
			}
		case nodes.CopyStmt:
			if i != len(statements)-1 {
				conv.Unexpected("CopyFrom is not the last statement in batch: ignoring following statements")
				conv.ErrorInStatement(prNodes([]nodes.Node{node}))
			}
			return processCopyStmt(conv, n)
		case nodes.CreateStmt:
			if conv.SchemaMode() {
				processCreateStmt(conv, n)
			}
		case nodes.InsertStmt:
			return processInsertStmt(conv, n)
		case nodes.VariableSetStmt:
			if conv.SchemaMode() {
				processVariableSetStmt(conv, n)
			}
		default:
			conv.SkipStatement(prNodes([]nodes.Node{node}))
		}
	}
	return nil
}

func processAlterTableStmt(conv *internal.Conv, n nodes.AlterTableStmt) {
	if n.Relation == nil {
		logStmtError(conv, n, fmt.Errorf("relation is nil"))
		return
	}
	table, err := getTableName(conv, *n.Relation)
	if err != nil {
		logStmtError(conv, n, fmt.Errorf("can't get table name: %w", err))
		return
	}
	if _, ok := conv.SrcSchema[table]; ok {
		for _, i := range n.Cmds.Items {
			switch a := i.(type) {
			case nodes.AlterTableCmd:
				switch {
				case a.Subtype == nodes.AT_SetNotNull && a.Name != nil:
					c := constraint{ct: nodes.CONSTR_NOTNULL, cols: []string{*a.Name}}
					updateSchema(conv, table, []constraint{c}, "ALTER TABLE")
					conv.SchemaStatement(prNodes([]nodes.Node{n, a}))
				case a.Subtype == nodes.AT_AddConstraint && a.Def != nil:
					switch d := a.Def.(type) {
					case nodes.Constraint:
						updateSchema(conv, table, extractConstraints(conv, n, table, []nodes.Node{d}), "ALTER TABLE")
						conv.SchemaStatement(prNodes([]nodes.Node{n, a, d}))
					default:
						conv.SkipStatement(prNodes([]nodes.Node{n, a, d}))
					}
				default:
					conv.SkipStatement(prNodes([]nodes.Node{n, a}))
				}
			default:
				conv.SkipStatement(prNodes([]nodes.Node{n, a}))
			}
		}
	} else {
		// In PostgreSQL, AlterTable statements can be applied to views,
		// sequences and indexes in addition to tables. Since we only
		// track tables created by "CREATE TABLE", this lookup can fail.
		// For debugging purposes we log the lookup failure if we're
		// in verbose mode, but otherwise  we just skip these statements.
		conv.SkipStatement(prNodes([]nodes.Node{n}))
		internal.VerbosePrintf("Processing %v statement: table %s not found", reflect.TypeOf(n), table)
	}
}

func processCreateStmt(conv *internal.Conv, n nodes.CreateStmt) {
	var colNames []string
	colDef := make(map[string]schema.Column)
	if n.Relation == nil {
		logStmtError(conv, n, fmt.Errorf("relation is nil"))
		return
	}
	table, err := getTableName(conv, *n.Relation)
	if err != nil {
		logStmtError(conv, n, fmt.Errorf("can't get table name: %w", err))
		return
	}
	var constraints []constraint
	for _, te := range n.TableElts.Items {
		switch i := te.(type) {
		case nodes.ColumnDef:
			name, col, cdConstraints, err := processColumn(conv, i, table)
			if err != nil {
				logStmtError(conv, n, err)
				return
			}
			colNames = append(colNames, name)
			colDef[name] = col
			constraints = append(constraints, cdConstraints...)
		case nodes.Constraint:
			// Note: there should be at most one Constraint node in
			// n.TableElts.Items. We don't check this. We just keep
			// collecting constraints.
			constraints = append(constraints, extractConstraints(conv, n, table, []nodes.Node{i})...)
		default:
			conv.Unexpected(fmt.Sprintf("Found %s node while processing CreateStmt TableElts", PrNodeType(i)))
		}
	}
	conv.SchemaStatement(prNodes([]nodes.Node{n}))
	conv.SrcSchema[table] = schema.Table{
		Name:     table,
		ColNames: colNames,
		ColDefs:  colDef}
	// Note: constraints contains all info about primary keys,
	// not-null keys and foreign keys.
	updateSchema(conv, table, constraints, "CREATE TABLE")
}

func processColumn(conv *internal.Conv, n nodes.ColumnDef, table string) (string, schema.Column, []constraint, error) {
	mods := getTypeMods(conv, n.TypeName.Typmods)
	if n.Colname == nil {
		return "", schema.Column{}, nil, fmt.Errorf("colname is nil")
	}
	name := *n.Colname
	tid, err := getTypeID(n.TypeName.Names.Items)
	if err != nil {
		return "", schema.Column{}, nil, fmt.Errorf("can't get type id for %s: %w", name, err)
	}
	ty := schema.Type{
		Name:        tid,
		Mods:        mods,
		ArrayBounds: getArrayBounds(conv, n.TypeName.ArrayBounds)}
	return name, schema.Column{Name: name, Type: ty}, analyzeColDefConstraints(conv, n, table, n.Constraints.Items, name), nil
}

func processInsertStmt(conv *internal.Conv, n nodes.InsertStmt) *copyOrInsert {
	if n.Relation == nil {
		logStmtError(conv, n, fmt.Errorf("relation is nil"))
		return nil
	}
	table, err := getTableName(conv, *n.Relation)
	if err != nil {
		logStmtError(conv, n, fmt.Errorf("can't get table name: %w", err))
		return nil
	}
	conv.StatsAddRow(table, conv.SchemaMode())
	colNames, err := getCols(conv, table, n.Cols.Items)
	if err != nil {
		logStmtError(conv, n, fmt.Errorf("can't get col name: %w", err))
		conv.StatsAddBadRow(table, conv.SchemaMode())
		return nil
	}
	var values []string
	switch sel := n.SelectStmt.(type) {
	case nodes.SelectStmt:
		values = getVals(conv, sel.ValuesLists, n)
		conv.DataStatement(prNodes([]nodes.Node{n}))
		if conv.DataMode() {
			return &copyOrInsert{stmt: insert, table: table, cols: colNames, vals: values}
		}
	default:
		conv.Unexpected(fmt.Sprintf("Found %s node while processing InsertStmt SelectStmt", PrNodeType(sel)))
	}
	return nil
}

func processCopyStmt(conv *internal.Conv, n nodes.CopyStmt) *copyOrInsert {
	// Always return a copyOrInsert{stmt: copyFrom, ...} even if we
	// encounter errors. Otherwise we won't be able to parse
	// the data portion of the COPY-FROM statement, and we'll
	// likely get stuck at this point in the pg_dump file.
	table := "BOGUS_COPY_FROM_TABLE"
	var err error
	if n.Relation != nil {
		table, err = getTableName(conv, *n.Relation)
		if err != nil {
			conv.Unexpected(fmt.Sprintf("Processing %v statement: %s", reflect.TypeOf(n), err))
		}
	} else {
		logStmtError(conv, n, fmt.Errorf("relation is nil"))
	}
	var cols []string
	for _, a := range n.Attlist.Items {
		s, err := getString(a)
		if err != nil {
			conv.Unexpected(fmt.Sprintf("Processing %v statement Attlist: %s", reflect.TypeOf(n), err))
			s = "BOGUS_COPY_FROM_COLUMN"
		}
		cols = append(cols, s)
	}
	conv.DataStatement(prNodes([]nodes.Node{n}))
	return &copyOrInsert{stmt: copyFrom, table: table, cols: cols}
}

func processVariableSetStmt(conv *internal.Conv, n nodes.VariableSetStmt) {
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

func getTypeMods(conv *internal.Conv, t nodes.List) (l []int64) {
	for _, x := range t.Items {
		switch t1 := x.(type) {
		case nodes.A_Const:
			switch t2 := t1.Val.(type) {
			case nodes.Integer:
				l = append(l, t2.Ival)
			default:
				conv.Unexpected(fmt.Sprintf("Found %s node while processing Typmods", PrNodeType(t1.Val)))
			}
		default:
			conv.Unexpected(fmt.Sprintf("Found %s node while processing Typmods", PrNodeType(x)))
		}
	}
	return l
}

func getArrayBounds(conv *internal.Conv, t nodes.List) (l []int64) {
	for _, x := range t.Items {
		switch t := x.(type) {
		case nodes.Integer:
			// 'Ival' provides the array bound (-1 for an array where bound is not specified).
			l = append(l, t.Ival)
		default:
			conv.Unexpected(fmt.Sprintf("Found %s node while processing ArrayBounds", PrNodeType(x)))
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

// getTableName extracts the table name from RangeVar n, and returns
// the raw extracted name (the PostgreSQL table name).
func getTableName(conv *internal.Conv, n nodes.RangeVar) (string, error) {
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
		return "", fmt.Errorf("relname is empty: can't build table name")
	}
	l = append(l, *n.Relname)
	return strings.Join(l, "."), nil
}

type constraint struct {
	ct   nodes.ConstrType
	cols []string
	/* Fields used for FOREIGN KEY constraints: */
	referCols  []string
	referTable string
}

// extractConstraints traverses a list of nodes (expecting them to be
// Constraint nodes), and collects the contraints they represent as
// a list of constraint-type/column-names pairs.
func extractConstraints(conv *internal.Conv, n nodes.Node, table string, l []nodes.Node) (cs []constraint) {
	for _, i := range l {
		switch d := i.(type) {
		case nodes.Constraint:
			var cols, referCols []string
			var referTable string
			switch d.Contype {
			case nodes.CONSTR_FOREIGN:
				t, err := getTableName(conv, *d.Pktable)
				if err == nil {
					referTable = t
				}
				if err != nil {
					conv.Unexpected(fmt.Sprintf("Processing %v statement: error processing constraints: %s", reflect.TypeOf(n), err.Error()))
					conv.ErrorInStatement(prNodes([]nodes.Node{n, d}))
					continue
				}
				for i := range d.FkAttrs.Items {
					k, err := getString(d.FkAttrs.Items[i])
					if err != nil {
						conv.Unexpected(fmt.Sprintf("Processing %v statement: error processing constraints: %s", reflect.TypeOf(n), err.Error()))
						conv.ErrorInStatement(prNodes([]nodes.Node{n, d}))
						continue
					}
					cols = append(cols, k)
				}
				for i := range d.PkAttrs.Items {
					f, err := getString(d.PkAttrs.Items[i])
					if err != nil {
						conv.Unexpected(fmt.Sprintf("Processing %v statement: error processing constraints: %s", reflect.TypeOf(n), err.Error()))
						conv.ErrorInStatement(prNodes([]nodes.Node{n, d}))
						continue
					}
					referCols = append(referCols, f)
				}
			default:
				for _, j := range d.Keys.Items {
					k, err := getString(j)
					if err != nil {
						conv.Unexpected(fmt.Sprintf("Processing %v statement: error processing constraints: %s", reflect.TypeOf(n), err.Error()))
						conv.ErrorInStatement(prNodes([]nodes.Node{n, d}))
						continue
					}
					cols = append(cols, k)
				}
			}
			cs = append(cs, constraint{ct: d.Contype, cols: cols, referCols: referCols, referTable: referTable})
		default:
			conv.Unexpected(fmt.Sprintf("Processing %v statement: found %s node while processing constraints\n", reflect.TypeOf(n), reflect.TypeOf(d)))
		}
	}
	return cs
}

// analyzeColDefConstraints is like extractConstraints, but is specifially for
// ColDef constraints. These constraints don't specify a key since they
// are constraints for the column defined by ColDef.
func analyzeColDefConstraints(conv *internal.Conv, n nodes.Node, table string, l []nodes.Node, pgCol string) (cs []constraint) {
	// Do generic constraint processing and then set the keys of each constraint
	// to {pgCol}.
	for _, c := range extractConstraints(conv, n, table, l) {
		if len(c.cols) != 0 {
			conv.Unexpected("ColumnDef constraint has keys")
		}
		c.cols = []string{pgCol}
		cs = append(cs, c)
	}
	return cs
}

// updateSchema updates the schema for table based on the given constraints.
// 's' is the statement type being processed, and is used for debug messages.
func updateSchema(conv *internal.Conv, table string, cs []constraint, stmtType string) {
	for _, c := range cs {
		switch c.ct {
		case nodes.CONSTR_PRIMARY:
			ct := conv.SrcSchema[table]
			checkEmpty(conv, ct.PrimaryKeys, stmtType)
			ct.PrimaryKeys = toSchemaKeys(conv, table, c.cols) // Drop any previous primary keys.
			// In Spanner, primary key columns are usually annotated with NOT NULL,
			// but this can be omitted to allow NULL values in key columns.
			// In PostgreSQL, the primary key constraint is a combination of
			// NOT NULL and UNIQUE i.e. primary keys must be NOT NULL.
			// We preserve PostgreSQL semantics and enforce NOT NULL.
			updateCols(nodes.CONSTR_NOTNULL, c.cols, ct.ColDefs)
			conv.SrcSchema[table] = ct
		case nodes.CONSTR_FOREIGN:
			ct := conv.SrcSchema[table]
			ct.ForeignKeys = append(ct.ForeignKeys, toForeignKeys(c)) // Append to previous foreign jeys.
			updateCols(nodes.CONSTR_FOREIGN, c.cols, ct.ColDefs)
			conv.SrcSchema[table] = ct
		default:
			ct := conv.SrcSchema[table]
			updateCols(c.ct, c.cols, ct.ColDefs)
			conv.SrcSchema[table] = ct
		}
	}
}

// updateCols updates colDef with new constraints. Specifically, we apply
// 'ct' to each column in colNames.
func updateCols(ct nodes.ConstrType, colNames []string, colDef map[string]schema.Column) {
	// TODO: add cases for other constraints.
	for _, c := range colNames {
		cd := colDef[c]
		switch ct {
		case nodes.CONSTR_NOTNULL:
			cd.NotNull = true
		case nodes.CONSTR_DEFAULT:
			cd.Ignored.Default = true
		}
		colDef[c] = cd
	}
}

// toSchemaKeys converts a string list of PostgreSQL primary keys to
// schema primary keys.
func toSchemaKeys(conv *internal.Conv, table string, s []string) (l []schema.Key) {
	for _, k := range s {
		// PostgreSQL primary keys have no notation of ascending/descending.
		// We map them all into ascending primarary keys.
		l = append(l, schema.Key{Column: k})
	}
	return l
}

// toForeignKeys converts a string list of PostgreSQL foreign keys to
// schema foreign keys.
func toForeignKeys(fk constraint) (fkey schema.ForeignKey) {
	fkey = schema.ForeignKey{Columns: fk.cols,
		ReferTable:   fk.referTable,
		ReferColumns: fk.referCols}
	return fkey
}

// getCols extracts and returns the column names for an InsertStatement.
func getCols(conv *internal.Conv, table string, l []nodes.Node) (cols []string, err error) {
	for _, n := range l {
		switch r := n.(type) {
		case nodes.ResTarget:
			if r.Name != nil {
				cols = append(cols, *r.Name)
			}
		default:
			return nil, fmt.Errorf("expecting ResTarget node but got %v node while processing Cols", reflect.TypeOf(r))
		}
	}
	return cols, nil
}

// getVals extracts and returns the values for an InsertStatement.
func getVals(conv *internal.Conv, l [][]nodes.Node, n nodes.InsertStmt) (values []string) {
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
					conv.Unexpected(fmt.Sprintf("Processing %v statement: found %s node for A_Const Val", reflect.TypeOf(n), reflect.TypeOf(c.Val)))
				}
			default:
				conv.Unexpected(fmt.Sprintf("Processing %v statement: found %s node in ValuesList", reflect.TypeOf(n), reflect.TypeOf(v)))
			}
		}
	}
	return values
}

func logStmtError(conv *internal.Conv, n nodes.Node, err error) {
	conv.Unexpected(fmt.Sprintf("Processing %v statement: %s", reflect.TypeOf(n), err))
	conv.ErrorInStatement(prNodes([]nodes.Node{n}))
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
func checkEmpty(conv *internal.Conv, pkeys []schema.Key, stmtType string) {
	if len(pkeys) != 0 {
		conv.Unexpected(fmt.Sprintf("%s statement is adding a second primary key", stmtType))
	}
}

// PrNodeType strips off "pg_query." prefix from nodes.Nodes type.
func PrNodeType(n nodes.Node) string {
	return strings.TrimPrefix(reflect.TypeOf(n).Name(), "pg_query.")
}

func prNodes(l []nodes.Node) string {
	var s []string
	for _, n := range l {
		s = append(s, PrNodeType(n))
	}
	return strings.Join(s, ".")
}

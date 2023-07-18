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

	pg_query "github.com/pganalyze/pg_query_go/v2"

	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/logger"
	"github.com/cloudspannerecosystem/harbourbridge/schema"
	"github.com/cloudspannerecosystem/harbourbridge/sources/common"
)

// DbDumpImpl Postgres specific implementation for DdlDumpImpl.
type DbDumpImpl struct{}

type copyOrInsert struct {
	stmt  stmtType
	table string
	cols  []string
	rows  [][]string // Empty for COPY-FROM.
}

type stmtType int

const (
	copyFrom stmtType = iota
	insert
)

// GetToDdl functions below implement the common.DbDump interface
func (ddi DbDumpImpl) GetToDdl() common.ToDdl {
	return ToDdlImpl{}
}

// ProcessDump calls processPgDump to read a Postgres dump file
func (ddi DbDumpImpl) ProcessDump(conv *internal.Conv, r *internal.Reader) error {
	return processPgDump(conv, r)
}

// processPgDump reads pg_dump data from r and does schema or data conversion,
// depending on whether conv is configured for schema mode or data mode.
// In schema mode, ProcessPgDump incrementally builds a schema (updating conv).
// In data mode, ProcessPgDump uses this schema to convert PostgreSQL data
// and writes it to Spanner, using the data sink specified in conv.
func processPgDump(conv *internal.Conv, r *internal.Reader) error {
	for {
		startLine := r.LineNumber
		startOffset := r.Offset
		b, stmts, err := readAndParseChunk(conv, r)
		if err != nil {
			return err
		}
		ci := processStatements(conv, stmts)
		internal.VerbosePrintf("Parsed SQL command at line=%d/fpos=%d: %d stmts (%d lines, %d bytes) ci=%v\n", startLine, startOffset, len(stmts), r.LineNumber-startLine, len(b), ci != nil)
		logger.Log.Debug(fmt.Sprintf("Parsed SQL command at line=%d/fpos=%d: %d stmts (%d lines, %d bytes) ci=%v\n", startLine, startOffset, len(stmts), r.LineNumber-startLine, len(b), ci != nil))
		if ci != nil {
			switch ci.stmt {
			case copyFrom:
				commonColIds, err := common.PrepareColumns(conv, ci.table, ci.cols)
				if err != nil && !conv.SchemaMode() {
					return err
				}
				processCopyBlock(conv, ci.table, commonColIds, ci.cols, r)
			case insert:
				if conv.SchemaMode() {
					continue
				}
				// Handle INSERT statements where columns are not
				// specified i.e. an insert for all table columns.
				var colNames []string
				if len(ci.cols) == 0 {
					for _, col := range conv.SrcSchema[ci.table].ColIds {
						colNames = append(colNames, conv.SrcSchema[ci.table].ColDefs[col].Name)
					}
				} else {
					colNames = ci.cols
				}
				commonColIds, err := common.PrepareColumns(conv, ci.table, colNames)
				if err != nil {
					return err
				}
				colNameIdMap := internal.GetSrcColNameIdMap(conv.SrcSchema[ci.table])
				for _, vals := range ci.rows {
					newVals, err := common.PrepareValues(conv, ci.table, colNameIdMap, commonColIds, colNames, vals)
					if err != nil {
						srcTableName := conv.SrcSchema[ci.table].Name
						conv.Unexpected(fmt.Sprintf("Error while converting data: %s\n", err))
						conv.StatsAddBadRow(srcTableName, conv.DataMode())
						conv.CollectBadRow(srcTableName, colNames, vals)
						continue
					}
					mapSrcColIdToVal := make(map[string]string)
					for i, srcolName := range colNames {
						mapSrcColIdToVal[colNameIdMap[srcolName]] = vals[i]
					}
					ProcessDataRow(conv, ci.table, commonColIds, newVals, internal.AdditionalDataAttributes{}, mapSrcColIdToVal)
				}
			}
		}
		if r.EOF {
			break
		}
	}
	internal.ResolveForeignKeyIds(conv.SrcSchema)
	return nil
}

// readAndParseChunk parses a chunk of pg_dump data, returning the bytes read,
// the parsed AST (nil if nothing read), and whether we've hit end-of-file.
func readAndParseChunk(conv *internal.Conv, r *internal.Reader) ([]byte, []*pg_query.RawStmt, error) {
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
				return s, tree.Stmts, nil
			}
			// Likely causes of failing to parse:
			// a) complex statements with embedded semicolons e.g. 'CREATE FUNCTION'
			// b) a semicolon embedded in a multi-line comment, or
			// c) a semicolon embedded a string constant or column/table name.
			// We deal with this case by reading another line and trying again.
			conv.Stats.Reparsed++
		}
		if r.EOF {
			return nil, nil, fmt.Errorf("error parsing last %d line(s) of input", len(l))
		}
	}
}

func processCopyBlock(conv *internal.Conv, tableId string, commonColIds, srcCols []string, r *internal.Reader) {
	srcTableName := conv.SrcSchema[tableId].Name
	internal.VerbosePrintf("Parsing COPY-FROM stdin block starting at line=%d/fpos=%d\n", r.LineNumber, r.Offset)
	logger.Log.Debug(fmt.Sprintf("Parsing COPY-FROM stdin block starting at line=%d/fpos=%d\n", r.LineNumber, r.Offset))
	for {
		b := r.ReadLine()
		if string(b) == "\\.\n" || string(b) == "\\.\r\n" {
			internal.VerbosePrintf("Parsed COPY-FROM stdin block ending at line=%d/fpos=%d\n", r.LineNumber, r.Offset)
			logger.Log.Debug(fmt.Sprintf("Parsed COPY-FROM stdin block ending at line=%d/fpos=%d\n", r.LineNumber, r.Offset))
			return
		}
		if r.EOF {
			conv.Unexpected("Reached eof while parsing copy-block")
			return
		}
		conv.StatsAddRow(srcTableName, conv.SchemaMode())
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
		values := strings.Split(strings.Trim(s, "\r\n"), "\t")
		colNameIdMap := internal.GetSrcColNameIdMap(conv.SrcSchema[tableId])
		newValues, err := common.PrepareValues(conv, tableId, colNameIdMap, commonColIds, srcCols, values)
		if err != nil {
			conv.Unexpected(fmt.Sprintf("Error while converting data: %s\n", err))
			conv.StatsAddBadRow(srcTableName, conv.DataMode())
			conv.CollectBadRow(srcTableName, srcCols, values)
			continue
		}
		mapSrcColIdToVal := make(map[string]string)
		for i, srcolName := range srcCols {
			mapSrcColIdToVal[colNameIdMap[srcolName]] = values[i]
		}
		ProcessDataRow(conv, tableId, commonColIds, newValues, internal.AdditionalDataAttributes{}, mapSrcColIdToVal)
	}
}

// processStatements extracts schema information and data from PostgreSQL
// statements, updating Conv with new schema information, and returning
// copyOrInsert if a COPY-FROM or INSERT statement is encountered.
// Note that the actual parsing/processing of COPY-FROM data blocks is
// handled elsewhere (see process.go).
func processStatements(conv *internal.Conv, rawStmts []*pg_query.RawStmt) *copyOrInsert {
	// Typically we'll have only one statement, but we handle the general case.
	for i, rawStmt := range rawStmts {
		node := rawStmt.Stmt
		switch n := node.GetNode().(type) {
		case *pg_query.Node_AlterTableStmt:
			if conv.SchemaMode() {
				processAlterTableStmt(conv, n.AlterTableStmt)
			}
		case *pg_query.Node_CopyStmt:
			if i != len(rawStmts)-1 {
				conv.Unexpected("CopyFrom is not the last statement in batch: ignoring following statements")
				conv.ErrorInStatement(printNodeType(n.CopyStmt))
			}
			return processCopyStmt(conv, n.CopyStmt)
		case *pg_query.Node_CreateStmt:
			if conv.SchemaMode() {
				processCreateStmt(conv, n.CreateStmt)
			}
		case *pg_query.Node_InsertStmt:
			return processInsertStmt(conv, n.InsertStmt)
		case *pg_query.Node_VariableSetStmt:
			if conv.SchemaMode() {
				processVariableSetStmt(conv, n.VariableSetStmt)
			}
		case *pg_query.Node_IndexStmt:
			if conv.SchemaMode() {
				processIndexStmt(conv, n.IndexStmt)
			}
		default:
			conv.SkipStatement(printNodeType(n))
		}
	}
	return nil
}

func processIndexStmt(conv *internal.Conv, n *pg_query.IndexStmt) {
	if n.Relation == nil {
		logStmtError(conv, n, fmt.Errorf("cannot process index statement with nil relation"))
		return
	}
	tableName, err := getTableName(conv, n.Relation)
	if err != nil {
		logStmtError(conv, n, fmt.Errorf("can't get table name: %w", err))
		return
	}
	if tbl, ok := internal.GetSrcTableByName(conv.SrcSchema, tableName); ok {
		ctable := conv.SrcSchema[tbl.Id]
		ctable.Indexes = append(ctable.Indexes, schema.Index{
			Id:     internal.GenerateIndexesId(),
			Name:   n.Idxname,
			Unique: n.Unique,
			Keys:   toIndexKeys(conv, n.Idxname, n.IndexParams, ctable.ColNameIdMap),
		})
		conv.SrcSchema[tbl.Id] = ctable
	} else {
		conv.Unexpected(fmt.Sprintf("Table %s not found while processing index statement", tableName))
		conv.SkipStatement(printNodeType(n))
	}
}

func processAlterTableStmt(conv *internal.Conv, n *pg_query.AlterTableStmt) {
	if n.Relation == nil {
		logStmtError(conv, n, fmt.Errorf("relation is nil"))
		return
	}
	tableName, err := getTableName(conv, n.Relation)
	if err != nil {
		logStmtError(conv, n, fmt.Errorf("can't get table name: %w", err))
		return
	}

	if tbl, ok := internal.GetSrcTableByName(conv.SrcSchema, tableName); ok {
		for _, i := range n.Cmds {
			cmd := i.GetNode()
			switch t := cmd.(type) {
			case *pg_query.Node_AlterTableCmd:
				a := t.AlterTableCmd
				switch {
				case a.Subtype == pg_query.AlterTableType_AT_SetNotNull && a.Name != "":
					c := constraint{ct: pg_query.ConstrType_CONSTR_NOTNULL, cols: []string{a.Name}}
					updateSchema(conv, tbl.Id, []constraint{c}, "ALTER TABLE")
					conv.SchemaStatement(strings.Join([]string{printNodeType(n), printNodeType(t)}, "."))
				case a.Subtype == pg_query.AlterTableType_AT_AddConstraint && a.Def != nil:
					switch at := a.Def.GetNode().(type) {
					case *pg_query.Node_Constraint:
						updateSchema(conv, tbl.Id, extractConstraints(conv, printNodeType(n), tableName, []*pg_query.Node{a.Def}), "ALTER TABLE")
						conv.SchemaStatement(strings.Join([]string{printNodeType(n), printNodeType(t), printNodeType(at)}, "."))
					default:
						conv.SkipStatement(strings.Join([]string{printNodeType(n), printNodeType(t), printNodeType(at)}, "."))
					}
				default:
					conv.SkipStatement(strings.Join([]string{printNodeType(n), printNodeType(t)}, "."))
				}
			default:
				conv.SkipStatement(strings.Join([]string{printNodeType(n), printNodeType(t)}, "."))
			}
		}
	} else {
		// In PostgreSQL, AlterTable statements can be applied to views,
		// sequences and indexes in addition to tables. Since we only
		// track tables created by "CREATE TABLE", this lookup can fail.
		// For debugging purposes we log the lookup failure if we're
		// in verbose mode, but otherwise  we just skip these statements.
		conv.SkipStatement(printNodeType(n))
		internal.VerbosePrintf("Processing %v statement: table %s not found", printNodeType(n), tableName)
		logger.Log.Debug(fmt.Sprintf("Processing %v statement: table %s not found", printNodeType(n), tableName))
	}
}

func processCreateStmt(conv *internal.Conv, n *pg_query.CreateStmt) {
	colDef := make(map[string]schema.Column)
	if n.Relation == nil {
		logStmtError(conv, n, fmt.Errorf("relation is nil"))
		return
	}
	table, err := getTableName(conv, n.Relation)
	if err != nil {
		logStmtError(conv, n, fmt.Errorf("can't get table name: %w", err))
		return
	}
	if len(n.InhRelations) > 0 {
		// Skip inherited tables.
		conv.SkipStatement(printNodeType(n))
		conv.Unexpected(fmt.Sprintf("Found inherited table %s -- we do not currently handle inherited tables", table))
		internal.VerbosePrintf("Processing %v statement: table %s is inherited table", printNodeType(n), table)
		logger.Log.Debug(fmt.Sprintf("Processing %v statement: table %s is inherited table", printNodeType(n), table))

		return
	}
	var constraints []constraint
	var colIds []string
	colNameIdMap := make(map[string]string)
	for _, te := range n.TableElts {
		switch te.GetNode().(type) {
		case *pg_query.Node_ColumnDef:
			_, col, cdConstraints, err := processColumn(conv, te.GetColumnDef(), table)
			if err != nil {
				logStmtError(conv, n, err)
				return
			}
			col.Id = internal.GenerateColumnId()
			colDef[col.Id] = col
			colIds = append(colIds, col.Id)
			colNameIdMap[col.Name] = col.Id
			constraints = append(constraints, cdConstraints...)
		case *pg_query.Node_Constraint:
			// Note: there should be at most one Constraint node in
			// n.TableElts. We don't check this. We just keep collecting
			// constraints.
			constraints = append(constraints, extractConstraints(conv, printNodeType(n), table, []*pg_query.Node{te})...)
		default:
			conv.Unexpected(fmt.Sprintf("Found %s node while processing CreateStmt TableElts", printNodeType(te)))
		}
	}
	conv.SchemaStatement(printNodeType(n))
	tableId := internal.GenerateTableId()
	conv.SrcSchema[tableId] = schema.Table{
		Id:           tableId,
		Name:         table,
		ColIds:       colIds,
		ColNameIdMap: colNameIdMap,
		ColDefs:      colDef,
	}
	// Note: constraints contains all info about primary keys, not-null keys
	// and foreign keys.
	updateSchema(conv, tableId, constraints, "CREATE TABLE")
}

func processColumn(conv *internal.Conv, n *pg_query.ColumnDef, table string) (string, schema.Column, []constraint, error) {
	mods := getTypeMods(conv, n.TypeName.Typmods)
	if n.Colname == "" {
		return "", schema.Column{}, nil, fmt.Errorf("colname is empty string")
	}
	name := n.Colname
	tid, err := getTypeID(n.TypeName.Names)
	if err != nil {
		return "", schema.Column{}, nil, fmt.Errorf("can't get type id for %s: %w", name, err)
	}
	ty := schema.Type{
		Name:        tid,
		Mods:        mods,
		ArrayBounds: getArrayBounds(conv, n.TypeName.ArrayBounds)}
	return name, schema.Column{Name: name, Type: ty}, analyzeColDefConstraints(conv, printNodeType(n), table, n.Constraints, name), nil
}

func processInsertStmt(conv *internal.Conv, n *pg_query.InsertStmt) *copyOrInsert {
	if n.Relation == nil {
		logStmtError(conv, n, fmt.Errorf("relation is nil"))
		return nil
	}
	table, err := getTableName(conv, n.Relation)
	if err != nil {
		logStmtError(conv, n, fmt.Errorf("can't get table name: %w", err))
		return nil
	}
	tableId, _ := internal.GetTableIdFromSrcName(conv.SrcSchema, table)
	if _, ok := conv.SrcSchema[tableId]; !ok {
		// If we don't have schema information for a table, we drop all insert
		// statements for it. The most likely reason we don't have schema information
		// for a table is that it is an inherited table - we skip all inherited tables.
		conv.SkipStatement(printNodeType(n))
		internal.VerbosePrintf("Processing %v statement: table %s not found", printNodeType(n), table)
		logger.Log.Debug(fmt.Sprintf("Processing %v statement: table %s is inherited table", printNodeType(n), table))

		return nil
	}
	conv.StatsAddRow(tableId, conv.SchemaMode())
	colNames, err := getCols(conv, table, n.Cols)
	if err != nil {
		logStmtError(conv, n, fmt.Errorf("can't get col name: %w", err))
		conv.StatsAddBadRow(table, conv.SchemaMode())
		return nil
	}

	switch sel := n.SelectStmt.GetNode().(type) {
	case *pg_query.Node_SelectStmt:
		rows := getRows(conv, sel.SelectStmt.ValuesLists, n)
		conv.DataStatement(printNodeType(sel))
		if conv.DataMode() {
			return &copyOrInsert{stmt: insert, table: tableId, cols: colNames, rows: rows}
		}
	default:
		conv.Unexpected(fmt.Sprintf("Found %s node while processing InsertStmt SelectStmt", printNodeType(sel)))
	}
	return nil
}

func processCopyStmt(conv *internal.Conv, n *pg_query.CopyStmt) *copyOrInsert {
	// Always return a copyOrInsert{stmt: copyFrom, ...} even if we
	// encounter errors. Otherwise we won't be able to parse
	// the data portion of the COPY-FROM statement, and we'll
	// likely get stuck at this point in the pg_dump file.
	table := "BOGUS_COPY_FROM_TABLE"
	var err error
	if n.Relation != nil {
		table, err = getTableName(conv, n.Relation)
		if err != nil {
			conv.Unexpected(fmt.Sprintf("Processing %v statement: %s", printNodeType(n), err))
		}
	} else {
		logStmtError(conv, n, fmt.Errorf("relation is nil"))
	}
	if !conv.SchemaMode() {
		table, _ = internal.GetTableIdFromSrcName(conv.SrcSchema, table)
	}

	if _, ok := conv.SrcSchema[table]; !ok {
		// If we don't have schema information for a table, we drop all copy
		// statements for it. The most likely reason we don't have schema information
		// for a table is that it is an inherited table - we skip all inherited tables.
		conv.SkipStatement(printNodeType(n))
		internal.VerbosePrintf("Processing %v statement: table %s not found", printNodeType(n), table)
		logger.Log.Debug(fmt.Sprintf("Processing %v statement: table %s is inherited table", printNodeType(n), table))
		return &copyOrInsert{stmt: copyFrom, table: table, cols: []string{}}
	}
	var cols []string
	for _, a := range n.Attlist {
		s, err := getString(a)
		if err != nil {
			conv.Unexpected(fmt.Sprintf("Processing %v statement Attlist: %s", printNodeType(n), err))
			s = "BOGUS_COPY_FROM_COLUMN"
		}
		cols = append(cols, s)
	}
	conv.DataStatement(printNodeType(n))
	return &copyOrInsert{stmt: copyFrom, table: table, cols: cols}
}

func processVariableSetStmt(conv *internal.Conv, n *pg_query.VariableSetStmt) {
	if n.Name == "timezone" {
		if len(n.Args) == 1 {
			arg := n.Args[0]
			switch c := arg.GetNode().(type) {
			case *pg_query.Node_AConst:
				tz, err := getString(c.AConst.Val)
				if err != nil {
					logStmtError(conv, c, fmt.Errorf("can't get Arg: %w", err))
					return
				}
				loc, err := time.LoadLocation(tz)
				if err != nil {
					logStmtError(conv, c, err)
					return
				}
				conv.SetLocation(loc)
			default:
				logStmtError(conv, arg, fmt.Errorf("found %s node in Arg", printNodeType(c)))
				return
			}
		}
	}
}

func getTypeMods(conv *internal.Conv, t []*pg_query.Node) (l []int64) {
	for _, x := range t {
		switch t1 := x.GetNode().(type) {
		case *pg_query.Node_AConst:
			switch t2 := t1.AConst.Val.GetNode().(type) {
			case *pg_query.Node_Integer:
				l = append(l, int64(t2.Integer.Ival))
			default:
				conv.Unexpected(fmt.Sprintf("Found %s node while processing Typmods", printNodeType(t2)))
			}
		default:
			conv.Unexpected(fmt.Sprintf("Found %s node while processing Typmods", printNodeType(t1)))
		}
	}
	return l
}

func getArrayBounds(conv *internal.Conv, t []*pg_query.Node) (l []int64) {
	for _, x := range t {
		switch t := x.GetNode().(type) {
		case *pg_query.Node_Integer:
			// 'Ival' provides the array bound (-1 for an array where bound is not specified).
			l = append(l, int64(t.Integer.Ival))
		default:
			conv.Unexpected(fmt.Sprintf("Found %s node while processing ArrayBounds", printNodeType(x)))
		}
	}
	return l
}

func getTypeID(nodes []*pg_query.Node) (string, error) {
	// The pg_query library generates a pg_catalog schema prefix for most
	// types, but not for all. Typically "aliases" don't have the prefix.
	// For example, "boolean" is parsed to ["pg_catalog", "bool"], but "bool" is
	// parsed to ["bool"]. However the exact rules are unclear e.g. "date"
	// is parsed to just ["date"].
	// For simplicity, we strip off the pg_catalog prefix.
	var ids []string
	for _, node := range nodes {
		s, err := getString(node)
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
func getTableName(conv *internal.Conv, n *pg_query.RangeVar) (string, error) {
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
	if n.Catalogname != "" {
		l = append(l, n.Catalogname)
	}
	if n.Schemaname != "" && n.Schemaname != "public" { // Don't include "public".
		l = append(l, n.Schemaname)
	}
	if n.Relname == "" {
		return "", fmt.Errorf("relname is empty: can't build table name")
	}
	l = append(l, n.Relname)
	return strings.Join(l, "."), nil
}

type constraint struct {
	ct   pg_query.ConstrType
	cols []string
	name string // Used for FOREIGN KEY or SECONDARY INDEX
	/* Fields used for FOREIGN KEY constraints: */
	referCols  []string
	referTable string
}

// extractConstraints traverses a list of nodes (expecting them to be
// Constraint nodes), and collects the constraints they represent.
func extractConstraints(conv *internal.Conv, stmtType, table string, l []*pg_query.Node) (cs []constraint) {
	for _, i := range l {
		switch d := i.GetNode().(type) {
		case *pg_query.Node_Constraint:
			c := d.Constraint
			var cols, referCols []string
			var referTable string
			var conName string
			switch c.Contype {
			case pg_query.ConstrType_CONSTR_FOREIGN:
				t, err := getTableName(conv, c.Pktable)
				if err != nil {
					conv.Unexpected(fmt.Sprintf("Processing %v statement: error processing constraints: %s", printNodeType(d), err.Error()))
					conv.ErrorInStatement(printNodeType(d))
					continue
				}
				referTable = t
				if c.Conname != "" {
					conName = c.Conname
				}
				for _, attr := range c.FkAttrs {
					k, err := getString(attr)
					if err != nil {
						conv.Unexpected(fmt.Sprintf("Processing %v statement: error processing constraints: %s", printNodeType(d), err.Error()))
						conv.ErrorInStatement(printNodeType(d))
						continue
					}
					cols = append(cols, k)
				}
				for _, attr := range c.PkAttrs {
					f, err := getString(attr)
					if err != nil {
						conv.Unexpected(fmt.Sprintf("Processing %v statement: error processing constraints: %s", printNodeType(d), err.Error()))
						conv.ErrorInStatement(printNodeType(d))
						continue
					}
					referCols = append(referCols, f)
				}
			default:
				if c.Conname != "" {
					conName = c.Conname
				}
				for _, key := range c.Keys {
					k, err := getString(key)
					if err != nil {
						conv.Unexpected(fmt.Sprintf("Processing %v statement: error processing constraints: %s", printNodeType(d), err.Error()))
						conv.ErrorInStatement(fmt.Sprintf("%v.%v", printNodeType(i), printNodeType(d)))
						continue
					}
					cols = append(cols, k)
				}
			}
			cs = append(cs, constraint{ct: c.Contype, cols: cols, name: conName, referCols: referCols, referTable: referTable})
		default:
			conv.Unexpected(fmt.Sprintf("Processing %v statement: found %s node while processing constraints\n", stmtType, printNodeType(d)))
		}
	}
	return cs
}

// analyzeColDefConstraints is like extractConstraints, but is specifially for
// ColDef constraints. These constraints don't specify a key since they
// are constraints for the column defined by ColDef.
func analyzeColDefConstraints(conv *internal.Conv, stmtType, table string, l []*pg_query.Node, pgCol string) (cs []constraint) {
	// Do generic constraint processing and then set the keys of each constraint
	// to {pgCol}.
	for _, c := range extractConstraints(conv, stmtType, table, l) {
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
func updateSchema(conv *internal.Conv, tableId string, cs []constraint, stmtType string) {
	colNameIdMap := conv.SrcSchema[tableId].ColNameIdMap
	for _, c := range cs {
		switch c.ct {
		case pg_query.ConstrType_CONSTR_PRIMARY:
			ct := conv.SrcSchema[tableId]
			checkEmpty(conv, ct.PrimaryKeys, stmtType)
			ct.PrimaryKeys = toSchemaKeys(conv, tableId, c.cols, colNameIdMap) // Drop any previous primary keys.
			// In Spanner, primary key columns are usually annotated with NOT NULL,
			// but this can be omitted to allow NULL values in key columns.
			// In PostgreSQL, the primary key constraint is a combination of
			// NOT NULL and UNIQUE i.e. primary keys must be NOT NULL.
			// We preserve PostgreSQL semantics and enforce NOT NULL.
			updateCols(pg_query.ConstrType_CONSTR_NOTNULL, c.cols, ct.ColDefs, colNameIdMap)
			conv.SrcSchema[tableId] = ct
		case pg_query.ConstrType_CONSTR_FOREIGN:
			ct := conv.SrcSchema[tableId]
			ct.ForeignKeys = append(ct.ForeignKeys, toForeignKeys(c)) // Append to previous foreign keys.
			conv.SrcSchema[tableId] = ct
		case pg_query.ConstrType_CONSTR_UNIQUE:
			// Convert unique column constraint in postgres to a corresponding unique index in Spanner since
			// Spanner doesn't support unique constraints on columns.
			// TODO: Avoid Spanner-specific schema transformations in this file -- they should only
			// appear in toddl.go. This file should focus on generic transformation from source
			// database schemas into schema.go.
			ct := conv.SrcSchema[tableId]
			ct.Indexes = append(ct.Indexes, schema.Index{Name: c.name, Unique: true, Keys: toSchemaKeys(conv, tableId, c.cols, colNameIdMap)})
			conv.SrcSchema[tableId] = ct
		default:
			ct := conv.SrcSchema[tableId]
			updateCols(c.ct, c.cols, ct.ColDefs, colNameIdMap)
			conv.SrcSchema[tableId] = ct
		}
	}
}

// updateCols updates colDef with new constraints. Specifically, we apply
// 'ct' to each column in colNames.
func updateCols(ct pg_query.ConstrType, colNames []string, colDef map[string]schema.Column, colNameIdMap map[string]string) {
	// TODO: add cases for other constraints.
	for _, cn := range colNames {
		cid := colNameIdMap[cn]
		cd := colDef[cid]
		switch ct {
		case pg_query.ConstrType_CONSTR_NOTNULL:
			cd.NotNull = true
		case pg_query.ConstrType_CONSTR_DEFAULT:
			cd.Ignored.Default = true
		}
		colDef[cid] = cd
	}
}

// toSchemaKeys converts a string list of PostgreSQL primary keys to
// schema primary keys.
func toSchemaKeys(conv *internal.Conv, tableId string, colNames []string, colNameIdMap map[string]string) (l []schema.Key) {
	for _, cn := range colNames {
		// PostgreSQL primary keys have no notation of ascending/descending.
		// We map them all into ascending primarary keys.
		l = append(l, schema.Key{ColId: colNameIdMap[cn]})
	}
	return l
}

// toIndexKeys converts a list of PostgreSQL index keys to schema index keys.
func toIndexKeys(conv *internal.Conv, idxName string, s []*pg_query.Node, colNameIdMap map[string]string) (l []schema.Key) {
	for _, k := range s {
		switch e := k.GetNode().(type) {
		case *pg_query.Node_IndexElem:
			if e.IndexElem.Name == "" {
				conv.Unexpected(fmt.Sprintf("Failed to process index %s: empty index column name", idxName))
				continue
			}
			desc := false
			if e.IndexElem.Ordering == pg_query.SortByDir_SORTBY_DESC {
				desc = true
			}
			l = append(l, schema.Key{ColId: colNameIdMap[e.IndexElem.Name], Desc: desc})
		}
	}
	return
}

// toForeignKeys converts a string list of PostgreSQL foreign keys to schema
// foreign keys.
func toForeignKeys(fk constraint) (fkey schema.ForeignKey) {
	fkey = schema.ForeignKey{
		Id:               internal.GenerateForeignkeyId(),
		Name:             fk.name,
		ColumnNames:      fk.cols,
		ReferTableName:   fk.referTable,
		ReferColumnNames: fk.referCols}
	return fkey
}

// getCols extracts and returns the column names for an InsertStatement.
func getCols(conv *internal.Conv, table string, nodes []*pg_query.Node) (cols []string, err error) {
	for _, n := range nodes {
		switch r := n.GetNode().(type) {
		case *pg_query.Node_ResTarget:
			if r.ResTarget.Name != "" {
				cols = append(cols, r.ResTarget.Name)
			}
		default:
			return nil, fmt.Errorf("expecting ResTarget node but got %v node while processing Cols", printNodeType(r))
		}
	}
	return cols, nil
}

// getRows extracts and returns the rows for an InsertStatement.
func getRows(conv *internal.Conv, vll []*pg_query.Node, n *pg_query.InsertStmt) (rows [][]string) {
	for _, vl := range vll {
		var values []string
		switch vals := vl.GetNode().(type) {
		case *pg_query.Node_List:
			for _, v := range vals.List.Items {
				switch val := v.GetNode().(type) {
				case *pg_query.Node_AConst:
					switch c := val.AConst.Val.GetNode().(type) {
					// Most data is dumped enclosed in quotes ('') lke 'abc', '12:30:45' etc which is classified
					// as type Node_String_ by the parser. Some data might not be quoted like (NULL, 14.67) and
					// the type assigned to them is Node_Null and Node_Float respectively.
					case *pg_query.Node_String_:
						values = append(values, trimString(c.String_))
					case *pg_query.Node_Integer:
						// For uniformity, convert to string and handle everything in
						// dataConversion(). If performance of insert statements becomes a
						// high priority (it isn't right now), then consider preserving int64
						// here to avoid the int64 -> string -> int64 conversions.
						values = append(values, strconv.FormatInt(int64(c.Integer.Ival), 10))
					case *pg_query.Node_Float:
						values = append(values, c.Float.Str)
					case *pg_query.Node_Null:
						values = append(values, "NULL")
					// TODO: There might be other Node types like Node_IntList, Node_List, Node_BitString etc that
					// need to be checked if they are handled or not.
					default:
						conv.Unexpected(fmt.Sprintf("Processing %v statement: found %s node for A_Const Val", printNodeType(n), printNodeType(c)))
					}
				default:
					conv.Unexpected(fmt.Sprintf("Processing %v statement: found %s node for ValuesList.Val", printNodeType(n), printNodeType(val)))
				}
			}
		default:
			conv.Unexpected(fmt.Sprintf("Processing %v statement: found %s in ValuesList", printNodeType(n), printNodeType(vals)))
		}
		// If some or all of vals failed to parse, then size of values will be
		// less than the number of columns, and the same will be caught as a
		// BadRow in ProcessDataRow.
		rows = append(rows, values)
	}
	return rows
}

func logStmtError(conv *internal.Conv, node interface{}, err error) {
	conv.Unexpected(fmt.Sprintf("Processing %v statement: %s", printNodeType(node), err))
	conv.ErrorInStatement(printNodeType(node))
}

func getString(node *pg_query.Node) (string, error) {
	switch n := node.GetNode().(type) {
	case *pg_query.Node_String_:
		return trimString(n.String_), nil
	default:
		return "", fmt.Errorf("node %v is a not String node", printNodeType(n))
	}
}

// checkEmpty verifies that pkeys is empty and generates a warning if it isn't.
// PostgreSQL explicitly forbids multiple primary keys.
func checkEmpty(conv *internal.Conv, pkeys []schema.Key, stmtType string) {
	if len(pkeys) != 0 {
		conv.Unexpected(fmt.Sprintf("%s statement is adding a second primary key", stmtType))
	}
}

// printNodeType returns string representation for the type of node. Trims
// "pg_query." and "Node_" prefixes from pg_query.Node_* types.
func printNodeType(node interface{}) string {
	return strings.TrimPrefix(strings.TrimPrefix(reflect.TypeOf(node).String(), "*pg_query."), "Node_")
}

func trimString(s *pg_query.String) string {
	str := strings.TrimPrefix(s.String(), "str:")
	str = trimEscapeChars(str)
	return trimQuote(str)
}

func trimEscapeChars(s string) string {
	return strings.ReplaceAll(s, "\\n", "\n")
}

func trimQuote(s string) string {
	if len(s) > 0 && s[0] == '"' {
		s = s[1:]
	}
	if len(s) > 0 && s[len(s)-1] == '"' {
		s = s[:len(s)-1]
	}
	return s
}

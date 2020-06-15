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

package mysql

import (
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"

	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/schema"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	_ "github.com/pingcap/tidb/types/parser_driver"
	driver "github.com/pingcap/tidb/types/parser_driver"
)

var valuesRegexp = regexp.MustCompile("\\((.*?)\\)")
var insertRegexp = regexp.MustCompile("INSERT\\sINTO\\s(.*?)\\sVALUES\\s")

// ProcessMySQLDump reads mysqldump data from r and does schema or data conversion,
// depending on whether conv is configured for schema mode or data mode.
// In schema mode, ProcessMySQLDump incrementally builds a schema (updating conv).
// In data mode, ProcessMySQLDump uses this schema to convert MySQL data
// and writes it to Spanner, using the data sink specified in conv.
func ProcessMySQLDump(conv *internal.Conv, r *internal.Reader) error {
	for {
		startLine := r.LineNumber
		startOffset := r.Offset
		b, stmts, err := readAndParseChunk(conv, r)
		if err != nil {
			return err
		}
		for _, stmt := range stmts {
			isInsert := processStatements(conv, stmt)
			internal.VerbosePrintf("Parsed SQL command at line=%d/fpos=%d: %d stmts (%d lines, %d bytes) Insert Statement=%v\n", startLine, startOffset, 1, r.LineNumber-startLine, len(b), isInsert)
			if isInsert {
				switch ddlstmt := stmt.(type) {
				case *ast.InsertStmt:
					processInsertStmt(conv, ddlstmt)
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

// readAndParseChunk parses a chunk of mysqldump data, returning the bytes read,
// the parsed AST (nil if nothing read), error and whether we've hit end-of-file.
func readAndParseChunk(conv *internal.Conv, r *internal.Reader) ([]byte, []ast.StmtNode, error) {
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
			query := string(s)
			tree, _, err := parser.New().Parse(query, "", "")
			if err == nil {
				return s, tree, nil
			} else {
				new_tree, err := handleParseError(conv, query, err, l)
				if err == false {
					return s, new_tree, nil
				}
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

// processStatements extracts schema information from MYSQL
// statements, updating Conv with new schema information, and returning
// true if INSERT statement is encountered.
func processStatements(conv *internal.Conv, stmt ast.StmtNode) bool {

	switch ddlStmt := stmt.(type) {
	case *ast.CreateTableStmt:
		if conv.SchemaMode() {
			processCreateTable(conv, ddlStmt)
		}
	case *ast.AlterTableStmt:
		if conv.SchemaMode() {
			processAlterTable(conv, ddlStmt)
		}
	case *ast.SetStmt:
		if conv.SchemaMode() {
			processSetStmt(conv, ddlStmt)
		}
	case *ast.InsertStmt:
		return true
	default:
		conv.SkipStatement(reflect.TypeOf(stmt).String())
	}
	return false
}

func processSetStmt(conv *internal.Conv, stmt *ast.SetStmt) {
	if stmt.Variables != nil && len(stmt.Variables) > 0 {
		for _, variable := range stmt.Variables {
			if variable.Name == "TIME_ZONE" {
				value := variable.Value
				switch val := value.(type) {
				case *driver.ValueExpr:
					if val.GetValue() == nil {
						logStmtError(conv, stmt, fmt.Errorf("found nil value in set statement"))
						return
					}
					conv.TimezoneOffset = fmt.Sprintf("%v", val.GetValue())
				default:
					logStmtError(conv, stmt, fmt.Errorf("found %s type value in set statement", reflect.TypeOf(val)))
					return
				}
			}
		}
	}
}

func processCreateTable(conv *internal.Conv, stmt *ast.CreateTableStmt) {
	var colNames []string
	colDef := make(map[string]schema.Column)
	if stmt.Table == nil {
		logStmtError(conv, stmt, fmt.Errorf("table is nil"))
		return
	}
	tableName, err := getTableName(stmt.Table)
	if err != nil {
		logStmtError(conv, stmt, fmt.Errorf("can't get table name: %w", err))
		return
	}
	columns := stmt.Cols
	var keys []schema.Key
	for _, element := range columns {
		colname, col, isPk, err := processColumn(conv, tableName, element)
		if err != nil {
			logStmtError(conv, stmt, err)
			return
		}
		colNames = append(colNames, colname)
		colDef[colname] = col
		if isPk {
			keys = append(keys, schema.Key{Column: colname})
		}
	}
	conv.SchemaStatement(reflect.TypeOf(stmt).String())
	conv.SrcSchema[tableName] = schema.Table{
		Name:        tableName,
		ColNames:    colNames,
		ColDefs:     colDef,
		PrimaryKeys: keys}
	for _, constraint := range stmt.Constraints {
		processConstraint(conv, tableName, constraint, "CREATE TABLE")
	}
}

func processConstraint(conv *internal.Conv, table string, constraint *ast.Constraint, s string) {
	st := conv.SrcSchema[table]
	switch ct := constraint.Tp; ct {
	case ast.ConstraintPrimaryKey:
		checkEmpty(conv, st.PrimaryKeys, s) // Drop any previous primary keys.
		st.PrimaryKeys = toSchemaKeys(constraint.Keys)
		// In Spanner, primary key columns are usually annotated with NOT NULL,
		// but this can be omitted to allow NULL values in key columns.
		// In MySQL, the primary key constraint is a combination of
		// NOT NULL and UNIQUE i.e. primary keys must be NOT NULL.
		// We preserve MySQL semantics and enforce NOT NULL.
		updateCols(conv, ast.ConstraintPrimaryKey, constraint.Keys, st.ColDefs, table)
	default:
		updateCols(conv, ct, constraint.Keys, st.ColDefs, table)
	}
	conv.SrcSchema[table] = st
}

// toSchemaKeys converts a string list of MySQL primary keys to
// schema primary keys.
func toSchemaKeys(columns []*ast.IndexPartSpecification) (keys []schema.Key) {
	for _, colname := range columns {
		// MySQL primary keys have no notation of ascending/descending.
		// We map them all into ascending primary keys.
		keys = append(keys, schema.Key{Column: colname.Column.Name.String()})
	}
	return keys
}

func updateCols(conv *internal.Conv, ct ast.ConstraintType, colNames []*ast.IndexPartSpecification, colDef map[string]schema.Column, tableName string) {
	for _, column := range colNames {
		colName := column.Column.OrigColName()
		cd := colDef[colName]
		switch ct {
		case ast.ConstraintForeignKey:
			cd.Ignored.ForeignKey = true
		case ast.ConstraintUniq:
			cd.Unique = true
		case ast.ConstraintCheck:
			cd.Ignored.Check = true
		case ast.ConstraintPrimaryKey:
			cd.NotNull = true
		default:
			conv.Unexpected(fmt.Sprintf("Found constraint type of const value `%v` in table : %s , column : %s. Mapping of ConstraintType can be found here: 'https://godoc.org/github.com/pingcap/parser/ast#ConstraintType'", ct, tableName, colName))
		}
		colDef[colName] = cd
	}
}

func processAlterTable(conv *internal.Conv, stmt *ast.AlterTableStmt) {
	if stmt.Table == nil {
		logStmtError(conv, stmt, fmt.Errorf("table is nil"))
		return
	}
	tableName, err := getTableName(stmt.Table)
	if err != nil {
		logStmtError(conv, stmt, fmt.Errorf("can't get table name: %w", err))
		return
	}
	if _, ok := conv.SrcSchema[tableName]; ok {
		for _, item := range stmt.Specs {
			switch alterType := item.Tp; alterType {
			case ast.AlterTableAddConstraint:
				processConstraint(conv, tableName, item.Constraint, "ALTER TABLE")
				conv.SchemaStatement(reflect.TypeOf(stmt).String())
			case ast.AlterTableModifyColumn:
				var keys []schema.Key
				colname, col, isPk, err := processColumn(conv, tableName, item.NewColumns[0])
				if err != nil {
					logStmtError(conv, stmt, err)
				}
				conv.SrcSchema[tableName].ColDefs[colname] = col
				if isPk {
					keys = append(keys, schema.Key{Column: colname})
					ctable := conv.SrcSchema[tableName]
					ctable.PrimaryKeys = keys
					conv.SrcSchema[tableName] = ctable
				}
				conv.SchemaStatement(reflect.TypeOf(stmt).String())
			default:
				conv.SkipStatement(reflect.TypeOf(stmt).String())
			}
		}

	} else {
		conv.SkipStatement(reflect.TypeOf(stmt).String())
	}
}

// getTableName extracts the table name from *ast.TableName table, and returns
// the raw extracted name (the MySQL table name).
// *ast.TableName is used to represent table names. It consists of two components:
//  Schema: schemas are MySQL db often unspecified;
//  Name: name of the table
// We build a table name from these components as follows:
// a) nil components are dropped.
// b) if more than one component is specified, they are joined using "."
//    (Note that Spanner doesn't allow "." in table names, so this
//    will eventually get re-mapped when we construct the Spanner table name).
// c) return error if Table is nil or "".
func getTableName(table *ast.TableName) (string, error) {
	var l []string

	if table.Schema.String() != "" {
		l = append(l, table.Schema.String())
	}
	if table.Name.String() == "" {
		return "", fmt.Errorf("tablename is empty: can't build table name")
	}
	l = append(l, table.Name.String())
	return strings.Join(l, "."), nil
}

func processColumn(conv *internal.Conv, tableName string, col *ast.ColumnDef) (string, schema.Column, bool, error) {
	if col.Name == nil {
		return "", schema.Column{}, false, fmt.Errorf("column name is nil")
	}
	name := col.Name.OrigColName()
	if col.Tp == nil {
		return "", schema.Column{}, false, fmt.Errorf("can't get column type for %s: %w", name, fmt.Errorf("found nil *ast.ColumnDef.Tp"))
	}
	tid, mods := getTypeModsAndID(col.Tp.String())
	ty := schema.Type{
		Name:        tid,
		Mods:        mods,
		ArrayBounds: getArrayBounds(col.Tp.String(), col.Tp.Elems)}
	column := schema.Column{Name: name, Type: ty}
	return name, column, updateColsByOption(conv, tableName, col, &column), nil
}

// updateColsByOption is specifially for ColDef constraints.
// ColumnOption type is used for parsing column constraint info from MySQL
func updateColsByOption(conv *internal.Conv, tableName string, col *ast.ColumnDef, column *schema.Column) bool {
	isPk := false
	for _, elem := range col.Options {
		switch op := elem.Tp; op {
		case ast.ColumnOptionPrimaryKey:
			column.NotNull = true
			// If primary key is defined in a column then `isPk` will be true
			// and this column will be added in colDef as primary keys
			isPk = true
		case ast.ColumnOptionNotNull:
			column.NotNull = true
		case ast.ColumnOptionDefaultValue:
			column.Ignored.Default = true
		case ast.ColumnOptionUniqKey:
			column.Unique = true
		case ast.ColumnOptionCheck:
			column.Ignored.Check = true
		case ast.ColumnOptionReference:
			column.Ignored.ForeignKey = true
		default:
			conv.Unexpected(fmt.Sprintf("Not supported type option of const value `%v` found in table : %s , column : %s. Mapping of columnOptionType can be found here: 'https://godoc.org/github.com/pingcap/parser/ast#ColumnOptionType'", op, tableName, column.Name))
		}
	}
	return isPk
}

// getTypeModsAndID returns ID and mods of column datatype
func getTypeModsAndID(columnType string) (string, []int64) {
	// There are no methods in pincap parser to retirieve ID and mods
	// We will process columnType eg:'varchar(40)' and split ID from the string.
	// We retrieve mods using regex expression and convert it to INT64
	id := columnType
	var mods []int64
	if strings.Contains(columnType, "(") {
		id = strings.Split(columnType, "(")[0]
		// For 'set' and 'enum' datatypes, values provided are not maxLength
		if id == "set" || id == "enum" {
			return id, nil
		}
		values := valuesRegexp.FindString(columnType)
		strMods := strings.Split(values[1:len(values)-1], ",")
		for _, i := range strMods {
			j, err := strconv.ParseInt(i, 10, 64)
			if err != nil {
				panic(err)
			}
			mods = append(mods, j)
		}
	}
	// 'BINARY' keyword will be added to all blob datatypes by parser.
	// Eg: mediumblob BINARY. It needs to be ignored to retrieve ID
	if strings.Contains(id, " ") {
		id = strings.Split(columnType, " ")[0]
	}
	return id, mods
}

// Error can be due to unsupported Spatial datatypes in Create statement or some invalid entries
// in insert statement. handleParseError attempts at creating parsable query.
func handleParseError(conv *internal.Conv, query string, err error, l [][]byte) ([]ast.StmtNode, bool) {
	new_query := query
	// Check if error is due to Insert statement.
	insert_stmt := insertRegexp.FindString(new_query)
	if insert_stmt != "" {
		// Likely causes of failing to parse Insert statement:
		// a) Due to some invalid value
		// b) Query size is more than what pingcap parser could handle (more than 40MB in size)
		return handleInsertStatement(conv, query, insert_stmt)
	}

	// Check if error is due to spatial datatype as it is not supported by Pingcap parser.
	var isSpatial bool
	for _, spatial := range internal.Spatial {
		if strings.Contains(strings.ToLower(err.Error()), spatial) && isSpatial == false {
			isSpatial = true
			conv.Unexpected(fmt.Sprintf("Unsupported datatype '%s' encountered while parsing following statement at line number %d : \n%s", spatial, len(l), query))
			internal.VerbosePrintf("Converting datatype '%s' to 'Text' and retrying to parse the statement\n", spatial)
		}
	}
	if isSpatial {
		return handleSpatialDatatype(conv, query, l)
	}

	return nil, true
}

// We deal with this case by extracting all rows and create
// extended insert statements. Then parse one Insert statement
// at a time, ensuring no size issue and skipping only invalid entries.
func handleInsertStatement(conv *internal.Conv, query, insert_stmt string) ([]ast.StmtNode, bool) {
	var stmts []ast.StmtNode
	values := valuesRegexp.FindAllString(query, -1)
	if len(values) == 0 {
		return nil, true
	}
	for _, value := range values {
		query = insert_stmt + value + ";"
		new_tree, _, err := parser.New().Parse(query, "", "")
		if err != nil {
			if conv.SchemaMode() {
				conv.Unexpected(fmt.Sprintf("Either unsupported value is encountered or syntax is incorrect for following statement : \n%s", query))
			}
			conv.SkipStatement("*ast.InsertStmt")
			continue
		}
		stmts = append(stmts, new_tree[0])
	}
	return stmts, false
}

// If error is due to spatial datatype then try to replace it with 'text' and parse query again.
func handleSpatialDatatype(conv *internal.Conv, query string, l [][]byte) ([]ast.StmtNode, bool) {
	if conv.SchemaMode() {
		for _, spatial := range internal.Spatial {
			query = strings.Replace(strings.ToLower(query), " "+spatial, " text", -1)
		}
		new_tree, _, err := parser.New().Parse(query, "", "")
		if err != nil {
			conv.SkipStatement(query)
			return nil, false
		}
		return new_tree, false
	}
	return nil, false
}

// We calculate array bound for only set type
// we do not expect multidimensional array.
func getArrayBounds(ft string, elem []string) []int64 {
	var arraybound []int64
	if strings.HasPrefix(ft, "set") {
		arraybound = append(arraybound, int64(len(elem)))
	}
	return arraybound
}

func processInsertStmt(conv *internal.Conv, stmt *ast.InsertStmt) {
	if stmt.Table == nil {
		logStmtError(conv, stmt, fmt.Errorf("source table is nil"))
		return
	}
	srcTable, err := getTableNameInsert(stmt.Table)
	if err != nil {
		logStmtError(conv, stmt, fmt.Errorf("can't get source table name: %w", err))
		return
	}
	if conv.SchemaMode() {
		conv.Stats.Rows[srcTable] += int64(len(stmt.Lists))
		return
	}
	spTable, err1 := internal.GetSpannerTable(conv, srcTable)
	if err1 != nil {
		logStmtError(conv, stmt, fmt.Errorf("can't get spanner table name for source table '%s' : err=%w", srcTable, err1))
		return
	}
	spSchema, ok1 := conv.SpSchema[spTable]
	srcSchema, ok2 := conv.SrcSchema[srcTable]
	if !ok1 || !ok2 {
		conv.Unexpected(fmt.Sprintf("Can't get schemas for table %s", srcTable))
		conv.Stats.BadRows[srcTable] += conv.Stats.Rows[srcTable]
		return
	}
	srcCols, err2 := getCols(stmt)
	if err2 != nil {
		// In MySQL, column names might not be specified in insert statement so instead of
		// throwing error we will try to retrieve columns from source schema
		srcCols = conv.SrcSchema[srcTable].ColNames
		if len(srcCols) == 0 {
			conv.Unexpected(fmt.Sprintf("Can't get columns for table %s", srcTable))
			conv.Stats.BadRows[srcTable] += conv.Stats.Rows[srcTable]
			return
		}
	}
	spCols, err3 := internal.GetSpannerCols(conv, srcTable, srcCols)
	if err3 != nil {
		conv.Unexpected(fmt.Sprintf("Can't get spanner columnss for table %s: err=%s", srcTable, err3))
		conv.Stats.BadRows[srcTable] += conv.Stats.Rows[srcTable]
		return
	}
	var values []string
	if stmt.Lists == nil {
		logStmtError(conv, stmt, fmt.Errorf("Can't get column values"))
		return
	}
	for _, row := range stmt.Lists {
		values, err = getVals(row)
		ProcessDataRow(conv, srcTable, srcCols, srcSchema, spTable, spCols, spSchema, values)
	}
}

func getCols(stmt *ast.InsertStmt) ([]string, error) {
	if stmt.Columns == nil {
		return nil, fmt.Errorf("No columns found in insert statement ")
	}
	var colnames []string
	for _, column := range stmt.Columns {
		colnames = append(colnames, column.OrigColName())
	}
	return colnames, nil
}

func getVals(row []ast.ExprNode) ([]string, error) {
	if len(row) == 0 {
		return nil, fmt.Errorf("Found row with zero length")
	}
	var values []string
	for _, item := range row {

		switch valueNode := item.(type) {
		case *driver.ValueExpr:
			values = append(values, fmt.Sprintf("%v", valueNode.GetValue()))
		default:
			return nil, fmt.Errorf("unexpected value node %T", valueNode)
		}
	}
	return values, nil
}

func getTableNameInsert(stmt *ast.TableRefsClause) (string, error) {
	if stmt.TableRefs == nil {
		return "", fmt.Errorf("can't build table name as tablerefs is empty")
	}
	if stmt.TableRefs.Left == nil {
		return "", fmt.Errorf("can't build table name as Tablerefs.Left is empty")
	}
	if table, ok := stmt.TableRefs.Left.(*ast.TableSource); ok {
		if tablenode, ok := table.Source.(*ast.TableName); ok {
			return getTableName(tablenode)
		} else {
			return "", fmt.Errorf("Can't build table name as table source is of different type")
		}
	} else {
		return "", fmt.Errorf("stmt.TableRefs.Left is different type, can't build table name")
	}
}

func logStmtError(conv *internal.Conv, stmt ast.StmtNode, err error) {
	conv.Unexpected(fmt.Sprintf("Processing %v statement: %s", reflect.TypeOf(stmt), err))
	conv.ErrorInStatement(reflect.TypeOf(stmt).String())
}

// checkEmpty verifies that pkeys is empty and generates a warning if it isn't.
// MySql explicitly forbids multiple primary keys.
func checkEmpty(conv *internal.Conv, pkeys []schema.Key, s string) {
	if len(pkeys) != 0 {
		conv.Unexpected(fmt.Sprintf("Multiple primary keys found. `%s` statement is overwriting primary key", s))
	}
}

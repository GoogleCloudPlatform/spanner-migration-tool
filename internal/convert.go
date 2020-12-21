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

package internal

import (
	"fmt"
	"sort"
	"time"

	"github.com/cloudspannerecosystem/harbourbridge/schema"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
)

// Conv contains all schema and data conversion state.
type Conv struct {
	mode           mode                                // Schema mode or data mode.
	SpSchema       map[string]ddl.CreateTable          // Maps Spanner table name to Spanner schema.
	SyntheticPKeys map[string]SyntheticPKey            // Maps Spanner table name to synthetic primary key (if needed).
	SrcSchema      map[string]schema.Table             // Maps source-DB table name to schema information.
	Issues         map[string]map[string][]SchemaIssue // Maps source-DB table/col to list of schema conversion issues.
	ToSpanner      map[string]NameAndCols              // Maps from source-DB table name to Spanner name and column mapping.
	ToSource       map[string]NameAndCols              // Maps from Spanner table name to source-DB table name and column mapping.
	dataSink       func(table string, cols []string, values []interface{})
	Location       *time.Location // Timezone (for timestamp conversion).
	sampleBadRows  rowSamples     // Rows that generated errors during conversion.
	Stats          stats
	TimezoneOffset string // Timezone offset for timestamp conversion.
}

type mode int

const (
	schemaOnly mode = iota
	dataOnly
)

// SyntheticPKey specifies a synthetic primary key and current sequence
// count for a table, if needed. We use a synthetic primary key when
// the source DB table has no primary key.
type SyntheticPKey struct {
	Col      string
	Sequence int64
}

// SchemaIssue specifies a schema conversion issue.
type SchemaIssue int

// Defines all of the schema issues we track. Includes issues
// with type mappings, as well as features (such as source
// DB constraints) that aren't supported in Spanner.
const (
	DefaultValue SchemaIssue = iota
	ForeignKey
	MissingPrimaryKey
	MultiDimensionalArray
	NoGoodType
	Numeric
	NumericThatFits
	Decimal
	DecimalThatFits
	Serial
	AutoIncrement
	Timestamp
	Datetime
	Widened
	Time
)

// NameAndCols contains the name of a table and its columns.
// Used to map between source DB and Spanner table and column names.
type NameAndCols struct {
	Name string
	Cols map[string]string
}

type rowSamples struct {
	rows       []*row
	bytes      int64 // Bytes consumed by l.
	bytesLimit int64 // Limit on bytes consumed by l.
}

// row represents a single data row for a table. Used for tracking bad data rows.
type row struct {
	table string
	cols  []string
	vals  []string
}

// Note on rows, bad rows and good rows: a data row is either:
// a) not processed (but still shows in rows)
// b) successfully converted and successfully written to Spanner.
// c) successfully converted, but an error occurs when writing the row to Spanner.
// d) unsuccessfully converted (we won't try to write such rows to Spanner).
type stats struct {
	Rows       map[string]int64          // Count of rows encountered during processing (a + b + c + d), broken down by source table.
	GoodRows   map[string]int64          // Count of rows successfully converted (b + c), broken down by source table.
	BadRows    map[string]int64          // Count of rows where conversion failed (d), broken down by source table.
	Statement  map[string]*statementStat // Count of processed statements, broken down by statement type.
	Unexpected map[string]int64          // Count of unexpected conditions, broken down by condition description.
	Reparsed   int64                     // Count of times we re-parse dump data looking for end-of-statement.
}

type statementStat struct {
	Schema int64
	Data   int64
	Skip   int64
	Error  int64
}

// MakeConv returns a default-configured Conv.
func MakeConv() *Conv {
	return &Conv{
		SpSchema:       make(map[string]ddl.CreateTable),
		SyntheticPKeys: make(map[string]SyntheticPKey),
		SrcSchema:      make(map[string]schema.Table),
		Issues:         make(map[string]map[string][]SchemaIssue),
		ToSpanner:      make(map[string]NameAndCols),
		ToSource:       make(map[string]NameAndCols),
		Location:       time.Local, // By default, use go's local time, which uses $TZ (when set).
		sampleBadRows:  rowSamples{bytesLimit: 10 * 1000 * 1000},
		Stats: stats{
			Rows:       make(map[string]int64),
			GoodRows:   make(map[string]int64),
			BadRows:    make(map[string]int64),
			Statement:  make(map[string]*statementStat),
			Unexpected: make(map[string]int64),
		},
		TimezoneOffset: "+00:00", // By default, use +00:00 offset which is equal to UTC timezone
	}
}

// SetDataSink configures conv to use the specified data sink.
func (conv *Conv) SetDataSink(ds func(table string, cols []string, values []interface{})) {
	conv.dataSink = ds
}

// Note on modes.
// We process the dump output twice. In the first pass (schema mode) we
// build the schema, and the second pass (data mode) we write data to
// Spanner.

// SetSchemaMode configures conv to process schema-related statements and
// build the Spanner schema. In schema mode we also process just enough
// of other statements to get an accurate count of the number of data rows
// (used for tracking progress when writing data to Spanner).
func (conv *Conv) SetSchemaMode() {
	conv.mode = schemaOnly
}

// SetDataMode configures conv to convert data and write it to Spanner.
// In this mode, we also do a complete re-processing of all statements
// for stats purposes (its hard to keep track of which stats are
// collected in each phase, so we simply reset and recollect),
// but we don't modify the schema.
func (conv *Conv) SetDataMode() {
	conv.mode = dataOnly
}

// GetDDL Schema returns the current Spanner schema.
// We return DDL in alphabetical order with one exception: interleaved tables are
// potentially out of order since they must appear after the definition of their
// parent table.
func (conv *Conv) GetDDL(c ddl.Config) []string {
	var tables []string
	for t := range conv.SpSchema {
		tables = append(tables, t)
	}
	sort.Strings(tables)
	tableQueue := tables
	var ddl []string
	printed := make(map[string]bool)
	for len(tableQueue) > 0 {
		t := tableQueue[0]
		tableQueue = tableQueue[1:]
		_, found := printed[conv.SpSchema[t].Parent]
		// Print table t if either:
		// a) t is not interleaved in another table, or
		// b) t is interleaved in another table and that table has already been printed.
		if conv.SpSchema[t].Parent == "" || found {
			ddl = append(ddl, conv.SpSchema[t].PrintCreateTable(c))
			printed[t] = true
		} else {
			// We can't print table t now because its parent hasn't been printed.
			// Add it at end of tables and we'll try again later.
			// We might need multiple iterations to print chains of interleaved tables,
			// but we will always make progress because interleaved tables can't
			// have cycles. In principle this could be O(n^2), but in practice chains
			// of interleaved tables are small.
			tableQueue = append(tableQueue, t)
		}
	}

	// Append foreign key constraints to DDL.
	// We always use alter table statements for foreign key constraints.
	// The alternative of putting foreign key constraints in-line as part of create
	// table statements is tricky because of table order (need to define tables
	// before they are referenced by foreign key constraints) and the possibility
	// of circular foreign keys definitions. We opt for simplicity.
	if c.ForeignKeys {
		for _, t := range tables {
			for _, fk := range conv.SpSchema[t].Fks {
				ddl = append(ddl, fk.PrintForeignKeyAlterTable(c, t))
			}
		}
	}
	return ddl
}

// WriteRow calls dataSink and updates row stats.
func (conv *Conv) WriteRow(srcTable, spTable string, spCols []string, spVals []interface{}) {
	if conv.dataSink == nil {
		msg := "Internal error: ProcessDataRow called but dataSink not configured"
		VerbosePrintf("%s\n", msg)
		conv.Unexpected(msg)
		conv.StatsAddBadRow(srcTable, conv.DataMode())
	} else {
		conv.dataSink(spTable, spCols, spVals)
		conv.statsAddGoodRow(srcTable, conv.DataMode())
	}
}

// Rows returns the total count of data rows processed.
func (conv *Conv) Rows() int64 {
	n := int64(0)
	for _, c := range conv.Stats.Rows {
		n += c
	}
	return n
}

// BadRows returns the total count of bad rows encountered during
// data conversion.
func (conv *Conv) BadRows() int64 {
	n := int64(0)
	for _, c := range conv.Stats.BadRows {
		n += c
	}
	return n
}

// Statements returns the total number of statements processed.
func (conv *Conv) Statements() int64 {
	n := int64(0)
	for _, x := range conv.Stats.Statement {
		n += x.Schema + x.Data + x.Skip + x.Error
	}
	return n
}

// StatementErrors returns the number of statement errors encountered.
func (conv *Conv) StatementErrors() int64 {
	n := int64(0)
	for _, x := range conv.Stats.Statement {
		n += x.Error
	}
	return n
}

// Unexpecteds returns the total number of distinct unexpected conditions
// encountered during processing.
func (conv *Conv) Unexpecteds() int64 {
	return int64(len(conv.Stats.Unexpected))
}

// CollectBadRow updates the list of bad rows, while respecting
// the byte limit for bad rows.
func (conv *Conv) CollectBadRow(srcTable string, srcCols, vals []string) {
	r := &row{table: srcTable, cols: srcCols, vals: vals}
	bytes := byteSize(r)
	// Cap storage used by badRows. Keep at least one bad row.
	if len(conv.sampleBadRows.rows) == 0 || bytes+conv.sampleBadRows.bytes < conv.sampleBadRows.bytesLimit {
		conv.sampleBadRows.rows = append(conv.sampleBadRows.rows, r)
		conv.sampleBadRows.bytes += bytes
	}
}

// SampleBadRows returns a string-formatted list of rows that generated errors.
// Returns at most n rows.
func (conv *Conv) SampleBadRows(n int) []string {
	var l []string
	for _, x := range conv.sampleBadRows.rows {
		l = append(l, fmt.Sprintf("table=%s cols=%v data=%v\n", x.table, x.cols, x.vals))
		if len(l) > n {
			break
		}
	}
	return l
}

// AddPrimaryKeys analyzes all tables in conv.schema and adds synthetic primary
// keys for any tables that don't have primary key.
func (conv *Conv) AddPrimaryKeys() {
	for t, ct := range conv.SpSchema {
		if len(ct.Pks) == 0 {
			k := conv.buildPrimaryKey(t)
			ct.ColNames = append(ct.ColNames, k)
			ct.ColDefs[k] = ddl.ColumnDef{Name: k, T: ddl.Type{Name: ddl.Int64}}
			ct.Pks = []ddl.IndexKey{ddl.IndexKey{Col: k}}
			conv.SpSchema[t] = ct
			conv.SyntheticPKeys[t] = SyntheticPKey{k, 0}
		}
	}
}

// SetLocation configures the timezone for data conversion.
func (conv *Conv) SetLocation(loc *time.Location) {
	conv.Location = loc
}

func (conv *Conv) buildPrimaryKey(spTable string) string {
	base := "synth_id"
	if _, ok := conv.ToSource[spTable]; !ok {
		conv.Unexpected(fmt.Sprintf("ToSource lookup fails for table %s: ", spTable))
		return base
	}
	count := 0
	key := base
	for {
		// Check key isn't already a column in the table.
		if _, ok := conv.ToSource[spTable].Cols[key]; !ok {
			return key
		}
		key = fmt.Sprintf("%s%d", base, count)
		count++
	}
}

// Unexpected records stats about corner-cases and conditions
// that were not expected. Note that the counts maybe not
// be completely reliable due to potential double-counting
// because we process dump data twice.
func (conv *Conv) Unexpected(u string) {
	VerbosePrintf("Unexpected condition: %s\n", u)
	// Limit size of unexpected map. If over limit, then only
	// update existing entries.
	if _, ok := conv.Stats.Unexpected[u]; ok || len(conv.Stats.Unexpected) < 1000 {
		conv.Stats.Unexpected[u]++
	}
}

// StatsAddRow increments the count of rows for 'srcTable' if b is
// true.  The boolean arg 'b' is used to avoid double counting of
// stats. Specifically, some code paths that report row stats run in
// both schema-mode and data-mode e.g. statement.go.  To avoid double
// counting, we explicitly choose a mode-for-stats-collection for each
// place where row stats are collected. When specifying this mode take
// care to ensure that the code actually runs in the mode you specify,
// otherwise stats will be dropped.
func (conv *Conv) StatsAddRow(srcTable string, b bool) {
	if b {
		conv.Stats.Rows[srcTable]++
	}
}

// statsAddGoodRow increments the good-row stats for 'srcTable' if b
// is true.  See StatsAddRow comments for context.
func (conv *Conv) statsAddGoodRow(srcTable string, b bool) {
	if b {
		conv.Stats.GoodRows[srcTable]++
	}
}

// StatsAddBadRow increments the bad-row stats for 'srcTable' if b is
// true.  See StatsAddRow comments for context.
func (conv *Conv) StatsAddBadRow(srcTable string, b bool) {
	if b {
		conv.Stats.BadRows[srcTable]++
	}
}

func (conv *Conv) getStatementStat(s string) *statementStat {
	if conv.Stats.Statement[s] == nil {
		conv.Stats.Statement[s] = &statementStat{}
	}
	return conv.Stats.Statement[s]
}

// SkipStatement increments the skip statement stats for 'stmtType'.
func (conv *Conv) SkipStatement(stmtType string) {
	if conv.SchemaMode() { // Record statement stats on first pass only.
		VerbosePrintf("Skipping statement: %s\n", stmtType)
		conv.getStatementStat(stmtType).Skip++
	}
}

// ErrorInStatement increments the error statement stats for 'stmtType'.
func (conv *Conv) ErrorInStatement(stmtType string) {
	if conv.SchemaMode() { // Record statement stats on first pass only.
		VerbosePrintf("Error processing statement: %s\n", stmtType)
		conv.getStatementStat(stmtType).Error++
	}
}

// SchemaStatement increments the schema statement stats for 'stmtType'.
func (conv *Conv) SchemaStatement(stmtType string) {
	if conv.SchemaMode() { // Record statement stats on first pass only.
		conv.getStatementStat(stmtType).Schema++
	}
}

// DataStatement increments the data statement stats for 'stmtType'.
func (conv *Conv) DataStatement(stmtType string) {
	if conv.SchemaMode() { // Record statement stats on first pass only.
		conv.getStatementStat(stmtType).Data++
	}
}

// SchemaMode returns true if conv is configured to schemaOnly.
func (conv *Conv) SchemaMode() bool {
	return conv.mode == schemaOnly
}

// DataMode returns true if conv is configured to dataOnly.
func (conv *Conv) DataMode() bool {
	return conv.mode == dataOnly
}

func byteSize(r *row) int64 {
	n := int64(len(r.table))
	for _, c := range r.cols {
		n += int64(len(c))
	}
	for _, v := range r.vals {
		n += int64(len(v))
	}
	return n
}

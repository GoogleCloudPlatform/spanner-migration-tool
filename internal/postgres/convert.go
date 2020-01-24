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

package postgres

import (
	"fmt"
	"harbourbridge/internal"
	"harbourbridge/spanner/ddl"
	"reflect"
	"strings"
	"time"

	nodes "github.com/lfittl/pg_query_go/nodes"
)

// Cvt contains all schema and data conversion state.
type Cvt struct {
	mode           bool                       // False is schema mode, true is data mode.
	sSchema        map[string]ddl.CreateTable // Maps Spanner table name to Spanner schema.
	syntheticPKeys map[string]syntheticPKey   // Maps Spanner table name to synthetic primary key (if needed).
	pgSchema       map[string]pgTableDef      // Maps PostgreSQL table name Postgres schema information.
	toSpanner      map[string]nameAndCols     // Maps from PostgreSQL table name to Spanner name and column mapping.
	toPostgres     map[string]nameAndCols     // Maps from Spanner table name to PostgreSQL name and column mapping.
	dataSink       func(table string, cols []string, values []interface{})
	location       *time.Location // Timezone (for timestamp conversion).
	sampleBadRows  rowSamples     // Rows that generated errors during conversion.
	stats          stats
}

// syntheticPKey specifies a synthetic primary key and current sequence
// count for a table, if needed. We use a synthetic primary key when
// a PostgreSQL table has no primary key.
type syntheticPKey struct {
	col      string
	sequence int64
}

// pgTableDef captures data about a table's PostgreSQL schema.
// Note: we only keep a minimal set of PostgreSQL schema information.
type pgTableDef struct {
	cols map[string]pgColDef
}

// pgColDef collects key PostgreSQL schema parameters for a table column.
type pgColDef struct {
	id     string        // Type id.
	mods   []int64       // List of modifiers (aka type parameters e.g. varchar(8) or numeric(6, 4).
	array  []int64       // Array bound information. Empty for scalar types.
	issues []schemaIssue // List of issues encountered mapping this col to Spanner.
}

type schemaIssue int

// Defines all of the schema issues we track. Includes issues
// with type mappings, as well as features (such as PostgreSQL
// constraints) that aren't supported in Spanner.
const (
	defaultValue schemaIssue = iota
	foreignKey
	missingPrimaryKey
	multiDimensionalArray
	noGoodType
	numeric
	numericThatFits
	serial
	timestamp
	widened
)

// nameAndCols contains the name of a table and its columns.
// Used to map between PostgreSQL and Spanner table and column names.
type nameAndCols struct {
	name string
	cols map[string]string
}

type rowSamples struct {
	l          []*row
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
	rows       map[string]int64          // Count of rows encountered during processing (a + b + c + d), broken down by Spanner table.
	goodRows   map[string]int64          // Count of rows successfully converted (b + c), broken down by Spanner table.
	badRows    map[string]int64          // Count of rows where conversion failed (d), broken down by Spanner table.
	statement  map[string]*statementStat // Count of processed statements, broken down by statement type.
	unexpected map[string]int64          // Count of unexpected conditions, broken down by condition description.
	reparsed   int64                     // Count of times we re-parse pg_dump data looking for end-of-statement.
}

type statementStat struct {
	schema int64
	data   int64
	skip   int64
	error  int64
}

// MakeCvt returns a default-configured Cvt.
func MakeCvt() *Cvt {
	return &Cvt{
		sSchema:        make(map[string]ddl.CreateTable),
		syntheticPKeys: make(map[string]syntheticPKey),
		pgSchema:       make(map[string]pgTableDef),
		toSpanner:      make(map[string]nameAndCols),
		toPostgres:     make(map[string]nameAndCols),
		location:       time.Local, // By default, use go's local time, which uses $TZ (when set).
		sampleBadRows:  rowSamples{bytesLimit: 10 * 1000 * 1000},
		stats: stats{
			rows:       make(map[string]int64),
			goodRows:   make(map[string]int64),
			badRows:    make(map[string]int64),
			statement:  make(map[string]*statementStat),
			unexpected: make(map[string]int64),
		},
	}
}

// SetDataSink configures cvt to use the specified data sink.
func (cvt *Cvt) SetDataSink(ds func(table string, cols []string, values []interface{})) {
	cvt.dataSink = ds
}

// Note on modes.
// We process the pg_dump output twice. In the first pass (schema mode) we
// build the schema, and the second pass (data mode) we write data to
// Spanner.

// SetSchemaMode configures cvt to process schema-related statements and
// build the Spanner schema. In schema mode we also process just enough
// of other statements to get an accurate count of the number of data rows
// (used for tracking progress when writing data to Spanner).
func (cvt *Cvt) SetSchemaMode() {
	cvt.mode = false
}

// SetDataMode configures cvt to convert data and write it to Spanner.
// In this mode, we also do a complete re-processing of all statements
// for stats purposes (its hard to keep track of which stats are
// collected in each phase, so we simply reset and recollect),
// but we don't modify the schema.
func (cvt *Cvt) SetDataMode() {
	cvt.mode = true
}

// GetDDL Schema returns the Spanner schema that has been constructed so far.
func (cvt *Cvt) GetDDL(c ddl.Config) []string {
	var ddl []string
	for _, ct := range cvt.sSchema {
		ddl = append(ddl, ct.PrintCreateTable(c))
	}
	return ddl
}

// Rows returns the total count of data rows processed.
func (cvt *Cvt) Rows() int64 {
	var n int64
	for _, c := range cvt.stats.rows {
		n += c
	}
	return n
}

// BadRows returns the total count of bad rows encountered during
// data conversion.
func (cvt *Cvt) BadRows() int64 {
	var n int64
	for _, c := range cvt.stats.badRows {
		n += c
	}
	return n
}

// Statements returns the total number of statements processed.
func (cvt *Cvt) Statements() int64 {
	var n int64
	for _, x := range cvt.stats.statement {
		n += x.schema + x.data + x.skip + x.error
	}
	return n
}

// StatementErrors returns the number of statement errors encountered.
func (cvt *Cvt) StatementErrors() int64 {
	var n int64
	for _, x := range cvt.stats.statement {
		n += x.error
	}
	return n
}

// Unexpecteds returns the total number of distinct unexpected conditions
// encountered during processing.
func (cvt *Cvt) Unexpecteds() int64 {
	return int64(len(cvt.stats.unexpected))
}

// SampleBadRows returns a string-formatted list of rows that generated errors.
// Returns at most n rows.
func (cvt *Cvt) SampleBadRows(n int) []string {
	var l []string
	for _, x := range cvt.sampleBadRows.l {
		l = append(l, fmt.Sprintf("table=%s cols=%v data=%v\n", x.table, x.cols, x.vals))
		if len(l) > n {
			break
		}
	}
	return l
}

// AddPrimaryKeys analyzes all tables in cvt.schema and adds synthetic primary
// keys for any tables that don't have primary key.
func (cvt *Cvt) AddPrimaryKeys() {
	for t, ct := range cvt.sSchema {
		if len(ct.Pks) == 0 {
			k := cvt.buildPrimaryKey(t)
			ct.Cols = append(ct.Cols, k)
			ct.Cds[k] = ddl.ColumnDef{Name: k, T: ddl.Int64{}}
			ct.Pks = []ddl.IndexKey{ddl.IndexKey{Col: k}}
			cvt.sSchema[t] = ct
			cvt.syntheticPKeys[t] = syntheticPKey{k, 0}
		}
	}
}

// SetLocation configures the timezone for data conversion.
func (cvt *Cvt) SetLocation(loc *time.Location) {
	cvt.location = loc
}

func (cvt *Cvt) buildPrimaryKey(spannerTable string) string {
	base := "synth_id"
	if _, ok := cvt.toPostgres[spannerTable]; !ok {
		cvt.unexpected(fmt.Sprintf("toPostgres lookup fails for table %s: ", spannerTable))
		return base
	}
	count := 0
	key := base
	for {
		if _, ok := cvt.toPostgres[spannerTable].cols[key]; !ok {
			return key
		}
		key = fmt.Sprintf("%s%d", base, count)
		count++
	}
}

// unexpected records stats about corner-cases and conditions
// that were not expected. Note that the counts maybe not
// be completely reliable due to potential double-counting
// because we process pg_dump data twice.
func (cvt *Cvt) unexpected(u string) {
	internal.VerbosePrintf("Unexpected condition: %s\n", u)
	// Limit size of unexpected map. If over limit, then only
	// update existing entries.
	if _, ok := cvt.stats.unexpected[u]; ok || len(cvt.stats.unexpected) < 1000 {
		cvt.stats.unexpected[u]++
	}
}

// statsAddRow increments the rows stats for table 'sTable' if b is true.
// This is used to avoid double counting of stats. Specifically, some code paths
// that report row stats run in both schema-mode and data-mode e.g. statement.go.
// To avoid double counting, we explicitly choose a mode-for-stats-collection
// for each place where row stats are collected. When specifying this mode
// Take care to ensure that the code actually runs in the mode you specify,
// otherwise stats will be dropped.
func (cvt *Cvt) statsAddRow(sTable string, b bool) {
	if b {
		cvt.stats.rows[sTable]++
	}
}

// statsAddGoodRow increments the good-row stats for table 'sTable' if b is true.
// See statsAddRow comments for context.
func (cvt *Cvt) statsAddGoodRow(sTable string, b bool) {
	if b {
		cvt.stats.goodRows[sTable]++
	}
}

// statsAddBadRow increments the bad-row stats for table 'sTable' if b is true.
// See statsAddRow comments for context.
func (cvt *Cvt) statsAddBadRow(sTable string, b bool) {
	if b {
		cvt.stats.badRows[sTable]++
	}
}

func prNodeType(n nodes.Node) string {
	// Strip off "pg_query." prefix from nodes.Nodes type.
	return strings.TrimPrefix(reflect.TypeOf(n).Name(), "pg_query.")
}

func prNodes(l []nodes.Node) string {
	var s []string
	for _, n := range l {
		s = append(s, prNodeType(n))
	}
	return strings.Join(s, ".")
}

func (cvt *Cvt) getStatementStat(s string) *statementStat {
	if cvt.stats.statement[s] == nil {
		cvt.stats.statement[s] = &statementStat{}
	}
	return cvt.stats.statement[s]
}

func (cvt *Cvt) skipStatement(l []nodes.Node) {
	if cvt.schemaMode() { // Record statement stats on first pass only.
		s := prNodes(l)
		internal.VerbosePrintf("Skipping statement: %s\n", s)
		cvt.getStatementStat(s).skip++
	}
}

func (cvt *Cvt) errorInStatement(l []nodes.Node) {
	if cvt.schemaMode() { // Record statement stats on first pass only.
		s := prNodes(l)
		internal.VerbosePrintf("Error processing statement: %s\n", s)
		cvt.getStatementStat(s).error++
	}
}

func (cvt *Cvt) schemaStatement(l []nodes.Node) {
	if cvt.schemaMode() { // Record statement stats on first pass only.
		cvt.getStatementStat(prNodes(l)).schema++
	}
}

func (cvt *Cvt) dataStatement(l []nodes.Node) {
	if cvt.schemaMode() { // Record statement stats on first pass only.
		cvt.getStatementStat(prNodes(l)).data++
	}
}

func (cvt *Cvt) schemaMode() bool {
	return !cvt.mode
}

func (cvt *Cvt) dataMode() bool {
	return cvt.mode
}

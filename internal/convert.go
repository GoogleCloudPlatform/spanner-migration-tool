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
	"sync"
	"time"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/proto/migration"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/schema"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
	"go.uber.org/zap"
	"google.golang.org/genproto/googleapis/type/datetime"
)

// Conv contains all schema and data conversion state.
type Conv struct {
	mode               mode                     // Schema mode or data mode.
	SpSchema           ddl.Schema               // Maps Spanner table name to Spanner schema.
	SyntheticPKeys     map[string]SyntheticPKey // Maps Spanner table name to synthetic primary key (if needed).
	SrcSchema          map[string]schema.Table  // Maps source-DB table name to schema information.
	SchemaIssues       map[string]TableIssues   // Maps source-DB table/col to list of schema conversion issues.
	ToSpanner          map[string]NameAndCols   `json:"-"` // Maps from source-DB table name to Spanner name and column mapping.
	ToSource           map[string]NameAndCols   `json:"-"` // Maps from Spanner table name to source-DB table name and column mapping.
	UsedNames          map[string]bool          `json:"-"` // Map storing the names that are already assigned to tables, indices or foreign key contraints.
	dataSink           func(table string, cols []string, values []interface{})
	DataFlush          func()                  `json:"-"` // Data flush is used to flush out remaining writes and wait for them to complete.
	Location           *time.Location          // Timezone (for timestamp conversion).
	sampleBadRows      rowSamples              // Rows that generated errors during conversion.
	Stats              stats                   `json:"-"`
	TimezoneOffset     string                  // Timezone offset for timestamp conversion.
	SpDialect          string                  // The dialect of the spanner database to which Spanner migration tool is writing.
	UniquePKey         map[string][]string     // Maps Spanner table name to unique column name being used as primary key (if needed).
	Audit              Audit                   `json:"-"` // Stores the audit information for the database conversion
	Rules              []Rule                  // Stores applied rules during schema conversion
	IsSharded          bool                    // Flag denoting if the migration is sharded or not
	ConvLock           sync.RWMutex            `json:"-"` // ConvLock prevents concurrent map read/write operations. This lock will be used in all the APIs that either read or write elements to the conv object.
	SpRegion           string                  // Leader Region for Spanner Instance
	ResourceValidation bool                    // Flag denoting if validation for resources to generated is complete
	UI                 bool                    // Flag if UI interface was used for migration. ToDo: Remove flag after resource generation is introduced to UI
	SpSequences        map[string]ddl.Sequence // Maps Spanner Sequences to Sequence Schema
	SrcSequences       map[string]ddl.Sequence // Maps source-DB Sequences to Sequence schema information
}

type TableIssues struct {
	ColumnLevelIssues map[string][]SchemaIssue
	TableLevelIssues  []SchemaIssue
}

type AdditionalSchemaAttributes struct {
	IsSharded bool
}

type AdditionalDataAttributes struct {
	ShardId string
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
	ColId    string
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
	UniqueIndexPrimaryKey
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
	StringOverflow
	HotspotTimestamp
	HotspotAutoIncrement
	RedundantIndex
	AutoIncrementIndex
	InterleaveIndex
	InterleavedNotInOrder
	InterleavedOrder
	InterleavedAddColumn
	IllegalName
	InterleavedRenameColumn
	InterleavedChangeColumnSize
	RowLimitExceeded
	ShardIdColumnAdded
	ShardIdColumnPrimaryKey
	ArrayTypeNotSupported
	SequenceCreated
)

const (
	ShardIdColumn       = "migration_shard_id"
	SyntheticPrimaryKey = "synth_id"
)

// NameAndCols contains the name of a table and its columns.
// Used to map between source DB and Spanner table and column names.
type NameAndCols struct {
	Name string
	Cols map[string]string
}

// FkeyAndIdxs contains the name of a table, its foreign keys and indexes
// Used to map between source DB and spanner table name, foreign key name and index names.
type FkeyAndIdxs struct {
	Name       string
	ForeignKey map[string]string
	Index      map[string]string
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

// Stores the audit information of conversion.
// Elements that do not affect the migration functionality but are relevant for the migration metadata.
type Audit struct {
	SchemaConversionDuration time.Duration                          `json:"-"` // Duration of schema conversion.
	DataConversionDuration   time.Duration                          `json:"-"` // Duration of data conversion.
	MigrationRequestId       string                                 `json:"-"` // Unique request id generated per migration
	MigrationType            *migration.MigrationData_MigrationType `json:"-"` // Type of migration: Schema migration, data migration or schema and data migration
	DryRun                   bool                                   `json:"-"` // Flag to identify if the migration is a dry run.
	StreamingStats           streamingStats                         `json:"-"` // Stores information related to streaming migration process.
	Progress                 Progress                               `json:"-"` // Stores information related to progress of the migration progress
	SkipMetricsPopulation    bool                                   `json:"-"` // Flag to identify if outgoing metrics metadata needs to skipped
}

// Stores information related to generated Dataflow Resources.
type DataflowResources struct {
	JobId     string `json:"JobId"`
	GcloudCmd string `json:"GcloudCmd"`
	Region    string `json:"Region"`
}

type GcsResources struct {
	BucketName string `json:"BucketName"`
}

// Stores information related to generated Datastream Resources.
type DatastreamResources struct {
	DatastreamName string `json:"DatastreamName"`
	Region         string `json:"Region"`
}

// Stores information related to generated Pubsub Resources.
type PubsubResources struct {
	TopicId        string
	SubscriptionId string
	NotificationId string
	BucketName     string
	Region         string
}

// Stores information related to Monitoring resources
type MonitoringResources struct {
	DashboardName string `json:"DashboardName"`
}

type ShardResources struct {
	DatastreamResources DatastreamResources
	PubsubResources     PubsubResources
	DataflowResources   DataflowResources
	GcsResources        GcsResources
	MonitoringResources MonitoringResources
}

// Stores information related to the streaming migration process.
type streamingStats struct {
	Streaming                bool                        // Flag for confirmation of streaming migration.
	TotalRecords             map[string]map[string]int64 // Tablewise count of records received for processing, broken down by record type i.e. INSERT, MODIFY & REMOVE.
	BadRecords               map[string]map[string]int64 // Tablewise count of records not converted successfully, broken down by record type.
	DroppedRecords           map[string]map[string]int64 // Tablewise count of records successfully converted but failed to written on Spanner, broken down by record type.
	SampleBadRecords         []string                    // Records that generated errors during conversion.
	SampleBadWrites          []string                    // Records that faced errors while writing to Cloud Spanner.
	DatastreamResources      DatastreamResources
	DataflowResources        DataflowResources
	PubsubResources          PubsubResources
	GcsResources             GcsResources
	MonitoringResources      MonitoringResources
	ShardToShardResourcesMap map[string]ShardResources
	AggMonitoringResources   MonitoringResources
}

type PubsubCfg struct {
	TopicId        string
	SubscriptionId string
	NotificationId string
	BucketName     string
}

type DataflowOutput struct {
	JobID     string
	GCloudCmd string
}

// Stores information related to rules during schema conversion
type Rule struct {
	Id                string
	Name              string
	Type              string
	ObjectType        string
	AssociatedObjects string
	Enabled           bool
	Data              interface{}
	AddedOn           datetime.DateTime
}

type Tables struct {
	TableList []string `json:"TableList"`
}

type SchemaDetails struct {
	TableDetails []TableDetails `json:TableDetails`
}

type TableDetails struct {
	TableName string `json:TableName`
}

// MakeConv returns a default-configured Conv.
func MakeConv() *Conv {
	return &Conv{
		SpSchema:       ddl.NewSchema(),
		SyntheticPKeys: make(map[string]SyntheticPKey),
		SrcSchema:      make(map[string]schema.Table),
		SchemaIssues:   make(map[string]TableIssues),
		ToSpanner:      make(map[string]NameAndCols),
		ToSource:       make(map[string]NameAndCols),
		UsedNames:      make(map[string]bool),
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
		UniquePKey:     make(map[string][]string),
		Audit: Audit{
			StreamingStats: streamingStats{},
			MigrationType:  migration.MigrationData_SCHEMA_ONLY.Enum(),
		},
		Rules:        []Rule{},
		SpSequences:  make(map[string]ddl.Sequence),
		SrcSequences: make(map[string]ddl.Sequence),
	}
}

func (conv *Conv) ResetStats() {
	conv.Stats = stats{
		Rows:       make(map[string]int64),
		GoodRows:   make(map[string]int64),
		BadRows:    make(map[string]int64),
		Statement:  make(map[string]*statementStat),
		Unexpected: make(map[string]int64),
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

// WriteRow calls dataSink and updates row stats.
func (conv *Conv) WriteRow(srcTable, spTable string, spCols []string, spVals []interface{}) {
	if conv.Audit.DryRun {
		conv.statsAddGoodRow(srcTable, conv.DataMode())
	} else if conv.dataSink == nil {
		msg := "Internal error: ProcessDataRow called but dataSink not configured"
		VerbosePrintf("%s\n", msg)
		logger.Log.Debug("Internal error: ProcessDataRow called but dataSink not configured")

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

func (conv *Conv) AddShardIdColumn() {
	for t, ct := range conv.SpSchema {
		if ct.ShardIdColumn == "" {
			colName := conv.buildColumnNameWithBase(t, ShardIdColumn)
			columnId := GenerateColumnId()
			ct.ColIds = append(ct.ColIds, columnId)
			ct.ColDefs[columnId] = ddl.ColumnDef{Name: colName, Id: columnId, T: ddl.Type{Name: ddl.String, Len: 50}, NotNull: false, AutoGen: ddl.AutoGenCol{Name: "", GenerationType: ""}}
			ct.ShardIdColumn = columnId
			conv.SpSchema[t] = ct
			var issues []SchemaIssue
			issues = append(issues, ShardIdColumnAdded, ShardIdColumnPrimaryKey)
			conv.SchemaIssues[ct.Id].ColumnLevelIssues[columnId] = issues
		}
	}
}

// AddPrimaryKeys analyzes all tables in conv.schema and adds synthetic primary
// keys for any tables that don't have primary key.
func (conv *Conv) AddPrimaryKeys() {
	for t, ct := range conv.SpSchema {
		if len(ct.PrimaryKeys) == 0 {
			primaryKeyPopulated := false
			// Populating column with unique constraint as primary key in case
			// table doesn't have primary key and removing the unique index.
			if len(ct.Indexes) != 0 {
				for i, index := range ct.Indexes {
					if index.Unique {
						for _, indexKey := range index.Keys {
							ct.PrimaryKeys = append(ct.PrimaryKeys, ddl.IndexKey{ColId: indexKey.ColId, Desc: indexKey.Desc, Order: indexKey.Order})
							conv.UniquePKey[t] = append(conv.UniquePKey[t], indexKey.ColId)
							addMissingPrimaryKeyWarning(ct.Id, indexKey.ColId, conv, UniqueIndexPrimaryKey)
						}
						primaryKeyPopulated = true
						ct.Indexes = append(ct.Indexes[:i], ct.Indexes[i+1:]...)
						break
					}
				}
			}
			if !primaryKeyPopulated {
				k := conv.buildColumnNameWithBase(t, SyntheticPrimaryKey)
				columnId := GenerateColumnId()
				ct.ColIds = append(ct.ColIds, columnId)
				ct.ColDefs[columnId] = ddl.ColumnDef{Name: k, Id: columnId, T: ddl.Type{Name: ddl.String, Len: 50}, AutoGen: ddl.AutoGenCol{Name: "", GenerationType: ""}}
				ct.PrimaryKeys = []ddl.IndexKey{{ColId: columnId, Order: 1}}
				conv.SyntheticPKeys[t] = SyntheticPKey{columnId, 0}
				addMissingPrimaryKeyWarning(ct.Id, columnId, conv, MissingPrimaryKey)
			}
			conv.SpSchema[t] = ct
		}
	}
}

// Add 'Missing Primary Key' as a Warning inside ColumnLevelIssues of conv object
func addMissingPrimaryKeyWarning(tableId string, colId string, conv *Conv, schemaIssue SchemaIssue) {
	tableLevelIssues := conv.SchemaIssues[tableId].TableLevelIssues
	var columnLevelIssues map[string][]SchemaIssue
	if tableIssues, ok := conv.SchemaIssues[tableId]; ok {
		columnLevelIssues = tableIssues.ColumnLevelIssues
	} else {
		columnLevelIssues = make(map[string][]SchemaIssue)
	}
	columnLevelIssues[colId] = append(columnLevelIssues[colId], schemaIssue)
	conv.SchemaIssues[tableId] = TableIssues{
		TableLevelIssues:  tableLevelIssues,
		ColumnLevelIssues: columnLevelIssues,
	}
}

// SetLocation configures the timezone for data conversion.
func (conv *Conv) SetLocation(loc *time.Location) {
	conv.Location = loc
}

func (conv *Conv) buildColumnNameWithBase(tableId, base string) string {
	if _, ok := conv.SpSchema[tableId]; !ok {
		conv.Unexpected(fmt.Sprintf("Table doesn't exist for tableId %s: ", tableId))
		return base
	}
	count := 0
	key := base
	for {
		// Check key isn't already a column in the table.
		ok := true
		for _, column := range conv.SpSchema[tableId].ColDefs {
			if column.Name == key {
				ok = false
				break
			}
		}
		if ok {
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
	logger.Log.Debug("Unexpected condition", zap.String("condition", u))

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
		logger.Log.Debug("Skipping statement", zap.String("stmtType", stmtType))
		conv.getStatementStat(stmtType).Skip++
	}
}

// ErrorInStatement increments the error statement stats for 'stmtType'.
func (conv *Conv) ErrorInStatement(stmtType string) {
	if conv.SchemaMode() { // Record statement stats on first pass only.
		VerbosePrintf("Error processing statement: %s\n", stmtType)
		logger.Log.Debug("Error processing statement", zap.String("stmtType", stmtType))
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

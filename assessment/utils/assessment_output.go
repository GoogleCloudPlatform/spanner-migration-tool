/*
	Copyright 2025 Google LLC

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
*/
package utils

import (
	"github.com/GoogleCloudPlatform/spanner-migration-tool/schema"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
)

type AssessmentOutput struct {
	CostAssessment        CostAssessmentOutput
	SchemaAssessment      *SchemaAssessmentOutput
	AppCodeAssessment     *AppCodeAssessmentOutput
	QueryAssessment       QueryAssessmentOutput
	PerformanceAssessment PerformanceAssessmentOutput
}

type CostAssessmentOutput struct {
	//TBD
}

type SchemaAssessmentOutput struct {
	TableAssessmentOutput           []TableAssessment                    //List of Table assessments - Entry per table which is converted + Tables only at source + Tables only at Spanner
	TriggerAssessmentOutput         map[string]TriggerAssessment         // Maps trigger id to source trigger definition.
	StoredProcedureAssessmentOutput map[string]StoredProcedureAssessment // Maps stored procedure id to stored procedure(source) definition.
	FunctionAssessmentOutput        map[string]FunctionAssessment        // Maps function id to function(source) definition
	ViewAssessmentOutput            map[string]ViewAssessment            // Maps view id to view details- source name and definition and spanner name
	SpSequences                     map[string]ddl.Sequence
	//CodeSnippets                    *[]Snippet // Affected code snippets TODO - move to AppCodeAssessment
}

type TableAssessment struct {
	SourceTableDef      *SrcTableDetails
	SpannerTableDef     *SpTableDetails
	Columns             []ColumnAssessment //List of columns of current table
	SourceIndexDef      []SrcIndexDetails  // Index name to index details
	SpannerIndexDef     []SpIndexDetails   // Index name to index details
	CompatibleCharset   bool               //Is the charset compatible
	SizeIncreaseInBytes int                //Increase in table size on spanner
}

type ColumnAssessment struct {
	SourceColDef        *SrcColumnDetails
	SpannerColDef       *SpColumnDetails
	CompatibleDataType  bool // Is the data type compatible with spanner
	SizeIncreaseInBytes int  // Increase in column size on spanner - can be negative is size is smaller
}

type TriggerAssessment struct {
	Id            string
	Name          string
	Operation     string
	TargetTable   string // Name of the target table on which trigger is created
	TargetTableId string
}

type StoredProcedureAssessment struct {
	Id             string
	Name           string
	Definition     string
	LinesOfCode    int
	TablesAffected []string // TODO(khajanchi): Add parsing logic to extract table names from SP definition.
}

type FunctionAssessment struct {
	Id             string
	Name           string
	Definition     string
	LinesOfCode    int
	TablesAffected []string // TODO(khajanchi): Add parsing logic to extract table names from function definition.
}

type ViewAssessment struct {
	Id            string
	SrcName       string
	SrcDefinition string
	SrcViewType   string
	SpName        string
}

type SrcTableDetails struct {
	Id               string
	Name             string
	Charset          string
	Collation        string
	Properties       map[string]string //any other table level properties
	CheckConstraints map[string]schema.CheckConstraint
	SourceForeignKey map[string]SourceForeignKey
}

type SourceForeignKey struct {
	Definition schema.ForeignKey
	Ddl        string
}

type SpTableDetails struct {
	Id                string
	Name              string
	CheckConstraints  map[string]ddl.CheckConstraint
	SpannerForeignKey map[string]SpannerForeignKey
}

type SpannerForeignKey struct {
	Definition      ddl.Foreignkey
	Ddl             string
	IsInterleavable bool   // TODO: display this information in report
	ParentTableName string // Applicable incase of interleaving
}

type SrcIndexDetails struct {
	Id        string
	Name      string
	TableId   string
	TableName string
	Type      string
	IsUnique  bool
	Ddl       string
}

type SpIndexDetails struct {
	Id        string
	Name      string
	TableId   string
	TableName string
	IsUnique  bool
	Ddl       string
}

type SrcColumnDetails struct {
	Id                     string
	Name                   string
	TableId                string
	TableName              string
	Datatype               string
	ArrayBounds            []int64
	Mods                   []int64
	NotNull                bool
	PrimaryKeyOrder        int
	ForeignKey             []string
	AutoGen                ddl.AutoGenCol
	DefaultValue           ddl.DefaultValue
	GeneratedColumn        GeneratedColumnInfo
	IsOnUpdateTimestampSet bool
	IsOnInsertTimestampSet bool
	IsUnsigned             bool
	MaxColumnSize          int64
}

type SpColumnDetails struct {
	Id              string
	Name            string
	TableId         string
	TableName       string
	Datatype        string
	IsArray         bool
	Len             int64
	NotNull         bool
	PrimaryKeyOrder int
	ForeignKey      []string
	AutoGen         ddl.AutoGenCol
	DefaultValue    ddl.DefaultValue
}

type AppCodeAssessmentOutput struct {
	Language               string
	Framework              string
	TotalLoc               int
	TotalFiles             int
	CodeSnippets           *[]Snippet // Affected code snippets
	QueryTranslationResult *[]QueryTranslationResult
}

type QueryAssessmentOutput struct {
	QueryTranslationResult *[]QueryTranslationResult
}

type PerformanceAssessmentOutput struct {
	//TBD
}

type QueryTranslationResult struct {
	OriginalQuery           string   `json:"old_query"`
	NormalizedQuery         string   `json:"normalized_query"`
	SpannerQuery            string   `json:"new_query"`
	Explanation             string   `json:"explanation"`
	Complexity              string   `json:"complexity"`
	TranslationError        string   `json:"translation_error,omitempty"`
	AssessmentSource        string   // "app_code" or "performance_schema" or "app_code,performance_schema"
	ExecutionCount          int      `json:"execution_count,omitempty"`
	SnippetId               string   `json:"snippet_id,omitempty"`
	NumberOfQueryOccurances int      `json:"number_of_query_occurances,omitempty"`
	SourceTablesAffected    []string `json:"tables_affected"`
	SpannerTablesAffected   []string
	CrossDBJoins            bool               `json:"cross_db_joins"`
	FunctionsUsed           []string           `json:"functions_used"`
	OperatorsUsed           []string           `json:"operators_used"`
	DatabasesReferenced     []string           `json:"databases_referenced"`
	SelectForUpdate         bool               `json:"select_for_update"`
	ComparisonAnalysis      ComparisonAnalysis `json:"comparison_analysis"`
	QueryType               string             // INSERT / UPDATE / DELETE / SELECT / CALL / DDL / OTHER
}

type ComparisonAnalysis struct {
	LiteralComparisons   *LiteralComparisonAnalysis   `json:"literal_comparisons,omitempty"`
	DataTypeComparisons  *DataTypeComparisonAnalysis  `json:"data_type_comparisons,omitempty"`
	TimestampComparisons *TimestampComparisonAnalysis `json:"timestamp_comparisons,omitempty"`
	DateComparisons      *DateComparisonAnalysis      `json:"date_comparisons,omitempty"`
}

type LiteralComparisonAnalysis struct {
	PrecisionIssues []string `json:"precision_issues"`
}

type DataTypeComparisonAnalysis struct {
	IncompatibleTypes []string `json:"incompatible_types"`
}

type TimestampComparisonAnalysis struct {
	TimezoneIssues []string `json:"timezone_issues"`
}

type DateComparisonAnalysis struct {
	FormatIssues []string `json:"format_issues"`
}

type QueryTranslationInput struct {
	Query string
	Count int
}

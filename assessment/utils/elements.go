/* Copyright 2025 Google LLC
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
// limitations under the License.*/

package utils

import (
	"github.com/GoogleCloudPlatform/spanner-migration-tool/schema"
)

// All the elements that will be a part of the assessment
// If this file becomes too big, or if type specific methods get added, consider splitting this file

// Idenitification of the database which can be referred to in the assessment
type DbIdentifier struct {
	DatabaseName string
	Namespace    string
}

// Information relevant to assessment of tables
type TableAssessmentInfo struct {
	Db                    DbIdentifier
	Name                  string
	TableDef              schema.Table
	Charset               string
	Collation             string
	ColumnAssessmentInfos map[string]ColumnAssessmentInfo[any]
}

// Information relevant to assessment of columns
type ColumnAssessmentInfo[T any] struct {
	Db                     DbIdentifier
	Name                   string
	TableName              string
	ColumnDef              schema.Column
	MaxValue               T
	MinValue               T
	IsUnsigned             bool
	IsOnUpdateTimestampSet bool
	GeneratedColumn        GeneratedColumnInfo
	MaxColumnSize          int64
}

type GeneratedColumnInfo struct {
	IsPresent bool
	Statement string
	IsVirtual bool
}

// Information relevant to assessment of indexes
type IndexAssessmentInfo struct {
	Db       DbIdentifier
	Name     string
	TableId  string
	Ty       string
	IndexDef schema.Index
}

// Information relevant to assessment of stored procedures
type StoredProcedureAssessmentInfo struct {
	Db               DbIdentifier
	Name             string
	LinesOfCode      int
	TablesAffected   []string
	ReferencesInCode int
	Definition       string
	IsDeterministic  bool
}

// Information relevant to assessment of triggers
type TriggerAssessmentInfo struct {
	Db                DbIdentifier
	Name              string
	Operation         string
	TargetTable       string
	ActionTiming      string // Whether the trigger activates before or after the triggering event. The value is BEFORE or AFTER.
	EventManipulation string // This is the type of operation on the associated table for which the trigger activates. The value is INSERT , DELETE , or UPDATE.
}

// Information relevant to assessment of functions
type FunctionAssessmentInfo struct {
	Db               DbIdentifier
	Name             string
	LinesOfCode      int
	TablesAffected   []string
	ReferencesInCode int
	Definition       string
	IsDeterministic  bool
	Datatype         string
}

// Information relevant to assessment of views
// TODO : Capture information about view permissions
type ViewAssessmentInfo struct {
	Db          DbIdentifier
	Name        string
	Definition  string
	CheckOption string // Determines how INSERT and UPDATE statements are handled when they affect a view. The value is one of NONE, CASCADE, or LOCAL.
	IsUpdatable bool
}

// Information relevant to assessment of queries
type QueryAssessmentInfo struct {
	Db             DbIdentifier
	Query          string
	LengthOfQuery  string
	TablesAffected []string
	Count          int
}

type Snippet struct {
	Id                       string // generated id
	TableName                string // will be empty if snippet is not a schema update
	ColumnName               string // will be empty if snippet is not a schema update
	SchemaChange             string // will be empty if snippet is not a schema update
	NumberOfAffectedLines    int
	Complexity               string
	SourceCodeSnippet        []string
	SuggestedCodeSnippet     []string
	SourceMethodSignature    string // will be empty if code impact is outside method.
	SuggestedMethodSignature string // will be empty if code impact is outside method.
	Explanation              string
	RelativeFilePath         string
	FilePath                 string
	IsDao                    bool
}

// Information relevant to assessment of queries
type CodeAssessment struct {
	ProjectPath     string
	Language        string
	Framework       string
	TotalLoc        int
	TotalFiles      int
	Snippets        *[]Snippet
	GeneralWarnings []string
}

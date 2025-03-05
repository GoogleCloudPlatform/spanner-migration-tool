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

import "github.com/GoogleCloudPlatform/spanner-migration-tool/schema"

// All the elements that will be a part of the assessment
// If this file becomes too big, or if type specific methods get added, consider splitting this file

// Idenitification of the database which can be referred to in the assessment
type DbIdentifier struct {
	DatabaseName string
	Namespace    string
}

// Information relevant to assessment of tables
type TableAssessment struct {
	Db                DbIdentifier
	Name              string
	TableDef          schema.Table
	ColumnAssessments []ColumnAssessment[any]
}

// Information relevant to assessment of columns
type ColumnAssessment[T any] struct {
	Db         DbIdentifier
	Name       string
	TableName  string
	ColumnDef  schema.Column
	MaxValue   T
	MinValue   T
	IsUnsigned bool
}

// Information relevant to assessment of indexes
type IndexAssessment struct {
	Db       DbIdentifier
	Name     string
	TableId  string
	Ty       string
	IndexDef schema.Index
}

// Information relevant to assessment of stored procedures
type StoredProcedureAssessment struct {
	Db               DbIdentifier
	Name             string
	LinesOfCode      int
	TablesAffected   []string
	ReferencesInCode int
	Definition       string
	IsDeterministic  bool
}

// Information relevant to assessment of triggers
type TriggerAssessment struct {
	Db                DbIdentifier
	Name              string
	Operation         string
	TargetTable       string
	ActionTiming      string // Whether the trigger activates before or after the triggering event. The value is BEFORE or AFTER.
	EventManipulation string // This is the type of operation on the associated table for which the trigger activates. The value is INSERT , DELETE , or UPDATE.
}

// Information relevant to assessment of queries
type QueryAssessment struct {
	db             DbIdentifier
	name           string
	lengthOfQuery  string
	tablesAffected []string
}

type Snippet struct {
	TableName                string // will be empty if snippet is not a schema update
	ColumnName               string // will be empty if snippet is not a schema update
	SchemaChange             string // will be empty if snippet is not a schema update
	NumberOfAffectedLines    string
	Complexity               string
	SourceCodeSnippet        []string
	SuggestedCodeSnippet     []string
	SourceMethodSignature    string // will be empty if code impact is outside method.
	SuggestedMethodSignature string // will be empty if code impact is outside method.
	Explanation              string
	FileName                 string
	IsDao                    bool
}

// Information relevant to assessment of queries
type CodeAssessment struct {
	Snippets        []Snippet
	GeneralWarnings []string
}

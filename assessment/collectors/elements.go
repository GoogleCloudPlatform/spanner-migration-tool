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

package assessment

import (
	"github.com/GoogleCloudPlatform/spanner-migration-tool/schema"
)

// All the elements that will be a part of the assessment
// If this file becomes too big, or if type specific methods get added, consider splitting this file

// Idenitification of the database which can be referred to in the assessment
type DbIdentifier struct {
	databaseName string
	namespace    string
}

// Information relevant to assessment of tables
type TableAssessment struct {
	db                DbIdentifier
	name              string
	tableDef          schema.Table
	columnAssessments []ColumnAssessment[any]
}

// Information relevant to assessment of columns
type ColumnAssessment[T any] struct {
	db       DbIdentifier
	tableDef schema.Table
	maxValue T
	minValue T
}

// Information relevant to assessment of stored procedures
type StoredProcedureAssessment struct {
	db              DbIdentifier
	name            string
	linesOfCode     int
	tablesAffected  []string
	referecesInCode int
}

// Information relevant to assessment of triggers
type TriggerAssessment struct {
	db           DbIdentifier
	name         string
	operation    string
	targetTables []string
}

// Information relevant to assessment of queries
type QueryAssessment struct {
	db             DbIdentifier
	name           string
	lengthOfQuery  string
	tablesAffected []string
}

type Snippet struct {
	tableName                string // will be empty if snippet is not a schema update
	columnName               string // will be empty if snippet is not a schema update
	schemaChange             string // will be empty if snippet is not a schema update
	numberOfAffectedLines    string
	complexity               string
	sourceCodeSnippet        []string
	suggestedCodeSnippet     []string
	sourceMethodSignature    string // will be empty if code impact is outside method.
	suggestedMethodSignature string // will be empty if code impact is outside method.
	explanation              string
	fileName                 string
	isDao                    bool
}

// Information relevant to assessment of queries
type CodeAssessment struct {
	snippets        []Snippet
	generalWarnings []string
}

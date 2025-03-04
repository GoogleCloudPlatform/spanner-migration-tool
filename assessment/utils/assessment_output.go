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

import "github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"

type AssessmentOutput struct {
	CostAssessment        CostAssessmentOutput
	SchemaAssessment      SchemaAssessmentOutput
	AppCodeAssessment     AppCodeAssessmentOutput
	QueryAssessment       QueryAssessmentOutput
	PerformanceAssessment PerformanceAssessmentOutput
}

type CostAssessmentOutput struct {
	//TBD
}

type SchemaAssessmentOutput struct {
	SourceTableDefs                 map[string]TableDetails
	SpannerTableDefs                map[string]TableDetails
	SourceColDefs                   map[string]SrcColumnDetails
	SpannerColDefs                  map[string]SpColumnDetails
	SourceIndexDef                  map[string]SrcIndexDetails
	SpannerIndexDef                 map[string]SpIndexDetails
	Triggers                        map[string]TriggerAssessmentOutput
	StoredProcedureAssessmentOutput map[string]StoredProcedureAssessmentOutput
}

type TriggerAssessmentOutput struct {
	Id            string
	Name          string
	Operation     string
	TargetTable   string
	TargetTableId string
}

type StoredProcedureAssessmentOutput struct {
	Id             string
	Name           string
	Definition     string
	TablesAffected []string // TODO(khajanchi): Add parsing logic to extract table names from SP definition.
}

type TableDetails struct {
	Id         string
	Name       string
	Charset    string
	Properties map[string]string //any other table level properties
}

type SrcIndexDetails struct {
	Id        string
	Name      string
	TableId   string
	TableName string
	Type      string
	IsUnique  bool
}

type SpIndexDetails struct {
	Id        string
	Name      string
	TableId   string
	TableName string
	IsUnique  bool
}

type SrcColumnDetails struct {
	Id              string
	TableId         string
	TableName       string
	Datatype        string
	ArrayBounds     []int64
	Mods            []int64
	IsNull          bool
	PrimaryKeyOrder int
	ForeignKey      []string
	AutoGen         ddl.AutoGenCol
	DefaultValue    ddl.DefaultValue
}

type SpColumnDetails struct {
	Id              string
	TableId         string
	TableName       string
	Datatype        string
	IsArray         bool
	Len             int64
	IsNull          bool
	PrimaryKeyOrder int
	ForeignKey      []string
	AutoGen         ddl.AutoGenCol
	DefaultValue    ddl.DefaultValue
}

type AppCodeAssessmentOutput struct {
	//TBD
}

type QueryAssessmentOutput struct {
	//TBD
}

type PerformanceAssessmentOutput struct {
	//TBD
}

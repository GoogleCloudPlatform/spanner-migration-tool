// Copyright 2025 Google LLC
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

package common

import (
	"github.com/GoogleCloudPlatform/spanner-migration-tool/assessment/utils"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/schema"
)

// All the data that is to be extracted from infoschema of source
type InfoSchema interface {
	GetIndexInfo(table string, index schema.Index) (utils.IndexAssessmentInfo, error)
	GetTriggerInfo() ([]utils.TriggerAssessmentInfo, error)
	GetStoredProcedureInfo() ([]utils.StoredProcedureAssessmentInfo, error)
	GetTableInfo(conv *internal.Conv) (map[string]utils.TableAssessmentInfo, error)
	GetFunctionInfo() ([]utils.FunctionAssessmentInfo, error)
	GetViewInfo() ([]utils.ViewAssessmentInfo, error)
}

type InfoSchemaImpl struct{}

type PerformanceSchema interface {
	GetAllQueries() ([]utils.QueryAssessmentInfo, error)
}

type PerformanceSchemaImpl struct{}

type SourceSpecificComparison interface {
	IsDataTypeCodeCompatible(srcColumnDef utils.SrcColumnDetails, spColumnDef utils.SpColumnDetails) bool
}

type SourceSpecificComparisonImpl struct{}

// Copyright 2022 Google LLC
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

package spanner

import (
	"github.com/cloudspannerecosystem/harbourbridge/common/constants"
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/schema"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
)

type ToDdlImpl struct {
}

// ToSpannerType maps a scalar source schema type (defined by id and
// mods) into a Spanner type. This is the core source-to-Spanner type
// mapping.  toSpannerType returns the Spanner type and a list of type
// conversion issues encountered.
// Functions below implement the common.ToDdl interface
func (tdi ToDdlImpl) ToSpannerType(conv *internal.Conv, columnType schema.Type) (ddl.Type, []internal.SchemaIssue) {
	ty, issues := toSpannerTypeInternal(conv, columnType.Name, columnType.Mods)
	if conv.TargetDb == constants.TargetExperimentalPostgres {
		ty, issues = overrideExperimentalType(columnType, ty, issues)
	} else {
		ty.IsArray = len(columnType.ArrayBounds) == 1
	}
	return ty, issues
}

// toSpannerType maps a scalar source schema type (defined by id and
// mods) into a Spanner type. This is the core source-to-Spanner type
// mapping.  toSpannerType returns the Spanner type and a list of type
// conversion issues encountered.
func toSpannerTypeInternal(conv *internal.Conv, id string, mods []int64) (ddl.Type, []internal.SchemaIssue) {
	switch id {
	case "BOOL":
		return ddl.Type{Name: ddl.Bool}, nil
	case "BYTES":
		return ddl.Type{Name: ddl.Bytes, Len: mods[0]}, nil
	case "DATE":
		return ddl.Type{Name: ddl.Date}, nil
	case "FLOAT64":
		return ddl.Type{Name: ddl.Float64}, nil
	case "INT64":
		return ddl.Type{Name: ddl.Int64}, nil
	case "JSON":
		return ddl.Type{Name: ddl.JSON}, nil
	case "NUMERIC":
		return ddl.Type{Name: ddl.Numeric}, nil
	case "STRING":
		return ddl.Type{Name: ddl.String, Len: mods[0]}, nil
	case "TIMESTAMP":
		return ddl.Type{Name: ddl.Timestamp}, nil
	}
	return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.NoGoodType}
}

// Override the types to map to experimental postgres types.
func overrideExperimentalType(columnType schema.Type, originalType ddl.Type, issues []internal.SchemaIssue) (ddl.Type, []internal.SchemaIssue) {
	switch columnType.Name {
	case "PG.NUMERIC":
		return ddl.Type{Name: ddl.Numeric}, nil
	case "PG.JSONB":
		return ddl.Type{Name: ddl.JSON}, nil
	}
	return originalType, issues
}

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

// Package postgres handles schema and data migrations from Postgres.
package sqlserver

import (
	"github.com/cloudspannerecosystem/harbourbridge/common/constants"
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/schema"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
)

// ToDdlImpl Postgres specific implementation for ToDdl.
type ToDdlImpl struct {
}

// ToSpannerType maps a scalar source schema type (defined by id and
// mods) into a Spanner type. This is the core source-to-Spanner type
// mapping.  toSpannerType returns the Spanner type and a list of type
// conversion issues encountered.
func (tdi ToDdlImpl) ToSpannerType(conv *internal.Conv, columnType schema.Type) (ddl.Type, []internal.SchemaIssue) {
	ty, issues := toSpannerTypeInternal(conv, columnType.Name, columnType.Mods)
	if conv.TargetDb == constants.TargetExperimentalPostgres {
		ty = overrideExperimentalType(columnType, ty)
	} else {
		if len(columnType.ArrayBounds) > 1 {
			ty = ddl.Type{Name: ddl.String, Len: ddl.MaxLength}
			issues = append(issues, internal.MultiDimensionalArray)
		}
		ty.IsArray = len(columnType.ArrayBounds) == 1
	}
	return ty, issues
}

// toSpannerType maps a scalar source schema type (defined by id and
// mods) into a Spanner type. This is the core source-to-Spanner type
// mapping.  toSpannerType returns the Spanner type and a list of type
// conversion issues encountered.
func toSpannerTypeInternal(conv *internal.Conv, id string, mods []int64) (ddl.Type, []internal.SchemaIssue) {
	// TODo :needs handle this type  =>  bit,uniqueidentifier,xml,spatial types
	// TODO : float real handle Precision.
	switch id {
	case "bit":
		return ddl.Type{Name: ddl.Bool}, nil
	case "uniqueidentifier":
		return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
	case "bigserial":
		return ddl.Type{Name: ddl.Int64}, []internal.SchemaIssue{internal.Serial}
	case "binary", "varbinary", "image":
		if len(mods) > 0 && mods[0] > 0 {
			return ddl.Type{Name: ddl.Bytes, Len: mods[0]}, nil
		}
		return ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}, nil
	// spanner date is 4 bytes sql server 3 bytes
	case "date":
		return ddl.Type{Name: ddl.Date}, []internal.SchemaIssue{internal.Widened}
	case "float4", "real":
		return ddl.Type{Name: ddl.Float64}, []internal.SchemaIssue{internal.Widened}
	case "bigint":
		return ddl.Type{Name: ddl.Int64}, nil
	case "tinyint", "smallint", "int":
		return ddl.Type{Name: ddl.Int64}, []internal.SchemaIssue{internal.Widened}
	case "numeric", "money", "smallmoney", "decimal":
		return ddl.Type{Name: ddl.Numeric}, nil
	case "serial":
		return ddl.Type{Name: ddl.Int64}, []internal.SchemaIssue{internal.Serial}
	case "ntext", "text":
		return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
		// TODO : need to check this mapping again
	case "datetimeoffset", "datetime2", "datetime":
		return ddl.Type{Name: ddl.Timestamp}, nil
	case "smalldatetime", "time":
		// Map timestamp without timezone to Spanner timestamp.
		return ddl.Type{Name: ddl.Timestamp}, []internal.SchemaIssue{internal.Timestamp}
	case "varchar", "char", "nvarchar", "nchar":
		if len(mods) > 0 && mods[0] > 0 {
			return ddl.Type{Name: ddl.String, Len: mods[0]}, nil
		}
		return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
	case "json", "jsonb":
		return ddl.Type{Name: ddl.JSON}, nil
	}
	return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.NoGoodType}
}

// Override the types to map to experimental postgres types.
func overrideExperimentalType(columnType schema.Type, originalType ddl.Type) ddl.Type {
	if len(columnType.ArrayBounds) > 0 {
		return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}
	} else if columnType.Name == "date" {
		return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}
	} else if columnType.Name == "json" || columnType.Name == "jsonb" {
		return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}
	}
	return originalType
}

// Copyright 2021 Google LLC
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

// Package sqlserver handles schema and data migrations from sqlserver.
package sqlserver

import (
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/schema"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
)

// ToDdlImpl sql server specific implementation for ToDdl.
type ToDdlImpl struct {
}

// ToSpannerType maps a scalar source schema type (defined by id and
// mods) into a Spanner type. This is the core source-to-Spanner type
// mapping.  toSpannerType returns the Spanner type and a list of type
// conversion issues encountered.
func (tdi ToDdlImpl) ToSpannerType(conv *internal.Conv, columnType schema.Type) (ddl.Type, []internal.SchemaIssue) {
	ty, issues := toSpannerTypeInternal(columnType.Name, columnType.Mods)
	return ty, issues
}

// toSpannerType maps a scalar source schema type (defined by id and
// mods) into a Spanner type. This is the core source-to-Spanner type
// mapping.  toSpannerType returns the Spanner type and a list of type
// conversion issues encountered.
func toSpannerTypeInternal(srcType string, mods []int64) (ddl.Type, []internal.SchemaIssue) {
	switch srcType {
	case "bit":
		return ddl.Type{Name: ddl.Bool}, nil
	case "uniqueidentifier":
		return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
	case "binary", "varbinary", "image":
		return ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}, nil
	case "date":
		return ddl.Type{Name: ddl.Date}, nil
	case "float", "real":
		return ddl.Type{Name: ddl.Float64}, []internal.SchemaIssue{internal.Widened}
	case "bigint":
		return ddl.Type{Name: ddl.Int64}, nil
	case "tinyint", "smallint", "int":
		return ddl.Type{Name: ddl.Int64}, []internal.SchemaIssue{internal.Widened}
	case "numeric", "money", "smallmoney", "decimal":
		return ddl.Type{Name: ddl.Numeric}, nil
	case "ntext", "text", "xml":
		return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
	case "smalldatetime", "datetimeoffset", "datetime2", "datetime":
		return ddl.Type{Name: ddl.Timestamp}, []internal.SchemaIssue{internal.Timestamp}
	case "time":
		return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Time}
	case "varchar", "char", "nvarchar", "nchar":
		// Sets the source length only if it falls within the allowed length range in Spanner.
		if len(mods) > 0 && mods[0] > 0 && mods[0] <= ddl.StringMaxLength {
			return ddl.Type{Name: ddl.String, Len: mods[0]}, nil
		}
		// Raises warning and sets length to MAX when -
		// Source length is greater than maximum allowed length
		// -OR-
		// Source length is "-1" which represents MAX in SQL Server
		if len(mods) > 0 && (mods[0] > ddl.StringMaxLength || mods[0] < 0) {
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.StringOverflow}
		}
		return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
	case "timestamp":
		return ddl.Type{Name: ddl.Int64}, nil
	default:
		return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.NoGoodType}
	}
}

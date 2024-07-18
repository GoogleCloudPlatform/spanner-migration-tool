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
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/schema"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
)

// ToDdlImpl sql server specific implementation for ToDdl.
type ToDdlImpl struct {
}

// ToSpannerType maps a scalar source schema type (defined by id and
// mods) into a Spanner type. This is the core source-to-Spanner type
// mapping.  toSpannerType returns the Spanner type and a list of type
// conversion issues encountered.
func (tdi ToDdlImpl) ToSpannerType(conv *internal.Conv, spType string, srcType schema.Type) (ddl.Type, []internal.SchemaIssue) {
	ty, issues := toSpannerTypeInternal(srcType, spType)
	return ty, issues
}

func (tdi ToDdlImpl) GetColumnAutoGen(conv *internal.Conv, autoGenCol ddl.AutoGenCol, colId string, tableId string) (*ddl.AutoGenCol, error) {
	return nil, nil
}

// toSpannerTypeInternal defines the mapping of source types into Spanner
// types. Each source type has a default Spanner type, as well as other potential
// Spanner types it could map to. When calling toSpannerTypeInternal, you specify
// the source type name (along with any modifiers), and optionally you specify
// a target Spanner type name (empty string if you don't have one). If the target
// Spanner type name is specified and is a potential mapping for this source type,
// then it will be used to build the returned ddl.Type. If not, the default
// Spanner type for this source type will be used.

func toSpannerTypeInternal(srcType schema.Type, spType string) (ddl.Type, []internal.SchemaIssue) {
	switch srcType.Name {
	case "bigint":
		switch spType {
		case ddl.String:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		case ddl.Int64:
			return ddl.Type{Name: ddl.Int64}, []internal.SchemaIssue{internal.Widened}
		default:
			return ddl.Type{Name: ddl.Int64}, nil
		}
	case "tinyint", "smallint", "int":
		switch spType {
		case ddl.String:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		case ddl.Int64:
			return ddl.Type{Name: ddl.Int64}, []internal.SchemaIssue{internal.Widened}
		default:
			return ddl.Type{Name: ddl.Int64}, []internal.SchemaIssue{internal.Widened}
		}
	case "float", "real":
		switch spType {
		case ddl.String:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		default:
			return ddl.Type{Name: ddl.Float64}, []internal.SchemaIssue{internal.Widened}
		}
	case "numeric", "decimal", "money", "smallmoney":
		switch spType {
		case ddl.String:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		default:
			// TODO: check mod[0] and mod[1] and generate a warning
			// if this numeric won't fit in Spanner's NUMERIC.
			return ddl.Type{Name: ddl.Numeric}, nil
		}

	case "bit":
		switch spType {
		case ddl.String:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
		default:
			return ddl.Type{Name: ddl.Bool}, nil
		}
	case "uniqueidentifier":
		switch spType {
		case ddl.Bytes:
			if len(srcType.Mods) > 0 && srcType.Mods[0] > 0 {
				return ddl.Type{Name: ddl.Bytes, Len: srcType.Mods[0]}, nil
			}
			return ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}, nil
		default:
			if len(srcType.Mods) > 0 && srcType.Mods[0] > 0 {
				return ddl.Type{Name: ddl.String, Len: srcType.Mods[0]}, nil
			}
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
		}
	case "varchar", "char", "nvarchar", "nchar":
		switch spType {
		case ddl.Bytes:
			if len(srcType.Mods) > 0 && srcType.Mods[0] > 0 {
				return ddl.Type{Name: ddl.Bytes, Len: srcType.Mods[0]}, nil
			}
			return ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}, nil
		default:
			// Sets the source length only if it falls within the allowed length range in Spanner.
			if len(srcType.Mods) > 0 && srcType.Mods[0] > 0 && srcType.Mods[0] <= ddl.StringMaxLength {
				return ddl.Type{Name: ddl.String, Len: srcType.Mods[0]}, nil
			}
			// Raises warning and sets length to MAX when -
			// Source length is greater than maximum allowed length
			// -OR-
			// Source length is "-1" which represents MAX in SQL Server
			if len(srcType.Mods) > 0 && (srcType.Mods[0] > ddl.StringMaxLength || srcType.Mods[0] < 0) {
				return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.StringOverflow}
			}
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
		}
	case "ntext", "text", "xml":
		switch spType {
		case ddl.Bytes:
			return ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}, nil
		default:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
		}

	case "binary", "varbinary", "image":
		switch spType {
		case ddl.String:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
		default:
			return ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}, nil
		}
	case "date":
		switch spType {
		case ddl.String:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		default:
			return ddl.Type{Name: ddl.Date}, nil
		}
	case "datetime2", "datetime", "datetimeoffset", "smalldatetime", "rowversion":
		switch spType {
		case ddl.String:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		default:
			return ddl.Type{Name: ddl.Timestamp}, []internal.SchemaIssue{internal.Timestamp}
		}
	case "timestamp":
		switch spType {
		case ddl.String:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		default:
			return ddl.Type{Name: ddl.Int64}, nil
		}
	case "time":
		return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Time}
	}
	return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.NoGoodType}
}

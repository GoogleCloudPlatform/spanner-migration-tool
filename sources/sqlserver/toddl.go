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

// ToSpannerGSQLDialectType maps a scalar source schema type (defined by id and
// mods) into a Spanner GOOGLE STANDARD SQL dialect type. ToSpannerGSQLDialectType returns
// the Spanner type and a list of type conversion issues encountered.
func (tdi ToDdlImpl) ToSpannerGSQLDialectType(conv *internal.Conv, spType string, srcType schema.Type) (ddl.Type, []internal.SchemaIssue) {
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

// ToSpannerPostgreSQLDialectType maps a scalar source schema type (defined by id and
// mods) into a Spanner PostgreSQL dialect type. ToSpannerPostgreSQLDialectType returns
// the Spanner type and a list of type conversion issues encountered.
func (tdi ToDdlImpl) ToSpannerPostgreSQLDialectType(conv *internal.Conv, spType string, srcType schema.Type) (ddl.Type, []internal.SchemaIssue) {
	switch srcType.Name {
	case "bigint":
		switch spType {
		case ddl.PGVarchar:
			return ddl.Type{Name: ddl.PGVarchar, Len: ddl.PGMaxLength}, []internal.SchemaIssue{internal.Widened}
		case ddl.PGInt8:
			return ddl.Type{Name: ddl.PGInt8}, []internal.SchemaIssue{internal.Widened}
		default:
			return ddl.Type{Name: ddl.PGInt8}, nil
		}
	case "tinyint", "smallint", "int":
		switch spType {
		case ddl.PGVarchar:
			return ddl.Type{Name: ddl.PGVarchar, Len: ddl.PGMaxLength}, []internal.SchemaIssue{internal.Widened}
		case ddl.PGInt8:
			return ddl.Type{Name: ddl.PGInt8}, []internal.SchemaIssue{internal.Widened}
		default:
			return ddl.Type{Name: ddl.PGInt8}, []internal.SchemaIssue{internal.Widened}
		}
	case "float", "real":
		switch spType {
		case ddl.PGVarchar:
			return ddl.Type{Name: ddl.PGVarchar, Len: ddl.PGMaxLength}, []internal.SchemaIssue{internal.Widened}
		default:
			return ddl.Type{Name: ddl.PGFloat8}, []internal.SchemaIssue{internal.Widened}
		}
	case "numeric", "decimal", "money", "smallmoney":
		switch spType {
		case ddl.PGVarchar:
			return ddl.Type{Name: ddl.PGVarchar, Len: ddl.PGMaxLength}, []internal.SchemaIssue{internal.Widened}
		default:
			// TODO: check mod[0] and mod[1] and generate a warning
			// if this numeric won't fit in Spanner's NUMERIC.
			return ddl.Type{Name: ddl.PGNumeric}, nil
		}

	case "bit":
		switch spType {
		case ddl.PGVarchar:
			return ddl.Type{Name: ddl.PGVarchar, Len: ddl.PGMaxLength}, nil
		default:
			return ddl.Type{Name: ddl.PGBool}, nil
		}
	case "uniqueidentifier":
		switch spType {
		case ddl.PGBytea:
			if len(srcType.Mods) > 0 && srcType.Mods[0] > 0 {
				return ddl.Type{Name: ddl.PGBytea, Len: srcType.Mods[0]}, nil
			}
			return ddl.Type{Name: ddl.PGBytea, Len: ddl.PGMaxLength}, nil
		default:
			if len(srcType.Mods) > 0 && srcType.Mods[0] > 0 {
				return ddl.Type{Name: ddl.PGVarchar, Len: srcType.Mods[0]}, nil
			}
			return ddl.Type{Name: ddl.PGVarchar, Len: ddl.PGMaxLength}, nil
		}
	case "varchar", "char", "nvarchar", "nchar":
		switch spType {
		case ddl.PGBytea:
			if len(srcType.Mods) > 0 && srcType.Mods[0] > 0 {
				return ddl.Type{Name: ddl.PGBytea, Len: srcType.Mods[0]}, nil
			}
			return ddl.Type{Name: ddl.PGBytea, Len: ddl.PGMaxLength}, nil
		default:
			// Sets the source length only if it falls within the allowed length range in Spanner.
			if len(srcType.Mods) > 0 && srcType.Mods[0] > 0 && srcType.Mods[0] <= ddl.PGMaxLength {
				return ddl.Type{Name: ddl.PGVarchar, Len: srcType.Mods[0]}, nil
			}
			// Raises warning and sets length to MAX when -
			// Source length is greater than maximum allowed length
			// -OR-
			// Source length is "-1" which represents MAX in SQL Server
			if len(srcType.Mods) > 0 && (srcType.Mods[0] > ddl.PGMaxLength || srcType.Mods[0] < 0) {
				return ddl.Type{Name: ddl.PGVarchar, Len: ddl.PGMaxLength}, []internal.SchemaIssue{internal.StringOverflow}
			}
			return ddl.Type{Name: ddl.PGVarchar, Len: ddl.PGMaxLength}, nil
		}
	case "ntext", "text", "xml":
		switch spType {
		case ddl.PGBytea:
			return ddl.Type{Name: ddl.PGBytea, Len: ddl.PGMaxLength}, nil
		default:
			return ddl.Type{Name: ddl.PGVarchar, Len: ddl.PGMaxLength}, nil
		}

	case "binary", "varbinary", "image":
		switch spType {
		case ddl.PGVarchar:
			return ddl.Type{Name: ddl.PGVarchar, Len: ddl.PGMaxLength}, nil
		default:
			return ddl.Type{Name: ddl.PGBytea, Len: ddl.PGMaxLength}, nil
		}
	case "date":
		switch spType {
		case ddl.PGVarchar:
			return ddl.Type{Name: ddl.PGVarchar, Len: ddl.PGMaxLength}, []internal.SchemaIssue{internal.Widened}
		default:
			return ddl.Type{Name: ddl.PGDate}, nil
		}
	case "datetime2", "datetime", "datetimeoffset", "smalldatetime", "rowversion":
		switch spType {
		case ddl.PGVarchar:
			return ddl.Type{Name: ddl.PGVarchar, Len: ddl.PGMaxLength}, []internal.SchemaIssue{internal.Widened}
		default:
			return ddl.Type{Name: ddl.PGTimestamptz}, []internal.SchemaIssue{internal.Timestamp}
		}
	case "timestamp":
		switch spType {
		case ddl.PGVarchar:
			return ddl.Type{Name: ddl.PGVarchar, Len: ddl.PGMaxLength}, []internal.SchemaIssue{internal.Widened}
		default:
			return ddl.Type{Name: ddl.PGInt8}, nil
		}
	case "time":
		return ddl.Type{Name: ddl.PGVarchar, Len: ddl.PGMaxLength}, []internal.SchemaIssue{internal.Time}
	}
	return ddl.Type{Name: ddl.PGVarchar, Len: ddl.PGMaxLength}, []internal.SchemaIssue{internal.NoGoodType}
}

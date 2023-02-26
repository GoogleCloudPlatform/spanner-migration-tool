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
package postgres

import (
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/schema"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
)

// ToDdlImpl Postgres specific implementation for ToDdl.
type ToDdlImpl struct {
}

// ToSpannerGSQLDialectType maps a scalar source schema type (defined by id and
// mods) into a Spanner GOOGLE STANDARD SQL dialect type. ToSpannerGSQLDialectType returns the
// Spanner type and a list of type conversion issues encountered.
func (tdi ToDdlImpl) ToSpannerGSQLDialectType(conv *internal.Conv, spType string, srcType schema.Type) (ddl.Type, []internal.SchemaIssue) {
	ty, issues := ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.NoGoodType}
	switch srcType.Name {
	case "bool", "boolean":
		switch spType {
		case ddl.String:
			ty, issues = ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		case ddl.Int64:
			ty, issues = ddl.Type{Name: ddl.Int64}, []internal.SchemaIssue{internal.Widened}
		default:
			ty, issues = ddl.Type{Name: ddl.Bool}, nil
		}
	case "bigserial":
		switch spType {
		case ddl.String:
			ty, issues = ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened, internal.Serial}
		default:
			ty, issues = ddl.Type{Name: ddl.Int64}, []internal.SchemaIssue{internal.Serial}
		}
	case "bpchar", "character": // Note: Postgres internal name for char is bpchar (aka blank padded char).
		switch spType {
		case ddl.Bytes:
			if len(srcType.Mods) > 0 {
				ty, issues = ddl.Type{Name: ddl.Bytes, Len: srcType.Mods[0]}, nil
			} else {
				ty, issues = ddl.Type{Name: ddl.Bytes, Len: 1}, nil
			}
		default:
			if len(srcType.Mods) > 0 {
				ty, issues = ddl.Type{Name: ddl.String, Len: srcType.Mods[0]}, nil
			} else {
				// Note: bpchar without length specifier is equivalent to bpchar(1)
				ty, issues = ddl.Type{Name: ddl.String, Len: 1}, nil
			}
		}
	case "bytea":
		switch spType {
		case ddl.String:
			ty, issues = ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
		default:
			ty, issues = ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}, nil
		}
	case "date":
		switch spType {
		case ddl.String:
			ty, issues = ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		default:
			ty, issues = ddl.Type{Name: ddl.Date}, nil
		}
	case "float8", "double precision":
		switch spType {
		case ddl.String:
			ty, issues = ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		default:
			ty, issues = ddl.Type{Name: ddl.Float64}, nil
		}
	case "float4", "real":
		switch spType {
		case ddl.String:
			ty, issues = ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		default:
			ty, issues = ddl.Type{Name: ddl.Float64}, []internal.SchemaIssue{internal.Widened}
		}
	case "int8", "bigint":
		switch spType {
		case ddl.String:
			ty, issues = ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		default:
			ty, issues = ddl.Type{Name: ddl.Int64}, nil
		}
	case "int4", "integer":
		switch spType {
		case ddl.String:
			ty, issues = ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		default:
			ty, issues = ddl.Type{Name: ddl.Int64}, []internal.SchemaIssue{internal.Widened}
		}
	case "int2", "smallint":
		switch spType {
		case ddl.String:
			ty, issues = ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		default:
			ty, issues = ddl.Type{Name: ddl.Int64}, []internal.SchemaIssue{internal.Widened}
		}
	case "numeric":
		switch spType {
		case ddl.String:
			ty, issues = ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		default:
			// TODO: check mod[0] and mod[1] and generate a warning
			// if this numeric won't fit in Spanner's NUMERIC.
			ty, issues = ddl.Type{Name: ddl.Numeric}, nil
		}
	case "serial":
		switch spType {
		case ddl.String:
			ty, issues = ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened, internal.Serial}
		default:
			ty, issues = ddl.Type{Name: ddl.Int64}, []internal.SchemaIssue{internal.Serial}
		}
	case "text":
		switch spType {
		case ddl.Bytes:
			ty, issues = ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}, nil
		default:
			ty, issues = ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
		}
	case "timestamptz", "timestamp with time zone":
		switch spType {
		case ddl.String:
			ty, issues = ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		default:
			ty, issues = ddl.Type{Name: ddl.Timestamp}, nil
		}
	case "timestamp", "timestamp without time zone":
		// Map timestamp without timezone to Spanner timestamp.
		switch spType {
		case ddl.String:
			ty, issues = ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		default:
			ty, issues = ddl.Type{Name: ddl.Timestamp}, []internal.SchemaIssue{internal.Timestamp}
		}
	case "json", "jsonb":
		switch spType {
		case ddl.String:
			ty, issues = ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
		default:
			ty, issues = ddl.Type{Name: ddl.JSON}, nil
		}
	case "varchar", "character varying":
		switch spType {
		case ddl.Bytes:
			if len(srcType.Mods) > 0 {
				ty, issues = ddl.Type{Name: ddl.Bytes, Len: srcType.Mods[0]}, nil
			} else {
				ty, issues = ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}, nil
			}
		default:
			if len(srcType.Mods) > 0 {
				ty, issues = ddl.Type{Name: ddl.String, Len: srcType.Mods[0]}, nil
			} else {
				ty, issues = ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
			}
		}
	}
	if len(srcType.ArrayBounds) > 1 {
		ty = ddl.Type{Name: ddl.String, Len: ddl.MaxLength}
		issues = append(issues, internal.MultiDimensionalArray)
	}
	ty.IsArray = len(srcType.ArrayBounds) == 1
	return ty, issues
}

// ToSpannerPostgreSQLDialectType maps a scalar source schema type (defined by id and
// mods) into a Spanner PostgreSQL dialect type. ToSpannerPostgreSQLDialectType returns the
// Spanner type and a list of type conversion issues encountered.
func (tdi ToDdlImpl) ToSpannerPostgreSQLDialectType(conv *internal.Conv, spType string, srcType schema.Type) (ddl.Type, []internal.SchemaIssue) {
	if len(srcType.ArrayBounds) > 0 {
		return ddl.Type{Name: ddl.PGVarchar, Len: ddl.PGMaxLength}, nil
	}
	ty, issues := ddl.Type{Name: ddl.PGVarchar, Len: ddl.PGMaxLength}, []internal.SchemaIssue{internal.NoGoodType}
	switch srcType.Name {
	case "bool", "boolean":
		switch spType {
		case ddl.PGVarchar:
			ty, issues = ddl.Type{Name: ddl.PGVarchar, Len: ddl.PGMaxLength}, []internal.SchemaIssue{internal.Widened}
		case ddl.PGInt8:
			ty, issues = ddl.Type{Name: ddl.PGInt8}, []internal.SchemaIssue{internal.Widened}
		default:
			ty, issues = ddl.Type{Name: ddl.PGBool}, nil
		}
	case "bigserial":
		switch spType {
		case ddl.PGVarchar:
			ty, issues = ddl.Type{Name: ddl.PGVarchar, Len: ddl.PGMaxLength}, []internal.SchemaIssue{internal.Widened, internal.Serial}
		default:
			ty, issues = ddl.Type{Name: ddl.PGInt8}, []internal.SchemaIssue{internal.Serial}
		}
	case "bpchar", "character": // Note: Postgres internal name for char is bpchar (aka blank padded char).
		switch spType {
		case ddl.PGBytea:
			if len(srcType.Mods) > 0 {
				ty, issues = ddl.Type{Name: ddl.PGBytea, Len: srcType.Mods[0]}, nil
			} else {
				ty, issues = ddl.Type{Name: ddl.PGBytea, Len: 1}, nil
			}
		default:
			if len(srcType.Mods) > 0 {
				ty, issues = ddl.Type{Name: ddl.PGVarchar, Len: srcType.Mods[0]}, nil
			} else {
				// Note: bpchar without length specifier is equivalent to bpchar(1)
				ty, issues = ddl.Type{Name: ddl.PGVarchar, Len: 1}, nil
			}
		}
	case "bytea":
		switch spType {
		case ddl.PGVarchar:
			ty, issues = ddl.Type{Name: ddl.PGVarchar, Len: ddl.PGMaxLength}, nil
		default:
			ty, issues = ddl.Type{Name: ddl.PGBytea, Len: ddl.PGMaxLength}, nil
		}
	case "date":
		switch spType {
		case ddl.PGVarchar:
			ty, issues = ddl.Type{Name: ddl.PGVarchar, Len: ddl.PGMaxLength}, []internal.SchemaIssue{internal.Widened}
		default:
			ty, issues = ddl.Type{Name: ddl.PGDate}, nil
		}
	case "float8", "double precision":
		switch spType {
		case ddl.PGVarchar:
			ty, issues = ddl.Type{Name: ddl.PGVarchar, Len: ddl.PGMaxLength}, []internal.SchemaIssue{internal.Widened}
		default:
			ty, issues = ddl.Type{Name: ddl.PGFloat8}, nil
		}
	case "float4", "real":
		switch spType {
		case ddl.PGVarchar:
			ty, issues = ddl.Type{Name: ddl.PGVarchar, Len: ddl.PGMaxLength}, []internal.SchemaIssue{internal.Widened}
		default:
			ty, issues = ddl.Type{Name: ddl.PGFloat8}, []internal.SchemaIssue{internal.Widened}
		}
	case "int8", "bigint":
		switch spType {
		case ddl.PGVarchar:
			ty, issues = ddl.Type{Name: ddl.PGVarchar, Len: ddl.PGMaxLength}, []internal.SchemaIssue{internal.Widened}
		default:
			ty, issues = ddl.Type{Name: ddl.PGInt8}, nil
		}
	case "int4", "integer":
		switch spType {
		case ddl.PGVarchar:
			ty, issues = ddl.Type{Name: ddl.PGVarchar, Len: ddl.PGMaxLength}, []internal.SchemaIssue{internal.Widened}
		default:
			ty, issues = ddl.Type{Name: ddl.PGInt8}, []internal.SchemaIssue{internal.Widened}
		}
	case "int2", "smallint":
		switch spType {
		case ddl.PGVarchar:
			ty, issues = ddl.Type{Name: ddl.PGVarchar, Len: ddl.PGMaxLength}, []internal.SchemaIssue{internal.Widened}
		default:
			ty, issues = ddl.Type{Name: ddl.PGInt8}, []internal.SchemaIssue{internal.Widened}
		}
	case "numeric":
		switch spType {
		case ddl.PGVarchar:
			ty, issues = ddl.Type{Name: ddl.PGVarchar, Len: ddl.PGMaxLength}, []internal.SchemaIssue{internal.Widened}
		default:
			// TODO: check mod[0] and mod[1] and generate a warning
			// if this numeric won't fit in Spanner's NUMERIC.
			ty, issues = ddl.Type{Name: ddl.PGNumeric}, nil
		}
	case "serial":
		switch spType {
		case ddl.PGVarchar:
			ty, issues = ddl.Type{Name: ddl.PGVarchar, Len: ddl.PGMaxLength}, []internal.SchemaIssue{internal.Widened, internal.Serial}
		default:
			ty, issues = ddl.Type{Name: ddl.PGInt8}, []internal.SchemaIssue{internal.Serial}
		}
	case "text":
		switch spType {
		case ddl.PGBytea:
			ty, issues = ddl.Type{Name: ddl.PGBytea, Len: ddl.PGMaxLength}, nil
		default:
			ty, issues = ddl.Type{Name: ddl.PGVarchar, Len: ddl.PGMaxLength}, nil
		}
	case "timestamptz", "timestamp with time zone":
		switch spType {
		case ddl.PGVarchar:
			ty, issues = ddl.Type{Name: ddl.PGVarchar, Len: ddl.PGMaxLength}, []internal.SchemaIssue{internal.Widened}
		default:
			ty, issues = ddl.Type{Name: ddl.PGTimestamptz}, nil
		}
	case "timestamp", "timestamp without time zone":
		// Map timestamp without timezone to Spanner timestamp.
		switch spType {
		case ddl.PGVarchar:
			ty, issues = ddl.Type{Name: ddl.PGVarchar, Len: ddl.PGMaxLength}, []internal.SchemaIssue{internal.Widened}
		default:
			ty, issues = ddl.Type{Name: ddl.PGTimestamptz}, []internal.SchemaIssue{internal.Timestamp}
		}
	case "json", "jsonb":
		switch spType {
		case ddl.PGVarchar:
			ty, issues = ddl.Type{Name: ddl.PGVarchar, Len: ddl.PGMaxLength}, nil
		default:
			ty, issues = ddl.Type{Name: ddl.PGJSONB}, nil
		}
	case "varchar", "character varying":
		switch spType {
		case ddl.PGBytea:
			if len(srcType.Mods) > 0 {
				ty, issues = ddl.Type{Name: ddl.PGBytea, Len: srcType.Mods[0]}, nil
			} else {
				ty, issues = ddl.Type{Name: ddl.PGBytea, Len: ddl.PGMaxLength}, nil
			}
		default:
			if len(srcType.Mods) > 0 {
				ty, issues = ddl.Type{Name: ddl.PGVarchar, Len: srcType.Mods[0]}, nil
			} else {
				ty, issues = ddl.Type{Name: ddl.PGVarchar, Len: ddl.PGMaxLength}, nil
			}
		}
	}
	return ty, issues
}

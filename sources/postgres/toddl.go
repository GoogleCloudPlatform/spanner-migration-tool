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
	ty, issues := toSpannerTypeInternal(columnType.Name, "", columnType.Mods)
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

func ToSpannerTypeWeb(srcType string, spType string, mods []int64) (ddl.Type, []internal.SchemaIssue) {
	return toSpannerTypeInternal(srcType, spType, mods)
}

// toSpannerTypeInternal defines the mapping of source types into Spanner
// types. Each source type has a default Spanner type, as well as other potential
// Spanner types it could map to. When calling toSpannerTypeInternal, you specify
// the source type name (along with any modifiers), and optionally you specify
// a target Spanner type name (empty string if you don't have one). If the target
// Spanner type name is specified and is a potential mapping for this source type,
// then it will be used to build the returned ddl.Type. If not, the default
// Spanner type for this source type will be used.
func toSpannerTypeInternal(srcType string, spType string, mods []int64) (ddl.Type, []internal.SchemaIssue) {
	switch srcType {
	case "bool", "boolean":
		switch spType {
		case ddl.String:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		case ddl.Int64:
			return ddl.Type{Name: ddl.Int64}, []internal.SchemaIssue{internal.Widened}
		default:
			return ddl.Type{Name: ddl.Bool}, nil
		}
	case "bigserial":
		switch spType {
		case ddl.String:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened, internal.Serial}
		default:
			return ddl.Type{Name: ddl.Int64}, []internal.SchemaIssue{internal.Serial}
		}
	case "bpchar", "character": // Note: Postgres internal name for char is bpchar (aka blank padded char).
		switch spType {
		case ddl.Bytes:
			if len(mods) > 0 {
				return ddl.Type{Name: ddl.Bytes, Len: mods[0]}, nil
			}
			return ddl.Type{Name: ddl.Bytes, Len: 1}, nil
		default:
			if len(mods) > 0 {
				return ddl.Type{Name: ddl.String, Len: mods[0]}, nil
			}
			// Note: bpchar without length specifier is equivalent to bpchar(1)
			return ddl.Type{Name: ddl.String, Len: 1}, nil
		}
	case "bytea":
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
	case "float8", "double precision":
		switch spType {
		case ddl.String:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		default:
			return ddl.Type{Name: ddl.Float64}, nil
		}
	case "float4", "real":
		switch spType {
		case ddl.String:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		default:
			return ddl.Type{Name: ddl.Float64}, []internal.SchemaIssue{internal.Widened}
		}
	case "int8", "bigint":
		switch spType {
		case ddl.String:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		default:
			return ddl.Type{Name: ddl.Int64}, nil
		}
	case "int4", "integer":
		switch spType {
		case ddl.String:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		default:
			return ddl.Type{Name: ddl.Int64}, []internal.SchemaIssue{internal.Widened}
		}
	case "int2", "smallint":
		switch spType {
		case ddl.String:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		default:
			return ddl.Type{Name: ddl.Int64}, []internal.SchemaIssue{internal.Widened}
		}
	case "numeric":
		switch spType {
		case ddl.String:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		default:
			// TODO: check mod[0] and mod[1] and generate a warning
			// if this numeric won't fit in Spanner's NUMERIC.
			return ddl.Type{Name: ddl.Numeric}, nil
		}
	case "serial":
		switch spType {
		case ddl.String:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened, internal.Serial}
		default:
			return ddl.Type{Name: ddl.Int64}, []internal.SchemaIssue{internal.Serial}
		}
	case "text":
		switch spType {
		case ddl.Bytes:
			return ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}, nil
		default:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
		}
	case "timestamptz", "timestamp with time zone":
		switch spType {
		case ddl.String:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		default:
			return ddl.Type{Name: ddl.Timestamp}, nil
		}
	case "timestamp", "timestamp without time zone":
		// Map timestamp without timezone to Spanner timestamp.
		switch spType {
		case ddl.String:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		default:
			return ddl.Type{Name: ddl.Timestamp}, []internal.SchemaIssue{internal.Timestamp}
		}
	case "json", "jsonb":
		switch spType {
		case ddl.String:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
		default:
			return ddl.Type{Name: ddl.JSON}, nil
		}
	case "varchar", "character varying":
		switch spType {
		case ddl.Bytes:
			if len(mods) > 0 {
				return ddl.Type{Name: ddl.Bytes, Len: mods[0]}, nil
			}
			return ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}, nil
		default:
			if len(mods) > 0 {
				return ddl.Type{Name: ddl.String, Len: mods[0]}, nil
			}
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
		}
	}
	return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.NoGoodType}
}

// toSpannerType maps a scalar source schema type (defined by id and
// mods) into a Spanner type. This is the core source-to-Spanner type
// mapping.  toSpannerType returns the Spanner type and a list of type
// conversion issues encountered.
func toSpannerTypeIntern(conv *internal.Conv, id string, mods []int64) (ddl.Type, []internal.SchemaIssue) {
	switch id {
	case "bool", "boolean":
		return ddl.Type{Name: ddl.Bool}, nil
	case "bigserial":
		return ddl.Type{Name: ddl.Int64}, []internal.SchemaIssue{internal.Serial}
	case "bpchar", "character": // Note: Postgres internal name for char is bpchar (aka blank padded char).
		if len(mods) > 0 {
			return ddl.Type{Name: ddl.String, Len: mods[0]}, nil
		}
		// Note: bpchar without length specifier is equivalent to bpchar(1)
		return ddl.Type{Name: ddl.String, Len: 1}, nil
	case "bytea":
		return ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}, nil
	case "date":
		return ddl.Type{Name: ddl.Date}, nil
	case "float8", "double precision":
		return ddl.Type{Name: ddl.Float64}, nil
	case "float4", "real":
		return ddl.Type{Name: ddl.Float64}, []internal.SchemaIssue{internal.Widened}
	case "int8", "bigint":
		return ddl.Type{Name: ddl.Int64}, nil
	case "int4", "integer":
		return ddl.Type{Name: ddl.Int64}, []internal.SchemaIssue{internal.Widened}
	case "int2", "smallint":
		return ddl.Type{Name: ddl.Int64}, []internal.SchemaIssue{internal.Widened}
	case "numeric":
		// PostgreSQL's NUMERIC type can have a specified precision of up to 1000
		// digits (and scale can be anything from 0 up to the value of 'precision').
		// If precision and scale are not specified, then values of any precision
		// or scale can be stored, up to the implementation's limits (can be up to
		// 131072 digits before the decimal point and up to 16383 digits after
		// the decimal point).
		// Spanner's NUMERIC type can store up to 29 digits before the
		// decimal point and up to 9 after the decimal point -- it is
		// equivalent to PostgreSQL's NUMERIC(38,9) type.
		//
		// TODO: Generate appropriate SchemaIssue to warn of different precision
		// capabilities between PostgreSQL and Spanner NUMERIC.
		return ddl.Type{Name: ddl.Numeric}, nil
	case "serial":
		return ddl.Type{Name: ddl.Int64}, []internal.SchemaIssue{internal.Serial}
	case "text":
		return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
	case "timestamptz", "timestamp with time zone":
		return ddl.Type{Name: ddl.Timestamp}, nil
	case "timestamp", "timestamp without time zone":
		// Map timestamp without timezone to Spanner timestamp.
		return ddl.Type{Name: ddl.Timestamp}, []internal.SchemaIssue{internal.Timestamp}
	case "varchar", "character varying":
		if len(mods) > 0 {
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
	} else if columnType.Name == "json" || columnType.Name == "jsonb" {
		return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}
	}
	return originalType
}

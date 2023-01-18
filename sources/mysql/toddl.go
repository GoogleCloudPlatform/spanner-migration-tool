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

// Package MySQL handles schema and data migrations from MySQL.
package mysql

import (
	"github.com/cloudspannerecosystem/harbourbridge/common/constants"
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/schema"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
)

// ToDdlImpl MySQL specific implementation for ToDdl.
type ToDdlImpl struct {
}

// ToSpannerType maps a scalar source schema type (defined by id and
// mods) into a Spanner type. This is the core source-to-Spanner type
// mapping.  toSpannerType returns the Spanner type and a list of type
// conversion issues encountered.
// Functions below implement the common.ToDdl interface
func (tdi ToDdlImpl) ToSpannerType(conv *internal.Conv, spType string, srcType schema.Type) (ddl.Type, []internal.SchemaIssue) {
	ty, issues := toSpannerTypeInternal(srcType, spType)
	if conv.TargetDb == constants.TargetExperimentalPostgres {
		ty = overrideExperimentalType(srcType, ty)
	} else {
		if len(srcType.ArrayBounds) > 1 {
			ty = ddl.Type{Name: ddl.String, Len: ddl.MaxLength}
			issues = append(issues, internal.MultiDimensionalArray)
		}
		ty.IsArray = len(srcType.ArrayBounds) == 1
	}
	return ty, issues
}

func toSpannerTypeInternal(srcType schema.Type, spType string) (ddl.Type, []internal.SchemaIssue) {
	switch srcType.Name {
	case "bool", "boolean":
		switch spType {
		case ddl.String:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		case ddl.Int64:
			return ddl.Type{Name: ddl.Int64}, []internal.SchemaIssue{internal.Widened}
		default:
			return ddl.Type{Name: ddl.Bool}, nil
		}
	case "tinyint":
		switch spType {
		case ddl.String:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		case ddl.Int64:
			return ddl.Type{Name: ddl.Int64}, []internal.SchemaIssue{internal.Widened}
		default:
			// tinyint(1) is a bool in MySQL
			if len(srcType.Mods) > 0 && srcType.Mods[0] == 1 {
				return ddl.Type{Name: ddl.Bool}, nil
			}
			return ddl.Type{Name: ddl.Int64}, []internal.SchemaIssue{internal.Widened}
		}
	case "double":
		switch spType {
		case ddl.String:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		default:
			return ddl.Type{Name: ddl.Float64}, nil
		}
	case "float":
		switch spType {
		case ddl.String:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		default:
			return ddl.Type{Name: ddl.Float64}, []internal.SchemaIssue{internal.Widened}
		}
	case "numeric", "decimal":
		switch spType {
		case ddl.String:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		default:
			// MySQL's NUMERIC type can store up to 65 digits, with up to 30 after the
			// the decimal point. Spanner's NUMERIC type can store up to 29 digits before the
			// decimal point and up to 9 after the decimal point -- it is equivalent to
			// MySQL's NUMERIC(38,9) type.
			//
			// TODO: Generate appropriate SchemaIssue to warn of different precision
			// capabilities between MySQL and Spanner NUMERIC.
			return ddl.Type{Name: ddl.Numeric}, nil
		}
	case "bigint":
		switch spType {
		case ddl.String:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		default:
			return ddl.Type{Name: ddl.Int64}, nil
		}
	case "smallint", "mediumint", "integer", "int":
		switch spType {
		case ddl.String:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		default:
			return ddl.Type{Name: ddl.Int64}, []internal.SchemaIssue{internal.Widened}
		}
	case "bit":
		switch spType {
		case ddl.String:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
		default:
			return ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}, nil
		}
	case "varchar", "char":
		switch spType {
		case ddl.Bytes:
			if len(srcType.Mods) > 0 {
				return ddl.Type{Name: ddl.Bytes, Len: srcType.Mods[0]}, nil
			}
			return ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}, nil
		default:
			if len(srcType.Mods) > 0 {
				return ddl.Type{Name: ddl.String, Len: srcType.Mods[0]}, nil
			}
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
		}
	case "text", "tinytext", "mediumtext", "longtext":
		switch spType {
		case ddl.Bytes:
			return ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}, nil
		default:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
		}
	case "set", "enum":
		return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
	case "json":
		switch spType {
		case ddl.Bytes:
			return ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}, nil
		default:
			return ddl.Type{Name: ddl.JSON}, nil
		}
	case "binary", "varbinary":
		switch spType {
		case ddl.String:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
		default:
			return ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}, nil
		}
	case "tinyblob", "mediumblob", "blob", "longblob":
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
	case "datetime":
		switch spType {
		case ddl.String:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		default:
			return ddl.Type{Name: ddl.Timestamp}, []internal.SchemaIssue{internal.Datetime}
		}
	case "timestamp":
		switch spType {
		case ddl.String:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		default:
			return ddl.Type{Name: ddl.Timestamp}, nil
		}
	case "time", "year":
		return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Time}

	}
	return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.NoGoodType}
}

func overrideExperimentalTypeWeb(columnType string, originalType ddl.Type) ddl.Type {
	if columnType == "json" {
		return ddl.Type{Name: ddl.JSONB}
	}
	return originalType
}

// Override the types to map to experimental postgres types.
func overrideExperimentalType(srcType schema.Type, originalType ddl.Type) ddl.Type {
	if len(srcType.ArrayBounds) > 0 {
		return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}
	}
	return overrideExperimentalTypeWeb(columnType.Name, originalType)
}

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

// Package oracle handles schema and data migrations from oracle.
package oracle

import (
	"github.com/cloudspannerecosystem/harbourbridge/common/constants"
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
	ty, issues := toSpannerTypeInternal(conv, columnType.Name, columnType.Mods)
	if conv.TargetDb == constants.TargetExperimentalPostgres {
		ty = overrideExperimentalType(columnType, ty)
	}
	return ty, issues
}

func toSpannerTypeInternal(conv *internal.Conv, id string, mods []int64) (ddl.Type, []internal.SchemaIssue) {
	switch id {
	case "number":
		if len(mods) == 1 && mods[0] >= 1 && mods[0] < 19 {
			return ddl.Type{Name: ddl.Int64}, nil
		} else {
			return ddl.Type{Name: ddl.Numeric}, nil
		}
	case "bfile", "blob":
		return ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}, nil
	case "char", "charater":
		return ddl.Type{Name: ddl.String, Len: mods[0]}, nil
	case "clob":
		return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
	case "date", "datetime", "timestampwithtimezone":
		return ddl.Type{Name: ddl.Timestamp}, nil
	case "decimal", "dec", "smallint":
		return ddl.Type{Name: ddl.Numeric}, nil
	case "double", "float", "real":
		return ddl.Type{Name: ddl.Float64}, nil
	case "integer", "int":
		return ddl.Type{Name: ddl.Int64}, nil
	case "interval year to month", "interval day to second":
		if len(mods) > 0 {
			return ddl.Type{Name: ddl.String, Len: 30}, nil
		}
		return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
	case "long":
		return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
	case "longraw":
		return ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}, nil
	case "nchar", "ncharvarying", "nvarchar2", "varchar", "varchar2":
		if len(mods) > 0 {
			return ddl.Type{Name: ddl.String, Len: mods[0]}, nil
		}
		return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
	case "nclob":
		return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
	case "numeric":
		return ddl.Type{Name: ddl.Numeric}, nil
	case "rowid":
		return ddl.Type{Name: ddl.String, Len: 10}, nil
	case "urowid":
		if len(mods) > 0 {
			return ddl.Type{Name: ddl.String, Len: mods[0]}, nil
		}
		return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
	case "xmltype":
		return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
	default:
		return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.NoGoodType}
	}
}

// Override the types to map to experimental postgres types.
func overrideExperimentalType(columnType schema.Type, originalType ddl.Type) ddl.Type {
	if columnType.Name == "date" {
		return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}
	}
	return originalType
}

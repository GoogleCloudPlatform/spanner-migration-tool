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
	case "NUMBER":
		if len(mods) == 1 && mods[0] >= 1 && mods[0] < 19 {
			return ddl.Type{Name: ddl.Int64}, nil
		} else {
			return ddl.Type{Name: ddl.Numeric}, nil
		}
	case "BFILE", "BLOB":
		return ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}, nil
	case "CHAR", "CHARATER":
		return ddl.Type{Name: ddl.String, Len: mods[0]}, nil
	case "CLOB":
		return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
	case "DATE", "DATETIME", "TIMESTAMPWITHTIMEZONE", "TIMESTAMP":
		return ddl.Type{Name: ddl.Timestamp}, nil
	case "DECIMAL", "DEC", "SMALLINT":
		return ddl.Type{Name: ddl.Numeric}, nil
	case "BINARY_DOUBLE", "BINARY_FLOAT", "DOUBLE", "FLOAT", "REAL":
		return ddl.Type{Name: ddl.Float64}, nil
	case "INTEGER", "INT":
		return ddl.Type{Name: ddl.Int64}, nil
	case "INTERVAL YEAR", "INTERVAL DAY":
		if len(mods) > 0 {
			return ddl.Type{Name: ddl.String, Len: 30}, nil
		}
		return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
	case "LONG":
		return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
	case "RAW", "LONGRAW":
		return ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}, nil
	case "NCHAR", "NCHARVARYING", "NVARCHAR2", "VARCHAR", "VARCHAR2":
		if len(mods) > 0 {
			return ddl.Type{Name: ddl.String, Len: mods[0]}, nil
		}
		return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
	case "NCLOB":
		return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
	case "ROWID":
		return ddl.Type{Name: ddl.String, Len: 10}, nil
	case "UROWID":
		if len(mods) > 0 {
			return ddl.Type{Name: ddl.String, Len: mods[0]}, nil
		}
		return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
	case "XMLTYPE":
		return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
	default:
		return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.NoGoodType}
	}
}

// Override the types to map to experimental postgres types.
func overrideExperimentalType(columnType schema.Type, originalType ddl.Type) ddl.Type {
	if columnType.Name == "DATE" {
		return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}
	}
	return originalType
}

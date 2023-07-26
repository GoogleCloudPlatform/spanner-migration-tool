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

// Package oracle handles schema and data migrations from oracle.
package oracle

import (
	"regexp"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/schema"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
)

var (
	TimestampReg = regexp.MustCompile(`TIMESTAMP`)
	IntervalReg  = regexp.MustCompile(`INTERVAL`)
)

// ToDdlImpl oracle specific implementation for ToDdl.
type ToDdlImpl struct {
}

// ToSpannerType maps a scalar source schema type (defined by id and
// mods) into a Spanner type. This is the core source-to-Spanner type
// mapping.  toSpannerType returns the Spanner type and a list of type
// conversion issues encountered.
func (tdi ToDdlImpl) ToSpannerType(conv *internal.Conv, spType string, srcType schema.Type) (ddl.Type, []internal.SchemaIssue) {
	// passing empty spType to execute default case.will get other spType from web pkg
	ty, issues := toSpannerTypeInternal(conv, spType, srcType)
	if len(srcType.ArrayBounds) > 1 {
		ty = ddl.Type{Name: ddl.String, Len: ddl.MaxLength}
		issues = append(issues, internal.MultiDimensionalArray)
	}
	ty.IsArray = len(srcType.ArrayBounds) == 1
	return ty, issues
}

func toSpannerTypeInternal(conv *internal.Conv, spType string, srcType schema.Type) (ddl.Type, []internal.SchemaIssue) {
	// Oracle returns some datatype with the precision,
	// So will get TIMESTAMP as TIMESTAMP(6),TIMESTAMP(6) WITH TIME ZONE,TIMESTAMP(6) WITH LOCAL TIME ZONE.
	// To match this case timestampReg Regex defined.
	if TimestampReg.MatchString(srcType.Name) {
		switch spType {
		case ddl.String:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
		default:
			return ddl.Type{Name: ddl.Timestamp}, nil
		}
	}

	// Matching cases like INTERVAL YEAR(2) TO MONTH, INTERVAL DAY(2) TO SECOND(6),etc.
	if IntervalReg.MatchString(srcType.Name) {
		switch spType {
		case ddl.String:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
		default:
			if len(srcType.Mods) > 0 {
				return ddl.Type{Name: ddl.String, Len: 30}, nil
			}
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
		}
	}

	switch srcType.Name {
	case "NUMBER":
		switch spType {
		case ddl.String:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
		default:
			modsLen := len(srcType.Mods)
			if modsLen == 0 {
				return ddl.Type{Name: ddl.Numeric}, nil
			} else if modsLen == 1 { // Only precision is available.
				if srcType.Mods[0] > 29 {
					// Max precision in Oracle is 38. String representation of the number should not have more than 50 characters
					// https://docs.oracle.com/cd/B19306_01/server.102/b14237/limits001.htm#i287903
					return ddl.Type{Name: ddl.String, Len: 50}, nil
				}
				return ddl.Type{Name: ddl.Int64}, nil
			} else if srcType.Mods[0] > 29 || srcType.Mods[1] > 9 { // When both precision and scale are available and within limit
				// Max precision in Oracle is 38. String representation of the number should not have more than 50 characters
				// https://docs.oracle.com/cd/B19306_01/server.102/b14237/limits001.htm#i287903
				return ddl.Type{Name: ddl.String, Len: 50}, nil
			}

			return ddl.Type{Name: ddl.Numeric}, nil
		}

	case "BFILE", "BLOB":
		switch spType {
		case ddl.String:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
		default:
			return ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}, nil
		}
	case "CHAR":
		if len(srcType.Mods) > 0 {
			return ddl.Type{Name: ddl.String, Len: srcType.Mods[0]}, nil
		}
		return ddl.Type{Name: ddl.String}, nil
	case "CLOB":
		return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
	case "DATE":
		switch spType {
		case ddl.String:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
		default:
			return ddl.Type{Name: ddl.Date}, nil
		}
	case "BINARY_DOUBLE", "BINARY_FLOAT", "FLOAT":
		switch spType {
		case ddl.String:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
		default:
			return ddl.Type{Name: ddl.Float64}, nil
		}
	case "LONG":
		return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
	case "RAW", "LONG RAW":
		switch spType {
		case ddl.String:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
		default:
			return ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}, nil
		}
	case "NCHAR", "NVARCHAR2", "VARCHAR", "VARCHAR2":
		if len(srcType.Mods) > 0 {
			return ddl.Type{Name: ddl.String, Len: srcType.Mods[0]}, nil
		}
		return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
	case "NCLOB":
		return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
	case "ROWID":
		return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
	case "UROWID":
		if len(srcType.Mods) > 0 {
			return ddl.Type{Name: ddl.String, Len: srcType.Mods[0]}, nil
		}
		return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
	case "XMLTYPE":
		return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
	case "JSON", "OBJECT":
		return ddl.Type{Name: ddl.JSON}, nil
	default:
		return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.NoGoodType}
	}
}

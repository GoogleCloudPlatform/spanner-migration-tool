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

	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/schema"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
)

var (
	TimestampReg = regexp.MustCompile(`TIMESTAMP`)
	IntervalReg  = regexp.MustCompile(`INTERVAL`)
)

// ToDdlImpl oracle specific implementation for ToDdl.
type ToDdlImpl struct {
}

// ToSpannerGSQLDialectType maps a scalar source schema type (defined by id and
// mods) into a Spanner GOOGLE STANDARD SQL dialect type. ToSpannerGSQLDialectType
// returns the Spanner type and a list of type conversion issues encountered.
func (tdi ToDdlImpl) ToSpannerGSQLDialectType(conv *internal.Conv, spType string, srcType schema.Type) (ddl.Type, []internal.SchemaIssue) {
	ty, issues := ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.NoGoodType}

	// Oracle ty, issues =s some datatype with the precision,
	// So will get TIMESTAMP as TIMESTAMP(6),TIMESTAMP(6) WITH TIME ZONE,TIMESTAMP(6) WITH LOCAL TIME ZONE.
	// To match this case timestampReg Regex defined.
	if TimestampReg.MatchString(srcType.Name) {
		switch spType {
		case ddl.String:
			ty, issues = ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
		default:
			ty, issues = ddl.Type{Name: ddl.Timestamp}, nil
		}
	}

	// Matching cases like INTERVAL YEAR(2) TO MONTH, INTERVAL DAY(2) TO SECOND(6),etc.
	if IntervalReg.MatchString(srcType.Name) {
		switch spType {
		case ddl.String:
			ty, issues = ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
		default:
			if len(srcType.Mods) > 0 {
				ty, issues = ddl.Type{Name: ddl.String, Len: 30}, nil
			} else {
				ty, issues = ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
			}
		}
	}

	switch srcType.Name {
	case "NUMBER":
		switch spType {
		case ddl.String:
			ty, issues = ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
		default:
			modsLen := len(srcType.Mods)
			if modsLen == 0 {
				ty, issues = ddl.Type{Name: ddl.Numeric}, nil
			} else if modsLen == 1 { // Only precision is available.
				if srcType.Mods[0] > 29 {
					// Max precision in Oracle is 38. String representation of the number should not have more than 50 characters
					// https://docs.oracle.com/cd/B19306_01/server.102/b14237/limits001.htm#i287903
					ty, issues = ddl.Type{Name: ddl.String, Len: 50}, nil
				}
				ty, issues = ddl.Type{Name: ddl.Int64}, nil
			} else if srcType.Mods[0] > 29 || srcType.Mods[1] > 9 { // When both precision and scale are available and within limit
				// Max precision in Oracle is 38. String representation of the number should not have more than 50 characters
				// https://docs.oracle.com/cd/B19306_01/server.102/b14237/limits001.htm#i287903
				ty, issues = ddl.Type{Name: ddl.String, Len: 50}, nil
			} else {
				ty, issues = ddl.Type{Name: ddl.Numeric}, nil
			}
		}

	case "BFILE", "BLOB":
		switch spType {
		case ddl.String:
			ty, issues = ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
		default:
			ty, issues = ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}, nil
		}
	case "CHAR":
		if len(srcType.Mods) > 0 {
			ty, issues = ddl.Type{Name: ddl.String, Len: srcType.Mods[0]}, nil
		} else {
			ty, issues = ddl.Type{Name: ddl.String}, nil
		}
	case "CLOB":
		ty, issues = ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
	case "DATE":
		switch spType {
		case ddl.String:
			ty, issues = ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
		default:
			ty, issues = ddl.Type{Name: ddl.Date}, nil
		}
	case "BINARY_DOUBLE", "BINARY_FLOAT", "FLOAT":
		switch spType {
		case ddl.String:
			ty, issues = ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
		default:
			ty, issues = ddl.Type{Name: ddl.Float64}, nil
		}
	case "LONG":
		ty, issues = ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
	case "RAW", "LONG RAW":
		switch spType {
		case ddl.String:
			ty, issues = ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
		default:
			ty, issues = ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}, nil
		}
	case "NCHAR", "NVARCHAR2", "VARCHAR", "VARCHAR2":
		if len(srcType.Mods) > 0 {
			ty, issues = ddl.Type{Name: ddl.String, Len: srcType.Mods[0]}, nil
		} else {
			ty, issues = ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
		}
	case "NCLOB":
		ty, issues = ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
	case "ROWID":
		ty, issues = ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
	case "UROWID":
		if len(srcType.Mods) > 0 {
			ty, issues = ddl.Type{Name: ddl.String, Len: srcType.Mods[0]}, nil
		} else {
			ty, issues = ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
		}
	case "XMLTYPE":
		ty, issues = ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
	case "JSON", "OBJECT":
		ty, issues = ddl.Type{Name: ddl.JSON}, nil
	}
	if len(srcType.ArrayBounds) > 1 {
		ty = ddl.Type{Name: ddl.String, Len: ddl.MaxLength}
		issues = append(issues, internal.MultiDimensionalArray)
	}
	ty.IsArray = len(srcType.ArrayBounds) == 1
	return ty, issues
}

// ToSpannerPostgreSQLDialectType maps a scalar source schema type (defined by id and
// mods) into a Spanner PostgreSQL dialect type. ToSpannerPostgreSQLDialectType
// returns the Spanner type and a list of type conversion issues encountered.
func (tdi ToDdlImpl) ToSpannerPostgreSQLDialectType(conv *internal.Conv, spType string, srcType schema.Type) (ddl.Type, []internal.SchemaIssue) {
	ty, issues := ddl.Type{Name: ddl.PGVarchar, Len: ddl.PGMaxLength}, []internal.SchemaIssue{internal.NoGoodType}

	// Oracle ty, issues =s some datatype with the precision,
	// So will get TIMESTAMP as TIMESTAMP(6),TIMESTAMP(6) WITH TIME ZONE,TIMESTAMP(6) WITH LOCAL TIME ZONE.
	// To match this case timestampReg Regex defined.
	if TimestampReg.MatchString(srcType.Name) {
		switch spType {
		case ddl.PGVarchar:
			ty, issues = ddl.Type{Name: ddl.PGVarchar, Len: ddl.PGMaxLength}, nil
		default:
			ty, issues = ddl.Type{Name: ddl.PGTimestamptz}, nil
		}
	}

	// Matching cases like INTERVAL YEAR(2) TO MONTH, INTERVAL DAY(2) TO SECOND(6),etc.
	if IntervalReg.MatchString(srcType.Name) {
		switch spType {
		case ddl.PGVarchar:
			ty, issues = ddl.Type{Name: ddl.PGVarchar, Len: ddl.PGMaxLength}, nil
		default:
			if len(srcType.Mods) > 0 {
				ty, issues = ddl.Type{Name: ddl.PGVarchar, Len: 30}, nil
			} else {
				ty, issues = ddl.Type{Name: ddl.PGVarchar, Len: ddl.PGMaxLength}, nil
			}
		}
	}

	switch srcType.Name {
	case "NUMBER":
		switch spType {
		case ddl.PGVarchar:
			ty, issues = ddl.Type{Name: ddl.PGVarchar, Len: ddl.PGMaxLength}, nil
		default:
			modsLen := len(srcType.Mods)
			if modsLen == 0 {
				ty, issues = ddl.Type{Name: ddl.PGNumeric}, nil
			} else if modsLen == 1 { // Only precision is available.
				if srcType.Mods[0] > 29 {
					// Max precision in Oracle is 38. String representation of the number should not have more than 50 characters
					// https://docs.oracle.com/cd/B19306_01/server.102/b14237/limits001.htm#i287903
					ty, issues = ddl.Type{Name: ddl.PGVarchar, Len: 50}, nil
				}
				ty, issues = ddl.Type{Name: ddl.PGInt8}, nil
			} else if srcType.Mods[0] > 29 || srcType.Mods[1] > 9 { // When both precision and scale are available and within limit
				// Max precision in Oracle is 38. String representation of the number should not have more than 50 characters
				// https://docs.oracle.com/cd/B19306_01/server.102/b14237/limits001.htm#i287903
				ty, issues = ddl.Type{Name: ddl.PGVarchar, Len: 50}, nil
			} else {
				ty, issues = ddl.Type{Name: ddl.PGNumeric}, nil
			}
		}

	case "BFILE", "BLOB":
		switch spType {
		case ddl.PGVarchar:
			ty, issues = ddl.Type{Name: ddl.PGVarchar, Len: ddl.PGMaxLength}, nil
		default:
			ty, issues = ddl.Type{Name: ddl.PGBytea, Len: ddl.PGMaxLength}, nil
		}
	case "CHAR":
		if len(srcType.Mods) > 0 {
			ty, issues = ddl.Type{Name: ddl.PGVarchar, Len: srcType.Mods[0]}, nil
		} else {
			ty, issues = ddl.Type{Name: ddl.PGVarchar}, nil
		}
	case "CLOB":
		ty, issues = ddl.Type{Name: ddl.PGVarchar, Len: ddl.PGMaxLength}, nil
	case "DATE":
		switch spType {
		case ddl.PGVarchar:
			ty, issues = ddl.Type{Name: ddl.PGVarchar, Len: ddl.PGMaxLength}, nil
		default:
			ty, issues = ddl.Type{Name: ddl.PGDate}, nil
		}
	case "BINARY_DOUBLE", "BINARY_FLOAT", "FLOAT":
		switch spType {
		case ddl.PGVarchar:
			ty, issues = ddl.Type{Name: ddl.PGVarchar, Len: ddl.PGMaxLength}, nil
		default:
			ty, issues = ddl.Type{Name: ddl.PGFloat8}, nil
		}
	case "LONG":
		ty, issues = ddl.Type{Name: ddl.PGVarchar, Len: ddl.PGMaxLength}, nil
	case "RAW", "LONG RAW":
		switch spType {
		case ddl.PGVarchar:
			ty, issues = ddl.Type{Name: ddl.PGVarchar, Len: ddl.PGMaxLength}, nil
		default:
			ty, issues = ddl.Type{Name: ddl.PGBytea, Len: ddl.PGMaxLength}, nil
		}
	case "NCHAR", "NVARCHAR2", "VARCHAR", "VARCHAR2":
		if len(srcType.Mods) > 0 {
			ty, issues = ddl.Type{Name: ddl.PGVarchar, Len: srcType.Mods[0]}, nil
		} else {
			ty, issues = ddl.Type{Name: ddl.PGVarchar, Len: ddl.PGMaxLength}, nil
		}
	case "NCLOB":
		ty, issues = ddl.Type{Name: ddl.PGVarchar, Len: ddl.PGMaxLength}, nil
	case "ROWID":
		ty, issues = ddl.Type{Name: ddl.PGVarchar, Len: ddl.PGMaxLength}, nil
	case "UROWID":
		if len(srcType.Mods) > 0 {
			ty, issues = ddl.Type{Name: ddl.PGVarchar, Len: srcType.Mods[0]}, nil
		} else {
			ty, issues = ddl.Type{Name: ddl.PGVarchar, Len: ddl.PGMaxLength}, nil
		}
	case "XMLTYPE":
		ty, issues = ddl.Type{Name: ddl.PGVarchar, Len: ddl.PGMaxLength}, nil
	case "JSON", "OBJECT":
		ty, issues = ddl.Type{Name: ddl.PGJSONB}, nil
	}
	if len(srcType.ArrayBounds) > 1 {
		ty = ddl.Type{Name: ddl.PGVarchar, Len: ddl.PGMaxLength}
		issues = append(issues, internal.MultiDimensionalArray)
	}
	ty.IsArray = len(srcType.ArrayBounds) == 1
	return ty, issues
}

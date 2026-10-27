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

package oracle

import (
	"regexp"
	"strings"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/schema"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/common"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
)

type ToDdlImpl struct{}

func (td ToDdlImpl) ToSpannerType(conv *internal.Conv, spType string, srcType schema.Type, isPk bool) (ddl.Type, []internal.SchemaIssue) {
	ty, issues := toSpannerTypeInternal(conv, spType, srcType, isPk)
	if conv.SpDialect == constants.DIALECT_POSTGRESQL {
		var pg_issues []internal.SchemaIssue
		ty, pg_issues = common.ToPGDialectType(ty, isPk)
		issues = append(issues, pg_issues...)
	}
	return ty, issues
}

func (td ToDdlImpl) GetColumnAutoGen(conv *internal.Conv, autoGenCol ddl.AutoGenCol, colId string, tableId string) (*ddl.AutoGenCol, error) {
	if autoGenCol.GenerationType == "AUTO_INCREMENT" {
		autoGen := &ddl.AutoGenCol{
			GenerationType: "SEQUENCE",
		}
		return autoGen, nil
	}
	return nil, nil
}

func toSpannerTypeInternal(conv *internal.Conv, spType string, srcType schema.Type, isPk bool) (ddl.Type, []internal.SchemaIssue) {
	ty, issues := toSpannerType(conv, spType, srcType.Name, srcType.Mods, isPk)
	return ty, issues
}

const (
	// maxInt64SafeDigits is the largest number of integer digits that always fits
	// in INT64 (max 9223372036854775807 has 19 digits, but not every 19 digit
	// value fits).
	maxInt64SafeDigits = 18
	// maxNumericIntDigits and maxNumericScale describe GoogleSQL NUMERIC, which
	// holds 29 digits before and 9 digits after the decimal point.
	maxNumericIntDigits = 29
	maxNumericScale     = 9
	// maxFloat64BinaryPrecision is the number of mantissa bits of FLOAT64. Oracle
	// FLOAT(p) precision is expressed in binary digits.
	maxFloat64BinaryPrecision = 53
)

// numberFit describes which Spanner type can hold every value of an Oracle NUMBER(p,s).
type numberFit int

const (
	numberFitsInt64 numberFit = iota
	numberFitsNumeric
	numberExceedsNumeric
)

// classifyNumber classifies an Oracle NUMBER based on its precision and scale
// (mods). A NUMBER without precision (NUMBER, NUMBER(*,s)) can hold up to 38
// significant digits in the range 1e-130 to 1e125, which exceeds GoogleSQL NUMERIC.
func classifyNumber(mods []int64) numberFit {
	if len(mods) == 0 {
		return numberExceedsNumeric
	}
	precision, scale := mods[0], int64(0)
	if len(mods) > 1 {
		scale = mods[1]
	}
	intDigits := precision - scale
	switch {
	case scale <= 0 && intDigits <= maxInt64SafeDigits:
		return numberFitsInt64
	case scale <= maxNumericScale && intDigits <= maxNumericIntDigits:
		return numberFitsNumeric
	default:
		return numberExceedsNumeric
	}
}

// numericOverflowIssues returns the Numeric issue when the source values may not
// fit in NUMERIC. PostgreSQL dialect NUMERIC has arbitrary precision, so it is
// only reported for the GoogleSQL dialect.
func numericOverflowIssues(conv *internal.Conv, fit numberFit) []internal.SchemaIssue {
	if fit == numberExceedsNumeric && conv.SpDialect != constants.DIALECT_POSTGRESQL {
		return []internal.SchemaIssue{internal.Numeric}
	}
	return nil
}

// numberToSpannerType maps Oracle NUMBER(p,s) (and its ANSI aliases NUMERIC,
// DECIMAL, DEC) based on precision and scale:
//   - integers with at most 18 digits map to INT64.
//   - values that fit NUMERIC (29 integer digits, 9 fractional digits) map to NUMERIC.
//   - everything else maps to NUMERIC with a warning in the GoogleSQL dialect.
//
// TODO: Foreign keys between NUMBER columns of different precision can map to
// different types (e.g. INT64 -> NUMERIC); Spanner rejects them and they are skipped.
// Reconcile FK column types during schema conversion.
func numberToSpannerType(conv *internal.Conv, spType string, mods []int64) (ddl.Type, []internal.SchemaIssue) {
	fit := classifyNumber(mods)
	switch spType {
	case ddl.Float64:
		return ddl.Type{Name: ddl.Float64}, []internal.SchemaIssue{internal.Widened}
	case ddl.String:
		return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
	case ddl.Int64:
		if fit == numberFitsInt64 {
			return ddl.Type{Name: ddl.Int64}, nil
		}
		return ddl.Type{Name: ddl.Int64}, []internal.SchemaIssue{internal.PossibleOverflow}
	case ddl.Numeric:
		return ddl.Type{Name: ddl.Numeric}, numericOverflowIssues(conv, fit)
	default:
		if fit == numberFitsInt64 {
			return ddl.Type{Name: ddl.Int64}, nil
		}
		return ddl.Type{Name: ddl.Numeric}, numericOverflowIssues(conv, fit)
	}
}

func toSpannerType(conv *internal.Conv, spType string, srcType string, mods []int64, isPk bool) (ddl.Type, []internal.SchemaIssue) {
	// Preemptively strip precision from srcType
	re := regexp.MustCompile(`\(\d+(,\s*\d+)?\)`)
	baseType := strings.ToUpper(re.ReplaceAllString(srcType, ""))
	baseType = strings.TrimSpace(baseType)

	var issues []internal.SchemaIssue
	switch baseType {
	case "VARCHAR2", "VARCHAR", "CHAR", "CHARACTER", "NVARCHAR2", "NCHAR", "NCHAR VARYING", "NATIONAL CHARACTER", "NATIONAL CHAR", "NATIONAL CHARACTER VARYING", "NATIONAL CHAR VARYING":
		// The declared length is carried over to STRING(n) and BYTES(n), matching the
		// MySQL and PostgreSQL sources.
		switch spType {
		case ddl.Bytes:
			if len(mods) > 0 && mods[0] > 0 && mods[0] <= ddl.BytesMaxLength {
				return ddl.Type{Name: ddl.Bytes, Len: mods[0]}, nil
			}
			return ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		default:
			if len(mods) > 0 && mods[0] > 0 && mods[0] <= ddl.StringMaxLength {
				return ddl.Type{Name: ddl.String, Len: mods[0]}, nil
			}
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
		}
	case "NUMBER", "NUMERIC", "DECIMAL", "DEC":
		return numberToSpannerType(conv, spType, mods)
	case "FLOAT":
		// Oracle FLOAT(p) (also reported for REAL = FLOAT(63) and DOUBLE PRECISION =
		// FLOAT(126)) is a floating point NUMBER whose precision p is in binary
		// digits. FLOAT64 covers its full range (1e-130 to 1e125) but holds only 53
		// binary digits of precision, so higher precisions are flagged.
		switch spType {
		case ddl.Numeric:
			return ddl.Type{Name: ddl.Numeric}, numericOverflowIssues(conv, numberExceedsNumeric)
		case ddl.String:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		case ddl.Int64:
			return ddl.Type{Name: ddl.Int64}, []internal.SchemaIssue{internal.Widened}
		default:
			if isPk {
				return ddl.Type{Name: ddl.Numeric}, nil
			}
			if len(mods) == 0 || mods[0] > maxFloat64BinaryPrecision {
				return ddl.Type{Name: ddl.Float64}, []internal.SchemaIssue{internal.Numeric}
			}
			return ddl.Type{Name: ddl.Float64}, nil
		}
	case "DOUBLE PRECISION":
		switch spType {
		case ddl.Numeric:
			return ddl.Type{Name: ddl.Numeric}, []internal.SchemaIssue{internal.Widened}
		case ddl.String:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		case ddl.Int64:
			return ddl.Type{Name: ddl.Int64}, []internal.SchemaIssue{internal.Widened}
		default:
			if isPk {
				return ddl.Type{Name: ddl.Numeric}, nil
			}
			return ddl.Type{Name: ddl.Float64}, nil
		}
	case "REAL", "BINARY_DOUBLE":
		switch spType {
		case ddl.String:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		case ddl.Numeric:
			return ddl.Type{Name: ddl.Numeric}, []internal.SchemaIssue{internal.Widened}
		case ddl.Int64:
			return ddl.Type{Name: ddl.Int64}, []internal.SchemaIssue{internal.Widened}
		default:
			if isPk {
				return ddl.Type{Name: ddl.Numeric}, nil
			}
			return ddl.Type{Name: ddl.Float64}, nil
		}
	case "BINARY_FLOAT":
		switch spType {
		case ddl.Float64:
			return ddl.Type{Name: ddl.Float64}, []internal.SchemaIssue{internal.Widened}
		case ddl.String:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		case ddl.Numeric:
			return ddl.Type{Name: ddl.Numeric}, []internal.SchemaIssue{internal.Widened}
		case ddl.Int64:
			return ddl.Type{Name: ddl.Int64}, []internal.SchemaIssue{internal.Widened}
		default:
			if isPk {
				return ddl.Type{Name: ddl.Numeric}, nil
			}
			return ddl.Type{Name: ddl.Float32}, nil
		}
	case "INTEGER", "INT", "SMALLINT":
		// INTEGER, INT and SMALLINT are NUMBER(*,0) in Oracle: integers with up to
		// 38 significant digits and a magnitude up to 1e125. They are mapped to
		// INT64 as that is the natural type for such (mostly ID) columns, with a
		// warning since larger values do not fit.
		switch spType {
		case ddl.Numeric:
			return ddl.Type{Name: ddl.Numeric}, numericOverflowIssues(conv, numberExceedsNumeric)
		case ddl.String:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		case ddl.Float64:
			return ddl.Type{Name: ddl.Float64}, []internal.SchemaIssue{internal.Widened}
		default:
			return ddl.Type{Name: ddl.Int64}, []internal.SchemaIssue{internal.PossibleOverflow}
		}
	case "DATE":
		// Oracle DATE holds a date and a time of day without a time zone.
		switch spType {
		case ddl.Date:
			return ddl.Type{Name: ddl.Date}, []internal.SchemaIssue{internal.Widened}
		case ddl.String:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		case ddl.Int64:
			return ddl.Type{Name: ddl.Int64}, []internal.SchemaIssue{internal.Widened}
		default:
			return ddl.Type{Name: ddl.Timestamp}, []internal.SchemaIssue{internal.Datetime}
		}
	case "TIMESTAMP", "TIMESTAMP WITH TIME ZONE", "TIMESTAMP WITH LOCAL TIME ZONE":
		switch spType {
		case ddl.String:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		case ddl.Int64:
			return ddl.Type{Name: ddl.Int64}, []internal.SchemaIssue{internal.Widened}
		default:
			if baseType == "TIMESTAMP" {
				// Unlike the time zone aware variants, TIMESTAMP has no time zone.
				return ddl.Type{Name: ddl.Timestamp}, []internal.SchemaIssue{internal.Datetime}
			}
			return ddl.Type{Name: ddl.Timestamp}, nil
		}
	case "INTERVAL YEAR TO MONTH", "INTERVAL DAY TO SECOND":
		// Spanner has no interval type, so intervals are stored as strings (the
		// PostgreSQL source reports interval the same way).
		switch spType {
		case ddl.Int64:
			return ddl.Type{Name: ddl.Int64}, []internal.SchemaIssue{internal.Widened}
		case ddl.Float64:
			return ddl.Type{Name: ddl.Float64}, []internal.SchemaIssue{internal.Widened}
		default:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		}
	case "RAW":
		// RAW(n) is variable length binary data of at most n bytes, so its length is
		// carried over to BYTES(n). Spanner PostgreSQL BYTEA does not support a length.
		switch spType {
		case ddl.String:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		default:
			if len(mods) > 0 && mods[0] > 0 && mods[0] <= ddl.BytesMaxLength {
				return ddl.Type{Name: ddl.Bytes, Len: mods[0]}, nil
			}
			return ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}, nil
		}
	case "LONG RAW", "BLOB":
		// LONG RAW (up to 2 GB) and BLOB (up to 4 GB and more) can hold values larger
		// than a Spanner BYTES/STRING value (10 MiB).
		switch spType {
		case ddl.String:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened, internal.PossibleOverflow}
		default:
			return ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.PossibleOverflow}
		}
	case "SDO_GEORASTER", "ANYDATA", "ANYTYPE", "ANYDATASET", "ORDAUDIO", "ORDIMAGE", "ORDVIDEO", "ORDDOC", "SI_STILLIMAGE", "SI_COLOR", "SI_AVERAGECOLOR", "SI_COLORHISTOGRAM", "SI_POSITIONALCOLOR", "SI_TEXTURE", "SI_FEATURELIST":
		switch spType {
		case ddl.String:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		default:
			return ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}, nil
		}
	case "CLOB", "NCLOB", "LONG":
		// CLOB, NCLOB (up to 4 GB and more) and LONG (up to 2 GB) can hold values
		// larger than a Spanner STRING(MAX)/BYTES value.
		switch spType {
		case ddl.Bytes:
			return ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened, internal.PossibleOverflow}
		default:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.PossibleOverflow}
		}
	case "XMLTYPE":
		// XMLTYPE values (stored as a LOB, up to 4 GB and more) can be larger than a
		// Spanner STRING(MAX)/BYTES value.
		switch spType {
		case ddl.Bytes:
			return ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened, internal.PossibleOverflow}
		default:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.PossibleOverflow}
		}
	case "URITYPE", "DBURITYPE", "XDBURITYPE", "HTTPURITYPE", "EXPRESSION", "MLSLABEL", "REF":
		switch spType {
		case ddl.Bytes:
			return ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		default:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
		}
	case "BFILE":
		// A BFILE is a locator of a file stored outside of the database. The file
		// content cannot be migrated, so the column is reported as having no
		// appropriate Spanner type (which also makes it nullable).
		switch spType {
		case ddl.Bytes:
			return ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.NoGoodType}
		default:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.NoGoodType}
		}
	case "ROWID", "UROWID":
		switch spType {
		case ddl.Bytes:
			return ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		case ddl.Int64:
			return ddl.Type{Name: ddl.Int64}, []internal.SchemaIssue{internal.Widened}
		default:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
		}
	case "BOOLEAN":
		switch spType {
		case ddl.Int64:
			return ddl.Type{Name: ddl.Int64}, []internal.SchemaIssue{internal.Widened}
		case ddl.String:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		default:
			return ddl.Type{Name: ddl.Bool}, nil
		}
	case "JSON":
		switch spType {
		case ddl.String:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		case ddl.Bytes:
			return ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		default:
			return ddl.Type{Name: ddl.JSON}, nil
		}
	case "SDO_GEOMETRY", "SDO_TOPO_GEOMETRY":
		switch spType {
		case ddl.Bytes:
			return ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		case ddl.JSON:
			return ddl.Type{Name: ddl.JSON}, []internal.SchemaIssue{internal.Widened}
		default:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
		}
	case "VECTOR":
		switch spType {
		case ddl.Float64:
			return ddl.Type{Name: ddl.Float64, IsArray: true}, []internal.SchemaIssue{internal.Widened}
		case ddl.Bytes:
			return ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength, IsArray: true}, []internal.SchemaIssue{internal.Widened}
		default:
			return ddl.Type{Name: ddl.Float32, IsArray: true}, nil
		}
	case "VARRAY":
		switch spType {
		case ddl.JSON:
			return ddl.Type{Name: ddl.JSON, IsArray: true}, []internal.SchemaIssue{internal.Widened}
		default:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength, IsArray: true}, nil
		}
	case "NESTED TABLE", "ASSOCIATIVE ARRAY", "OBJECT TYPE":
		switch spType {
		case ddl.String:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		default:
			return ddl.Type{Name: ddl.JSON}, nil
		}
	default:
		issues = append(issues, internal.NoGoodType)
		return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, issues
	}
}

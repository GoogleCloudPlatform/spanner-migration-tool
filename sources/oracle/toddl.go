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
	ty, issues := toSpannerTypeInternal(conv, spType, srcType.Name, isPk)
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

func toSpannerTypeInternal(conv *internal.Conv, spType string, srcType string, isPk bool) (ddl.Type, []internal.SchemaIssue) {
	ty, issues := toSpannerType(conv, spType, srcType, isPk)
	return ty, issues
}

func toSpannerType(conv *internal.Conv, spType string, srcType string, isPk bool) (ddl.Type, []internal.SchemaIssue) {
	// Preemptively strip precision from srcType
	re := regexp.MustCompile(`\(\d+(,\s*\d+)?\)`)
	baseType := strings.ToUpper(re.ReplaceAllString(srcType, ""))
	baseType = strings.TrimSpace(baseType)

	var issues []internal.SchemaIssue
	switch baseType {
	case "VARCHAR2", "VARCHAR", "CHAR", "CHARACTER", "NVARCHAR2", "NCHAR", "NCHAR VARYING", "NATIONAL CHARACTER", "NATIONAL CHAR", "NATIONAL CHARACTER VARYING", "NATIONAL CHAR VARYING":
		switch spType {
		case ddl.Bytes:
			return ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		default:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
		}
	case "NUMBER", "NUMERIC", "DECIMAL", "DEC", "FLOAT":
		switch spType {
		case ddl.Float64:
			return ddl.Type{Name: ddl.Float64}, []internal.SchemaIssue{internal.Widened}
		case ddl.String:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		case ddl.Int64:
			return ddl.Type{Name: ddl.Int64}, []internal.SchemaIssue{internal.Widened}
		default:
			return ddl.Type{Name: ddl.Numeric}, nil
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
		switch spType {
		case ddl.Numeric:
			return ddl.Type{Name: ddl.Numeric}, []internal.SchemaIssue{internal.Widened}
		case ddl.String:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		case ddl.Float64:
			return ddl.Type{Name: ddl.Float64}, []internal.SchemaIssue{internal.Widened}
		default:
			return ddl.Type{Name: ddl.Int64}, nil
		}
	case "DATE":
		switch spType {
		case ddl.Date:
			return ddl.Type{Name: ddl.Date}, []internal.SchemaIssue{internal.Widened}
		case ddl.String:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		case ddl.Int64:
			return ddl.Type{Name: ddl.Int64}, []internal.SchemaIssue{internal.Widened}
		default:
			return ddl.Type{Name: ddl.Timestamp}, nil
		}
	case "TIMESTAMP", "TIMESTAMP WITH TIME ZONE", "TIMESTAMP WITH LOCAL TIME ZONE":
		switch spType {
		case ddl.String:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		case ddl.Int64:
			return ddl.Type{Name: ddl.Int64}, []internal.SchemaIssue{internal.Widened}
		default:
			return ddl.Type{Name: ddl.Timestamp}, nil
		}
	case "INTERVAL YEAR TO MONTH", "INTERVAL DAY TO SECOND":
		switch spType {
		case ddl.Int64:
			return ddl.Type{Name: ddl.Int64}, []internal.SchemaIssue{internal.Widened}
		case ddl.Float64:
			return ddl.Type{Name: ddl.Float64}, []internal.SchemaIssue{internal.Widened}
		default:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
		}
	case "RAW":
		switch spType {
		case ddl.String:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		default:
			return ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}, nil
		}
	case "LONG RAW", "BLOB", "SDO_GEORASTER", "ANYDATA", "ANYTYPE", "ANYDATASET", "ORDAUDIO", "ORDIMAGE", "ORDVIDEO", "ORDDOC", "SI_STILLIMAGE", "SI_COLOR", "SI_AVERAGECOLOR", "SI_COLORHISTOGRAM", "SI_POSITIONALCOLOR", "SI_TEXTURE", "SI_FEATURELIST":
		switch spType {
		case ddl.String:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		default:
			return ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}, nil
		}
	case "CLOB", "NCLOB", "LONG", "XMLTYPE", "URITYPE", "DBURITYPE", "XDBURITYPE", "HTTPURITYPE", "EXPRESSION", "MLSLABEL", "REF":
		switch spType {
		case ddl.Bytes:
			return ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		default:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
		}
	case "BFILE":
		switch spType {
		case ddl.Bytes:
			return ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		default:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
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

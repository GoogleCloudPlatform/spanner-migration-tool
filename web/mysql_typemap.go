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

package web

import (
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
)

// TODO(searce): Decide on the issue message that needs to be shown
// in frontend.
var mysqlTypeMap = map[string][]typeIssue{
	"bool": []typeIssue{
		typeIssue{T: ddl.Bool},
		typeIssue{T: ddl.String, Brief: internal.IssueDB[internal.Widened].Brief},
		typeIssue{T: ddl.Int64, Brief: internal.IssueDB[internal.Widened].Brief}},
	"boolean": []typeIssue{
		typeIssue{T: ddl.Bool},
		typeIssue{T: ddl.String, Brief: internal.IssueDB[internal.Widened].Brief},
		typeIssue{T: ddl.Int64, Brief: internal.IssueDB[internal.Widened].Brief}},
	"varchar": []typeIssue{
		typeIssue{T: ddl.String},
		typeIssue{T: ddl.Bytes, Brief: internal.IssueDB[internal.Widened].Brief}},
	"char": []typeIssue{
		typeIssue{T: ddl.String},
		typeIssue{T: ddl.Bytes, Brief: internal.IssueDB[internal.Widened].Brief}},
	"text": []typeIssue{
		typeIssue{T: ddl.String},
		typeIssue{T: ddl.Bytes, Brief: internal.IssueDB[internal.Widened].Brief}},
	"tinytext": []typeIssue{
		typeIssue{T: ddl.String},
		typeIssue{T: ddl.Bytes, Brief: internal.IssueDB[internal.Widened].Brief}},
	"mediumtext": []typeIssue{
		typeIssue{T: ddl.String},
		typeIssue{T: ddl.Bytes, Brief: internal.IssueDB[internal.Widened].Brief}},
	"longtext": []typeIssue{
		typeIssue{T: ddl.String},
		typeIssue{T: ddl.Bytes, Brief: internal.IssueDB[internal.Widened].Brief}},
	"set": []typeIssue{
		typeIssue{T: ddl.String, Brief: "SET datatype only supports STRING values"}},
	"enum": []typeIssue{
		typeIssue{T: ddl.String, Brief: "ENUM datatype only supports STRING values"}},
	"json": []typeIssue{
		typeIssue{T: ddl.String},
		typeIssue{T: ddl.Bytes, Brief: internal.IssueDB[internal.Widened].Brief}},
	"bit": []typeIssue{
		typeIssue{T: ddl.Bytes},
		typeIssue{T: ddl.String}},
	"binary": []typeIssue{
		typeIssue{T: ddl.Bytes},
		typeIssue{T: ddl.String}},
	"varbinary": []typeIssue{
		typeIssue{T: ddl.Bytes},
		typeIssue{T: ddl.String}},
	"blob": []typeIssue{
		typeIssue{T: ddl.Bytes},
		typeIssue{T: ddl.String}},
	"tinyblob": []typeIssue{
		typeIssue{T: ddl.Bytes},
		typeIssue{T: ddl.String}},
	"mediumblob": []typeIssue{
		typeIssue{T: ddl.Bytes},
		typeIssue{T: ddl.String}},
	"longblob": []typeIssue{
		typeIssue{T: ddl.Bytes},
		typeIssue{T: ddl.String}},
	"tinyint": []typeIssue{
		typeIssue{T: ddl.Int64},
		typeIssue{T: ddl.String, Brief: internal.IssueDB[internal.Widened].Brief}},
	"smallint": []typeIssue{
		typeIssue{T: ddl.Int64},
		typeIssue{T: ddl.String, Brief: internal.IssueDB[internal.Widened].Brief}},
	"mediumint": []typeIssue{
		typeIssue{T: ddl.Int64},
		typeIssue{T: ddl.String, Brief: internal.IssueDB[internal.Widened].Brief}},
	"int": []typeIssue{
		typeIssue{T: ddl.Int64},
		typeIssue{T: ddl.String, Brief: internal.IssueDB[internal.Widened].Brief}},
	"integer": []typeIssue{
		typeIssue{T: ddl.Int64},
		typeIssue{T: ddl.String, Brief: internal.IssueDB[internal.Widened].Brief}},
	"bigint": []typeIssue{
		typeIssue{T: ddl.Int64},
		typeIssue{T: ddl.String, Brief: internal.IssueDB[internal.Widened].Brief}},
	"double": []typeIssue{
		typeIssue{T: ddl.Float64},
		typeIssue{T: ddl.String, Brief: internal.IssueDB[internal.Widened].Brief}},
	"float": []typeIssue{
		typeIssue{T: ddl.Float64},
		typeIssue{T: ddl.String, Brief: internal.IssueDB[internal.Widened].Brief}},
	"numeric": []typeIssue{
		typeIssue{T: ddl.Float64},
		typeIssue{T: ddl.String, Brief: internal.IssueDB[internal.Widened].Brief}},
	"decimal": []typeIssue{
		typeIssue{T: ddl.Float64},
		typeIssue{T: ddl.String, Brief: internal.IssueDB[internal.Widened].Brief}},
	"date": []typeIssue{
		typeIssue{T: ddl.Date},
		typeIssue{T: ddl.String, Brief: internal.IssueDB[internal.Widened].Brief}},
	"datetime": []typeIssue{
		typeIssue{T: ddl.Timestamp},
		typeIssue{T: ddl.String, Brief: internal.IssueDB[internal.Widened].Brief}},
	"timestamp": []typeIssue{
		typeIssue{T: ddl.Timestamp},
		typeIssue{T: ddl.String, Brief: internal.IssueDB[internal.Widened].Brief}},
	"time": []typeIssue{
		typeIssue{T: ddl.String}},
	"year": []typeIssue{
		typeIssue{T: ddl.String}},
}

func toSpannerTypeMySQL(srcType string, spType string, mods []int64) (ddl.Type, []internal.SchemaIssue) {

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
	case "tinyint":
		switch spType {
		case ddl.String:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		case ddl.Int64:
			return ddl.Type{Name: ddl.Int64}, []internal.SchemaIssue{internal.Widened}
		default:
			// tinyint(1) is a bool in MySQL
			if len(mods) > 0 && mods[0] == 1 {
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
	case "numeric", "decimal": // Map all numeric and decimal types to float64.
		switch spType {
		case ddl.String:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		default:
			if len(mods) > 0 && mods[0] <= 15 {
				// float64 can represent this numeric type faithfully.
				// Note: int64 has 53 bits for mantissa, which is ~15.96
				// decimal digits.
				return ddl.Type{Name: ddl.Float64}, []internal.SchemaIssue{internal.DecimalThatFits}
			}
			return ddl.Type{Name: ddl.Float64}, []internal.SchemaIssue{internal.Decimal}
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
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		default:
			return ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}, nil
		}
	case "varchar", "char":
		switch spType {
		case ddl.Bytes:
			return ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		default:
			if len(mods) > 0 {
				return ddl.Type{Name: ddl.String, Len: mods[0]}, nil
			}
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
		}
	case "text", "tinytext", "mediumtext", "longtext":
		switch spType {
		case ddl.Bytes:
			return ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		default:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
		}
	case "set", "enum":
		return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
	case "json":
		switch spType {
		case ddl.Bytes:
			return ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		default:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
		}
	case "binary", "varbinary":
		switch spType {
		case ddl.String:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
		default:
			return ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}, nil
		}
	case "tinyblob", "mediumblob", "blob", "longblob":
		switch spType {
		case ddl.String:
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.Widened}
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

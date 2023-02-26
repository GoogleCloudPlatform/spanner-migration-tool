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

package spanner

import (
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/schema"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
)

type ToDdlImpl struct {
}

// ToSpannerGSQLDialectType maps a scalar source schema type (defined by id and
// mods) into a Spanner GOOGLE STANDARD SQL dialect type. ToSpannerGSQLDialectType returns
// the Spanner type and a list of type conversion issues encountered.
// Functions below implement the common.ToDdl interface
func (tdi ToDdlImpl) ToSpannerGSQLDialectType(conv *internal.Conv, spType string, srcType schema.Type) (ddl.Type, []internal.SchemaIssue) {
	ty, issues := ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.NoGoodType}
	switch srcType.Name {
	case "BOOL":
		ty, issues = ddl.Type{Name: ddl.Bool}, nil
	case "BYTES":
		ty, issues = ddl.Type{Name: ddl.Bytes, Len: srcType.Mods[0]}, nil
	case "DATE":
		ty, issues = ddl.Type{Name: ddl.Date}, nil
	case "FLOAT64":
		ty, issues = ddl.Type{Name: ddl.Float64}, nil
	case "INT64":
		ty, issues = ddl.Type{Name: ddl.Int64}, nil
	case "JSON":
		ty, issues = ddl.Type{Name: ddl.JSON}, nil
	case "NUMERIC":
		ty, issues = ddl.Type{Name: ddl.Numeric}, nil
	case "STRING":
		ty, issues = ddl.Type{Name: ddl.String, Len: srcType.Mods[0]}, nil
	case "TIMESTAMP":
		ty, issues = ddl.Type{Name: ddl.Timestamp}, nil
	}
	ty.IsArray = len(srcType.ArrayBounds) == 1
	return ty, issues
}

// ToSpannerPostgreSQLDialectType maps a scalar source schema type (defined by id and
// mods) into a Spanner PostgreSQL dialect type. ToSpannerPostgreSQLDialectType returns
// the Spanner type and a list of type conversion issues encountered.
func (tdi ToDdlImpl) ToSpannerPostgreSQLDialectType(conv *internal.Conv, spType string, srcType schema.Type) (ddl.Type, []internal.SchemaIssue) {
	ty, issues := ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.NoGoodType}
	switch srcType.Name {
	case "BOOL":
		ty, issues = ddl.Type{Name: ddl.PGBool}, nil
	case "BYTEA":
		ty, issues = ddl.Type{Name: ddl.PGBytea, Len: srcType.Mods[0]}, nil
	case "DATE":
		ty, issues = ddl.Type{Name: ddl.PGDate}, nil
	case "FLOAT8":
		ty, issues = ddl.Type{Name: ddl.PGFloat8}, nil
	case "INT8":
		ty, issues = ddl.Type{Name: ddl.PGInt8}, nil
	case "JSONB":
		ty, issues = ddl.Type{Name: ddl.PGJSONB}, nil
	case "NUMERIC":
		ty, issues = ddl.Type{Name: ddl.PGNumeric}, nil
	case "VARCHAR":
		ty, issues = ddl.Type{Name: ddl.PGVarchar, Len: srcType.Mods[0]}, nil
	case "TIMESTAMPTZ":
		ty, issues = ddl.Type{Name: ddl.PGTimestamptz}, nil
	}
	return ty, issues
}

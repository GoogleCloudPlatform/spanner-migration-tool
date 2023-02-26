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

// Package dynamodb handles schema and data migrations from DynamoDB.
package dynamodb

import (
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/schema"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
)

// ToDdl implementation for DynamoDB
type ToDdlImpl struct {
}

// Functions below implement the common.ToDdl interface
// ToSpannerGSQLDialectType maps a scalar source schema type (defined by id and
// mods) into a Spanner GOOGLE STANDARD SQL dialect type. This is the core source-to-Spanner type
// mapping.  ToSpannerGSQLDialectType returns the Spanner type and a list of type
// conversion issues encountered.
func (tdi ToDdlImpl) ToSpannerGSQLDialectType(conv *internal.Conv, spType string, srcType schema.Type) (ddl.Type, []internal.SchemaIssue) {
	switch srcType.Name {
	case typeNumber:
		return ddl.Type{Name: ddl.Numeric}, nil
	case typeNumberString, typeString, typeList, typeMap:
		return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
	case typeBool:
		return ddl.Type{Name: ddl.Bool}, nil
	case typeBinary:
		return ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}, nil
	case typeStringSet, typeNumberStringSet:
		return ddl.Type{Name: ddl.String, Len: ddl.MaxLength, IsArray: true}, nil
	case typeNumberSet:
		return ddl.Type{Name: ddl.Numeric, IsArray: true}, nil
	case typeBinarySet:
		return ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength, IsArray: true}, nil
	default:
		return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.NoGoodType}
	}
}

// ToSpannerPostgreSQLDialectType maps a scalar source schema type (defined by id and
// mods) into a Spanner PostgreSQL dialect type. This is the core source-to-Spanner type
// mapping.
func (tdi ToDdlImpl) ToSpannerPostgreSQLDialectType(conv *internal.Conv, spType string, srcType schema.Type) (ddl.Type, []internal.SchemaIssue) {
	switch srcType.Name {
	case typeNumber:
		return ddl.Type{Name: ddl.PGNumeric}, nil
	case typeNumberString, typeString, typeList, typeMap:
		return ddl.Type{Name: ddl.PGVarchar, Len: ddl.PGMaxLength}, nil
	case typeBool:
		return ddl.Type{Name: ddl.PGBool}, nil
	case typeBinary:
		return ddl.Type{Name: ddl.PGBytea, Len: ddl.PGMaxLength}, nil
	case typeStringSet, typeNumberStringSet:
		return ddl.Type{Name: ddl.PGVarchar, Len: ddl.PGMaxLength}, nil
	case typeNumberSet:
		return ddl.Type{Name: ddl.PGVarchar, Len: ddl.PGMaxLength}, nil
	case typeBinarySet:
		return ddl.Type{Name: ddl.PGVarchar, Len: ddl.PGMaxLength}, nil
	default:
		return ddl.Type{Name: ddl.PGVarchar, Len: ddl.PGMaxLength}, []internal.SchemaIssue{internal.NoGoodType}
	}
}

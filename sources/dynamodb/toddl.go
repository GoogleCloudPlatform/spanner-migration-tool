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
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/schema"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/common"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
)

// ToDdl implementation for DynamoDB
type ToDdlImpl struct {
}

// Functions below implement the common.ToDdl interface
// toSpannerType maps a scalar source schema type (defined by id and
// mods) into a Spanner type. This is the core source-to-Spanner type
// mapping.  toSpannerType returns the Spanner type and a list of type
// conversion issues encountered.
func (tdi ToDdlImpl) ToSpannerType(conv *internal.Conv, spType string, srcType schema.Type) (ddl.Type, []internal.SchemaIssue) {
	ty, issues := toSpannerTypeInternal(conv, srcType)
	if conv.SpDialect == constants.DIALECT_POSTGRESQL {
		ty = common.ToPGDialectType(ty)
	}
	return ty, issues
}

func (tdi ToDdlImpl) GetColumnAutoGen(conv *internal.Conv, autoGenCol ddl.AutoGenCol, colId string, tableId string) (*ddl.AutoGenCol, error) {
	return nil, nil
}

func toSpannerTypeInternal(conv *internal.Conv, srcType schema.Type) (ddl.Type, []internal.SchemaIssue) {
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

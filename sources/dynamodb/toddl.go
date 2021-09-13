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

package dynamodb

import (
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/schema"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
)

const (
	typeString          = "String"
	typeBool            = "Bool"
	typeNumber          = "Number"
	typeNumberString    = "NumberString"
	typeBinary          = "Binary"
	typeList            = "List"
	typeMap             = "Map"
	typeStringSet       = "StringSet"
	typeNumberSet       = "NumberSet"
	typeNumberStringSet = "NumberStringSet"
	typeBinarySet       = "BinarySet"

	errThreshold      = float64(0.001)
	conflictThreshold = float64(0.05)
)

// DynamoDb specific implementation for ToDdl
type DynamoToSpannerDdl struct {
}

// toSpannerType maps a scalar source schema type (defined by id and
// mods) into a Spanner type. This is the core source-to-Spanner type
// mapping.  toSpannerType returns the Spanner type and a list of type
// conversion issues encountered.
func (msc DynamoToSpannerDdl) ToSpannerType(conv *internal.Conv, columnType schema.Type) (ddl.Type, []internal.SchemaIssue) {
	switch columnType.Name {
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

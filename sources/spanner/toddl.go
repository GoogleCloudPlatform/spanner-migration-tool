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
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/schema"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
)

type ToDdlImpl struct {
}

// ToSpannerType maps a scalar source schema type (defined by id and
// mods) into a Spanner type. This is the core source-to-Spanner type
// mapping.  toSpannerType returns the Spanner type and a list of type
// conversion issues encountered.
// Functions below implement the common.ToDdl interface
func (tdi ToDdlImpl) ToSpannerType(conv *internal.Conv, spType string, srcType schema.Type) (ddl.Type, []internal.SchemaIssue) {
	ty, issues := toSpannerTypeInternal(conv, srcType)
	ty.IsArray = len(srcType.ArrayBounds) == 1
	return ty, issues
}

// toSpannerType maps a scalar source schema type (defined by id and
// mods) into a Spanner type. This is the core source-to-Spanner type
// mapping.  toSpannerType returns the Spanner type and a list of type
// conversion issues encountered.
func toSpannerTypeInternal(conv *internal.Conv, srcType schema.Type) (ddl.Type, []internal.SchemaIssue) {
	switch srcType.Name {
	case "BOOL", "boolean":
		return ddl.Type{Name: ddl.Bool}, nil
	case "BYTES":
		return ddl.Type{Name: ddl.Bytes, Len: srcType.Mods[0]}, nil
	case "bytea":
		return ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}, nil
	case "DATE", "date":
		return ddl.Type{Name: ddl.Date}, nil
	case "FLOAT32":
		return ddl.Type{Name: ddl.Float32}, nil
	case "FLOAT64", "double precision":
		return ddl.Type{Name: ddl.Float64}, nil
	case "INT64", "bigint":
		return ddl.Type{Name: ddl.Int64}, nil
	case "JSON", "jsonb":
		return ddl.Type{Name: ddl.JSON}, nil
	case "NUMERIC", "numeric":
		return ddl.Type{Name: ddl.Numeric}, nil
	case "STRING":
		return ddl.Type{Name: ddl.String, Len: srcType.Mods[0]}, nil
	case "character varying":
		if len(srcType.Mods) == 0 {
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
		}
		return ddl.Type{Name: ddl.String, Len: srcType.Mods[0]}, nil
	case "TIMESTAMP", "timestamp with time zone":
		return ddl.Type{Name: ddl.Timestamp}, nil
	}
	return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.NoGoodType}
}

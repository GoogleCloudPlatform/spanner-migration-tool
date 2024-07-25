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

package csv

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
)

func ToSpannerType(columnType string) (ddl.Type, error) {
	ty := strings.ToUpper(columnType)
	switch {
	case ty == "BOOL":
		return ddl.Type{Name: ddl.Bool}, nil
	// We accept variations including BYTES, BYTES(), BYTES(0) since the length doesn't matter.
	case strings.HasPrefix(ty, "BYTES"):
		match, _ := regexp.MatchString(`^BYTES\([0-9]*\)$`, ty)
		if match || ty == "BYTES" {
			return ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}, nil
		}
		return ddl.Type{}, fmt.Errorf("%v is not a valid Spanner column type", columnType)
	case ty == "DATE":
		return ddl.Type{Name: ddl.Date}, nil
	case ty == "FLOAT32":
		return ddl.Type{Name: ddl.Float32}, nil
	case ty == "FLOAT64":
		return ddl.Type{Name: ddl.Float64}, nil
	case ty == "INT64":
		return ddl.Type{Name: ddl.Int64}, nil
	case ty == "NUMERIC":
		return ddl.Type{Name: ddl.Numeric}, nil
	// We accept variations including STRING, STRING(), STRING(0) since the length doesn't matter.
	case strings.HasPrefix(ty, "STRING"):
		match, _ := regexp.MatchString(`^STRING\([0-9]*\)$`, ty)
		if match || ty == "STRING" {
			return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
		}
		return ddl.Type{}, fmt.Errorf("%v is not a valid Spanner column type", columnType)
	case ty == "TIMESTAMP":
		return ddl.Type{Name: ddl.Timestamp}, nil
	case ty == "JSON":
		return ddl.Type{Name: ddl.JSON}, nil
	default:
		return ddl.Type{}, fmt.Errorf("%v is not a valid Spanner column type", columnType)
	}
}

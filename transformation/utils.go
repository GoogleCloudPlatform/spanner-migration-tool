// Copyright 2023 Google LLC
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

package transformation

import (
	"fmt"
	"math/big"
	"strconv"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	"github.com/cloudspannerecosystem/harbourbridge/common/constants"
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
)

type SupportedFunctions struct {
	Functions map[string]FunctionDefinition
}

type FunctionDefinition struct {
	Name        string
	Description string
	Comment     string
	Input       [][]string
	Output      []string
}

func convScalar(conv *internal.Conv, dataType string, val string) (interface{}, error) {
	// Whitespace within the val string is considered part of the data value.
	// Note that many of the underlying conversions functions we use (like
	// strconv.ParseFloat and strconv.ParseInt) return "invalid syntax"
	// errors if whitespace were to appear at the start or end of a string.
	// We do not expect mysqldump to generate such output.
	switch dataType {
	case ddl.Bool:
		return convBool(conv, val)
	case ddl.Bytes:
		return convBytes(val)
	case ddl.Date:
		return convDate(val)
	case ddl.Float64:
		return convFloat64(val)
	case ddl.Int64:
		return convInt64(val)
	case ddl.Numeric:
		return convNumeric(conv, val)
	case ddl.String:
		return val, nil
	case ddl.Timestamp:
		return convTimestamp(val)
	case ddl.JSON:
		return val, nil
	default:
		return val, fmt.Errorf("data conversion not implemented for type %v", dataType)
	}
}

func convBool(conv *internal.Conv, val string) (bool, error) {
	b, err := strconv.ParseBool(val)
	if err != nil {
		return b, fmt.Errorf("can't convert to bool: %w", err)
	}
	return b, err
}

func convBytes(val string) ([]byte, error) {
	// convert a string to a byte slice.
	b := []byte(val)
	return b, nil
}

func convDate(val string) (civil.Date, error) {
	d, err := civil.ParseDate(val)
	if err != nil {
		return d, fmt.Errorf("can't convert to date: %w", err)
	}
	return d, err
}

func convFloat64(val string) (float64, error) {
	f, err := strconv.ParseFloat(val, 64)
	if err != nil {
		return f, fmt.Errorf("can't convert to float64: %w", err)
	}
	return f, err
}

func convInt64(val string) (int64, error) {
	i, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return i, fmt.Errorf("can't convert to int64: %w", err)
	}
	return i, err
}

// convNumeric maps a source database string value (representing a numeric)
// into a string representing a valid Spanner numeric.
func convNumeric(conv *internal.Conv, val string) (interface{}, error) {
	if conv.SpDialect == constants.DIALECT_POSTGRESQL {
		return spanner.PGNumeric{Numeric: val, Valid: true}, nil
	} else {
		r := new(big.Rat)
		if _, ok := r.SetString(val); !ok {
			return "", fmt.Errorf("can't convert %q to big.Rat", val)
		}
		return r, nil
	}
}

func convTimestamp(val string) (t time.Time, err error) {

	t, err = time.Parse("2006-01-02 15:04:05", val)
	if err != nil {
		return t, fmt.Errorf("can't convert to timestamp, value:", val)
	}
	return t, err
}

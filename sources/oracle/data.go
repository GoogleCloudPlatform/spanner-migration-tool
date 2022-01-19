// Copyright 2021 Google LLC
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
	"fmt"
	"math/big"
	"math/bits"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/cloudspannerecosystem/harbourbridge/common/constants"
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/schema"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
	"github.com/golang-sql/civil"
)

func ProcessDataRow(conv *internal.Conv, srcTable string, srcCols []string, srcSchema schema.Table, spTable string, spCols []string, spSchema ddl.CreateTable, vals []string) {
	spTable, cvtCols, cvtVals, err := ConvertData(conv, srcTable, srcCols, srcSchema, spTable, spCols, spSchema, vals)
	if err != nil {
		conv.Unexpected(fmt.Sprintf("Error while converting data: %s\n", err))
		conv.StatsAddBadRow(srcTable, conv.DataMode())
		conv.CollectBadRow(srcTable, srcCols, vals)
	} else {
		conv.WriteRow(srcTable, spTable, cvtCols, cvtVals)
	}
}

// ConvertData maps the source DB data in vals into Spanner data,
// based on the Spanner and source DB schemas. Note that since entries
// in vals may be empty, we also return the list of columns (empty
// cols are dropped).
func ConvertData(conv *internal.Conv, srcTable string, srcCols []string, srcSchema schema.Table, spTable string, spCols []string, spSchema ddl.CreateTable, vals []string) (string, []string, []interface{}, error) {
	var c []string
	var v []interface{}
	if len(spCols) != len(srcCols) || len(spCols) != len(vals) {
		return "", []string{}, []interface{}{}, fmt.Errorf("ConvertData: spCols, srcCols and vals don't all have the same lengths: len(spCols)=%d, len(srcCols)=%d, len(vals)=%d", len(spCols), len(srcCols), len(vals))
	}
	for i, spCol := range spCols {
		srcCol := srcCols[i]
		// Skip columns with 'NULL' values., these values
		// 'NULL' values are represented as "NULL" (because we retrieve the values as strings).
		if vals[i] == "NULL" {
			continue
		}
		spColDef, ok1 := spSchema.ColDefs[spCol]
		srcColDef, ok2 := srcSchema.ColDefs[srcCol]
		if !ok1 || !ok2 {
			return "", []string{}, []interface{}{}, fmt.Errorf("can't find Spanner and source-db schema for col %s", spCol)
		}
		var x interface{}
		var err error
		x, err = convScalar(conv, spColDef.T, srcColDef.Type.Name, conv.TimezoneOffset, vals[i])
		if err != nil {
			return "", []string{}, []interface{}{}, err
		}
		v = append(v, x)
		c = append(c, spCol)
	}
	if aux, ok := conv.SyntheticPKeys[spTable]; ok {
		c = append(c, aux.Col)
		v = append(v, int64(bits.Reverse64(uint64(aux.Sequence))))
		aux.Sequence++
		conv.SyntheticPKeys[spTable] = aux
	}
	return spTable, c, v, nil
}

// convScalar converts a source database string value to an
// appropriate Spanner value. It is the caller's responsibility to
// detect and handle NULL values: convScalar will return error if a
// NULL value is passed.
func convScalar(conv *internal.Conv, spannerType ddl.Type, srcTypeName string, TimezoneOffset string, val string) (interface{}, error) {
	// Whitespace within the val string is considered part of the data value.
	// Note that many of the underlying conversions functions we use (like
	// strconv.ParseFloat and strconv.ParseInt) return "invalid syntax"
	// errors if whitespace were to appear at the start or end of a string.
	switch spannerType.Name {
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
		return convTimestamp(srcTypeName, TimezoneOffset, val)
	case ddl.JSON:
		return val, nil
	default:
		return val, fmt.Errorf("data conversion not implemented for type %v", spannerType.Name)
	}
}

func convBool(conv *internal.Conv, val string) (bool, error) {
	b, err := strconv.ParseBool(val)
	if err != nil {
		i, err2 := convInt64(val)
		if err2 == nil && i >= -128 && i <= 127 {
			b = i != 0
			conv.Unexpected(fmt.Sprintf("Expected boolean value, but found integer value %v; mapping it to %v\n", val, b))
			return b, err2
		}
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
	date := strings.Split(val, "T")[0]
	d, err := civil.ParseDate(date)
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
	if conv.TargetDb == constants.TargetExperimentalPostgres {
		return spanner.PGNumeric{Numeric: val, Valid: true}, nil
	} else {
		r := new(big.Rat)
		if _, ok := r.SetString(val); !ok {
			return "", fmt.Errorf("can't convert %q to big.Rat", val)
		}
		return r, nil
	}
}

// convTimestamp maps a source DB timestamp into a go Time Spanner timestamp
// It handles both datetime and timestamp conversions.
func convTimestamp(srcTypeName string, TimezoneOffset string, val string) (t time.Time, err error) {
	// We consider timezone for timestamp datatype.
	// If timezone is not specified in mysqldump, we consider UTC time.
	if TimezoneOffset == "" {
		TimezoneOffset = "+00:00"
	}
	// convert timestamp from format "2006-01-02 15:04:05" to
	// "2006-01-02T15:04:05+00:00".
	t, err = time.Parse(time.RFC3339, val)
	// Avalible format
	//"2022-01-19T09:34:06.47Z"
	//"2021-02-18T07:22:11.871+66:00"
	//"2022-02-15T11:22:11.871Z"
	if err != nil {
		return t, fmt.Errorf("can't convert to timestamp (mysql type: %s)", srcTypeName)
	}
	return t, err
}

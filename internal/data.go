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

package internal

import (
	"encoding/hex"
	"fmt"
	"math/bits"
	"reflect"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/civil"

	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
)

// ConvertData maps the source DB data in vals into Spanner data,
// based on the Spanner and source DB schemas. Note that since entries
// in vals may be empty, we also return the list of columns (empty
// cols are dropped).
func ConvertData(conv *Conv, srcTable string, srcCols []string, vals []string) (string, []string, []interface{}, error) {
	// Note: the following functionality gets repeated for every
	// row.  We could factor this out when we have many rows for
	// the same srcTable/srcCols if cost of this repetition is an
	// issue.
	spTable, err := GetSpannerTable(conv, srcTable)
	if err != nil {
		return "", []string{}, []interface{}{}, fmt.Errorf("can't map source table %s", srcTable)
	}
	spCols, err := GetSpannerCols(conv, srcTable, srcCols)
	if err != nil {
		return "", []string{}, []interface{}{}, fmt.Errorf("can't map source columsn %v", srcCols)
	}
	spSchema, ok1 := conv.spSchema[spTable]
	srcSchema, ok2 := conv.srcSchema[srcTable]
	if !ok1 || !ok2 {
		return "", []string{}, []interface{}{}, fmt.Errorf("can't find table %s in schema", spTable)
	}
	var c []string
	var v []interface{}
	if len(spCols) != len(srcCols) || len(spCols) != len(vals) {
		return "", []string{}, []interface{}{}, fmt.Errorf("ConvertData: spCols, srcCols and vals don't all have the same lengths: len(spCols)=%d, len(srcCols)=%d, len(vals)=%d", len(spCols), len(srcCols), len(vals))
	}
	for i, spCol := range spCols {
		srcCol := srcCols[i]
		if vals[i] == "\\N" { // PostgreSQL representation of empty column in COPY-FROM blocks.
			continue
		}
		spColDef, ok1 := spSchema.Cds[spCol]
		srcColDef, ok2 := srcSchema.ColDef[srcCol]
		if !ok1 || !ok2 {
			return "", []string{}, []interface{}{}, fmt.Errorf("can't find Spanner and source-db schema for col %s", spCol)
		}
		var x interface{}
		var err error
		if spColDef.IsArray {
			x, err = convArray(spColDef.T, srcColDef.Type.Name, conv.location, vals[i])
		} else {
			x, err = convScalar(spColDef.T, srcColDef.Type.Name, conv.location, vals[i])
		}
		if err != nil {
			return "", []string{}, []interface{}{}, err
		}
		v = append(v, x)
		c = append(c, spCol)
	}
	if aux, ok := conv.syntheticPKeys[spTable]; ok {
		c = append(c, aux.col)
		v = append(v, int64(bits.Reverse64(uint64(aux.sequence))))
		aux.sequence++
		conv.syntheticPKeys[spTable] = aux
	}
	return spTable, c, v, nil
}

func convScalar(spannerType ddl.ScalarType, srcTypeName string, location *time.Location, val string) (interface{}, error) {
	// Whitespace within the val string is considered part of the data value.
	// Note that many of the underlying conversions functions we use (like
	// strconv.ParseFloat and strconv.ParseInt) return "invalid syntax"
	// errors if whitespace were to appear at the start or end of a string.
	// We do not expect pg_dump to generate such output.
	switch spannerType.(type) {
	case ddl.Bool:
		return convBool(val)
	case ddl.Bytes:
		return convBytes(val)
	case ddl.Date:
		return convDate(val)
	case ddl.Float64:
		return convFloat64(val)
	case ddl.Int64:
		return convInt64(val)
	case ddl.String:
		return val, nil
	case ddl.Timestamp:
		return convTimestamp(srcTypeName, location, val)
	default:
		return val, fmt.Errorf("data conversion not implemented for type %v", reflect.TypeOf(spannerType))
	}
}

func convBool(val string) (bool, error) {
	b, err := strconv.ParseBool(val)
	if err != nil {
		return b, fmt.Errorf("can't convert to bool: %w", err)
	}
	return b, err
}

func convBytes(val string) ([]byte, error) {
	if val[0:3] != `\\x` {
		return []byte{}, fmt.Errorf("can't convert to bytes: doesn't start with \\x prefix")
	}
	b, err := hex.DecodeString(val[3:])
	if err != nil {
		return b, fmt.Errorf("can't convert to bytes: %w", err)
	}
	return b, err
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

// convTimestamp maps a source DB timestamp into a go Time (which
// is translated to a Spanner timestamp by the go Spanner client library).
// It handles both timestamptz and timestamp conversions.
// Note that PostgreSQL supports a wide variety of different timestamp
// formats (see https://www.postgresql.org/docs/9.1/datatype-datetime.html).
// We don't attempt to support all of these timestamp formats. Our goal
// is more modest: we just need to support the formats generated by
// pg_dump.
func convTimestamp(srcTypeName string, location *time.Location, val string) (t time.Time, err error) {
	// pg_dump outputs timestamps as ISO 8601, except:
	// a) it uses space instead of T
	// b) timezones are abbreviated to just hour (minute is specified only if non-zero).
	if srcTypeName == "timestamptz" {
		// PostgreSQL abbreviates timezone to just hour where possible.
		t, err = time.Parse("2006-01-02 15:04:05Z07", val)
		if err != nil {
			// Try using hour and min for timezone e.g. PGTZ set to 'Asia/Kolkata'.
			t, err = time.Parse("2006-01-02 15:04:05Z07:00", val)
		}
		if err != nil {
			// Try parsing without timezone. Some pg_dump files
			// generate timestamps without timezone for timestampz data
			// e.g. the Pagila port of Sakila. We interpret these timestamps
			// using the current time location (default is local time).
			// Note: we might want to look for "SET TIME ZONE" in the pg_dump
			// and interpret wrt that timezone.
			t, err = time.ParseInLocation("2006-01-02 15:04:05", val, location)
		}
	} else {
		// timestamp without time zone: data should just consist of date and time.
		// timestamp conversion should ignore timezone. We mimic this using Parse
		// i.e. treat it as UTC, so it will be stored 'as-is' in Spanner.
		t, err = time.Parse("2006-01-02 15:04:05", val)
	}
	if err != nil {
		return t, fmt.Errorf("can't convert to timestamp (posgres type: %s)", srcTypeName)
	}
	return t, err
}

func convArray(spannerType ddl.ScalarType, srcTypeName string, location *time.Location, v string) (interface{}, error) {
	v = strings.TrimSpace(v)
	if v[0] != '{' || v[len(v)-1] != '}' {
		return []interface{}{}, fmt.Errorf("unrecognized data format for array: expected {v1, v2, ...}")
	}
	a := strings.Split(v[1:len(v)-1], ",")

	// The Spanner client for go does not accept []interface{} for arrays.
	// Instead it only accepts slices of a specific type e.g. []int64, []string.
	// Hence we have to do the following case analysis.
	switch spannerType.(type) {
	case ddl.Bool:
		var r []bool
		for _, s := range a {
			b, err := convBool(s)
			if err != nil {
				return []bool{}, err
			}
			r = append(r, b)
		}
		return r, nil
	case ddl.Bytes:
		var r [][]byte
		for _, s := range a {
			b, err := convBytes(s)
			if err != nil {
				return [][]byte{}, err
			}
			r = append(r, b)
		}
		return r, nil
	case ddl.Date:
		var r []civil.Date
		for _, s := range a {
			d, err := convDate(s)
			if err != nil {
				return []civil.Date{}, err
			}
			r = append(r, d)
		}
		return r, nil
	case ddl.Float64:
		var r []float64
		for _, s := range a {
			f, err := convFloat64(s)
			if err != nil {
				return []float64{}, err
			}
			r = append(r, f)
		}
		return r, nil
	case ddl.Int64:
		var r []int64
		for _, s := range a {
			i, err := convInt64(s)
			if err != nil {
				return r, err
			}
			r = append(r, i)
		}
		return r, nil
	case ddl.String:
		var r []string
		for _, s := range a {
			r = append(r, s)
		}
		return r, nil
	case ddl.Timestamp:
		var r []time.Time
		for _, s := range a {
			t, err := convTimestamp(srcTypeName, location, s)
			if err != nil {
				return []time.Time{}, err
			}
			r = append(r, t)
		}
		return r, nil
	}
	return []interface{}{}, fmt.Errorf("array type conversion not implemented for type %v", reflect.TypeOf(spannerType))
}

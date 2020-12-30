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

package postgres

import (
	"encoding/hex"
	"fmt"
	"math/big"
	"math/bits"
	"reflect"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
)

// ProcessDataRow converts a row of data and writes it out to Spanner.
// srcTable and srcCols are the source table and columns respectively,
// and vals contains string data to be converted to appropriate types
// to send to Spanner.  ProcessDataRow is only called in DataMode.
func ProcessDataRow(conv *internal.Conv, srcTable string, srcCols, vals []string) {
	spTable, spCols, spVals, err := ConvertData(conv, srcTable, srcCols, vals)
	if err != nil {
		conv.Unexpected(fmt.Sprintf("Error while converting data: %s\n", err))
		conv.StatsAddBadRow(srcTable, conv.DataMode())
		conv.CollectBadRow(srcTable, srcCols, vals)
	} else {
		conv.WriteRow(srcTable, spTable, spCols, spVals)
	}
}

// ConvertData maps the source DB data in vals into Spanner data,
// based on the Spanner and source DB schemas. Note that since entries
// in vals may be empty, we also return the list of columns (empty
// cols are dropped).
func ConvertData(conv *internal.Conv, srcTable string, srcCols []string, vals []string) (string, []string, []interface{}, error) {
	// Note: if there are many rows for the same srcTable/srcCols,
	// then the following functionality will be (redundantly)
	// repeated for every row converted. If this becomes a
	// performance issue, we could consider moving this block of
	// code to the callers of ConverData to avoid the redundancy.
	spTable, err := internal.GetSpannerTable(conv, srcTable)
	if err != nil {
		return "", []string{}, []interface{}{}, fmt.Errorf("can't map source table %s", srcTable)
	}
	spCols, err := internal.GetSpannerCols(conv, srcTable, srcCols)
	if err != nil {
		return "", []string{}, []interface{}{}, fmt.Errorf("can't map source columns %v", srcCols)
	}
	spSchema, ok1 := conv.SpSchema[spTable]
	srcSchema, ok2 := conv.SrcSchema[srcTable]
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
		spColDef, ok1 := spSchema.ColDefs[spCol]
		srcColDef, ok2 := srcSchema.ColDefs[srcCol]
		if !ok1 || !ok2 {
			return "", []string{}, []interface{}{}, fmt.Errorf("can't find Spanner and source-db schema for col %s", spCol)
		}
		var x interface{}
		var err error
		if spColDef.T.IsArray {
			x, err = convArray(spColDef.T, srcColDef.Type.Name, conv.Location, vals[i])
		} else {
			x, err = convScalar(spColDef.T, srcColDef.Type.Name, conv.Location, vals[i])
		}
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
func convScalar(spannerType ddl.Type, srcTypeName string, location *time.Location, val string) (interface{}, error) {
	// Whitespace within the val string is considered part of the data value.
	// Note that many of the underlying conversions functions we use (like
	// strconv.ParseFloat and strconv.ParseInt) return "invalid syntax"
	// errors if whitespace were to appear at the start or end of a string.
	// We do not expect pg_dump to generate such output.
	switch spannerType.Name {
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
	case ddl.Numeric:
		return convNumeric(val)
	case ddl.String:
		return val, nil
	case ddl.Timestamp:
		return convTimestamp(srcTypeName, location, val)
	default:
		return val, fmt.Errorf("data conversion not implemented for type %v", spannerType.Name)
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
	if val[0:2] != `\x` {
		return []byte{}, fmt.Errorf("can't convert to bytes: doesn't start with \\x prefix")
	}
	b, err := hex.DecodeString(val[2:])
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

// convNumeric maps a source database string value (representing a numeric)
// into a string representing a valid Spanner numeric.
// Ideally we would just return a *big.Rat, but spanner.Mutation
// doesn't currently support use of *big.Rat.
// TODO: return *big.Rat when client library supports it.
func convNumeric(val string) (string, error) {
	r := new(big.Rat)
	if _, ok := r.SetString(val); !ok {
		return "", fmt.Errorf("can't convert %q to big.Rat", val)
	}
	return spanner.NumericString(r), nil
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
	if srcTypeName == "timestamptz" || srcTypeName == "timestamp with time zone" {
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

// convArray converts a source database string value (representing an
// array) to an appropriate Spanner array value. It is the caller's
// responsibility to detect and handle the case where the entire array
// is NULL. However, convArray does handle the case where individual
// array elements are NULL. In other words, convArray handles "{1,
// NULL, 2}", but it does not handle "NULL" (it returns error).
func convArray(spannerType ddl.Type, srcTypeName string, location *time.Location, v string) (interface{}, error) {
	v = strings.TrimSpace(v)
	// Handle empty array. Note that we use an empty NullString array
	// for all Spanner array types since this will be converted to the
	// appropriate type by the Spanner client.
	if v == "{}" {
		return []spanner.NullString{}, nil
	}
	if v[0] != '{' || v[len(v)-1] != '}' {
		return []interface{}{}, fmt.Errorf("unrecognized data format for array: expected {v1, v2, ...}")
	}
	a := strings.Split(v[1:len(v)-1], ",")

	// The Spanner client for go does not accept []interface{} for arrays.
	// Instead it only accepts slices of a specific type e.g. []int64, []string.
	// Hence we have to do the following case analysis.
	switch spannerType.Name {
	case ddl.Bool:
		var r []spanner.NullBool
		for _, s := range a {
			if s == "NULL" {
				r = append(r, spanner.NullBool{Valid: false})
				continue
			}
			s, err := processQuote(s)
			if err != nil {
				return []spanner.NullBool{}, err
			}
			b, err := convBool(s)
			if err != nil {
				return []spanner.NullBool{}, err
			}
			r = append(r, spanner.NullBool{Bool: b, Valid: true})
		}
		return r, nil
	case ddl.Bytes:
		var r [][]byte
		for _, s := range a {
			if s == "NULL" {
				r = append(r, nil)
				continue
			}
			s, err := processQuote(s)
			if err != nil {
				return [][]byte{}, err
			}
			b, err := convBytes(s)
			if err != nil {
				return [][]byte{}, err
			}
			r = append(r, b)
		}
		return r, nil
	case ddl.Date:
		var r []spanner.NullDate
		for _, s := range a {
			if s == "NULL" {
				r = append(r, spanner.NullDate{Valid: false})
				continue
			}
			s, err := processQuote(s)
			if err != nil {
				return []spanner.NullDate{}, err
			}
			d, err := convDate(s)
			if err != nil {
				return []spanner.NullDate{}, err
			}
			r = append(r, spanner.NullDate{Date: d, Valid: true})
		}
		return r, nil
	case ddl.Float64:
		var r []spanner.NullFloat64
		for _, s := range a {
			if s == "NULL" {
				r = append(r, spanner.NullFloat64{Valid: false})
				continue
			}
			s, err := processQuote(s)
			if err != nil {
				return []spanner.NullFloat64{}, err
			}
			f, err := convFloat64(s)
			if err != nil {
				return []spanner.NullFloat64{}, err
			}
			r = append(r, spanner.NullFloat64{Float64: f, Valid: true})
		}
		return r, nil
	case ddl.Int64:
		var r []spanner.NullInt64
		for _, s := range a {
			if s == "NULL" {
				r = append(r, spanner.NullInt64{Valid: false})
				continue
			}
			s, err := processQuote(s)
			if err != nil {
				return []spanner.NullInt64{}, err
			}
			i, err := convInt64(s)
			if err != nil {
				return r, err
			}
			r = append(r, spanner.NullInt64{Int64: i, Valid: true})
		}
		return r, nil
	case ddl.String:
		var r []spanner.NullString
		for _, s := range a {
			if s == "NULL" {
				r = append(r, spanner.NullString{Valid: false})
				continue
			}
			s, err := processQuote(s)
			if err != nil {
				return []spanner.NullString{}, err
			}
			r = append(r, spanner.NullString{StringVal: s, Valid: true})
		}
		return r, nil
	case ddl.Timestamp:
		var r []spanner.NullTime
		for _, s := range a {
			if s == "NULL" {
				r = append(r, spanner.NullTime{Valid: false})
				continue
			}
			s, err := processQuote(s)
			if err != nil {
				return []spanner.NullTime{}, err
			}
			t, err := convTimestamp(srcTypeName, location, s)
			if err != nil {
				return []spanner.NullTime{}, err
			}
			r = append(r, spanner.NullTime{Time: t, Valid: true})
		}
		return r, nil
	}
	return []interface{}{}, fmt.Errorf("array type conversion not implemented for type %v", reflect.TypeOf(spannerType))
}

// processQuote returns the unquoted version of s.
// Note: The element values of PostgreSQL arrays may have double
// quotes around them.  The array output routine will put double
// quotes around element values if they are empty strings, contain
// curly braces, delimiter characters, double quotes, backslashes, or
// white space, or match the word NULL. Double quotes and backslashes
// embedded in element values will be backslash-escaped.  See section
// 8.14.6.of www.postgresql.org/docs/9.1/arrays.html.
func processQuote(s string) (string, error) {
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		return strconv.Unquote(s)
	}
	return s, nil
}

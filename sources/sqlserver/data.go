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

package sqlserver

import (
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	"github.com/cloudspannerecosystem/harbourbridge/common/constants"
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/schema"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
	"github.com/cloudspannerecosystem/harbourbridge/transformation"
)

// ProcessDataRow converts a row of data and writes it out to Spanner.
// srcTable and srcCols are the source table and columns respectively,
// and vals contains string data to be converted to appropriate types
// to send to Spanner.  ProcessDataRow is only called in DataMode.
func ProcessDataRow(conv *internal.Conv, tableId string, colIds []string, srcSchema schema.Table, spSchema ddl.CreateTable, vals []string, additionalAttributes internal.AdditionalDataAttributes, mapSrcColIdToVal map[string]string) {
	spTableName, cvtCols, cvtVals, err := ConvertData(conv, tableId, colIds, srcSchema, spSchema, vals)
	srcTableName := srcSchema.Name
	srcCols := []string{}
	for _, colId := range colIds {
		srcCols = append(srcCols, srcSchema.ColDefs[colId].Name)
	}
	if err != nil {
		conv.Unexpected(fmt.Sprintf("Error while converting data: %s\n", err))
		conv.StatsAddBadRow(srcTableName, conv.DataMode())
		conv.CollectBadRow(srcTableName, srcCols, vals)
		return
	}
	toddl := InfoSchemaImpl{}.GetToDdl()
	cvtCols, cvtVals, err = transformation.ProcessTransformation(conv, tableId, cvtCols, cvtVals, mapSrcColIdToVal, toddl, additionalAttributes)
	if err != nil {
		conv.Unexpected(fmt.Sprintf("Error while transforming data: %s\n", err))
		conv.StatsAddBadRow(srcTableName, conv.DataMode())
		conv.CollectBadRow(srcTableName, srcCols, vals)
		return
	}
	if cvtVals != nil {
		conv.WriteRow(srcTableName, spTableName, cvtCols, cvtVals)
	}
}

// ConvertData maps the source DB data in vals into Spanner data,
// based on the Spanner and source DB schemas. Note that since entries
// in vals may be empty, we also return the list of columns (empty
// cols are dropped).
func ConvertData(conv *internal.Conv, tableId string, colIds []string, srcSchema schema.Table, spSchema ddl.CreateTable, vals []string) (string, []string, []interface{}, error) {
	var c []string
	var v []interface{}
	if len(colIds) != len(vals) {
		return "", []string{}, []interface{}{}, fmt.Errorf("ConvertData: colId and vals don't all have the same lengths: len(colIds)=%d, len(vals)=%d", len(colIds), len(vals))
	}
	for i, colId := range colIds {
		// Skip columns with 'NULL' values.
		if vals[i] == "NULL" {
			continue
		}

		spColDef, ok1 := spSchema.ColDefs[colId]
		srcColDef, ok2 := srcSchema.ColDefs[colId]
		if !ok1 || !ok2 {
			return "", []string{}, []interface{}{}, fmt.Errorf("can't find Spanner and source-db schema for colId %s", colId)
		}
		var x interface{}
		var err error
		x, err = convScalar(conv, spColDef.T, srcColDef.Type.Name, conv.TimezoneOffset, vals[i])
		if err != nil {
			return "", []string{}, []interface{}{}, err
		}
		v = append(v, x)
		c = append(c, colId)
	}
	return spSchema.Name, c, v, nil
}

// convScalar converts a source database string value to an
// appropriate Spanner value. It is the caller's responsibility to
// detect and handle NULL values: convScalar will return error if a
// NULL value is passed.
func convScalar(conv *internal.Conv, spannerType ddl.Type, srcTypeName string, timezoneOffset string, val string) (interface{}, error) {
	// Whitespace within the val string is considered part of the data value.
	// Note that many of the underlying conversions functions we use (like
	// strconv.ParseFloat and strconv.ParseInt) return "invalid syntax"
	// errors if whitespace were to appear at the start or end of a string.
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
		return convNumeric(conv, val)
	case ddl.String:
		return val, nil
	case ddl.Timestamp:
		return convTimestamp(srcTypeName, val)
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
	// convert a string to a byte slice.
	b := []byte(val)
	return b, nil
}

func convDate(val string) (civil.Date, error) {
	date := strings.Fields(val)
	d, err := civil.ParseDate(date[0])
	if err != nil {
		return d, fmt.Errorf("can't convert to date: %w", err)
	}
	return d, err
}

func convFloat64(val string) (float64, error) {
	float, err := strconv.ParseFloat(val, 64)
	if err != nil {
		return float, fmt.Errorf("can't convert to float64: %w", err)
	}
	return float, err
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

// convTimestamp maps a source DB datetime types to Spanner timestamp
func convTimestamp(srcTypeName string, val string) (t time.Time, err error) {
	// the query returns the datetime in ISO8601
	// e.g. 2021-12-15T07:39:52.943 			(datetime)
	// e.g. 2021-12-15T07:39:52.9433333 		(datetime2)
	// e.g. 2021-12-15T07:40:00 				(smalldatetime)
	// e.g. 2021-12-08T03:00:52.9500000+01:00 	(datetimeoffset)

	if srcTypeName == dateTimeOffsetType {
		t, err = time.Parse(time.RFC3339, val)
	} else {
		t, err = time.Parse("2006-01-02T15:04:05", val)
	}
	if err != nil {
		return t, fmt.Errorf("can't convert to timestamp (mssql type: %s)", srcTypeName)
	}
	return t, err
}

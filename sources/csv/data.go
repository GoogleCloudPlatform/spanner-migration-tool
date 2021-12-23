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

package csv

import (
	csvReader "encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/big"
	"os"
	"strconv"
	"time"

	"cloud.google.com/go/civil"
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/schema"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
)

type Column struct {
	Column_name string `json:"column_name"`
	Type_name   string `json:"type_name"`
}

// Harbourbridge accepts a manifest file in the form of a json which unmarshalls into the Table struct.
type Table struct {
	Table_name    string   `json:"table_name"`
	File_patterns []string `json:"file_patterns"`
	Columns       []Column `json:"columns"`
}

// LoadManifest reads the manifest file and unmarshalls it into a list of Table struct.
// It also performs certain checks on the manifest.
func LoadManifest(conv *internal.Conv, manifestFile string) ([]Table, error) {
	manifest, err := ioutil.ReadFile(manifestFile)
	if err != nil {
		return nil, fmt.Errorf("can't read manifest file due to: %v", err)
	}
	tables := []Table{}
	err = json.Unmarshal(manifest, &tables)
	if err != nil {
		return nil, fmt.Errorf("unable to unmarshall json due to: %v", err)
	}
	err = VerifyManifest(conv, tables)
	if err != nil {
		return nil, fmt.Errorf("manifest is incomplete: %v", err)
	}
	return tables, nil
}

// VerifyManifest performs certain prechecks on the structure of the manifest while populating the conv with
// the ddl types. Also checks on valid file paths and empty CSVs are handled as conv.Unexpected errors later during processing.
func VerifyManifest(conv *internal.Conv, tables []Table) error {
	if len(tables) == 0 {
		return fmt.Errorf("no tables found")
	}
	for i, table := range tables {
		name := table.Table_name
		if name == "" {
			return fmt.Errorf("table number %d (0-indexed) does not have a name", i)
		}
		if len(table.File_patterns) == 0 {
			return fmt.Errorf("no file path provided for table %s", name)
		}
		cols := table.Columns
		if len(cols) == 0 {
			return fmt.Errorf("`columns` field for table %s is empty", name)
		}
		// Populating just the table names in the conv for SrcSchema and SpSchema
		// so the report for row stats is generated.
		conv.SrcSchema[table.Table_name] = schema.Table{Name: table.Table_name}

		// The map colDefs stores the mapping from column names to their final types.
		colDefs := make(map[string]ddl.ColumnDef)
		for j, col := range cols {
			if col.Column_name == "" || col.Type_name == "" {
				return fmt.Errorf("please provide column_name and type_name in `columns` field at position %d (0-indexed)", j)
			}
			ty, err := ToSpannerType(col.Type_name)
			if err != nil {
				return fmt.Errorf("can't map to spanner type: %v. Please use the data types as in your spanner database", err)
			}
			colDefs[col.Column_name] = ddl.ColumnDef{Name: col.Column_name, T: ty}
		}
		conv.SpSchema[table.Table_name] = ddl.CreateTable{Name: table.Table_name, ColDefs: colDefs}
	}
	return nil
}

// SetRowStats calculates the number of rows per table.
func SetRowStats(conv *internal.Conv, tables []Table, delimiter rune) {
	for _, table := range tables {
		for _, filePath := range table.File_patterns {
			csvFile, err := os.Open(filePath)
			if err != nil {
				fmt.Printf("can't read csv file: %s due to: %v\n", filePath, err)
			}
			r := csvReader.NewReader(csvFile)
			r.Comma = delimiter
			count, err := getCSVRowCount(r)
			if err != nil {
				conv.Unexpected(fmt.Sprintf("Couldn't get number of rows for table %s", table.Table_name))
				continue
			}
			if count == 0 {
				conv.Unexpected(fmt.Sprintf("File %s is empty.", filePath))
				continue
			}
			conv.Stats.Rows[table.Table_name] += count - 1
		}
	}
}

// getCSVRowCount returns the number of rows in the CSV file.
func getCSVRowCount(r *csvReader.Reader) (int64, error) {
	count := int64(0)
	for {
		_, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, fmt.Errorf("can't read row")
		}
		count++
	}
	return count, nil
}

// ProcessCSV writes data across the tables provided in the manifest file. Each table's data can be provided
// across multiple CSV files hence, the manifest accepts a list of file paths in the input.
func ProcessCSV(conv *internal.Conv, tables []Table, nullStr string, delimiter rune) error {
	for _, table := range tables {
		for _, filePath := range table.File_patterns {
			csvFile, err := os.Open(filePath)
			if err != nil {
				return fmt.Errorf(fmt.Sprintf("can't read csv file: %s due to: %v\n", filePath, err))
			}
			r := csvReader.NewReader(csvFile)
			r.Comma = delimiter

			// First row is expected to be the column headers.
			srcCols, err := r.Read()
			if err == io.EOF {
				conv.Unexpected(fmt.Sprintf("File %s is empty.", filePath))
				continue
			}
			if err != nil {
				return fmt.Errorf("can't read csv headers for col names due to: %v", err)
			}
			for {
				values, err := r.Read()
				if err == io.EOF {
					break
				}
				if err != nil {
					return fmt.Errorf(fmt.Sprintf("can't read row  names due to: %v", err))
				}
				processDataRow(conv, nullStr, table.Table_name, srcCols, values)
			}
		}
	}
	return nil
}

// processDataRow converts a row into go data types as per the client libs.
func processDataRow(conv *internal.Conv, nullStr, tableName string, srcCols []string, values []string) {
	// Pass nullStr from source-profile.
	cvtCols, cvtVals, err := convertData(conv, nullStr, tableName, srcCols, values)
	if err != nil {
		conv.Unexpected(fmt.Sprintf("Error while converting data: %s\n", err))
		conv.StatsAddBadRow(tableName, conv.DataMode())
		conv.CollectBadRow(tableName, srcCols, values)
	} else {
		conv.WriteRow(tableName, tableName, cvtCols, cvtVals)
	}
}

// convertData currently only supports scalar data types.
func convertData(conv *internal.Conv, nullStr, tableName string, srcCols []string, values []string) ([]string, []interface{}, error) {
	var v []interface{}
	var cvtCols []string
	colDefs := conv.SpSchema[tableName].ColDefs
	for i, val := range values {
		if val == nullStr {
			continue
		}
		colName := srcCols[i]
		x, err := convScalar(colDefs[colName].T, val)
		if err != nil {
			return nil, nil, err
		}
		v = append(v, x)
		cvtCols = append(cvtCols, colName)
	}
	return cvtCols, v, nil
}

func convScalar(spannerType ddl.Type, val string) (interface{}, error) {
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
		return convTimestamp(val)
	case ddl.JSON:
		return val, nil
	default:
		return val, fmt.Errorf("data conversion not implemented for type %v", spannerType)
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
func convNumeric(val string) (interface{}, error) {
	r := new(big.Rat)
	if _, ok := r.SetString(val); !ok {
		return "", fmt.Errorf("can't convert %q to big.Rat", val)
	}
	return r, nil
}

func convTimestamp(val string) (t time.Time, err error) {
	t, err = time.Parse("2006-01-02 15:04:05", val)
	if err != nil {
		return t, fmt.Errorf("can't convert to timestamp: %s", val)
	}
	return t, err
}

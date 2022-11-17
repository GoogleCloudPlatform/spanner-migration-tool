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
	"strings"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	"github.com/cloudspannerecosystem/harbourbridge/common/constants"
	"github.com/cloudspannerecosystem/harbourbridge/common/utils"
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/profiles"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
)

// GetCSVFiles finds the appropriate files paths and downloads gcs files in any.
func GetCSVFiles(conv *internal.Conv, sourceProfile profiles.SourceProfile) (tables []utils.ManifestTable, err error) {
	// If manifest file not provided, we assume the csvs exist in the same directory
	// in table_name.csv format.
	if sourceProfile.Csv.Manifest == "" {
		fmt.Println("Manifest file not provided, checking for files named `[table_name].csv` in current working directory...")
		for t := range conv.SpSchema {
			tables = append(tables, utils.ManifestTable{Table_name: t, File_patterns: []string{fmt.Sprintf("%s.csv", t)}})
		}
	} else {
		fmt.Println("Manifest file provided, reading csv file paths...")
		// Read paths provided in manifest.
		tables, err = loadManifest(conv, sourceProfile.Csv.Manifest)
		if err != nil {
			return nil, err
		}
	}

	// Download gcs files if any.
	tables, err = utils.PreloadGCSFiles(tables)
	if err != nil {
		return nil, fmt.Errorf("gcs file download error: %v", err)
	}
	return tables, nil
}

// loadManifest reads the manifest file and unmarshalls it into a list of Table struct.
// It also performs certain checks on the manifest.
func loadManifest(conv *internal.Conv, manifestFile string) ([]utils.ManifestTable, error) {
	manifest, err := ioutil.ReadFile(manifestFile)
	if err != nil {
		return nil, fmt.Errorf("can't read manifest file due to: %v", err)
	}
	tables := []utils.ManifestTable{}
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
func VerifyManifest(conv *internal.Conv, tables []utils.ManifestTable) error {
	if len(tables) == 0 {
		return fmt.Errorf("no tables found")
	}
	missing := []string{}
	for _, v := range conv.SrcSchema {
		found := false
		for _, table := range tables {
			if v.Name == table.Table_name {
				found = true
				break
			}
		}
		if !found {
			missing = append(missing, v.Name)
		}
	}
	if len(missing) > 0 {
		fmt.Printf("WARNING: did not find manifest entries for tables [ %s ], ignoring and proceeding...\n", strings.Join(missing, ", "))
		conv.Unexpected(fmt.Sprintf("did not find manifest entries for tables [ %s ]", strings.Join(missing, ", ")))
	}
	for i, table := range tables {
		name := table.Table_name
		if name == "" {
			return fmt.Errorf("table number %d (0-indexed) does not have a name", i)
		}
		_, err := internal.GetTableIdFromSrcName(conv.SrcSchema, name)
		if err != nil {
			return fmt.Errorf("table %s provided in manifest does not exist in spanner", name)
		}
		if len(table.File_patterns) == 0 {
			return fmt.Errorf("no file path provided for table %s", name)
		}
	}
	return nil
}

// SetRowStats calculates the number of rows per table.
func SetRowStats(conv *internal.Conv, tables []utils.ManifestTable, delimiter rune) error {
	for _, table := range tables {
		for _, filePath := range table.File_patterns {
			csvFile, err := os.Open(filePath)
			if err != nil {
				return fmt.Errorf("can't read csv file: %s due to: %v", filePath, err)
			}
			r := csvReader.NewReader(csvFile)
			r.Comma = delimiter

			tableId := internal.GetTableIdFromSpName(conv.SpSchema, table.Table_name)
			if tableId == "" {
				return fmt.Errorf("table Id not found for spanner table %v", table.Table_name)
			}
			colNames := []string{}
			for _, colIds := range conv.SpSchema[tableId].ColIds {
				colNames = append(colNames, conv.SpSchema[tableId].ColDefs[colIds].Name)
			}
			count, err := getCSVDataRowCount(r, colNames)
			if err != nil {
				return fmt.Errorf("error reading file %s for table %s: %v", filePath, table.Table_name, err)
			}
			if count == 0 {
				conv.Unexpected(fmt.Sprintf("error processing table %s: file %s is empty.", table.Table_name, filePath))
				continue
			}
			conv.Stats.Rows[table.Table_name] += count
		}
	}
	return nil
}

// getCSVDataRowCount returns the number of data rows in the CSV file. This excludes the headers if present.
func getCSVDataRowCount(r *csvReader.Reader, colNames []string) (int64, error) {
	count := int64(0)
	srcCols, err := r.Read()
	if err == io.EOF {
		return count, nil
	}
	if err != nil {
		return count, fmt.Errorf("can't read csv headers for col names due to: %v", err)
	}
	if len(srcCols) != len(colNames) {
		return 0, fmt.Errorf("found %d columns in csv, expected %d as per Spanner schema", len(srcCols), len(colNames))
	}
	// If the row read was not a header, increase count.
	if !utils.CheckEqualSets(srcCols, colNames) {
		count += 1
	}
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
func ProcessCSV(conv *internal.Conv, tables []utils.ManifestTable, nullStr string, delimiter rune) error {
	orderedTableIds := ddl.OrderTables(conv.SpSchema)
	nameToFiles := map[string][]string{}
	for _, table := range tables {
		nameToFiles[table.Table_name] = table.File_patterns
	}
	orderedTables := []utils.ManifestTable{}
	for _, id := range orderedTableIds {
		orderedTables = append(orderedTables, utils.ManifestTable{conv.SpSchema[id].Name, nameToFiles[conv.SpSchema[id].Name]})
	}

	for _, table := range orderedTables {
		for _, filePath := range table.File_patterns {
			csvFile, err := os.Open(filePath)
			if err != nil {
				return fmt.Errorf(fmt.Sprintf("can't read csv file: %s due to: %v\n", filePath, err))
			}
			r := csvReader.NewReader(csvFile)
			r.Comma = delimiter

			// Default column order is same as in Spanner schema.
			tableId := internal.GetTableIdFromSpName(conv.SpSchema, table.Table_name)
			if tableId == "" {
				return fmt.Errorf("table Id not found for spanner table %v", table.Table_name)
			}

			colNames := []string{}
			for _, v := range conv.SpSchema[tableId].ColIds {
				colNames = append(colNames, conv.SpSchema[tableId].ColDefs[v].Name)
			}

			srcCols, err := r.Read()
			if err == io.EOF {
				conv.Unexpected(fmt.Sprintf("error processing table %s: file %s is empty.", table.Table_name, filePath))
				continue
			}
			if err != nil {
				return fmt.Errorf("can't read row for %s due to: %v", filePath, err)
			}
			// If first row is some permutation of Spanner schema columns, we assume the first row is headers.
			if utils.CheckEqualSets(srcCols, colNames) {
				colNames = srcCols
			} else {
				// Write the first row since it was not a column header.
				processDataRow(conv, nullStr, table.Table_name, colNames, srcCols)
			}

			for {
				values, err := r.Read()
				if err == io.EOF {
					break
				}
				if err != nil {
					return fmt.Errorf("can't read row for %s due to: %v", filePath, err)
				}
				processDataRow(conv, nullStr, table.Table_name, colNames, values)
			}
		}
		if conv.DataFlush != nil {
			conv.DataFlush()
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

	tableId := internal.GetTableIdFromSpName(conv.SpSchema, tableName)
	if tableId == "" {
		return cvtCols, v, fmt.Errorf("table Id not found for spanner table %v", tableName)
	}

	colDefs := conv.SpSchema[tableId].ColDefs
	for i, val := range values {
		if val == nullStr {
			continue
		}
		colName := srcCols[i]
		colId := internal.GetColIdFromSpName(conv.SpSchema[tableId].ColDefs, colName)
		if colId == "" {
			return cvtCols, v, fmt.Errorf("column Id not found for spanner table %v column %v", tableName, colName)
		}
		spColDef := colDefs[colId]

		var x interface{}
		var err error
		if spColDef.T.IsArray {
			x, err = convArray(spColDef.T, val)
		} else {
			x, err = convScalar(conv, spColDef.T, val)
		}
		if err != nil {
			return nil, nil, err
		}
		v = append(v, x)
		cvtCols = append(cvtCols, colName)
	}
	return cvtCols, v, nil
}

func convArray(spannerType ddl.Type, val string) (interface{}, error) {
	val = strings.TrimSpace(val)
	// Handle empty array. Note that we use an empty NullString array
	// for all Spanner array types since this will be converted to the
	// appropriate type by the Spanner client.
	if val == "{}" || val == "[]" {
		return []spanner.NullString{}, nil
	}
	braces := val[:1] + val[len(val)-1:]
	if braces != "{}" && braces != "[]" {
		return []interface{}{}, fmt.Errorf("unrecognized data format for array: expected {v1, v2, ...} or [v1, v2, ...]")
	}
	a := strings.Split(val[1:len(val)-1], ",")

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
			date, err := convDate(s)
			if err != nil {
				return []spanner.NullDate{}, err
			}
			r = append(r, spanner.NullDate{Date: date, Valid: true})
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
	case ddl.Numeric:
		var r []spanner.NullNumeric
		for _, s := range a {
			if s == "NULL" {
				r = append(r, spanner.NullNumeric{Valid: false})
				continue
			}
			s, err := processQuote(s)
			if err != nil {
				return []spanner.NullNumeric{}, err
			}
			n, err := convNumeric(s)
			if err != nil {
				return []spanner.NullNumeric{}, err
			}
			r = append(r, spanner.NullNumeric{Numeric: n, Valid: true})
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
			t, err := convTimestamp(s)
			if err != nil {
				return []spanner.NullTime{}, err
			}
			r = append(r, spanner.NullTime{Time: t, Valid: true})
		}
		return r, nil
	}
	return []interface{}{}, fmt.Errorf("array type conversion not implemented for type []%v", spannerType.Name)
}

func convScalar(conv *internal.Conv, spannerType ddl.Type, val string) (interface{}, error) {
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
		if conv.TargetDb == constants.TargetExperimentalPostgres {
			return spanner.PGNumeric{Numeric: val, Valid: true}, nil
		}
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
func convNumeric(val string) (big.Rat, error) {
	r := new(big.Rat)
	if _, ok := r.SetString(val); !ok {
		return big.Rat{}, fmt.Errorf("can't convert %q to big.Rat", val)
	}
	return *r, nil
}

func convTimestamp(val string) (t time.Time, err error) {
	t, err = time.Parse("2006-01-02 15:04:05", val)
	if err != nil {
		return t, fmt.Errorf("can't convert to timestamp: %s", val)
	}
	return t, err
}

func processQuote(s string) (string, error) {
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		return strconv.Unquote(s)
	}
	return s, nil
}

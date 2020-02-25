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
	"fmt"
	"strconv"
)

// GetSpannerTable maps a source DB table name into a legal Spanner table
// name. Note that source DB column names can be essentially any string, but
// Spanner column names must use a limited character set. This means that
// getSpannerTable may have to change a name to make it legal, we must ensure
// that:
// a) the new table name is legal
// b) the new table name doesn't clash with other Spanner table names
// c) we consistently return the same name for this table.
func GetSpannerTable(conv *Conv, srcTable string) (string, error) {
	if srcTable == "" {
		return "", fmt.Errorf("bad parameter: table string is empty")
	}
	if sp, found := conv.toSpanner[srcTable]; found {
		return sp.name, nil
	}
	spTable, _ := FixName(srcTable)
	if _, found := conv.toSource[spTable]; found {
		// s has been used before i.e. FixName caused a collision.
		// Add unique postfix: use number of tables so far.
		// However, there is a chance this has already been used,
		// so need to iterate.
		id := len(conv.toSpanner)
		for {
			t := spTable + "_" + strconv.Itoa(id)
			if _, found := conv.toSource[t]; !found {
				spTable = t
				break
			}
			id++
		}
	}
	if spTable != srcTable {
		VerbosePrintf("Mapping source DB table %s to Spanner table %s\n", srcTable, spTable)
	}
	conv.toSpanner[srcTable] = nameAndCols{name: spTable, cols: make(map[string]string)}
	conv.toSource[spTable] = nameAndCols{name: srcTable, cols: make(map[string]string)}
	return spTable, nil
}

// GetSpannerCol maps a source DB table/column into a legal Spanner column
// name. If mustExist is true, we return error if the column is new.
// Note that source DB column names can be essentially any string, but
// Spanner column names must use a limited character set. This means that
// getSpannerCol may have to change a name to make it legal, we must ensure
// that:
// a) the new col name is legal
// b) the new col name doesn't clash with other col names in the same table
// c) we consistently return the same name for the same col.
func GetSpannerCol(conv *Conv, srcTable, srcCol string, mustExist bool) (string, error) {
	if srcTable == "" {
		return "", fmt.Errorf("bad parameter: table string is empty")
	}
	if srcCol == "" {
		return "", fmt.Errorf("bad parameter: col string is empty")
	}
	sp, found := conv.toSpanner[srcTable]
	if !found {
		return "", fmt.Errorf("unknown table %s", srcTable)
	}
	// Sanity check: do reverse mapping and check consistency.
	// Consider dropping this check.
	src, found := conv.toSource[sp.name]
	if !found || src.name != srcTable {
		return "", fmt.Errorf("internal error: table mapping inconsistency for table %s (%s)", srcTable, src.name)
	}
	if spCol, found := sp.cols[srcCol]; found {
		return spCol, nil
	}
	if mustExist {
		return "", fmt.Errorf("table %s does not have a column %s", srcTable, srcCol)
	}
	spCol, _ := FixName(srcCol)
	if _, found := conv.toSource[sp.name].cols[spCol]; found {
		// spCol has been used before i.e. FixName caused a collision.
		// Add unique postfix: use number of cols in this table so far.
		// However, there is a chance this has already been used,
		// so need to iterate.
		id := len(sp.cols)
		for {
			c := spCol + "_" + strconv.Itoa(id)
			if _, found := conv.toSource[sp.name].cols[c]; !found {
				spCol = c
				break
			}
			id++
		}
	}
	if spCol != srcCol {
		VerbosePrintf("Mapping source DB col %s (table %s) to Spanner col %s\n", srcCol, srcTable, spCol)
	}
	conv.toSpanner[srcTable].cols[srcCol] = spCol
	conv.toSource[sp.name].cols[spCol] = srcCol
	return spCol, nil
}

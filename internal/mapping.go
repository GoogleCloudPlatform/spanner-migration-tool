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
	if sp, found := conv.ToSpanner[srcTable]; found {
		return sp.Name, nil
	}
	spTable, _ := FixName(srcTable)
	if _, found := conv.ToSource[spTable]; found {
		// s has been used before i.e. FixName caused a collision.
		// Add unique postfix: use number of tables so far.
		// However, there is a chance this has already been used,
		// so need to iterate.
		id := len(conv.ToSpanner)
		for {
			t := spTable + "_" + strconv.Itoa(id)
			if _, found := conv.ToSource[t]; !found {
				spTable = t
				break
			}
			id++
		}
	}
	if spTable != srcTable {
		VerbosePrintf("Mapping source DB table %s to Spanner table %s\n", srcTable, spTable)
	}
	conv.ToSpanner[srcTable] = NameAndCols{Name: spTable, Cols: make(map[string]string)}
	conv.ToSource[spTable] = NameAndCols{Name: srcTable, Cols: make(map[string]string)}
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
	sp, found := conv.ToSpanner[srcTable]
	if !found {
		return "", fmt.Errorf("unknown table %s", srcTable)
	}
	// Sanity check: do reverse mapping and check consistency.
	// Consider dropping this check.
	src, found := conv.ToSource[sp.Name]
	if !found || src.Name != srcTable {
		return "", fmt.Errorf("internal error: table mapping inconsistency for table %s (%s)", srcTable, src.Name)
	}
	if spCol, found := sp.Cols[srcCol]; found {
		return spCol, nil
	}
	if mustExist {
		return "", fmt.Errorf("table %s does not have a column %s", srcTable, srcCol)
	}
	spCol, _ := FixName(srcCol)
	if _, found := conv.ToSource[sp.Name].Cols[spCol]; found {
		// spCol has been used before i.e. FixName caused a collision.
		// Add unique postfix: use number of cols in this table so far.
		// However, there is a chance this has already been used,
		// so need to iterate.
		id := len(sp.Cols)
		for {
			c := spCol + "_" + strconv.Itoa(id)
			if _, found := conv.ToSource[sp.Name].Cols[c]; !found {
				spCol = c
				break
			}
			id++
		}
	}
	if spCol != srcCol {
		VerbosePrintf("Mapping source DB col %s (table %s) to Spanner col %s\n", srcCol, srcTable, spCol)
	}
	conv.ToSpanner[srcTable].Cols[srcCol] = spCol
	conv.ToSource[sp.Name].Cols[spCol] = srcCol
	return spCol, nil
}

// GetSpannerCols maps a slice of source columns into their corresponding
// Spanner columns using GetSpannerCol.
func GetSpannerCols(conv *Conv, srcTable string, srcCols []string) ([]string, error) {
	var spCols []string
	for _, srcCol := range srcCols {
		spCol, err := GetSpannerCol(conv, srcTable, srcCol, false)
		if err != nil {
			return nil, err
		}
		spCols = append(spCols, spCol)
	}
	return spCols, nil
}

// GetSpannerKeyName maps source foreign key name to
// legal Spanner foreign key name.
// If the srcKeyName is empty string we can just return
// empty string without error.
// If the srcKeyName is not empty we need to make sure
// of the following things:
// a) the new foreign key name is legal
// b) the new foreign key name doesn't clash with other Spanner
//    foreign key names
// Note that foreign key constraint names in Spanner have to be globally unique
// (across the database). But in some source databases, such as PostgreSQL,
// they only have to be unique for a table. Hence we must map each source
// constraint name to a unique spanner constraint name.
func GetSpannerKeyName(srcKeyName string, schemaForeignKeys map[string]bool) string {
	if srcKeyName == "" {
		return ""
	}
	spKeyName, _ := FixName(srcKeyName)
	if _, found := schemaForeignKeys[spKeyName]; found {
		// spKeyName has been used before.
		// Add unique postfix: use number of foreign keys so far.
		// However, there is a chance this has already been used,
		// so need to iterate.
		id := len(schemaForeignKeys)
		for {
			c := spKeyName + "_" + strconv.Itoa(id)
			if _, found := schemaForeignKeys[c]; !found {
				spKeyName = c
				break
			}
			id++
		}
	}
	schemaForeignKeys[spKeyName] = true
	return spKeyName
}

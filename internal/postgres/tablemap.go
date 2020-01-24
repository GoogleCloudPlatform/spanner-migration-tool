// Copyright 2019 Google LLC
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
	"fmt"
	"harbourbridge/internal"
	"strconv"
)

// GetSpannerTable maps a PostgreSQL table name into a legal Spanner table
// name. Note that PostgreSQL column names can be essentially any string, but
// Spanner column names must use a limited character set. This means that
// getSpannerTable may have to change a name to make it legal, we must ensure
// that:
// a) the new table name is legal
// b) the new table name doesn't clash with other Spanner table names
// c) we consistently return the same name for this table.
func GetSpannerTable(conv *Conv, pgTable string) (string, error) {
	if pgTable == "" {
		return "", fmt.Errorf("Bad parameter: table string is empty")
	}
	if sp, found := conv.toSpanner[pgTable]; found {
		return sp.name, nil
	}
	spTable, _ := internal.FixName(pgTable)
	if _, found := conv.toPostgres[spTable]; found {
		// s has been used before i.e. FixName caused a collision.
		// Add unique postfix: use number of tables so far.
		// However, there is a chance this has already been used,
		// so need to iterate.
		id := len(conv.toSpanner)
		for {
			t := spTable + "_" + strconv.Itoa(id)
			if _, found := conv.toPostgres[t]; !found {
				spTable = t
				break
			}
			id++
		}
	}
	if spTable != pgTable {
		internal.VerbosePrintf("Mapping PostgreSQL table %s to Spanner table %s\n", pgTable, spTable)
	}
	conv.toSpanner[pgTable] = nameAndCols{name: spTable, cols: make(map[string]string)}
	conv.toPostgres[spTable] = nameAndCols{name: pgTable, cols: make(map[string]string)}
	return spTable, nil
}

// GetSpannerCol maps a PostgreSQL table/column into a legal Spanner column
// name. If mustExist is true, we return error if the column is new.
// Note that PostgreSQL column names can be essentially any string, but
// Spanner column names must use a limited character set. This means that
// getSpannerCol may have to change a name to make it legal, we must ensure
// that:
// a) the new col name is legal
// b) the new col name doesn't clash with other col names in the same table
// c) we consistently return the same name for the same col.
func GetSpannerCol(conv *Conv, pgTable, pgCol string, mustExist bool) (string, error) {
	if pgTable == "" {
		return "", fmt.Errorf("Bad parameter: table string is empty")
	}
	if pgCol == "" {
		return "", fmt.Errorf("Bad parameter: col string is empty")
	}
	sp, found := conv.toSpanner[pgTable]
	if !found {
		return "", fmt.Errorf("Unknown table %s", pgTable)
	}
	// Sanity check: do reverse mapping and check consistency.
	// Consider dropping this check.
	pg, found := conv.toPostgres[sp.name]
	if !found || pg.name != pgTable {
		return "", fmt.Errorf("Internal error: table mapping inconsistency for table %s (%s)", pgTable, pg.name)
	}
	if spCol, found := sp.cols[pgCol]; found {
		return spCol, nil
	}
	if mustExist {
		return "", fmt.Errorf("Table %s does not have a column %s", pgTable, pgCol)
	}
	spCol, _ := internal.FixName(pgCol)
	if _, found := conv.toPostgres[sp.name].cols[spCol]; found {
		// spCol has been used before i.e. FixName caused a collision.
		// Add unique postfix: use number of cols in this table so far.
		// However, there is a chance this has already been used,
		// so need to iterate.
		id := len(sp.cols)
		for {
			c := spCol + "_" + strconv.Itoa(id)
			if _, found := conv.toPostgres[sp.name].cols[c]; !found {
				spCol = c
				break
			}
			id++
		}
	}
	if spCol != pgCol {
		internal.VerbosePrintf("Mapping PostgreSQL col %s (table %s) to Spanner col %s\n", pgCol, pgTable, spCol)
	}
	conv.toSpanner[pgTable].cols[pgCol] = spCol
	conv.toPostgres[sp.name].cols[spCol] = pgCol
	return spCol, nil
}

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

package common

import (
	"fmt"
	"sort"

	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/schema"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
)

// ToNotNull returns true if a column is not nullable and false if it is.
func ToNotNull(conv *internal.Conv, isNullable string) bool {
	switch isNullable {
	case "YES":
		return false
	case "NO":
		return true
	}
	conv.Unexpected(fmt.Sprintf("isNullable column has unknown value: %s", isNullable))
	return false
}

// GetColsAndSchemas provides information about columns and schema for a table.
func GetColsAndSchemas(conv *internal.Conv, tableId string) (schema.Table, string, []string, ddl.CreateTable, error) {
	srcSchema := conv.SrcSchema[tableId]
	spTableName, err1 := internal.GetSpannerTable(conv, tableId)
	srcCols := []string{}
	for _, colId := range srcSchema.ColIds {
		srcCols = append(srcCols, srcSchema.ColDefs[colId].Name)
	}
	spCols, err2 := internal.GetSpannerCols(conv, tableId, srcCols)
	spSchema, ok := conv.SpSchema[tableId]
	var err error
	if err1 != nil || err2 != nil || !ok {
		err = fmt.Errorf(fmt.Sprintf("err1=%s, err2=%s, ok=%t", err1, err2, ok))
	}
	return srcSchema, spTableName, spCols, spSchema, err
}

func GetSortedTableIdsBySrcName(srcSchema map[string]schema.Table) []string {
	tableNameIdMap := map[string]string{}
	var tableNames, sortedTableIds []string
	for id, srcTable := range srcSchema {
		tableNames = append(tableNames, srcTable.Name)
		tableNameIdMap[srcTable.Name] = id
	}
	sort.Strings(tableNames)
	for _, name := range tableNames {
		sortedTableIds = append(sortedTableIds, tableNameIdMap[name])
	}
	return sortedTableIds
}

func initPrimaryKeyOrder(conv *internal.Conv) {
	for k, table := range conv.SrcSchema {
		for i := range table.PrimaryKeys {
			conv.SrcSchema[k].PrimaryKeys[i].Order = i + 1
		}
	}
}

func initIndexOrder(conv *internal.Conv) {
	for k, table := range conv.SrcSchema {
		for i, index := range table.Indexes {
			for j := range index.Keys {
				conv.SrcSchema[k].Indexes[i].Keys[j].Order = j + 1
			}
		}
	}
}

func IntersectionOfTwoStringSlices(a []string, b []string) []string {
	set := make([]string, 0)
	hash := make(map[string]struct{})

	for _, v := range a {
		hash[v] = struct{}{}
	}

	for _, v := range b {
		if _, ok := hash[v]; ok {
			set = append(set, v)
		}
	}

	return set
}

func RemoveSynthId(conv *internal.Conv, tableId string, colIds []string) []string {
	synthPk, found := conv.SyntheticPKeys[tableId]
	if !found {
		return colIds
	}
	for i, colId := range colIds {
		if synthPk.ColId == colId {
			colIds = append(colIds[:i], colIds[i+1:]...)
		}
	}

	return colIds
}

func PrepareColumns(conv *internal.Conv, tableId string, srcCols []string) ([]string, error) {
	spColIds := conv.SpSchema[tableId].ColIds
	srcColIds := []string{}
	for _, colName := range srcCols {
		colId, err := internal.GetColIdFromSrcName(conv.SrcSchema[tableId].ColDefs, colName)
		if err != nil {
			return []string{}, err
		}
		srcColIds = append(srcColIds, colId)
	}
	commonIds := IntersectionOfTwoStringSlices(spColIds, srcColIds)
	if len(commonIds) == 0 {
		return []string{}, fmt.Errorf("no common columns between source and spanner table")
	}
	return commonIds, nil
}

func PrepareValues[T interface{}](conv *internal.Conv, tableId string, colNameIdMap map[string]string, commonColIds, srcCols []string, values []T) ([]T, error) {
	if len(srcCols) != len(values) {
		return []T{}, fmt.Errorf("PrepareValues: srcCols and vals don't all have the same lengths: len(srcCols)=%d, len(values)=%d", len(srcCols), len(values))
	}
	var newValues []T
	mapColIdToVal := map[string]T{}
	for i, srcolName := range srcCols {
		mapColIdToVal[colNameIdMap[srcolName]] = values[i]
	}
	for _, id := range commonColIds {
		newValues = append(newValues, mapColIdToVal[id])
	}
	return newValues, nil
}

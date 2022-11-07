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
	"testing"

	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/schema"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
	"github.com/stretchr/testify/assert"
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
func GetColsAndSchemas(conv *internal.Conv, srcTable string) (schema.Table, string, []string, ddl.CreateTable, error) {
	srcSchema := conv.SrcSchema[srcTable]
	spTable, err1 := internal.GetSpannerTable(conv, srcTable)
	spCols, err2 := internal.GetSpannerCols(conv, srcTable, srcSchema.ColIds)
	spSchema, ok := conv.SpSchema[spTable]
	var err error
	if err1 != nil || err2 != nil || !ok {
		err = fmt.Errorf(fmt.Sprintf("err1=%s, err2=%s, ok=%t", err1, err2, ok))
	}
	return srcSchema, spTable, spCols, spSchema, err
}

func GetSpTableIdFromName(conv *internal.Conv, tableName string) string {
	for tableId, table := range conv.SpSchema {
		if tableName == table.Name {
			return tableId
		}
	}
	return ""
}

func GetSpColIdFromName(conv *internal.Conv, tableId, colName string) string {
	for colId, col := range conv.SpSchema[tableId].ColDefs {
		if col.Name == colName {
			return colId
		}
	}
	return ""
}

func getSpFkIdFromName(conv *internal.Conv, tableId, fkName string) string {
	for _, fk := range conv.SpSchema[tableId].ForeignKeys {
		if fk.Name == fkName {
			return fk.Id
		}
	}
	return ""
}

func getSpIndexFromName(conv *internal.Conv, tableId, indexName string) (ddl.CreateIndex, error) {
	for _, index := range conv.SpSchema[tableId].Indexes {
		if index.Name == indexName {
			return index, nil
		}
	}
	return ddl.CreateIndex{}, fmt.Errorf("spanner index not found")
}

func AssertSpSchema(conv *internal.Conv, t *testing.T, expectedSchema, actualSchema map[string]ddl.CreateTable) {
	assert.Equal(t, len(expectedSchema), len(actualSchema))
	for tableName, expectedTable := range expectedSchema {
		tableId := GetSpTableIdFromName(conv, tableName)
		assert.NotEqual(t, tableId, "")
		assertSpColDef(conv, t, tableId, expectedTable.ColDefs, actualSchema[tableId].ColDefs)
		assertSpPk(conv, t, tableId, expectedTable.PrimaryKeys, actualSchema[tableId].PrimaryKeys)
		assertSpFk(conv, t, tableId, expectedTable.ForeignKeys, actualSchema[tableId].ForeignKeys)
		assertSpIndexes(conv, t, tableId, expectedTable.Indexes, actualSchema[tableId].Indexes)
	}
}

func assertSpColDef(conv *internal.Conv, t *testing.T, tableId string, expectedColDef, actualColDef map[string]ddl.ColumnDef) {
	assert.Equal(t, len(expectedColDef), len(actualColDef))
	for colName, col := range expectedColDef {
		colId := GetSpColIdFromName(conv, tableId, colName)
		assert.NotEqual(t, colId, "")
		actualCol := actualColDef[colId]
		actualCol.Id = ""
		actualCol.Comment = ""
		assert.Equal(t, col, actualCol)
	}
}

func assertSpPk(conv *internal.Conv, t *testing.T, tableId string, expectedPks, actualPks []ddl.IndexKey) {
	assert.Equal(t, len(expectedPks), len(actualPks))
	for i, pk := range expectedPks {
		colId := GetSpColIdFromName(conv, tableId, pk.ColId)
		assert.NotEqual(t, colId, "")
		expectedPks[i].ColId = colId
	}
	assert.ElementsMatch(t, expectedPks, actualPks)
}
func assertSpFk(conv *internal.Conv, t *testing.T, tableId string, expectedFks, actualFks []ddl.Foreignkey) {
	assert.Equal(t, len(expectedFks), len(actualFks))
	for i, fk := range expectedFks {
		fkId := getSpFkIdFromName(conv, tableId, fk.Name)
		assert.NotEqual(t, fkId, "")
		expectedFks[i].Id = fkId
		referTableId := GetSpTableIdFromName(conv, fk.ReferTableId)
		assert.NotEqual(t, referTableId, "")
		expectedFks[i].ReferTableId = referTableId
		for j, col := range fk.ColIds {
			colId := GetSpColIdFromName(conv, tableId, col)
			assert.NotEqual(t, colId, "")
			expectedFks[i].ColIds[j] = colId
		}
		for j, col := range fk.ReferColumnIds {
			colId := GetSpColIdFromName(conv, referTableId, col)
			assert.NotEqual(t, colId, "")
			expectedFks[i].ReferColumnIds[j] = colId
		}
	}
	assert.ElementsMatch(t, expectedFks, actualFks)
}

func assertSpIndexes(conv *internal.Conv, t *testing.T, tableId string, expectedIndexes, actualIndexes []ddl.CreateIndex) {
	assert.Equal(t, len(expectedIndexes), len(actualIndexes))
	for _, index := range expectedIndexes {
		actualIndex, err := getSpIndexFromName(conv, tableId, index.Name)
		assert.Equal(t, err, nil)
		if index.TableId != "" {
			indexTableId := GetSpTableIdFromName(conv, index.TableId)
			assert.Equal(t, indexTableId, actualIndex.TableId)
		}
		assert.Equal(t, index.Unique, actualIndex.Unique)
		assert.Equal(t, len(index.Keys), len(actualIndex.Keys))
		for j, indexKey := range index.Keys {
			colId := GetSpColIdFromName(conv, tableId, indexKey.ColId)
			index.Keys[j].ColId = colId
		}
		assert.ElementsMatch(t, index.Keys, actualIndex.Keys)
		for j, storedColumn := range index.StoredColumnIds {
			colId := GetSpColIdFromName(conv, tableId, storedColumn)
			index.StoredColumnIds[j] = colId
		}
		assert.ElementsMatch(t, index.StoredColumnIds, actualIndex.StoredColumnIds)
	}
}

func AssertTableIssues(conv *internal.Conv, t *testing.T, tableId string, expectedIssues, actualIssues map[string][]internal.SchemaIssue) {
	assert.Equal(t, len(expectedIssues), len(actualIssues))
	for col, issues := range expectedIssues {
		colId := GetSpColIdFromName(conv, tableId, col)
		assert.ElementsMatch(t, issues, actualIssues[colId])
	}
}

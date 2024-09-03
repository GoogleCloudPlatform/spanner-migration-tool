// Copyright 2023 Google LLC
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
	"testing"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/schema"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
	"github.com/stretchr/testify/assert"
)

func AssertSpSchema(conv *Conv, t *testing.T, expectedSchema, actualSchema map[string]ddl.CreateTable) {
	assert.Equal(t, len(expectedSchema), len(actualSchema))
	for tableName, expectedTable := range expectedSchema {
		tableId, err := GetTableIdFromSpName(conv.SpSchema, tableName)
		assert.Equal(t, nil, err)
		assertSpColDef(conv, t, tableId, expectedTable.ColDefs, actualSchema[tableId].ColDefs)
		assertSpPk(conv, t, tableId, expectedTable.PrimaryKeys, actualSchema[tableId].PrimaryKeys)
		assertSpFk(conv, t, tableId, expectedTable.ForeignKeys, actualSchema[tableId].ForeignKeys)
		assertSpIndexes(conv, t, tableId, expectedTable.Indexes, actualSchema[tableId].Indexes)
	}
}

func assertSpColDef(conv *Conv, t *testing.T, tableId string, expectedColDef, actualColDef map[string]ddl.ColumnDef) {
	assert.Equal(t, len(expectedColDef), len(actualColDef))
	for colName, col := range expectedColDef {
		colId, err := GetColIdFromSpName(conv.SpSchema[tableId].ColDefs, colName)
		assert.Equal(t, nil, err)
		actualCol := actualColDef[colId]
		actualCol.Id = ""
		actualCol.Comment = ""
		assert.Equal(t, col, actualCol)
	}
}

func assertSpPk(conv *Conv, t *testing.T, tableId string, expectedPks, actualPks []ddl.IndexKey) {
	assert.Equal(t, len(expectedPks), len(actualPks))
	for i, pk := range expectedPks {
		colId, err := GetColIdFromSpName(conv.SpSchema[tableId].ColDefs, pk.ColId)
		assert.Equal(t, nil, err)
		expectedPks[i].ColId = colId
	}
	assert.ElementsMatch(t, expectedPks, actualPks)
}

func assertSpFk(conv *Conv, t *testing.T, tableId string, expectedFks, actualFks []ddl.Foreignkey) {
	assert.Equal(t, len(expectedFks), len(actualFks))
	for i, fk := range expectedFks {
		fkId := getFkIdFromSpName(conv.SpSchema[tableId].ForeignKeys, fk.Name)
		assert.NotEqual(t, fkId, "")
		expectedFks[i].Id = fkId
		referTableId, err := GetTableIdFromSpName(conv.SpSchema, fk.ReferTableId)
		assert.Equal(t, nil, err)
		expectedFks[i].ReferTableId = referTableId
		for j, col := range fk.ColIds {
			colId, err := GetColIdFromSpName(conv.SpSchema[tableId].ColDefs, col)
			assert.Equal(t, nil, err)
			expectedFks[i].ColIds[j] = colId
		}
		for j, col := range fk.ReferColumnIds {
			colId, err := GetColIdFromSpName(conv.SpSchema[referTableId].ColDefs, col)
			assert.Equal(t, nil, err)
			expectedFks[i].ReferColumnIds[j] = colId
		}
		assert.Equal(t, expectedFks[i].OnDelete, actualFks[i].OnDelete)
		assert.Equal(t, expectedFks[i].OnUpdate, actualFks[i].OnUpdate)
	}
	assert.ElementsMatch(t, expectedFks, actualFks)
}

func assertSpIndexes(conv *Conv, t *testing.T, tableId string, expectedIndexes, actualIndexes []ddl.CreateIndex) {
	assert.Equal(t, len(expectedIndexes), len(actualIndexes))
	for _, index := range expectedIndexes {
		actualIndex, err := getIndexFromSpName(conv.SpSchema[tableId].Indexes, index.Name)
		assert.Equal(t, err, nil)
		if index.TableId != "" {
			indexTableId, err := GetTableIdFromSpName(conv.SpSchema, index.TableId)
			assert.Equal(t, nil, err)
			assert.Equal(t, indexTableId, actualIndex.TableId)
		}
		assert.Equal(t, index.Unique, actualIndex.Unique)
		assert.Equal(t, len(index.Keys), len(actualIndex.Keys))
		for j, indexKey := range index.Keys {
			colId, err := GetColIdFromSpName(conv.SpSchema[tableId].ColDefs, indexKey.ColId)
			assert.Equal(t, nil, err)
			index.Keys[j].ColId = colId
		}
		assert.ElementsMatch(t, index.Keys, actualIndex.Keys)
		for j, storedColumn := range index.StoredColumnIds {
			colId, err := GetColIdFromSpName(conv.SpSchema[tableId].ColDefs, storedColumn)
			assert.Equal(t, nil, err)
			index.StoredColumnIds[j] = colId
		}
		assert.ElementsMatch(t, index.StoredColumnIds, actualIndex.StoredColumnIds)
	}
}

func getFkIdFromSpName(fks []ddl.Foreignkey, fkName string) string {
	for _, fk := range fks {
		if fk.Name == fkName {
			return fk.Id
		}
	}
	return ""
}

func getIndexFromSpName(indexes []ddl.CreateIndex, indexName string) (ddl.CreateIndex, error) {
	for _, index := range indexes {
		if index.Name == indexName {
			return index, nil
		}
	}
	return ddl.CreateIndex{}, fmt.Errorf("spanner index not found")
}

func AssertSrcSchema(t *testing.T, conv *Conv, expectedSchema, actualSchema map[string]schema.Table) {
	assert.Equal(t, len(expectedSchema), len(actualSchema))
	for tableName, expectedTable := range expectedSchema {
		tableId, _ := GetTableIdFromSrcName(conv.SrcSchema, tableName)
		assert.NotEqual(t, tableId, "")
		assertSrcColDef(t, conv, tableId, expectedTable.ColDefs, actualSchema[tableId].ColDefs)
		assertSrcPk(t, conv, tableId, expectedTable.PrimaryKeys, actualSchema[tableId].PrimaryKeys)
		assertSrcFk(t, conv, tableId, expectedTable.ForeignKeys, actualSchema[tableId].ForeignKeys)
		assertSrcIndexes(t, conv, tableId, expectedTable.Indexes, actualSchema[tableId].Indexes)
	}
}

func assertSrcColDef(t *testing.T, conv *Conv, tableId string, expectedColDef, actualColDef map[string]schema.Column) {
	assert.Equal(t, len(expectedColDef), len(actualColDef))
	for colName, col := range expectedColDef {
		colId := conv.SrcSchema[tableId].ColNameIdMap[colName]
		assert.NotEqual(t, colId, "")
		actualCol := actualColDef[colId]
		actualCol.Id = ""
		assert.Equal(t, col, actualCol)
	}
}

func assertSrcPk(t *testing.T, conv *Conv, tableId string, expectedPks, actualPks []schema.Key) {
	assert.Equal(t, len(expectedPks), len(actualPks))
	for i, pk := range expectedPks {
		colId := conv.SrcSchema[tableId].ColNameIdMap[pk.ColId]
		assert.NotEqual(t, colId, "")
		expectedPks[i].ColId = colId
	}
	assert.ElementsMatch(t, expectedPks, actualPks)
}

func assertSrcFk(t *testing.T, conv *Conv, tableId string, expectedFks, actualFks []schema.ForeignKey) {
	assert.Equal(t, len(expectedFks), len(actualFks))

	for i, fk := range expectedFks {
		fkId := getFkIdFromSrcName(conv.SrcSchema[tableId].ForeignKeys, fk.Name)
		assert.NotEqual(t, fkId, "")
		expectedFks[i].Id = fkId
		referTableId, _ := GetTableIdFromSrcName(conv.SrcSchema, fk.ReferTableId)
		assert.NotEqual(t, referTableId, "")
		expectedFks[i].ReferTableId = referTableId
		for j, col := range fk.ColIds {
			colId := conv.SrcSchema[tableId].ColNameIdMap[col]
			assert.NotEqual(t, colId, "")
			expectedFks[i].ColIds[j] = colId
		}
		for j, col := range fk.ReferColumnIds {
			colId := conv.SrcSchema[referTableId].ColNameIdMap[col]
			assert.NotEqual(t, colId, "")
			expectedFks[i].ReferColumnIds[j] = colId
		}
		assert.Equal(t, expectedFks[i].OnDelete, actualFks[i].OnDelete)
		assert.Equal(t, expectedFks[i].OnUpdate, actualFks[i].OnUpdate)
	}
	assert.ElementsMatch(t, expectedFks, actualFks)
}

func assertSrcIndexes(t *testing.T, conv *Conv, tableId string, expectedIndexes, actualIndexes []schema.Index) {
	assert.Equal(t, len(expectedIndexes), len(actualIndexes))
	for _, index := range expectedIndexes {
		actualIndex, err := getIndexFromSrcName(conv.SrcSchema[tableId].Indexes, index.Name)
		assert.Equal(t, err, nil)
		assert.Equal(t, index.Unique, actualIndex.Unique)
		assert.Equal(t, len(index.Keys), len(actualIndex.Keys))
		for j, indexKey := range index.Keys {
			colId := conv.SrcSchema[tableId].ColNameIdMap[indexKey.ColId]
			index.Keys[j].ColId = colId
		}
		assert.ElementsMatch(t, index.Keys, actualIndex.Keys)
		for j, storedColumn := range index.StoredColumnIds {
			colId := conv.SrcSchema[tableId].ColNameIdMap[storedColumn]
			index.StoredColumnIds[j] = colId
		}
		assert.ElementsMatch(t, index.StoredColumnIds, actualIndex.StoredColumnIds)
	}
}

func getFkIdFromSrcName(fks []schema.ForeignKey, fkName string) string {
	for _, fk := range fks {
		if fk.Name == fkName {
			return fk.Id
		}
	}
	return ""
}

func getIndexFromSrcName(indexes []schema.Index, indexName string) (schema.Index, error) {
	for _, index := range indexes {
		if index.Name == indexName {
			return index, nil
		}
	}
	return schema.Index{}, fmt.Errorf("spanner index not found")
}

func AssertTableIssues(conv *Conv, t *testing.T, tableId string, expectedIssues, actualIssues map[string][]SchemaIssue) {
	assert.Equal(t, len(expectedIssues), len(actualIssues))
	for col, issues := range expectedIssues {
		colId, err := GetColIdFromSpName(conv.SpSchema[tableId].ColDefs, col)
		assert.Equal(t, nil, err)
		assert.ElementsMatch(t, issues, actualIssues[colId])
	}
}

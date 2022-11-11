// Copyright 2022 Google LLC
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
	"strings"

	"github.com/cloudspannerecosystem/harbourbridge/schema"
)

type Counter struct {
	ObjectId string
}

var Cntr Counter

// contains check string present in list.
func contains(l []string, str string) bool {
	for _, s := range l {
		if s == str {
			return true
		}
	}
	return false
}

func GenerateId() string {

	counter, _ := strconv.Atoi(Cntr.ObjectId)

	counter = counter + 1

	Cntr.ObjectId = strconv.Itoa(counter)
	return Cntr.ObjectId
}

func GenerateTableId() string {
	tablePrefix := "t"
	id := GenerateId()
	tableId := tablePrefix + id
	return tableId
}

func GenerateColumnId() string {

	columnPrefix := "c"
	id := GenerateId()
	columnId := columnPrefix + id
	return columnId
}

func GenerateForeignkeyId() string {

	foreignKeyPrefix := "f"
	id := GenerateId()
	foreignKeyId := foreignKeyPrefix + id
	return foreignKeyId
}

func GenerateIndexesId() string {

	indexesPrefix := "i"
	id := GenerateId()

	indexesId := indexesPrefix + id
	return indexesId
}

func GetColumnIdFromName(conv *Conv, tableId string, columnName string) (string, error) {
	for _, v := range conv.SrcSchema[tableId].ColDefs {
		if v.Name == columnName {
			return v.Id, nil
		}
	}
	return "", fmt.Errorf("ColumnId is empty: can't find column")
}
func GetTableIdFromName(conv *Conv, tableName string) (string, error) {
	for _, v := range conv.SrcSchema {
		if v.Name == tableName {
			return v.Id, nil
		}
	}
	return "", fmt.Errorf("TableId is empty: can't find table")
}

func GetSourceFkFromId(conv *Conv, tableId, fkId string) (schema.ForeignKey, error) {
	for _, v := range conv.SrcSchema[tableId].ForeignKeys {
		if v.Id == fkId {
			return v, nil
		}
	}
	return schema.ForeignKey{}, fmt.Errorf("foreign key not found")
}

func GetSourceIndexFromId(conv *Conv, tableId, indexId string) (schema.Index, error) {
	for _, v := range conv.SrcSchema[tableId].Indexes {
		if v.Id == indexId {
			return v, nil
		}
	}
	return schema.Index{}, fmt.Errorf("index not found")
}

func ComputeUsedNames(conv *Conv) map[string]bool {
	usedNames := make(map[string]bool)
	for _, table := range conv.SpSchema {
		usedNames[table.Name] = true
		for _, index := range table.Indexes {
			usedNames[index.Name] = true
		}
		for _, fk := range table.ForeignKeys {
			usedNames[fk.Name] = true
		}
	}
	return usedNames
}

func ComputeToSource(conv *Conv) map[string]NameAndCols {
	toSource := make(map[string]NameAndCols)
	for id, spTable := range conv.SpSchema {
		if srcTable, ok := conv.SrcSchema[id]; ok {
			colMap := make(map[string]string)
			for colId, spCol := range spTable.ColDefs {
				if srcCol, ok := srcTable.ColDefs[colId]; ok {
					colMap[strings.ToLower(spCol.Name)] = strings.ToLower(srcCol.Name)
				}
			}
			toSource[strings.ToLower(spTable.Name)] = NameAndCols{Name: strings.ToLower(srcTable.Name), Cols: colMap}
		}
	}
	return toSource
}

func ComputeToSpanner(conv *Conv) map[string]NameAndCols {
	toSpanner := make(map[string]NameAndCols)
	for id, srcTable := range conv.SrcSchema {
		if spTable, ok := conv.SpSchema[id]; ok {
			colMap := make(map[string]string)
			for colId, srcCol := range srcTable.ColDefs {
				if spCol, ok := srcTable.ColDefs[colId]; ok {
					colMap[strings.ToLower(srcCol.Name)] = strings.ToLower(spCol.Name)
				}
			}
			toSpanner[strings.ToLower(spTable.Name)] = NameAndCols{Name: strings.ToLower(spTable.Name), Cols: colMap}
		}
	}
	return toSpanner
}

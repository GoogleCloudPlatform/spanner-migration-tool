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

	"github.com/cloudspannerecosystem/harbourbridge/schema"
)

func (conv *Conv) AssignIdToSourceSchema() {
	conv.AssignTableId()
	conv.AssignColumnId()
	conv.AssignPkId()
	conv.AssignIndexId()
	conv.AssginFkId()
}

func (conv *Conv) AssignTableId() {
	srcSchema := map[string]schema.Table{}

	for _, v := range conv.SrcSchema {
		tableId := GenerateTableId()
		srcTable := v
		srcTable.Id = tableId
		srcSchema[tableId] = srcTable
	}
	conv.SrcSchema = nil
	conv.SrcSchema = srcSchema

}

func (conv *Conv) AssignColumnId() {
	for tableId, table := range conv.SrcSchema {

		colNames := make([]string, 0, len(table.ColDefs))
		for colName := range table.ColDefs {
			colNames = append(colNames, colName)
		}

		for _, columnName := range colNames {
			columnId := GenerateColumnId()
			column := table.ColDefs[columnName]
			column.Id = columnId
			conv.SrcSchema[tableId].ColDefs[columnId] = column
			delete(table.ColDefs, columnName)
		}
		for k, v := range conv.SrcSchema[tableId].ColIds {
			columnId, _ := GetColIdFromSrcName(conv.SrcSchema[tableId].ColDefs, v)
			conv.SrcSchema[tableId].ColIds[k] = columnId
		}
	}
}

func (conv *Conv) AssignPkId() {
	for tableId, table := range conv.SrcSchema {
		for i, pk := range table.PrimaryKeys {
			columnId, err := GetColIdFromSrcName(conv.SrcSchema[tableId].ColDefs, pk.ColId)
			if err != nil {
				fmt.Println("ColumnId doesn't exist.")
				continue
			}
			conv.SrcSchema[tableId].PrimaryKeys[i].ColId = columnId
		}
	}
}

func (conv *Conv) AssignIndexId() {
	for tableId, table := range conv.SrcSchema {
		for i, index := range table.Indexes {
			indexId := GenerateIndexesId()
			for k, v := range index.Keys {
				columnId, err := GetColIdFromSrcName(conv.SrcSchema[tableId].ColDefs, v.ColId)
				if err != nil {
					fmt.Println("ColumnId doesn't exist.")
					continue
				}
				conv.SrcSchema[tableId].Indexes[i].Keys[k].ColId = columnId
			}
			var storedColumnIds []string
			for _, v := range index.StoredColumnIds {
				storedColumnId, err := GetColIdFromSrcName(conv.SrcSchema[tableId].ColDefs, v)
				if err != nil {
					fmt.Println("StoreColumnId doesn't exist.")
					continue
				}
				storedColumnIds = append(storedColumnIds, storedColumnId)
			}
			conv.SrcSchema[tableId].Indexes[i].StoredColumnIds = storedColumnIds
			conv.SrcSchema[tableId].Indexes[i].Id = indexId
		}
	}
}

func (conv *Conv) AssginFkId() {
	for tableId, table := range conv.SrcSchema {
		for i, fk := range table.ForeignKeys {
			fkId := GenerateForeignkeyId()
			var columnIds []string
			for _, columnName := range fk.ColIds {
				columnId, err := GetColIdFromSrcName(conv.SrcSchema[tableId].ColDefs, columnName)
				if err != nil {
					fmt.Println("ReferColumnId doesn't exist.")
					continue
				}
				columnIds = append(columnIds, columnId)
			}

			referTableId, err := GetTableIdFromSrcName(conv.SrcSchema, fk.ReferTableId)
			if err != nil {
				fmt.Println("TableId doesn't exist.")
				continue
			}
			var referColumnIds []string
			for _, referColumnName := range fk.ReferColumnIds {
				referColumnId, err := GetColIdFromSrcName(conv.SrcSchema[referTableId].ColDefs, referColumnName)
				if err != nil {
					fmt.Println("ReferColumnId doesn't exist.")
					continue
				}
				referColumnIds = append(referColumnIds, referColumnId)
			}
			conv.SrcSchema[tableId].ForeignKeys[i].Id = fkId
			conv.SrcSchema[tableId].ForeignKeys[i].ColIds = columnIds
			conv.SrcSchema[tableId].ForeignKeys[i].ReferTableId = referTableId
			conv.SrcSchema[tableId].ForeignKeys[i].ReferColumnIds = referColumnIds
		}
	}
}

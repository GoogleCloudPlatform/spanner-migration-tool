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
	"sort"

	"github.com/cloudspannerecosystem/harbourbridge/schema"
)

func (conv *Conv) AssignIdToSourceSchema() {
	conv.AssignTableId()
	conv.AssignColumnId()
	conv.AssignPkId()
	conv.AssignIndexId()
	conv.AssginFkId()
	conv.AssignStatsId()
}

func (conv *Conv) AssignTableId() {
	srcSchema := map[string]schema.Table{}
	keys := make([]string, 0, len(conv.SrcSchema))
	for key := range conv.SrcSchema {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, v := range keys {
		tableId := GenerateTableId()
		srcTable := conv.SrcSchema[v]
		srcTable.Id = tableId
		srcSchema[tableId] = srcTable
	}
	conv.SrcSchema = nil
	conv.SrcSchema = srcSchema

}

func (conv *Conv) AssignColumnId() {
	tableIds := GetSortedTableIds(conv)
	for _, tableId := range tableIds {
		table := conv.SrcSchema[tableId]

		keys := make([]string, 0, len(table.ColDefs))
		for key := range table.ColDefs {
			keys = append(keys, key)
		}
		sort.Strings(keys)

		for _, columnName := range keys {
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
	tableIds := GetSortedTableIds(conv)
	for _, tableId := range tableIds {
		table := conv.SrcSchema[tableId]
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
	tableIds := GetSortedTableIds(conv)
	for _, tableId := range tableIds {
		table := conv.SrcSchema[tableId]
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
	tableIds := GetSortedTableIds(conv)
	for _, tableId := range tableIds {
		table := conv.SrcSchema[tableId]
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

			referTableId, err := GetTableIdFromName(conv, fk.ReferTableId)
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
func (conv *Conv) AssignStatsId() {
	rows := map[string]int64{}
	for k, v := range conv.Stats.Rows {
		tableId, err := GetTableIdFromName(conv, k)
		if err != nil {
			continue
		}
		rows[tableId] = v
	}
	conv.Stats.Rows = nil
	conv.Stats.Rows = rows

	goodRows := map[string]int64{}
	for k, v := range conv.Stats.GoodRows {
		tableId, err := GetTableIdFromName(conv, k)
		if err != nil {
			continue
		}
		goodRows[tableId] = v
	}
	conv.Stats.GoodRows = nil
	conv.Stats.GoodRows = goodRows

	badRows := map[string]int64{}
	for k, v := range conv.Stats.BadRows {
		tableId, err := GetTableIdFromName(conv, k)
		if err != nil {
			continue
		}
		badRows[tableId] = v
	}
	conv.Stats.BadRows = nil
	conv.Stats.BadRows = badRows

}

func GetSortedTableIds(conv *Conv) []string {
	tableIds := []string{}
	for tableId, _ := range conv.SrcSchema {
		tableIds = append(tableIds, tableId)
	}
	sort.Strings(tableIds)
	return tableIds
}

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

func (conv *Conv) AssignIdToSourceSchema() error {
	conv.AssignTableId()
	conv.AssignColumnId()
	err := conv.AssignPkId()
	if err != nil {
		return err
	}
	err = conv.AssignIndexId()
	if err != nil {
		return err
	}
	conv.AssignFkId()
	return nil
}

func (conv *Conv) AssignTableId() {
	srcSchema := map[string]schema.Table{}

	for _, v := range conv.SrcSchema {
		tableId := GenerateTableId()
		srcTable := v
		srcTable.Id = tableId
		srcSchema[tableId] = srcTable
	}
	conv.SrcSchema = srcSchema

}

func (conv *Conv) AssignColumnId() {
	for tableId, table := range conv.SrcSchema {
		for k, columnName := range table.ColIds {
			columnId := GenerateColumnId()
			column := table.ColDefs[columnName]
			column.Id = columnId
			conv.SrcSchema[tableId].ColDefs[columnId] = column
			conv.SrcSchema[tableId].ColIds[k] = columnId
			delete(table.ColDefs, columnName)
		}
	}
}

func (conv *Conv) AssignPkId() error {
	for tableId, table := range conv.SrcSchema {
		for i, pk := range table.PrimaryKeys {
			columnId, err := GetColIdFromSrcName(conv.SrcSchema[tableId].ColDefs, pk.ColId)
			if err != nil {
				return fmt.Errorf("column id not found for table %s and column %s", table.Name, pk.ColId)
			}
			conv.SrcSchema[tableId].PrimaryKeys[i].ColId = columnId
		}
	}
	return nil
}

func (conv *Conv) AssignIndexId() error {
	for tableId, table := range conv.SrcSchema {
		for i, index := range table.Indexes {
			indexId := GenerateIndexesId()
			for k, v := range index.Keys {
				columnId, err := GetColIdFromSrcName(conv.SrcSchema[tableId].ColDefs, v.ColId)
				if err != nil {
					return fmt.Errorf("column id not found for table %s and column %s", table.Name, v.ColId)
				}
				conv.SrcSchema[tableId].Indexes[i].Keys[k].ColId = columnId
			}
			var storedColumnIds []string
			for _, v := range index.StoredColumnIds {
				storedColumnId, err := GetColIdFromSrcName(conv.SrcSchema[tableId].ColDefs, v)
				if err != nil {
					return fmt.Errorf("stored column id not found for table %s and column %s", table.Name, v)
				}
				storedColumnIds = append(storedColumnIds, storedColumnId)
			}
			conv.SrcSchema[tableId].Indexes[i].StoredColumnIds = storedColumnIds
			conv.SrcSchema[tableId].Indexes[i].Id = indexId
		}
	}
	return nil
}

func (conv *Conv) AssignFkId() {
	for tableId, table := range conv.SrcSchema {
		for i, fk := range table.ForeignKeys {
			fkId := GenerateForeignkeyId()
			var columnIds []string
			for _, columnName := range fk.ColIds {
				columnId, err := GetColIdFromSrcName(conv.SrcSchema[tableId].ColDefs, columnName)
				if err != nil {
					continue
				}
				columnIds = append(columnIds, columnId)
			}

			referTableId, err := GetTableIdFromSrcName(conv.SrcSchema, fk.ReferTableId)
			if err != nil {
				continue
			}
			var referColumnIds []string
			for _, referColumnName := range fk.ReferColumnIds {
				referColumnId, err := GetColIdFromSrcName(conv.SrcSchema[referTableId].ColDefs, referColumnName)
				if err != nil {
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

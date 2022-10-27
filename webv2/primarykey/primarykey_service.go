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

package primarykey

import (
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
	"github.com/cloudspannerecosystem/harbourbridge/webv2/session"
	utilities "github.com/cloudspannerecosystem/harbourbridge/webv2/utilities"
)

// getColumnId return ColumnId for given columnName.
func getColumnId(spannerTable ddl.CreateTable, columnName string) string {

	var id string
	for _, col := range spannerTable.ColDefs {
		if col.Name == columnName {
			id = col.Id
		}
	}
	return id
}

// getSpannerTable return spannerTable for given TableId.
func getSpannerTable(sessionState *session.SessionState, pkRequest PrimaryKeyRequest) (spannerTable ddl.CreateTable, found bool) {

	for _, table := range sessionState.Conv.SpSchema {

		if pkRequest.TableId == table.Id {
			spannerTable = table
			found = true
		}
	}
	return spannerTable, found
}

// getColumnName return columnName for given columnId.
func getColumnName(spannerTable ddl.CreateTable, columnId string) string {

	var columnName string
	for _, col := range spannerTable.ColDefs {
		if col.Id == columnId {
			columnName = col.Name
		}
	}
	return columnName
}

// getColumnIdListFromPrimaryKeyRequest return list of column Id from PrimaryKeyRequest.
func getColumnIdListFromPrimaryKeyRequest(pkRequest PrimaryKeyRequest) []string {

	cidlist := []string{}

	for i := 0; i < len(pkRequest.Columns); i++ {
		cidlist = append(cidlist, pkRequest.Columns[i].ColumnId)
	}
	return cidlist
}

// getColumnIdListOfSpannerTablePrimaryKey return list of column Id from spannerTable PrimaryKey.
func getColumnIdListOfSpannerTablePrimaryKey(spannerTable ddl.CreateTable) []string {
	cidlist := []string{}

	for i := 0; i < len(spannerTable.Pks); i++ {
		cid := getColumnId(spannerTable, spannerTable.Pks[i].Col)
		cidlist = append(cidlist, cid)
	}
	return cidlist
}

// getColumnIdListOfSpannerTable return list of column Id from spannerTable ColDefs.
func getColumnIdListOfSpannerTable(spannerTable ddl.CreateTable) []string {
	cidlist := []string{}

	for _, column := range spannerTable.ColDefs {
		cidlist = append(cidlist, column.Id)
	}
	return cidlist
}

// isValidColumnIds checks columnId is already present in schema.
func isValidColumnIds(pkRequest PrimaryKeyRequest, spannertable ddl.CreateTable) bool {

	cidRequestList := getColumnIdListFromPrimaryKeyRequest(pkRequest)
	cidSpannerTableList := getColumnIdListOfSpannerTable(spannertable)
	leftjoin := utilities.Difference(cidRequestList, cidSpannerTableList)

	if len(leftjoin) > 0 {
		return false
	}
	return true
}

func RemoveInterleave(conv *internal.Conv, spannertable ddl.CreateTable) {
	if spannertable.Parent != "" {
		var childPkFirstColumn string
		var parentPkFirstColumn string
		for i := 0; i < len(spannertable.Pks); i++ {
			if spannertable.Pks[i].Order == 1 {
				childPkFirstColumn = spannertable.Pks[i].Col
			}
		}
		for i := 0; i < len(conv.SpSchema[spannertable.Parent].Pks); i++ {
			if conv.SpSchema[spannertable.Parent].Pks[i].Order == 1 {
				parentPkFirstColumn = conv.SpSchema[spannertable.Parent].Pks[i].Col
			}
		}
		if childPkFirstColumn != parentPkFirstColumn {
			spannertable.Parent = ""
			conv.SpSchema[spannertable.Name] = spannertable
		}
	}
}

// isValidColumnOrder make sure two primary key column can not have same order.
func isValidColumnOrder(pkRequest PrimaryKeyRequest) bool {

	list := []int{}

	for i := 0; i < len(pkRequest.Columns); i++ {
		list = append(list, pkRequest.Columns[i].Order)
	}

	if utilities.DuplicateInArray(list) == -1 {
		return false
	}

	return true
}

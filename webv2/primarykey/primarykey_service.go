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

// Package web defines web APIs to be used with harbourbridge frontend.
// Apart from schema conversion, this package involves API to update
// converted schema.

package primarykey

import (
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
	"github.com/cloudspannerecosystem/harbourbridge/webv2/session"
	utilities "github.com/cloudspannerecosystem/harbourbridge/webv2/utilities"
)

// getColumnId return ColumnId for given columnName.
func getColumnId(spannerTable ddl.CreateTable, columnName string) int {

	var id int
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
func getColumnName(spannerTable ddl.CreateTable, columnId int) string {

	var columnName string
	for _, col := range spannerTable.ColDefs {
		if col.Id == columnId {
			columnName = col.Name
		}
	}
	return columnName
}

// getColumnIdListFromPrimaryKeyRequest return list of column Id from PrimaryKeyRequest.
func getColumnIdListFromPrimaryKeyRequest(pkRequest PrimaryKeyRequest) []int {

	cidlist := []int{}

	for i := 0; i < len(pkRequest.Columns); i++ {
		cidlist = append(cidlist, pkRequest.Columns[i].ColumnId)
	}
	return cidlist
}

// getColumnIdListOfSpannerTablePrimaryKey return list of column Id from spannerTable PrimaryKey.
func getColumnIdListOfSpannerTablePrimaryKey(spannerTable ddl.CreateTable) []int {
	cidlist := []int{}

	for i := 0; i < len(spannerTable.Pks); i++ {
		cid := getColumnId(spannerTable, spannerTable.Pks[i].Col)
		cidlist = append(cidlist, cid)
	}
	return cidlist
}

// getColumnIdListOfSpannerTable return list of column Id from spannerTable ColDefs.
func getColumnIdListOfSpannerTable(spannerTable ddl.CreateTable) []int {
	cidlist := []int{}

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

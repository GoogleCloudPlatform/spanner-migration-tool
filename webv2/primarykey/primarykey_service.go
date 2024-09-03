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
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/session"
	utilities "github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/utilities"
)

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

// getColumnIdListFromPrimaryKeyRequest return list of column Id from PrimaryKeyRequest.
func getColumnIdListFromPrimaryKeyRequest(pkRequest PrimaryKeyRequest) []string {

	cidlist := []string{}

	for i := 0; i < len(pkRequest.Columns); i++ {
		cidlist = append(cidlist, pkRequest.Columns[i].ColId)
	}
	return cidlist
}

// getColumnIdListOfSpannerTablePrimaryKey return list of column Id from spannerTable PrimaryKey.
func getColumnIdListOfSpannerTablePrimaryKey(spannerTable ddl.CreateTable) []string {
	cidlist := []string{}

	for i := 0; i < len(spannerTable.PrimaryKeys); i++ {
		cidlist = append(cidlist, spannerTable.PrimaryKeys[i].ColId)
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
	if spannertable.ParentTable.Id != "" {
		var childPkFirstColumn string
		var parentPkFirstColumn string
		for i := 0; i < len(spannertable.PrimaryKeys); i++ {
			if spannertable.PrimaryKeys[i].Order == 1 {
				childPkFirstColumn = spannertable.PrimaryKeys[i].ColId
			}
		}
		for i := 0; i < len(conv.SpSchema[spannertable.ParentTable.Id].PrimaryKeys); i++ {
			if conv.SpSchema[spannertable.ParentTable.Id].PrimaryKeys[i].Order == 1 {
				parentPkFirstColumn = conv.SpSchema[spannertable.ParentTable.Id].PrimaryKeys[i].ColId
			}
		}
		if childPkFirstColumn != parentPkFirstColumn {
			spannertable.ParentTable.Id = ""
			spannertable.ParentTable.OnDelete = ""
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

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
)

// updateprimaryKey updates primary key desc and order for primaryKey.
func updatePrimaryKey(pkRequest PrimaryKeyRequest, spannerTable ddl.CreateTable) ddl.CreateTable {

	for i := 0; i < len(pkRequest.Columns); i++ {

		for j := 0; j < len(spannerTable.Pks); j++ {

			id := getColumnId(spannerTable, spannerTable.Pks[j].Col)

			if pkRequest.Columns[i].ColumnId == id {

				spannerTable.Pks[j].Desc = pkRequest.Columns[i].Desc
				spannerTable.Pks[j].Order = pkRequest.Columns[i].Order
			}

		}
	}

	spannerTable = insertOrRemovePrimarykey(pkRequest, spannerTable)
	return spannerTable
}

// insertOrRemovePrimarykey performs insert or remove primary key operation based on
// difference of two pkRequest and spannerTable.Pks.
func insertOrRemovePrimarykey(pkRequest PrimaryKeyRequest, spannerTable ddl.CreateTable) ddl.CreateTable {

	cidRequestList := getColumnIdListFromPrimaryKeyRequest(pkRequest)
	cidSpannerTableList := getColumnIdListOfSpannerTablePrimaryKey(spannerTable)

	//primary key Id only presnt in pkeyrequest
	// hence new primary key add primary key into  spannerTable.Pk list
	leftjoin := difference(cidRequestList, cidSpannerTableList)
	insert := addPrimaryKey(leftjoin, pkRequest, spannerTable)

	spannerTable.Pks = append(spannerTable.Pks, insert...)

	//primary key Id only presnt in spannertable.Pks
	// hence remove primary key from  spannertable.Pks
	rightjoin := difference(cidSpannerTableList, cidRequestList)

	if len(rightjoin) > 0 {
		nlist := removePrimaryKey(rightjoin, spannerTable)
		spannerTable.Pks = nlist
	}

	cidRequestList = []int{}
	cidSpannerTableList = []int{}
	return spannerTable
}

// addPrimaryKey insert primary key into list of IndexKey
func addPrimaryKey(add []int, pkRequest PrimaryKeyRequest, spannerTable ddl.CreateTable) []ddl.IndexKey {

	list := []ddl.IndexKey{}

	for _, val := range add {

		for i := 0; i < len(pkRequest.Columns); i++ {

			if val == pkRequest.Columns[i].ColumnId {

				pkey := ddl.IndexKey{}
				pkey.Col = getColumnName(spannerTable, pkRequest.Columns[i].ColumnId)
				pkey.Desc = pkRequest.Columns[i].Desc
				pkey.Order = pkRequest.Columns[i].Order
				list = append(list, pkey)
			}
		}
	}
	return list
}

// removePrimaryKey removed primary key from list of IndexKey
func removePrimaryKey(remove []int, spannerTable ddl.CreateTable) []ddl.IndexKey {

	list := []ddl.IndexKey{}

	for _, val := range remove {

		colname := getColumnName(spannerTable, val)

		for i := 0; i < len(spannerTable.Pks); i++ {

			if spannerTable.Pks[i].Col == colname {

				list = append(spannerTable.Pks[:i], spannerTable.Pks[i+1:]...)
			}
		}
	}
	return list
}

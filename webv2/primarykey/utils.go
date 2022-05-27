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

import "github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"

// difference gives list of element that are only present in first list.
func difference(listone, listtwo []int) []int {

	hashmap := make(map[int]int, len(listtwo))

	for _, val := range listtwo {
		hashmap[val]++
	}

	var diff []int

	for _, val := range listone {

		_, found := hashmap[val]
		if !found {
			diff = append(diff, val)
		}
	}
	return diff
}

// prepareResponse prepare response for primary key api
func prepareResponse(pkRequest PrimaryKeyRequest, spannerTable ddl.CreateTable) PrimaryKeyResponse {

	var pKeyResponse PrimaryKeyResponse

	pKeyResponse.TableId = pkRequest.TableId
	pKeyResponse.PrimaryKeyId = pkRequest.PrimaryKeyId

	var isSynthPrimaryKey bool

	//todo check with team
	for i := 0; i < len(spannerTable.ColNames); i++ {
		if spannerTable.ColNames[i] == "synth_id" {
			isSynthPrimaryKey = true
		}
	}

	pKeyResponse.Synth = isSynthPrimaryKey

	for _, indexkey := range spannerTable.Pks {

		responseColumn := Column{}
		id := getColumnId(spannerTable, indexkey.Col)
		responseColumn.ColumnId = id
		responseColumn.ColName = indexkey.Col
		responseColumn.Desc = indexkey.Desc
		responseColumn.Order = indexkey.Order
		pKeyResponse.Columns = append(pKeyResponse.Columns, responseColumn)
	}
	return pKeyResponse
}

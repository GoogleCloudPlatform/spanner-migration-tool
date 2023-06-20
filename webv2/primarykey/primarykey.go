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

// updateprimaryKey insert or delete primary key column.
// updateprimaryKey also update desc and order for primaryKey column.
func updatePrimaryKey(pkRequest PrimaryKeyRequest, spannerTable ddl.CreateTable, synthColId string) (ddl.CreateTable, bool) {

	spannerTable, isSynthPkRemoved := insertOrRemovePrimarykey(pkRequest, spannerTable, synthColId)

	for i := 0; i < len(pkRequest.Columns); i++ {

		for j := 0; j < len(spannerTable.PrimaryKeys); j++ {

			if pkRequest.Columns[i].ColumnId == spannerTable.PrimaryKeys[j].ColId && spannerTable.PrimaryKeys[j].ColId == pkRequest.Columns[i].ColumnId {

				spannerTable.PrimaryKeys[j].Desc = pkRequest.Columns[i].Desc
				spannerTable.PrimaryKeys[j].Order = pkRequest.Columns[i].Order
			}

		}
	}

	return spannerTable, isSynthPkRemoved
}

// insertOrRemovePrimarykey performs insert or remove primary key operation based on
// difference of two pkRequest and spannerTable.PrimaryKeys.
func insertOrRemovePrimarykey(pkRequest PrimaryKeyRequest, spannerTable ddl.CreateTable, synthColId string) (ddl.CreateTable, bool) {

	cidRequestList := getColumnIdListFromPrimaryKeyRequest(pkRequest)
	cidSpannerTableList := getColumnIdListOfSpannerTablePrimaryKey(spannerTable)

	// primary key Id only presnt in pkeyrequest.
	// hence new primary key add primary key into  spannerTable.Pk list
	leftjoin := utilities.Difference(cidRequestList, cidSpannerTableList)
	insert := addPrimaryKey(leftjoin, pkRequest, spannerTable)

	isHotSpot(insert, spannerTable)

	spannerTable.PrimaryKeys = append(spannerTable.PrimaryKeys, insert...)

	// primary key Id only presnt in spannertable.PrimaryKeys
	// hence remove primary key from  spannertable.PrimaryKeys
	rightjoin := utilities.Difference(cidSpannerTableList, cidRequestList)

	isSynthPkRemoved := false
	for _, colId := range rightjoin {
		if colId == synthColId {
			isSynthPkRemoved = true
		}
	}

	if len(rightjoin) > 0 {
		nlist := removePrimaryKey(rightjoin, spannerTable)
		spannerTable.PrimaryKeys = nlist

	}

	cidRequestList = []string{}
	cidSpannerTableList = []string{}
	return spannerTable, isSynthPkRemoved
}

// addPrimaryKey insert primary key into list of IndexKey.
func addPrimaryKey(add []string, pkRequest PrimaryKeyRequest, spannerTable ddl.CreateTable) []ddl.IndexKey {

	list := []ddl.IndexKey{}

	for _, val := range add {

		for i := 0; i < len(pkRequest.Columns); i++ {

			if val == pkRequest.Columns[i].ColumnId {

				pkey := ddl.IndexKey{}
				pkey.ColId = pkRequest.Columns[i].ColumnId
				pkey.Desc = pkRequest.Columns[i].Desc
				pkey.Order = pkRequest.Columns[i].Order

				{
					schemaissue := []internal.SchemaIssue{}

					sessionState := session.GetSessionState()
					schemaissue = sessionState.Conv.SchemaIssues[spannerTable.Name].ColumnLevelIssues[pkey.ColId]

					if len(schemaissue) > 0 {

						schemaissue = utilities.RemoveSchemaIssues(schemaissue)

						sessionState.Conv.SchemaIssues[spannerTable.Name].ColumnLevelIssues[pkey.ColId] = schemaissue

						if sessionState.Conv.SchemaIssues[spannerTable.Name].ColumnLevelIssues[pkey.ColId] == nil {

							s := map[string][]internal.SchemaIssue{
								pkey.ColId: schemaissue,
							}
							sessionState.Conv.SchemaIssues = map[string]internal.TableIssues{}

							sessionState.Conv.SchemaIssues[spannerTable.Name] = internal.TableIssues{
								ColumnLevelIssues: s,
							}
						} else {
							sessionState.Conv.SchemaIssues[spannerTable.Name].ColumnLevelIssues[pkey.ColId] = schemaissue
						}

					}
				}

				list = append(list, pkey)
			}
		}
	}
	return list
}

// removePrimaryKey removes primary key from list of IndexKey.
func removePrimaryKey(remove []string, spannerTable ddl.CreateTable) []ddl.IndexKey {

	list := spannerTable.PrimaryKeys

	for _, val := range remove {

		for i := 0; i < len(spannerTable.PrimaryKeys); i++ {

			if spannerTable.PrimaryKeys[i].ColId == val {

				{
					schemaissue := []internal.SchemaIssue{}
					sessionState := session.GetSessionState()
					schemaissue = sessionState.Conv.SchemaIssues[spannerTable.Name].ColumnLevelIssues[spannerTable.PrimaryKeys[i].ColId]

					if len(schemaissue) > 0 {

						schemaissue = utilities.RemoveSchemaIssues(schemaissue)

						if sessionState.Conv.SchemaIssues[spannerTable.Name].ColumnLevelIssues[spannerTable.PrimaryKeys[i].ColId] == nil {

							s := map[string][]internal.SchemaIssue{
								spannerTable.PrimaryKeys[i].ColId: schemaissue,
							}
							sessionState.Conv.SchemaIssues = map[string]internal.TableIssues{}

							sessionState.Conv.SchemaIssues[spannerTable.Name] = internal.TableIssues{
								ColumnLevelIssues: s,
							}

						} else {

							sessionState.Conv.SchemaIssues[spannerTable.Name].ColumnLevelIssues[spannerTable.PrimaryKeys[i].ColId] = schemaissue

						}

					}

				}

				list = utilities.RemoveIndex(list, i)

				break
			}

		}
	}

	return list
}

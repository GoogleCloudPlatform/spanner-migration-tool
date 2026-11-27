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
	"github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/utilities"
)

// DetectHotspot adds hotspot detected suggestion in schema conversion process for database.
func DetectHotspot() {

	sessionState := session.GetSessionState()

	for _, spannerTable := range sessionState.Conv.SpSchema {

		isHotSpot(spannerTable.PrimaryKeys, spannerTable)
	}

}

// Helper method for hotspot detection. pks must be the table's full primary
// key. Earlier hotspot issues of the table are cleared first, so calling it
// again (e.g. after a primary key edit) does not leave stale or duplicate
// entries.
func isHotSpot(pks []ddl.IndexKey, spannerTable ddl.CreateTable) {

	clearHotspotIssues(spannerTable)
	firstColId := firstPrimaryKeyColId(pks)
	if firstColId == "" {
		return
	}
	hotspotTimestamp(firstColId, spannerTable)
	hotspotAutoincrement(firstColId, spannerTable)
}

// firstPrimaryKeyColId returns the column id of the first primary key column:
// the key with the lowest Order. The slice is not always sorted by Order (keys
// added in the UI are appended), and some sources leave Order unset (0), so a
// tie keeps the earlier key in the slice.
func firstPrimaryKeyColId(pks []ddl.IndexKey) string {
	first := -1
	for i, pk := range pks {
		if first == -1 || pk.Order < pks[first].Order {
			first = i
		}
	}
	if first == -1 {
		return ""
	}
	return pks[first].ColId
}

// hotspotTimestamp checks Timestamp hotspot.
// A TIMESTAMP column causes a hotspot only as the first primary key column:
// rows are then written in increasing key order. If so, adds HotspotTimestamp
// as an issue in Issues.
func hotspotTimestamp(firstColId string, spannerTable ddl.CreateTable) {
	if c, ok := spannerTable.ColDefs[firstColId]; ok && c.T.Name == ddl.Timestamp {
		addColumnIssue(spannerTable.Id, firstColId, internal.HotspotTimestamp)
	}
}

// hotspotAutoincrement check AutoIncrement hotspot.
// Like a timestamp, it only matters for the first primary key column. If so,
// adds HotspotAutoIncrement as an issue in Issues.
func hotspotAutoincrement(firstColId string, spannerTable ddl.CreateTable) {
	if c, ok := spannerTable.ColDefs[firstColId]; ok {
		detecthotspotAutoincrement(spannerTable, c.Id)
	}
}

// detecthotspotAutoincrement checks for autoincrement hotspot.
// If present it adds HotspotAutoIncrement as an issue in Issues.
func detecthotspotAutoincrement(spannerTable ddl.CreateTable, spannerColumnId string) {

	sessionState := session.GetSessionState()
	sourcetable := sessionState.Conv.SrcSchema[spannerTable.Id]

	for _, s := range sourcetable.ColDefs {

		if s.Id == spannerColumnId {

			if s.Ignored.AutoIncrement {
				addColumnIssue(spannerTable.Id, s.Id, internal.HotspotAutoIncrement)
			}

		}
	}
}

// clearHotspotIssues removes HotspotTimestamp and HotspotAutoIncrement from
// every column of the table.
func clearHotspotIssues(spannerTable ddl.CreateTable) {
	sessionState := session.GetSessionState()
	tableIssues, ok := sessionState.Conv.SchemaIssues[spannerTable.Id]
	if !ok {
		return
	}
	for colId, issues := range tableIssues.ColumnLevelIssues {
		issues = utilities.RemoveSchemaIssue(issues, internal.HotspotTimestamp)
		issues = utilities.RemoveSchemaIssue(issues, internal.HotspotAutoIncrement)
		tableIssues.ColumnLevelIssues[colId] = issues
	}
}

// addColumnIssue appends issue to the column's issues.
func addColumnIssue(tableId, colId string, issue internal.SchemaIssue) {
	sessionState := session.GetSessionState()
	if sessionState.Conv.SchemaIssues == nil {
		sessionState.Conv.SchemaIssues = make(map[string]internal.TableIssues)
	}
	tableIssues := sessionState.Conv.SchemaIssues[tableId]
	if tableIssues.ColumnLevelIssues == nil {
		tableIssues.ColumnLevelIssues = make(map[string][]internal.SchemaIssue)
	}
	tableIssues.ColumnLevelIssues[colId] = append(tableIssues.ColumnLevelIssues[colId], issue)
	sessionState.Conv.SchemaIssues[tableId] = tableIssues
}

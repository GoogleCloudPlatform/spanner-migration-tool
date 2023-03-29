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
)

// DetectHotspot adds hotspot detected suggestion in schema conversion process for database.
func DetectHotspot() {

	sessionState := session.GetSessionState()

	for _, spannerTable := range sessionState.Conv.SpSchema {

		isHotSpot(spannerTable.PrimaryKeys, spannerTable)
	}

}

// Helper method for hotspot detection.
func isHotSpot(insert []ddl.IndexKey, spannerTable ddl.CreateTable) {

	hotspotTimestamp(insert, spannerTable)
	hotspotAutoincrement(insert, spannerTable)
}

// hotspotTimestamp checks Timestamp hotspot.
// If present adds HotspotTimestamp as an issue in Issues.
func hotspotTimestamp(insert []ddl.IndexKey, spannerTable ddl.CreateTable) {

	for i := 0; i < len(insert); i++ {

		for _, c := range spannerTable.ColDefs {

			if insert[i].ColId == c.Id {

				if c.T.Name == ddl.Timestamp {

					columnId := insert[i].ColId
					sessionState := session.GetSessionState()
					schemaissue := sessionState.Conv.SchemaIssues[spannerTable.Id][columnId]

					schemaissue = append(schemaissue, internal.HotspotTimestamp)
					sessionState.Conv.SchemaIssues[spannerTable.Id][columnId] = schemaissue
				}

			}
		}

	}
}

// hotspotAutoincrement check AutoIncrement hotspot.
// If present adds AutoIncrement as an issue in Issues.
func hotspotAutoincrement(insert []ddl.IndexKey, spannerTable ddl.CreateTable) {

	for i := 0; i < len(insert); i++ {
		for _, c := range spannerTable.ColDefs {
			if insert[i].ColId == c.Name {
				spannerColumnId := c.Id
				detecthotspotAutoincrement(spannerTable, spannerColumnId)
			}

		}
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

				columnId := s.Id
				sessionState := session.GetSessionState()
				schemaissue := sessionState.Conv.SchemaIssues[spannerTable.Id][columnId]

				schemaissue = append(schemaissue, internal.HotspotAutoIncrement)
				sessionState.Conv.SchemaIssues[spannerTable.Id][columnId] = schemaissue

			}

		}
	}
}

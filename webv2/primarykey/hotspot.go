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
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
	"github.com/cloudspannerecosystem/harbourbridge/webv2/session"
)

// DetectHotspot add hotspot suggetion in schema conversion process for schema.
func DetectHotspot() {

	sessionState := session.GetSessionState()

	for _, spannerTable := range sessionState.Conv.SpSchema {

		isHotSpot(spannerTable.Pks, spannerTable)
	}

}

func isHotSpot(insert []ddl.IndexKey, spannerTable ddl.CreateTable) {

	hotspotTimestamp(insert, spannerTable)
	hotspotAutoincrement(insert, spannerTable)
}

// hotspotTimestamp check Timestamp hotspot.
// if prseent add Hotspot_Timestamp as issue in Issues.
func hotspotTimestamp(insert []ddl.IndexKey, spannerTable ddl.CreateTable) {

	for i := 0; i < len(insert); i++ {

		for _, c := range spannerTable.ColDefs {

			if insert[i].Col == c.Name {

				if c.T.Name == ddl.Timestamp {

					columnname := insert[i].Col
					sessionState := session.GetSessionState()
					schemaissue := sessionState.Conv.Issues[spannerTable.Name][columnname]

					schemaissue = append(schemaissue, internal.HotspotTimestamp)
					sessionState.Conv.Issues[spannerTable.Name][columnname] = schemaissue
				}

			}
		}

	}
}

// hotspotAutoincrement check Timestamp hotspot.
// if prseent add Hotspot_AutoIncrement as issue in Issues.
func hotspotAutoincrement(insert []ddl.IndexKey, spannerTable ddl.CreateTable) {

	for i := 0; i < len(insert); i++ {
		for _, c := range spannerTable.ColDefs {
			if insert[i].Col == c.Name {
				spannerColumnId := c.Id
				suggesthotspotAutoincrement(spannerTable, spannerColumnId)
			}

		}
	}
}

func suggesthotspotAutoincrement(spannerTable ddl.CreateTable, spannerColumnId int) {

	sessionState := session.GetSessionState()
	sourcetable := sessionState.Conv.SrcSchema[spannerTable.Name]

	for _, s := range sourcetable.ColDefs {

		if s.Id == spannerColumnId {

			columnname := s.Name
			sessionState := session.GetSessionState()
			schemaissue := sessionState.Conv.Issues[spannerTable.Name][columnname]

			schemaissue = append(schemaissue, internal.HotspotAutoIncrement)
			sessionState.Conv.Issues[spannerTable.Name][columnname] = schemaissue

		}
	}
}

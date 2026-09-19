// Copyright 2026 Google LLC
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

package ddl

import (
	"fmt"
)

func getNeo4jGraphTables(tableSchema Schema) (CreateTable, CreateTable, bool) {
	if len(tableSchema) != 2 {
		return CreateTable{}, CreateTable{}, false
	}
	var parent, child CreateTable
	for _, table := range tableSchema {
		if table.ParentTable.Id == "" {
			parent = table
		} else {
			child = table
		}
	}
	if parent.Id != "" && child.ParentTable.Id == parent.Id {
		return parent, child, true
	}
	return CreateTable{}, CreateTable{}, false
}

func getNeo4jGraphColumns(nodeTable CreateTable, edgeTable CreateTable) (nodeIdCol, nodeLabelCol, nodePropsCol, edgeSrcCol, edgeDestCol, edgeLabelCol, edgePropsCol string, ok bool) {
	if len(nodeTable.PrimaryKeys) < 1 || len(edgeTable.PrimaryKeys) < 3 {
		return "", "", "", "", "", "", "", false
	}

	nodeIdCol = nodeTable.ColDefs[nodeTable.PrimaryKeys[0].ColId].Name

	for _, colId := range nodeTable.ColIds {
		col := nodeTable.ColDefs[colId]
		if col.Name == nodeIdCol {
			continue
		}
		if col.T.Name == JSON {
			nodePropsCol = col.Name
		} else if col.T.Name == String {
			nodeLabelCol = col.Name
		}
	}

	edgeSrcCol = edgeTable.ColDefs[edgeTable.PrimaryKeys[0].ColId].Name
	edgeDestCol = edgeTable.ColDefs[edgeTable.PrimaryKeys[1].ColId].Name

	edgePkIds := make(map[string]bool)
	for _, pk := range edgeTable.PrimaryKeys {
		edgePkIds[pk.ColId] = true
	}

	for _, colId := range edgeTable.ColIds {
		col := edgeTable.ColDefs[colId]
		if col.T.Name == JSON {
			edgePropsCol = col.Name
		} else if !edgePkIds[colId] && col.T.Name == String {
			edgeLabelCol = col.Name
		}
	}

	if nodeIdCol == "" || nodeLabelCol == "" || nodePropsCol == "" || edgeSrcCol == "" || edgeDestCol == "" || edgeLabelCol == "" || edgePropsCol == "" {
		return "", "", "", "", "", "", "", false
	}

	return nodeIdCol, nodeLabelCol, nodePropsCol, edgeSrcCol, edgeDestCol, edgeLabelCol, edgePropsCol, true
}

func getNeo4jGraphDDL(tableSchema Schema, c Config) []string {
	var ddl []string
	nodeTable, edgeTable, ok := getNeo4jGraphTables(tableSchema)
	if !ok {
		return nil
	}
	nodeIdCol, nodeLabelCol, nodePropsCol, edgeSrcCol, edgeDestCol, edgeLabelCol, edgePropsCol, colsOk := getNeo4jGraphColumns(nodeTable, edgeTable)
	if !colsOk {
		return nil
	}

	// 1. Alter statements for lowercase constraints
	ddl = append(ddl, fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s_label_lower_case CHECK(LOWER(%s) = %s)",
		c.quote(nodeTable.Name), nodeTable.Name, c.quote(nodeLabelCol), c.quote(nodeLabelCol)))
	ddl = append(ddl, fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s_label_lower_case CHECK(LOWER(%s) = %s)",
		c.quote(edgeTable.Name), edgeTable.Name, c.quote(edgeLabelCol), c.quote(edgeLabelCol)))

	// 2. CREATE PROPERTY GRAPH statement
	graphDDL := fmt.Sprintf("CREATE OR REPLACE PROPERTY GRAPH Neo4jGraph\n"+
		"  NODE TABLES (\n"+
		"    %s\n"+
		"      DYNAMIC LABEL (%s)\n"+
		"      DYNAMIC PROPERTIES (%s)\n"+
		"  )\n"+
		"  EDGE TABLES (\n"+
		"    %s\n"+
		"      SOURCE KEY (%s) REFERENCES %s (%s)\n"+
		"      DESTINATION KEY (%s) REFERENCES %s (%s)\n"+
		"      DYNAMIC LABEL (%s)\n"+
		"      DYNAMIC PROPERTIES (%s)\n"+
		"  )",
		c.quote(nodeTable.Name), c.quote(nodeLabelCol), c.quote(nodePropsCol),
		c.quote(edgeTable.Name),
		c.quote(edgeSrcCol), c.quote(nodeTable.Name), c.quote(nodeIdCol),
		c.quote(edgeDestCol), c.quote(nodeTable.Name), c.quote(nodeIdCol),
		c.quote(edgeLabelCol), c.quote(edgePropsCol))
	ddl = append(ddl, graphDDL)

	return ddl
}

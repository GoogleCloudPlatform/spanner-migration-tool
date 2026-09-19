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
	"testing"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/stretchr/testify/assert"
)

func TestGetNeo4jGraphTables(t *testing.T) {
	// 1. Valid case
	validSchema := Schema{
		"t1": CreateTable{
			Id:   "t1",
			Name: "CustomNode",
		},
		"t2": CreateTable{
			Id:          "t2",
			Name:        "CustomEdge",
			ParentTable: InterleavedParent{Id: "t1", OnDelete: constants.FK_CASCADE, InterleaveType: "IN PARENT"},
		},
	}
	node, edge, ok := getNeo4jGraphTables(validSchema)
	assert.True(t, ok)
	assert.Equal(t, "CustomNode", node.Name)
	assert.Equal(t, "CustomEdge", edge.Name)

	// 2. Invalid case: 3 tables
	invalidSchemaThreeTables := Schema{
		"t1": CreateTable{Id: "t1"},
		"t2": CreateTable{Id: "t2", ParentTable: InterleavedParent{Id: "t1"}},
		"t3": CreateTable{Id: "t3"},
	}
	_, _, ok = getNeo4jGraphTables(invalidSchemaThreeTables)
	assert.False(t, ok)

	// 3. Invalid case: 2 tables, no parent-child relationship
	invalidSchemaNoInterleave := Schema{
		"t1": CreateTable{Id: "t1"},
		"t2": CreateTable{Id: "t2"},
	}
	_, _, ok = getNeo4jGraphTables(invalidSchemaNoInterleave)
	assert.False(t, ok)
}

func TestGetNeo4jGraphColumns(t *testing.T) {
	// 1. Valid case
	nodeTable := CreateTable{
		Name:   "CustomNode",
		Id:     "t1",
		ColIds: []string{"c1", "c2", "c3"},
		ColDefs: map[string]ColumnDef{
			"c1": {Name: "node_id", Id: "c1", T: Type{Name: String}},
			"c2": {Name: "node_label", Id: "c2", T: Type{Name: String}},
			"c3": {Name: "node_props", Id: "c3", T: Type{Name: JSON}},
		},
		PrimaryKeys: []IndexKey{{ColId: "c1"}},
	}
	edgeTable := CreateTable{
		Name:   "CustomEdge",
		Id:     "t2",
		ColIds: []string{"c4", "c5", "c6", "c7", "c8"},
		ColDefs: map[string]ColumnDef{
			"c4": {Name: "node_id", Id: "c4", T: Type{Name: String}},
			"c5": {Name: "node_dest_id", Id: "c5", T: Type{Name: String}},
			"c6": {Name: "edge_unique_id", Id: "c6", T: Type{Name: String}},
			"c7": {Name: "edge_label", Id: "c7", T: Type{Name: String}},
			"c8": {Name: "edge_props", Id: "c8", T: Type{Name: JSON}},
		},
		PrimaryKeys: []IndexKey{{ColId: "c4"}, {ColId: "c5"}, {ColId: "c6"}},
	}

	nodeId, nodeLabel, nodeProps, edgeSrc, edgeDest, edgeLabel, edgeProps, ok := getNeo4jGraphColumns(nodeTable, edgeTable)
	assert.True(t, ok)
	assert.Equal(t, "node_id", nodeId)
	assert.Equal(t, "node_label", nodeLabel)
	assert.Equal(t, "node_props", nodeProps)
	assert.Equal(t, "node_id", edgeSrc)
	assert.Equal(t, "node_dest_id", edgeDest)
	assert.Equal(t, "edge_label", edgeLabel)
	assert.Equal(t, "edge_props", edgeProps)

	// 2. Invalid case: Missing PK in Node
	invalidNodeNoPK := CreateTable{
		ColIds: []string{"c1", "c2"},
		ColDefs: map[string]ColumnDef{
			"c1": {Name: "node_id", Id: "c1", T: Type{Name: String}},
			"c2": {Name: "node_label", Id: "c2", T: Type{Name: String}},
		},
	}
	_, _, _, _, _, _, _, ok = getNeo4jGraphColumns(invalidNodeNoPK, edgeTable)
	assert.False(t, ok)

	// 3. Invalid case: Edge table has < 3 PKs
	invalidEdgeKeys := CreateTable{
		ColIds: []string{"c4", "c5"},
		ColDefs: map[string]ColumnDef{
			"c4": {Name: "node_id", Id: "c4", T: Type{Name: String}},
			"c5": {Name: "node_dest_id", Id: "c5", T: Type{Name: String}},
		},
		PrimaryKeys: []IndexKey{{ColId: "c4"}, {ColId: "c5"}},
	}
	_, _, _, _, _, _, _, ok = getNeo4jGraphColumns(nodeTable, invalidEdgeKeys)
	assert.False(t, ok)
}

func TestGetNeo4jGraphDDL(t *testing.T) {
	s := Schema{
		"t1": CreateTable{
			Name:   "CustomNode",
			Id:     "t1",
			ColIds: []string{"c1", "c2", "c3"},
			ColDefs: map[string]ColumnDef{
				"c1": {Name: "node_id", Id: "c1", T: Type{Name: String}},
				"c2": {Name: "node_label", Id: "c2", T: Type{Name: String}},
				"c3": {Name: "node_props", Id: "c3", T: Type{Name: JSON}},
			},
			PrimaryKeys: []IndexKey{{ColId: "c1"}},
		},
		"t2": CreateTable{
			Name:   "CustomEdge",
			Id:     "t2",
			ColIds: []string{"c4", "c5", "c6", "c7", "c8"},
			ColDefs: map[string]ColumnDef{
				"c4": {Name: "node_id", Id: "c4", T: Type{Name: String}},
				"c5": {Name: "node_dest_id", Id: "c5", T: Type{Name: String}},
				"c6": {Name: "edge_unique_id", Id: "c6", T: Type{Name: String}},
				"c7": {Name: "edge_label", Id: "c7", T: Type{Name: String}},
				"c8": {Name: "edge_props", Id: "c8", T: Type{Name: JSON}},
			},
			PrimaryKeys: []IndexKey{{ColId: "c4"}, {ColId: "c5"}, {ColId: "c6"}},
			ParentTable: InterleavedParent{Id: "t1", OnDelete: constants.FK_CASCADE, InterleaveType: "IN PARENT"},
		},
	}

	config := Config{
		ProtectIds: true,
	}

	ddl := getNeo4jGraphDDL(s, config)

	assert.Len(t, ddl, 3)

	assert.Contains(t, ddl[0], "ALTER TABLE `CustomNode` ADD CONSTRAINT CustomNode_label_lower_case CHECK(LOWER(`node_label`) = `node_label`)")
	assert.Contains(t, ddl[1], "ALTER TABLE `CustomEdge` ADD CONSTRAINT CustomEdge_label_lower_case CHECK(LOWER(`edge_label`) = `edge_label`)")

	expectedGraphStatement := "CREATE OR REPLACE PROPERTY GRAPH Neo4jGraph\n" +
		"  NODE TABLES (\n" +
		"    `CustomNode`\n" +
		"      DYNAMIC LABEL (`node_label`)\n" +
		"      DYNAMIC PROPERTIES (`node_props`)\n" +
		"  )\n" +
		"  EDGE TABLES (\n" +
		"    `CustomEdge`\n" +
		"      SOURCE KEY (`node_id`) REFERENCES `CustomNode` (`node_id`)\n" +
		"      DESTINATION KEY (`node_dest_id`) REFERENCES `CustomNode` (`node_id`)\n" +
		"      DYNAMIC LABEL (`edge_label`)\n" +
		"      DYNAMIC PROPERTIES (`edge_props`)\n" +
		"  )"
	assert.Equal(t, expectedGraphStatement, ddl[2])
}

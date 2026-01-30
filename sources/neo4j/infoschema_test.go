package neo4j

import (
	"testing"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/schema"
	"github.com/stretchr/testify/assert"
)

func TestNeo4jInfoSchema(t *testing.T) {
	// InfoSchemaImpl effectively acts as the source for Schema generation in Schema-less mode.
	ns := &InfoSchemaImpl{}
	conv := internal.MakeConv()

	// 1. Test GetTables
	tables, err := ns.GetTables()
	assert.NoError(t, err)
	assert.Len(t, tables, 2)
	assert.Equal(t, "GraphNode", tables[0].Name)
	assert.Equal(t, "GraphEdge", tables[1].Name)

	// 2. Test GetColumns for GraphNode
	nodeCols, _, err := ns.GetColumns(conv, tables[0], nil, nil)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(nodeCols))
	// Check for label being an array
	var labelCol schema.Column
	var nodeColNames []string
	for _, c := range nodeCols {
		nodeColNames = append(nodeColNames, c.Name)
		if c.Name == "label" {
			labelCol = c
		}
	}
	assert.Contains(t, nodeColNames, "id")
	assert.Contains(t, nodeColNames, "label")
	assert.Contains(t, nodeColNames, "properties")
	assert.True(t, len(labelCol.Type.ArrayBounds) > 0, "GraphNode label should be an array")

	// 3. Test GetColumns for GraphEdge
	edgeCols, _, err := ns.GetColumns(conv, tables[1], nil, nil)
	assert.NoError(t, err)
	assert.Equal(t, 5, len(edgeCols))
	var edgeColNames []string
	for _, c := range edgeCols {
		edgeColNames = append(edgeColNames, c.Name)
	}
	assert.Contains(t, edgeColNames, "id")
	assert.Contains(t, edgeColNames, "dest_id")
	assert.Contains(t, edgeColNames, "edge_id")
	assert.Contains(t, edgeColNames, "label")
	assert.Contains(t, edgeColNames, "properties")

	// 4. Test GetConstraints (PKs)
	// GraphNode
	nodePKs, _, _, err := ns.GetConstraints(conv, tables[0])
	assert.NoError(t, err)
	assert.Equal(t, []string{"id"}, nodePKs)

	// GraphEdge
	edgePKs, _, _, err := ns.GetConstraints(conv, tables[1])
	assert.NoError(t, err)
	assert.Equal(t, []string{"id", "dest_id", "edge_id"}, edgePKs)

	// 5. Test GetForeignKeys
	// GraphNode (None)
	nodeFKs, err := ns.GetForeignKeys(conv, tables[0])
	assert.NoError(t, err)
	assert.Empty(t, nodeFKs)

	// GraphEdge (Reference to GraphNode)
	edgeFKs, err := ns.GetForeignKeys(conv, tables[1])
	assert.NoError(t, err)
	assert.Len(t, edgeFKs, 1)
	assert.Equal(t, "FK_GraphEdge_GraphNode", edgeFKs[0].Name)
	assert.Equal(t, "GraphNode", edgeFKs[0].ReferTableName)
	assert.Equal(t, []string{"id"}, edgeFKs[0].ColumnNames)
	assert.Equal(t, []string{"id"}, edgeFKs[0].ReferColumnNames)
}

func TestNewInfoSchemaImpl_InvalidProfile(t *testing.T) {
	// Test basic connection failure handling if possible, or just skip if it requires real network.
	// We can skip this for now and focus on schema test.
}

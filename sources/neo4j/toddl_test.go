package neo4j

import (
	"testing"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/schema"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/common"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
	"github.com/stretchr/testify/assert"
)

func TestToSpannerType(t *testing.T) {
	toDdl := ToDdlImpl{}
	conv := internal.MakeConv()

	// Test STRING mapping
	spType, _ := toDdl.ToSpannerType(conv, "", schema.Type{Name: "STRING"}, false)
	assert.Equal(t, ddl.String, spType.Name)
	assert.False(t, spType.IsArray)

	// Test STRING array mapping
	spType, _ = toDdl.ToSpannerType(conv, "", schema.Type{Name: "STRING", ArrayBounds: []int64{-1}}, false)
	assert.Equal(t, ddl.String, spType.Name)
	assert.True(t, spType.IsArray)

	// Test JSON mapping
	spType, _ = toDdl.ToSpannerType(conv, "", schema.Type{Name: "JSON"}, false)
	assert.Equal(t, ddl.JSON, spType.Name)

	// Test fallback/default
	spType, issues := toDdl.ToSpannerType(conv, "", schema.Type{Name: "UNKNOWN"}, false)
	assert.Equal(t, ddl.String, spType.Name)
	assert.Contains(t, issues, internal.NoGoodType)
}

func TestSchemaToSpannerDDL_Neo4jInterleaving(t *testing.T) {
	conv := internal.MakeConv()
	conv.Source = constants.NEO4J

	// Define the source schema for Neo4j GraphNode and GraphEdge.
	conv.SrcSchema = map[string]schema.Table{
		"t1": schema.Table{
			Name:   "GraphNode",
			Id:     "t1",
			ColIds: []string{"c1", "c2", "c3"},
			ColDefs: map[string]schema.Column{
				"c1": {Name: "id", Id: "c1", Type: schema.Type{Name: "STRING"}},
				"c2": {Name: "label", Id: "c2", Type: schema.Type{Name: "STRING"}},
				"c3": {Name: "properties", Id: "c3", Type: schema.Type{Name: "JSON"}},
			},
			PrimaryKeys: []schema.Key{{ColId: "c1"}},
		},
		"t2": schema.Table{
			Name:   "GraphEdge",
			Id:     "t2",
			ColIds: []string{"c4", "c5", "c6", "c7", "c8"},
			ColDefs: map[string]schema.Column{
				"c4": {Name: "id", Id: "c4", Type: schema.Type{Name: "STRING"}},
				"c5": {Name: "dest_id", Id: "c5", Type: schema.Type{Name: "STRING"}},
				"c6": {Name: "edge_id", Id: "c6", Type: schema.Type{Name: "STRING"}},
				"c7": {Name: "label", Id: "c7", Type: schema.Type{Name: "STRING"}},
				"c8": {Name: "properties", Id: "c8", Type: schema.Type{Name: "JSON"}},
			},
			PrimaryKeys: []schema.Key{{ColId: "c4"}, {ColId: "c5"}, {ColId: "c6"}},
			ForeignKeys: []schema.ForeignKey{
				{
					Id:             "fk1",
					Name:           "FK_GraphEdge_GraphNode",
					ColIds:         []string{"c4"},
					ReferTableId:   "t1",
					ReferColumnIds: []string{"c1"},
					OnDelete:       "CASCADE",
				},
			},
		},
	}

	schemaToSpanner := common.SchemaToSpannerImpl{}
	err := schemaToSpanner.SchemaToSpannerDDL(conv, ToDdlImpl{}, internal.AdditionalSchemaAttributes{})
	assert.Nil(t, err)

	// Verify that GraphEdge is interleaved in GraphNode
	nodeTable := conv.SpSchema["t1"]
	edgeTable := conv.SpSchema["t2"]

	assert.Equal(t, "GraphNode", nodeTable.Name)
	assert.Equal(t, "GraphEdge", edgeTable.Name)

	assert.Equal(t, "t1", edgeTable.ParentTable.Id)
	assert.Equal(t, constants.FK_CASCADE, edgeTable.ParentTable.OnDelete)
	assert.Equal(t, "IN PARENT", edgeTable.ParentTable.InterleaveType)

	// Verify that the redundant FK is removed from SpSchema
	assert.Len(t, edgeTable.ForeignKeys, 0)
}

package neo4j

import (
	"testing"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/schema"
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

package neo4j

import (
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/schema"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
)

// ToDdlImpl Neo4j specific implementation for ToDdl.
type ToDdlImpl struct {
}

// ToSpannerType maps a scalar source schema type into a Spanner type.
func (tdi ToDdlImpl) ToSpannerType(conv *internal.Conv, spType string, srcType schema.Type, isPk bool) (ddl.Type, []internal.SchemaIssue) {
	var ty ddl.Type
	switch srcType.Name {
	case "STRING":
		ty = ddl.Type{Name: ddl.String, Len: ddl.MaxLength}
	case "JSON":
		ty = ddl.Type{Name: ddl.JSON}
	default:
		return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.NoGoodType}
	}

	if len(srcType.ArrayBounds) > 0 {
		ty.IsArray = true
	}
	return ty, nil
}

func (tdi ToDdlImpl) GetColumnAutoGen(conv *internal.Conv, autoGenCol ddl.AutoGenCol, colId string, tableId string) (*ddl.AutoGenCol, error) {
	return nil, nil
}

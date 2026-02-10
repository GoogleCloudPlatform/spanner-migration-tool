package neo4j

import (
	"context"
	"fmt"

	sp "cloud.google.com/go/spanner"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/profiles"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/schema"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/common"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
	"github.com/neo4j/neo4j-go-driver/v6/neo4j"
)

const GraphNodeTableName = "GraphNode"
const GraphEdgeTableName = "GraphEdge"

type InfoSchemaImpl struct {
	driver neo4j.Driver
	URI    string
	User   string
	Pwd    string
}

// NewInfoSchemaImpl creates a new Neo4j InfoSchema implementation.
func NewInfoSchemaImpl(ctx context.Context, sourceProfile profiles.SourceProfile) (*InfoSchemaImpl, error) {
	uri := sourceProfile.Conn.Neo4j.URI
	user := sourceProfile.Conn.Neo4j.User
	pwd := sourceProfile.Conn.Neo4j.Pwd

	auth := neo4j.BasicAuth(user, pwd, "")
	driver, err := neo4j.NewDriver(uri, auth)
	if err != nil {
		return nil, fmt.Errorf("failed to create Neo4j driver: %w", err)
	}

	// Verify connection
	err = driver.VerifyConnectivity(ctx)
	if err != nil {
		driver.Close(ctx)
		return nil, fmt.Errorf("failed to connect to Neo4j at %s: %w", uri, err)
	}

	return &InfoSchemaImpl{
		driver: driver,
		URI:    uri,
		User:   user,
		Pwd:    pwd,
	}, nil
}

// GetToDdl returns the ToDdl interface for Neo4j.
func (isi InfoSchemaImpl) GetToDdl() common.ToDdl {
	return ToDdlImpl{}
}

// GetTableName returns table name.
func (isi InfoSchemaImpl) GetTableName(schema string, tableName string) string {
	return tableName
}

// GetTables returns the fixed tables for Schema-less migration: GraphNode and GraphEdge.
func (isi InfoSchemaImpl) GetTables() ([]common.SchemaAndName, error) {
	return []common.SchemaAndName{
		{Name: GraphNodeTableName},
		{Name: GraphEdgeTableName},
	}, nil
}

// GetColumns returns the fixed columns for Schema-less migration.
func (isi InfoSchemaImpl) GetColumns(conv *internal.Conv, table common.SchemaAndName, constraints map[string][]string, primaryKeys []string) (map[string]schema.Column, []string, error) {
	colDefs := make(map[string]schema.Column)
	var colIds []string

	switch table.Name {
	case GraphNodeTableName:
		// GraphNode: id, label, properties
		cols := []struct {
			Name    string
			Type    string
			IsArray bool
		}{
			{"id", "STRING", false},
			{"label", "STRING", true},
			{"properties", "JSON", false},
		}
		for _, c := range cols {
			colId := internal.GenerateColumnId()
			colType := schema.Type{Name: c.Type}
			if c.IsArray {
				colType.ArrayBounds = []int64{-1}
			}
			colDefs[colId] = schema.Column{
				Id:      colId,
				Name:    c.Name,
				Type:    colType,
				NotNull: c.Name == "id",
			}
			colIds = append(colIds, colId)
		}
	case GraphEdgeTableName:
		// GraphEdge: id, dest_id, edge_id, label, properties
		cols := []struct {
			Name string
			Type string
		}{
			{"id", "STRING"}, // Parent ID
			{"dest_id", "STRING"},
			{"edge_id", "STRING"},
			{"label", "STRING"},
			{"properties", "JSON"},
		}
		for _, c := range cols {
			colId := internal.GenerateColumnId()
			colDefs[colId] = schema.Column{
				Id:      colId,
				Name:    c.Name,
				Type:    schema.Type{Name: c.Type},
				NotNull: c.Name == "id" || c.Name == "dest_id" || c.Name == "edge_id",
			}
			colIds = append(colIds, colId)
		}
	default:
		return nil, nil, fmt.Errorf("unknown table %s", table.Name)
	}

	return colDefs, colIds, nil
}

// GetRowsFromTable returns a sql Rows object for a table.
// TODO: Implement for data migration.
func (isi InfoSchemaImpl) GetRowsFromTable(conv *internal.Conv, tableId string) (interface{}, error) {
	return nil, fmt.Errorf("not implemented for Neo4j yet")
}

// GetRowCount returns the row count for the table.
// TODO: Implement for stats.
func (isi InfoSchemaImpl) GetRowCount(table common.SchemaAndName) (int64, error) {
	return 0, nil
}

// GetConstraints returns the constraints for the table.
func (isi InfoSchemaImpl) GetConstraints(conv *internal.Conv, table common.SchemaAndName) ([]string, []schema.CheckConstraint, map[string][]string, error) {
	var primaryKeys []string
	if table.Name == GraphNodeTableName {
		primaryKeys = []string{"id"}
	} else if table.Name == GraphEdgeTableName {
		primaryKeys = []string{"id", "dest_id", "edge_id"}
	}
	return primaryKeys, nil, nil, nil
}

// GetForeignKeys returns the foreign keys for the table.
func (isi InfoSchemaImpl) GetForeignKeys(conv *internal.Conv, table common.SchemaAndName) (foreignKeys []schema.ForeignKey, err error) {
	if table.Name == GraphEdgeTableName {
		// GraphEdge (id) -> GraphNode (id) INTERLEAVE IN PARENT
		// Note: We represent this as a FK here, but it will be converted to Interleave by the mapping logic if configured.
		// For Schema-less, we enforce Interleave.
		// But strictly speaking, GetForeignKeys should return Source FKs.
		// Neo4j doesn't have FKs. But we model the relationship structure here.
		// Let's return nothing here and handle Interleave in ProcessTable or manually ??
		// Actually, standard logic uses these FKs to allow user to choose Interleave.
		// Let's return a synthetic FK.
		fkId := internal.GenerateForeignkeyId()
		foreignKeys = append(foreignKeys, schema.ForeignKey{
			Id:               fkId,
			Name:             "FK_GraphEdge_GraphNode",
			ColumnNames:      []string{"id"},
			ReferTableName:   GraphNodeTableName,
			ReferColumnNames: []string{"id"},
			OnDelete:         "CASCADE",
		})
	}
	return foreignKeys, nil
}

// GetIndexes returns the indexes for the table.
func (isi InfoSchemaImpl) GetIndexes(conv *internal.Conv, table common.SchemaAndName, colNameIdMp map[string]string) ([]schema.Index, error) {
	return nil, nil
}

// ProcessData performs data conversion for source database.
func (isi InfoSchemaImpl) ProcessData(conv *internal.Conv, tableId string, srcSchema schema.Table, spCols []string, spSchema ddl.CreateTable, additionalAttributes internal.AdditionalDataAttributes) error {
	return fmt.Errorf("not implemented for Neo4j yet")
}

// StartChangeDataCapture is used for automatic triggering of Datastream job.
func (isi InfoSchemaImpl) StartChangeDataCapture(ctx context.Context, conv *internal.Conv) (map[string]interface{}, error) {
	return nil, fmt.Errorf("CDC not supported for Neo4j")
}

// StartStreamingMigration is used for automatic triggering of Dataflow job.
func (isi InfoSchemaImpl) StartStreamingMigration(ctx context.Context, migrationProjectId string, client *sp.Client, conv *internal.Conv, streamInfo map[string]interface{}) (internal.DataflowOutput, error) {
	return internal.DataflowOutput{}, fmt.Errorf("streaming migration not supported for Neo4j")
}

// Close closes the Neo4j driver connection.
func (isi *InfoSchemaImpl) Close(ctx context.Context) {
	if isi.driver != nil {
		isi.driver.Close(ctx)
	}
}

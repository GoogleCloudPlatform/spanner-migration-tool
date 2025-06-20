// Copyright 2020 Google LLC
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

package cassandra

import (
	"context"
	"fmt"

	sp "cloud.google.com/go/spanner"
	cc "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/clients/cassandra"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/profiles"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/schema"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/common"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
	"github.com/gocql/gocql"
)

// InfoSchemaImpl is Cassandra specific implementation for InfoSchema
type InfoSchemaImpl struct {
	KeyspaceMetadata cc.KeyspaceMetadataInterface
	SourceProfile    profiles.SourceProfile
	TargetProfile    profiles.TargetProfile
}

// GetToDdl implements the common.InfoSchema interface
func (isi InfoSchemaImpl) GetToDdl() common.ToDdl {
	return ToDdlImpl{
		typeMapper: NewCassandraTypeMapper(),
	}
}

// GetTableName returns table name
func (isi InfoSchemaImpl) GetTableName(schema string, tableName string) string {
	return tableName
}

// getTableMetadata returns metadata assosiated with specific table
func (isi InfoSchemaImpl) getTableMetadata(tableName string) (*gocql.TableMetadata, bool) {
	if isi.KeyspaceMetadata == nil {
		return nil, false
	}
	for _, tm := range isi.KeyspaceMetadata.Tables() {
		if tm.Name == tableName {
			return tm, true
		}
	}
	return nil, false
}

// GetTables return list of tables in selected keyspace
func (isi InfoSchemaImpl) GetTables() ([]common.SchemaAndName, error) {
	if isi.KeyspaceMetadata == nil {
		return nil, fmt.Errorf("keyspace metadata not initialized")
	}

	tableMetas := isi.KeyspaceMetadata.Tables()
	if len(tableMetas) == 0 {
		return nil, fmt.Errorf("no tables found in keyspace '%s'", isi.SourceProfile.Conn.Cassandra.Keyspace)
	}

	var tables []common.SchemaAndName
	keyspace := isi.SourceProfile.Conn.Cassandra.Keyspace
	for _, tableMeta := range tableMetas {
		tables = append(tables, common.SchemaAndName{Schema: keyspace, Name: tableMeta.Name})
	}
	return tables, nil
}

// getTypeString is a helper function to get collection types as string
func getTypeString(typeInfo gocql.TypeInfo) (string, error) {
	switch typeInfo.Type() {
	case gocql.TypeSet:
		collectionInfo, ok := typeInfo.(gocql.CollectionType)
		if !ok {
			return "", fmt.Errorf("invalid set type")
		}
		elemType, err := getTypeString(collectionInfo.Elem)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("set<%s>", elemType), nil

	case gocql.TypeList:
		collectionInfo, ok := typeInfo.(gocql.CollectionType)
		if !ok {
			return "", fmt.Errorf("invalid list type")
		}
		elemType, err := getTypeString(collectionInfo.Elem)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("list<%s>", elemType), nil

	case gocql.TypeMap:
		collectionInfo, ok := typeInfo.(gocql.CollectionType)
		if !ok {
			return "", fmt.Errorf("invalid map type")
		}
		keyType, errKey := getTypeString(collectionInfo.Key)
		if errKey != nil {
			return "", errKey
		}
		valueType, errVal := getTypeString(collectionInfo.Elem)
		if errVal != nil {
			return "", errVal
		}
		return fmt.Sprintf("map<%s,%s>", keyType, valueType), nil

	default:
		return typeInfo.Type().String(), nil
	}
}

// GetColumns returns a list of Column objects and names
func (isi InfoSchemaImpl) GetColumns(conv *internal.Conv, table common.SchemaAndName, constraints map[string][]string, primaryKeys []string) (map[string]schema.Column, []string, error) {
	if isi.KeyspaceMetadata == nil {
		return nil, nil, fmt.Errorf("keyspace metadata not initialized")
	}

	tableMetadata, ok := isi.getTableMetadata(table.Name)
	if !ok {
		return nil, nil, fmt.Errorf("table '%s' not found in keyspace metadata", table.Name)
	}

	colDefs := make(map[string]schema.Column)
	var colIds []string

	pkCols := make(map[string]bool)
	for _, pkCol := range tableMetadata.PartitionKey {
		pkCols[pkCol.Name] = true
	}
	for _, ckCol := range tableMetadata.ClusteringColumns {
		pkCols[ckCol.Name] = true
	}

	for _, colMeta := range tableMetadata.Columns {
		colId := internal.GenerateColumnId()
		isPrimaryKey := pkCols[colMeta.Name]

		colType, err := getTypeString(colMeta.Type)
		if err != nil {
			return nil, nil, err
		}

		c := schema.Column{
			Id:      colId,
			Name:    colMeta.Name,
			Type:    schema.Type{Name: colType},
			NotNull: isPrimaryKey,
			Ignored: schema.Ignored{},
		}
		colDefs[colId] = c
		colIds = append(colIds, colId)
	}
	return colDefs, colIds, nil
}

// GetConstraints returns a list of primary keys for a given table.
// Cassandra does not have check constraints and other constraints in the SQL sense.
func (isi InfoSchemaImpl) GetConstraints(conv *internal.Conv, table common.SchemaAndName) ([]string, []schema.CheckConstraint, map[string][]string, error) {
	if isi.KeyspaceMetadata == nil {
		return nil, nil, nil, fmt.Errorf("keyspace metadata not initialized")
	}

	tableMetadata, ok := isi.getTableMetadata(table.Name)
	if !ok {
		return nil, nil, nil, fmt.Errorf("table '%s' not found in keyspace metadata", table.Name)
	}

	var primaryKeys []string

	for _, colMeta := range tableMetadata.PartitionKey {
		primaryKeys = append(primaryKeys, colMeta.Name)
	}

	for _, colMeta := range tableMetadata.ClusteringColumns {
		primaryKeys = append(primaryKeys, colMeta.Name)
	}

	return primaryKeys, nil, make(map[string][]string), nil
}

// GetForeignKeys returns an empty list as Cassandra does not have foreign keys.
func (isi InfoSchemaImpl) GetForeignKeys(conv *internal.Conv, table common.SchemaAndName) ([]schema.ForeignKey, error) {
	return nil, nil
}

// TODO: gocql driver does not currently populate ColumnIndexMetadata in ColumnMetadata.
// GetIndexes returns a list of secondary indexes for a given table.
func (isi InfoSchemaImpl) GetIndexes(conv *internal.Conv, table common.SchemaAndName, colNameIdMap map[string]string) ([]schema.Index, error) {
	if isi.KeyspaceMetadata == nil {
		return nil, fmt.Errorf("keyspace metadata not initialized")
	}

	tableMetadata, ok := isi.getTableMetadata(table.Name)
	if !ok {
		return nil, fmt.Errorf("table '%s' not found in keyspace metadata", table.Name)
	}
	
	var indexes []schema.Index
	for _, colMeta := range tableMetadata.Columns {
		if colMeta.Index.Name != "" {
			indexMeta := colMeta.Index
			targetColumn := colMeta.Name

			spIndex := schema.Index{
				Id:     internal.GenerateIndexesId(),
				Name:   indexMeta.Name,
				Unique: false,
				Keys: []schema.Key{
					{
						ColId: colNameIdMap[targetColumn],
					},
				},
			}
			indexes = append(indexes, spIndex)
		}
	}

	return indexes, nil
}

// Data Migration Related Methods (Stubs for Schema Migration)

var errNotSupported = fmt.Errorf("operation not supported")

func (isi InfoSchemaImpl) GetRowsFromTable(conv *internal.Conv, tableId string) (interface{}, error) {
	return nil, errNotSupported
}

func (isi InfoSchemaImpl) GetRowCount(table common.SchemaAndName) (int64, error) {
	return 0, errNotSupported
}

func (isi InfoSchemaImpl) ProcessData(conv *internal.Conv, tableId string, srcSchema schema.Table, spCols []string, spSchema ddl.CreateTable, additionalAttributes internal.AdditionalDataAttributes) error {
	return errNotSupported
}

func (isi InfoSchemaImpl) StartChangeDataCapture(ctx context.Context, conv *internal.Conv) (map[string]interface{}, error) {
	return nil, errNotSupported
}

func (isi InfoSchemaImpl) StartStreamingMigration(ctx context.Context, migrationProjectId string, client *sp.Client, conv *internal.Conv, streamInfo map[string]interface{}) (internal.DataflowOutput, error) {
	return internal.DataflowOutput{}, errNotSupported
}

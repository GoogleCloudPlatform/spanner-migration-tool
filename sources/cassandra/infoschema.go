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
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gocql/gocql"

	sp "cloud.google.com/go/spanner"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/profiles"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/schema"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/common"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
)

// Connect establishes a session with the Cassandra cluster using parameters from SourceProfile.
func Connect(sourceProfile profiles.SourceProfile) (SessionInterface, error) {
	cfg := sourceProfile.Conn.Cassandra

	contactPoint := strings.TrimSpace(cfg.Host)

	cluster := gocql.NewCluster(contactPoint)
	cluster.Keyspace = cfg.Keyspace
	cluster.Consistency = gocql.Quorum
	cluster.Timeout = 10 * time.Second
	cluster.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: 3}

	if cfg.Port != "" {
		port, err := strconv.Atoi(cfg.Port)
		if err != nil {
			return nil, fmt.Errorf("invalid Cassandra port '%s': %w", cfg.Port, err)
		}
		cluster.Port = port
	}

	if cfg.User != "" {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: cfg.User,
			Password: cfg.Pwd,
		}
	}

	if cfg.DataCenter != "" {
		cluster.PoolConfig.HostSelectionPolicy = gocql.DCAwareRoundRobinPolicy(cfg.DataCenter)
	}

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("could not create Cassandra session: %w", err)
	}

	return NewSessionWrapper(session), nil
}

// InfoSchemaImpl is Cassandra specific implementation for InfoSchema
type InfoSchemaImpl struct {
	Session       SessionInterface
	SourceProfile profiles.SourceProfile
	TargetProfile profiles.TargetProfile
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

// GetTables return list of tables in selected keyspace
func (isi InfoSchemaImpl) GetTables() ([]common.SchemaAndName, error) {
	keyspace := isi.SourceProfile.Conn.Cassandra.Keyspace
	if keyspace == "" {
		return nil, fmt.Errorf("keyspace not specified in source profile")
	}

	q := "SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?"
	rows := isi.Session.Query(q, keyspace).Iter()

	defer rows.Close()

	var tableName string
	// Schema would be keyspace in case of cassandra
	var tables []common.SchemaAndName
	for rows.Scan(&tableName) {
		tables = append(tables, common.SchemaAndName{Schema: keyspace, Name: tableName})
	}
	return tables, nil
}

// GetColumns returns a list of Column objects and names
func (isi InfoSchemaImpl) GetColumns(conv *internal.Conv, table common.SchemaAndName, constraints map[string][]string, primaryKeys []string) (map[string]schema.Column, []string, error) {
	q := `SELECT column_name, type, kind FROM system_schema.columns WHERE keyspace_name = ? AND table_name = ?`
	cols := isi.Session.Query(q, table.Schema, table.Name).Iter()

	defer cols.Close()

	colDefs := make(map[string]schema.Column)
	var colIds []string
	var colName, dataType, columnType string
	for cols.Scan(&colName, &dataType, &columnType) {
		colId := internal.GenerateColumnId()
		isPrimaryKey := columnType == "partition_key" || columnType == "clustering"

		c := schema.Column{
			Id:      colId,
			Name:    colName,
			Type:    schema.Type{Name: dataType},
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
	query := `SELECT column_name, kind, position FROM system_schema.columns WHERE keyspace_name = ? AND table_name = ?`
	rows := isi.Session.Query(query, table.Schema, table.Name).Iter()
	defer rows.Close()

	var partitionKeys []string
	type clusteringKey struct {
		Name     string
		Position int
	}
	var clusteringKeys []clusteringKey
	var colName, colType string
	var position int
	for rows.Scan(&colName, &colType, &position) {
		if colType == "partition_key" {
			partitionKeys = append(partitionKeys, colName)
		} else if colType == "clustering" {
			clusteringKeys = append(clusteringKeys, clusteringKey{Name: colName, Position: position})
		}
	}
	sort.Slice(clusteringKeys, func(i, j int) bool {
		return clusteringKeys[i].Position < clusteringKeys[j].Position
	})

	// The full primary key is the partition key(s) concatenated by the clustering key(s).
	var primaryKeys []string
	primaryKeys = append(primaryKeys, partitionKeys...)
	for _, ck := range clusteringKeys {
		primaryKeys = append(primaryKeys, ck.Name)
	}

	return primaryKeys, nil, make(map[string][]string), nil
}

// GetForeignKeys returns an empty list as Cassandra does not have foreign keys.
func (isi InfoSchemaImpl) GetForeignKeys(conv *internal.Conv, table common.SchemaAndName) ([]schema.ForeignKey, error) {
	return nil, nil
}

// GetIndexes returns a list of secondary indexes for a given table.
// Note: Standard Cassandra secondary indexes are non-unique.
func (isi InfoSchemaImpl) GetIndexes(conv *internal.Conv, table common.SchemaAndName, colNameIdMap map[string]string) ([]schema.Index, error) {
	q := `SELECT index_name, options FROM system_schema.indexes WHERE keyspace_name = ? AND table_name = ?`
	rows := isi.Session.Query(q, table.Schema, table.Name).Iter()
	defer rows.Close()

	var indexes []schema.Index
	var indexName string
	var options map[string]string
	for rows.Scan(&indexName, &options) {
		targetColumn, ok := options["target"]
		if !ok {
			conv.Unexpected(fmt.Sprintf("Target column not found for index '%s' in table '%s'", indexName, table.Name))
			continue
		}
		targetColumn = strings.Trim(targetColumn, `"`)

		colId, ok := colNameIdMap[targetColumn]
		if !ok {
			conv.Unexpected(fmt.Sprintf("Target column '%s' for index '%s' not found in column map for table '%s'", targetColumn, indexName, table.Name))
			continue
		}

		spIndex := schema.Index{
			Id:     internal.GenerateIndexesId(),
			Name:   indexName,
			Unique: false,
			Keys: []schema.Key{
				{
					ColId: colId,
				},
			},
		}
		indexes = append(indexes, spIndex)
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

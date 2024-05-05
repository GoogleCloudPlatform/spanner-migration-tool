// Copyright 2023 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package streaming

import (
	"sort"
	"testing"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/profiles"
	"github.com/stretchr/testify/assert"
	datastreampb "google.golang.org/genproto/googleapis/cloud/datastream/v1"
)

func TestGetPostgreSQLSourceStreamConfig(t *testing.T) {
	testCases := []struct {
		name        string
		input       DatastreamCfg
		expectedCfg *datastreampb.SourceConfig_PostgresqlSourceConfig
		err         error
	}{
		{
			name: "Valid datastream configuration with two schemas",
			input: DatastreamCfg{
				Properties: "replicationSlot=rep1,publication=pub1",
				SchemaDetails: map[string]internal.SchemaDetails{
					"public": {
						TableDetails: []internal.TableDetails{
							{
								TableName: "table1",
							},
							{
								TableName: "table2",
							},
						},
					},
					"special": {
						TableDetails: []internal.TableDetails{
							{
								TableName: "special.table1",
							},
							{
								TableName: "special.table2",
							},
						},
					},
				},
			},
			expectedCfg: &datastreampb.SourceConfig_PostgresqlSourceConfig{
				PostgresqlSourceConfig: &datastreampb.PostgresqlSourceConfig{
					IncludeObjects: &datastreampb.PostgresqlRdbms{PostgresqlSchemas: []*datastreampb.PostgresqlSchema{
						{
							Schema: "public",
							PostgresqlTables: []*datastreampb.PostgresqlTable{
								{
									Table: "table1",
								},
								{
									Table: "table2",
								},
							},
						},
						{
							Schema: "special",
							PostgresqlTables: []*datastreampb.PostgresqlTable{
								{
									Table: "table1",
								},
								{
									Table: "table2",
								},
							},
						},
					}},
					MaxConcurrentBackfillTasks: 50,
					ReplicationSlot:            "rep1",
					Publication:                "pub1",
				},
			},
			err: nil,
		},
		{
			name: "Valid datastream configuration with only public schema",
			input: DatastreamCfg{
				Properties: "replicationSlot=rep1,publication=pub1",
				SchemaDetails: map[string]internal.SchemaDetails{
					"public": {
						TableDetails: []internal.TableDetails{
							{
								TableName: "table1",
							},
							{
								TableName: "table2",
							},
							{
								TableName: "table3",
							},
						},
					},
				},
			},
			expectedCfg: &datastreampb.SourceConfig_PostgresqlSourceConfig{
				PostgresqlSourceConfig: &datastreampb.PostgresqlSourceConfig{
					IncludeObjects: &datastreampb.PostgresqlRdbms{PostgresqlSchemas: []*datastreampb.PostgresqlSchema{
						{
							Schema: "public",
							PostgresqlTables: []*datastreampb.PostgresqlTable{
								{
									Table: "table1",
								},
								{
									Table: "table2",
								},
								{
									Table: "table3",
								},
							},
						},
					}},
					MaxConcurrentBackfillTasks: 50,
					ReplicationSlot:            "rep1",
					Publication:                "pub1",
				},
			},
			err: nil,
		},
		{
			name: "Valid datastream configuration with non-public schema",
			input: DatastreamCfg{
				Properties: "replicationSlot=rep1,publication=pub1",
				SchemaDetails: map[string]internal.SchemaDetails{
					"special": {
						TableDetails: []internal.TableDetails{
							{
								TableName: "special.table1",
							},
							{
								TableName: "special.table2",
							},
							{
								TableName: "special.table3",
							},
						},
					},
				},
			},
			expectedCfg: &datastreampb.SourceConfig_PostgresqlSourceConfig{
				PostgresqlSourceConfig: &datastreampb.PostgresqlSourceConfig{
					IncludeObjects: &datastreampb.PostgresqlRdbms{PostgresqlSchemas: []*datastreampb.PostgresqlSchema{
						{
							Schema: "special",
							PostgresqlTables: []*datastreampb.PostgresqlTable{
								{
									Table: "table1",
								},
								{
									Table: "table2",
								},
								{
									Table: "table3",
								},
							},
						},
					}},
					MaxConcurrentBackfillTasks: 50,
					ReplicationSlot:            "rep1",
					Publication:                "pub1",
				},
			},
			err: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := getPostgreSQLSourceStreamConfig(tc.input)
			assertEqualPostgresqlSourceConfig(t, tc.expectedCfg, result)
			assert.Equal(t, tc.err, err)
		})
	}
}

func TestGetMySQLSourceStreamConfig(t *testing.T) {
	testCases := []struct {
		name        string
		inputCfg    DatastreamCfg
		dbList      []profiles.LogicalShard
		expectedCfg *datastreampb.SourceConfig_MysqlSourceConfig
		err         error
	}{
		{
			name: "Valid datastream configuration with single database",
			inputCfg: DatastreamCfg{
				SchemaDetails: map[string]internal.SchemaDetails{
					"db1": {
						TableDetails: []internal.TableDetails{
							{
								TableName: "table1",
							},
							{
								TableName: "table2",
							},
						},
					},
				},
			},
			dbList: []profiles.LogicalShard{
				{
					DbName:         "db1",
					LogicalShardId: "l1",
					RefDataShardId: "x1",
				},
			},
			expectedCfg: &datastreampb.SourceConfig_MysqlSourceConfig{
				MysqlSourceConfig: &datastreampb.MysqlSourceConfig{
					IncludeObjects: &datastreampb.MysqlRdbms{MysqlDatabases: []*datastreampb.MysqlDatabase{
						{
							Database: "db1",
							MysqlTables: []*datastreampb.MysqlTable{
								{
									Table: "table1",
								},
								{
									Table: "table2",
								},
							},
						},
					}},
					MaxConcurrentBackfillTasks: 50,
					MaxConcurrentCdcTasks:      5,
				},
			},
			err: nil,
		},
		{
			name: "Valid datastream configuration with multiple database",
			inputCfg: DatastreamCfg{
				SchemaDetails: map[string]internal.SchemaDetails{
					"db1": {
						TableDetails: []internal.TableDetails{
							{
								TableName: "table1",
							},
							{
								TableName: "table2",
							},
						},
					},
				},
			},
			dbList: []profiles.LogicalShard{
				{
					DbName:         "db1",
					LogicalShardId: "l1",
					RefDataShardId: "x1",
				},
				{
					DbName:         "db2",
					LogicalShardId: "l2",
					RefDataShardId: "x2",
				},
			},
			expectedCfg: &datastreampb.SourceConfig_MysqlSourceConfig{
				MysqlSourceConfig: &datastreampb.MysqlSourceConfig{
					IncludeObjects: &datastreampb.MysqlRdbms{MysqlDatabases: []*datastreampb.MysqlDatabase{
						{
							Database: "db1",
							MysqlTables: []*datastreampb.MysqlTable{
								{
									Table: "table1",
								},
								{
									Table: "table2",
								},
							},
						},
						{
							Database: "db2",
							MysqlTables: []*datastreampb.MysqlTable{
								{
									Table: "table1",
								},
								{
									Table: "table2",
								},
							},
						},
					}},
					MaxConcurrentBackfillTasks: 50,
					MaxConcurrentCdcTasks:      5,
				},
			},
			err: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := getMysqlSourceStreamConfig(tc.dbList, tc.inputCfg)
			assertEqualMysqlSourceConfig(t, tc.expectedCfg, result)
			assert.Equal(t, tc.err, err)
		})
	}
}

func assertEqualMysqlDatabase(t *testing.T, expected, actual *datastreampb.MysqlDatabase) {
	assert.Equal(t, expected.Database, actual.Database)
	assert.Equal(t, len(expected.MysqlTables), len(actual.MysqlTables))

	// Sort tables to ensure order-independent comparison
	expectedTables := make([]string, len(expected.MysqlTables))
	actualTables := make([]string, len(actual.MysqlTables))

	for i, table := range expected.MysqlTables {
		expectedTables[i] = table.Table
	}
	for i, table := range actual.MysqlTables {
		actualTables[i] = table.Table
	}

	sort.Strings(expectedTables)
	sort.Strings(actualTables)

	assert.Equal(t, expectedTables, actualTables)
}

func assertEqualMysqlSourceConfig(t *testing.T, expected, actual *datastreampb.SourceConfig_MysqlSourceConfig) {
	assert.Equal(t, expected.MysqlSourceConfig.MaxConcurrentBackfillTasks, actual.MysqlSourceConfig.MaxConcurrentBackfillTasks)
	assert.Equal(t, expected.MysqlSourceConfig.MaxConcurrentCdcTasks, actual.MysqlSourceConfig.MaxConcurrentCdcTasks)

	assert.Equal(t, len(expected.MysqlSourceConfig.IncludeObjects.MysqlDatabases), len(actual.MysqlSourceConfig.IncludeObjects.MysqlDatabases))

	// Sort databases to ensure order-independent comparison
	expectedDatabases := make([]string, len(expected.MysqlSourceConfig.IncludeObjects.MysqlDatabases))
	actualDatabases := make([]string, len(actual.MysqlSourceConfig.IncludeObjects.MysqlDatabases))

	for i, db := range expected.MysqlSourceConfig.IncludeObjects.MysqlDatabases {
		expectedDatabases[i] = db.Database
	}
	for i, db := range actual.MysqlSourceConfig.IncludeObjects.MysqlDatabases {
		actualDatabases[i] = db.Database
	}

	sort.Strings(expectedDatabases)
	sort.Strings(actualDatabases)

	assert.Equal(t, expectedDatabases, actualDatabases)

	sort.Slice(expected.MysqlSourceConfig.IncludeObjects.MysqlDatabases, func(i, j int) bool {
		return expected.MysqlSourceConfig.IncludeObjects.MysqlDatabases[i].Database < expected.MysqlSourceConfig.IncludeObjects.MysqlDatabases[j].Database
	})
	sort.Slice(actual.MysqlSourceConfig.IncludeObjects.MysqlDatabases, func(i, j int) bool {
		return actual.MysqlSourceConfig.IncludeObjects.MysqlDatabases[i].Database < actual.MysqlSourceConfig.IncludeObjects.MysqlDatabases[j].Database
	})

	for i := range expected.MysqlSourceConfig.IncludeObjects.MysqlDatabases {
		assertEqualMysqlDatabase(t, expected.MysqlSourceConfig.IncludeObjects.MysqlDatabases[i], actual.MysqlSourceConfig.IncludeObjects.MysqlDatabases[i])
	}
}

func assertEqualPostgresqlSchema(t *testing.T, expected, actual *datastreampb.PostgresqlSchema) {
	assert.Equal(t, expected.Schema, actual.Schema)
	assert.Equal(t, len(expected.PostgresqlTables), len(actual.PostgresqlTables))

	// Sort tables to ensure order-independent comparison
	expectedTables := make([]string, len(expected.PostgresqlTables))
	actualTables := make([]string, len(actual.PostgresqlTables))

	for i, table := range expected.PostgresqlTables {
		expectedTables[i] = table.Table
	}
	for i, table := range actual.PostgresqlTables {
		actualTables[i] = table.Table
	}

	sort.Strings(expectedTables)
	sort.Strings(actualTables)

	assert.Equal(t, expectedTables, actualTables)
}

func assertEqualPostgresqlSourceConfig(t *testing.T, expected, actual *datastreampb.SourceConfig_PostgresqlSourceConfig) {
	assert.Equal(t, expected.PostgresqlSourceConfig.MaxConcurrentBackfillTasks, actual.PostgresqlSourceConfig.MaxConcurrentBackfillTasks)
	assert.Equal(t, expected.PostgresqlSourceConfig.ReplicationSlot, actual.PostgresqlSourceConfig.ReplicationSlot)
	assert.Equal(t, expected.PostgresqlSourceConfig.Publication, actual.PostgresqlSourceConfig.Publication)

	assert.Equal(t, len(expected.PostgresqlSourceConfig.IncludeObjects.PostgresqlSchemas), len(actual.PostgresqlSourceConfig.IncludeObjects.PostgresqlSchemas))

	// Sort schemas to ensure order-independent comparison
	expectedSchemas := make([]string, len(expected.PostgresqlSourceConfig.IncludeObjects.PostgresqlSchemas))
	actualSchemas := make([]string, len(actual.PostgresqlSourceConfig.IncludeObjects.PostgresqlSchemas))

	for i, schema := range expected.PostgresqlSourceConfig.IncludeObjects.PostgresqlSchemas {
		expectedSchemas[i] = schema.Schema
	}
	for i, schema := range actual.PostgresqlSourceConfig.IncludeObjects.PostgresqlSchemas {
		actualSchemas[i] = schema.Schema
	}

	sort.Strings(expectedSchemas)
	sort.Strings(actualSchemas)

	assert.Equal(t, expectedSchemas, actualSchemas)
	sort.Slice(expected.PostgresqlSourceConfig.IncludeObjects.PostgresqlSchemas, func(i, j int) bool {
		return expected.PostgresqlSourceConfig.IncludeObjects.PostgresqlSchemas[i].Schema < expected.PostgresqlSourceConfig.IncludeObjects.PostgresqlSchemas[j].Schema
	})
	sort.Slice(actual.PostgresqlSourceConfig.IncludeObjects.PostgresqlSchemas, func(i, j int) bool {
		return actual.PostgresqlSourceConfig.IncludeObjects.PostgresqlSchemas[i].Schema < actual.PostgresqlSourceConfig.IncludeObjects.PostgresqlSchemas[j].Schema
	})

	for i := range expected.PostgresqlSourceConfig.IncludeObjects.PostgresqlSchemas {
		assertEqualPostgresqlSchema(t, expected.PostgresqlSourceConfig.IncludeObjects.PostgresqlSchemas[i], actual.PostgresqlSourceConfig.IncludeObjects.PostgresqlSchemas[i])
	}
}

func TestGetUpdateDataStreamLRORetryBackoff(t *testing.T) {
	backoff := getUpdateDataStreamLRORetryBackoff()
	assert.Equal(t, backoff.InitialInterval, DEFAULT_DATASTREAM_LRO_POLL_BASE_DELAY)
	assert.Equal(t, backoff.Multiplier, DEFAULT_DATASTREAM_LRO_POLL_MULTIPLIER)
	assert.Equal(t, backoff.MaxInterval, DEFAULT_DATASTREAM_LRO_POLL_MAX_DELAY)
	assert.Equal(t, backoff.MaxElapsedTime, DEFAULT_DATASTREAM_LRO_POLL_MAX_ELAPSED_TIME)
}

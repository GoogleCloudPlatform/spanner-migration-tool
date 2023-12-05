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
	"testing"

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
				TableSchemaMap: map[string][]string{
					"public":  {"table1", "table2"},
					"special": {"special.table1", "special.table2"},
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
				TableSchemaMap: map[string][]string{
					"public": {"table1", "table2", "table3"},
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
				TableSchemaMap: map[string][]string{
					"special": {"special.table1", "special.table2", "special.table3"},
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
			assert.Equal(t, tc.expectedCfg, result)
			assert.Equal(t, tc.err, err)
		})
	}
}

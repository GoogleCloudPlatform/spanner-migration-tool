// Copyright 2020 Google LLC
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
package cassandra

import (
	"context"
	"sort"
	"testing"

	cc "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/clients/cassandra"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/profiles"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/schema"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/common"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
)

func TestGetToDdl(t *testing.T) {
	isi := InfoSchemaImpl{}
	assert.IsType(t, ToDdlImpl{}, isi.GetToDdl())
}

func TestGetTableName(t *testing.T) {
	isi := InfoSchemaImpl{}
	actual := isi.GetTableName("some_schema", "my_table")
	assert.Equal(t, "my_table", actual)
}

func TestGetTableMetadata(t *testing.T) {
	mockKeyspace := &cc.MockKeyspaceMetadata{}
	tables := map[string]*gocql.TableMetadata{
		"table1": {Name: "table1"},
		"table2": {Name: "table2"},
	}
	mockKeyspace.On("Tables").Return(tables)

	isi := InfoSchemaImpl{
		KeyspaceMetadata: mockKeyspace,
	}
	// Test Case 1: Table exists
	meta, found := isi.getTableMetadata("table1")
	assert.True(t, found, "Expected to find table 'table1'")
	assert.NotNil(t, meta)
	if meta != nil {
		assert.Equal(t, "table1", meta.Name)
	}
	// Test Case 2: Table not found
	_, found = isi.getTableMetadata("table3")
	assert.False(t, found, "Expected not to find table 'table3'")
	// Test Case 3: nil Keyspace
	isi = InfoSchemaImpl{
		KeyspaceMetadata: nil,
	}
	_, ok := isi.getTableMetadata("table1")
	assert.False(t, ok)
	mockKeyspace.AssertExpectations(t)
}

func TestGetTables(t *testing.T) {
	mockKeyspace := &cc.MockKeyspaceMetadata{}
	tables := map[string]*gocql.TableMetadata{
		"table_c": {Name: "table_c"},
		"table_a": {Name: "table_a"},
		"table_b": {Name: "table_b"},
	}
	mockKeyspace.On("Tables").Return(tables)

	isi := InfoSchemaImpl{
		KeyspaceMetadata: mockKeyspace,
		SourceProfile: profiles.SourceProfile{
			Conn: profiles.SourceProfileConnection{
				Cassandra: profiles.SourceProfileConnectionCassandra{Keyspace: "testkeyspace"},
			},
		},
	}
	actual, err := isi.GetTables()

	assert.NoError(t, err)
	expected := []common.SchemaAndName{
		{Schema: "testkeyspace", Name: "table_a"},
		{Schema: "testkeyspace", Name: "table_b"},
		{Schema: "testkeyspace", Name: "table_c"},
	}

	sort.Slice(actual, func(i, j int) bool { return actual[i].Name < actual[j].Name })
	sort.Slice(expected, func(i, j int) bool { return expected[i].Name < expected[j].Name })
	// Test Case 1: Returns existing tables
	assert.Equal(t, expected, actual)
	// Test Case 2: No tables in Keyspace
	mockKeyspace = &cc.MockKeyspaceMetadata{}
	mockKeyspace.On("Tables").Return(map[string]*gocql.TableMetadata{})
	isi = InfoSchemaImpl{
		KeyspaceMetadata: mockKeyspace,
	}
	_, err = isi.GetTables()
	assert.Error(t, err)
	assert.EqualError(t, err, "no tables found in keyspace ''")
	// Test Case 3: nil Keyspace
	isi = InfoSchemaImpl{
		KeyspaceMetadata: nil,
	}
	_, err = isi.GetTables()
	assert.Error(t, err)
	assert.EqualError(t, err, "keyspace metadata not initialized")
	mockKeyspace.AssertExpectations(t)
}


func TestGetTypeString(t *testing.T) {
	tests := []struct {
		name     string
		typeInfo gocql.TypeInfo 
		expected string
	}{
		{
			name:     "Basic Int Type",
			typeInfo: gocql.NewNativeType(0, gocql.TypeInt, ""),
			expected: "int",
		},
		{
			name:     "Set of Int",
			typeInfo: gocql.CollectionType{ 
				NativeType: gocql.NewNativeType(0, gocql.TypeSet, ""),
				Elem:       gocql.NewNativeType(0, gocql.TypeInt, ""),
			},
			expected: "set<int>",
		},
		{
			name:     "List of Text",
			typeInfo: gocql.CollectionType{
				NativeType: gocql.NewNativeType(0, gocql.TypeList, ""),
				Elem:       gocql.NewNativeType(0, gocql.TypeText, ""),
			},
			expected: "list<text>",
		},
		{
			name:     "Map of UUID to BigInt",
			typeInfo: gocql.CollectionType{
				NativeType: gocql.NewNativeType(0, gocql.TypeMap, ""),
				Key:        gocql.NewNativeType(0, gocql.TypeUUID, ""),
				Elem:       gocql.NewNativeType(0, gocql.TypeBigInt, ""),
			},
			expected: "map<uuid,bigint>",
		},
		{
			name:     "List of Maps of Int to Text",
			typeInfo: gocql.CollectionType{
				NativeType: gocql.NewNativeType(0, gocql.TypeList, ""),
				Elem: gocql.CollectionType{
					NativeType: gocql.NewNativeType(0, gocql.TypeMap, ""),
					Key:        gocql.NewNativeType(0, gocql.TypeInt, ""),
					Elem:       gocql.NewNativeType(0, gocql.TypeText, ""),
				},
			},
			expected: "list<map<int,text>>",
		},
		{
			name:     "TypeSet fallback",
			typeInfo: gocql.NewNativeType(0, gocql.TypeSet, ""), 
			expected: "",
		},
		{
			name:     "TypeList fallback",
			typeInfo: gocql.NewNativeType(0, gocql.TypeList, ""), 
			expected: "",
		},
		{
			name:     "TypeMap fallback",
			typeInfo: gocql.NewNativeType(0, gocql.TypeMap, ""), 
			expected: "",
		},
		{
			name:     "TypeSet err fallback",
			typeInfo: gocql.CollectionType{
				NativeType: gocql.NewNativeType(0, gocql.TypeSet, ""),
				Elem: gocql.NewNativeType(0, gocql.TypeSet, ""),
			},
			expected: "",
		},
		{
			name:     "TypeList err fallback",
			typeInfo: gocql.CollectionType{
				NativeType: gocql.NewNativeType(0, gocql.TypeList, ""),
				Elem: gocql.NewNativeType(0, gocql.TypeList, ""),
			},
			expected: "",
		},
		{
			name:     "TypeMap key err fallback",
			typeInfo: gocql.CollectionType{
				NativeType: gocql.NewNativeType(0, gocql.TypeMap, ""),
				Key: gocql.NewNativeType(0, gocql.TypeMap, ""),
				Elem: gocql.NewNativeType(0, gocql.TypeInt, ""),
			},
			expected: "",
		},
		{
			name:     "TypeMap value err fallback",
			typeInfo: gocql.CollectionType{
				NativeType: gocql.NewNativeType(0, gocql.TypeMap, ""),
				Key: gocql.NewNativeType(0, gocql.TypeInt, ""),
				Elem: gocql.NewNativeType(0, gocql.TypeMap, ""),
			},
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, _ := getTypeString(tt.typeInfo)
			assert.Equal(t, tt.expected, actual)
		})
	}
}

func TestGetColumns(t *testing.T) {
	mockKeyspace := &cc.MockKeyspaceMetadata{}
	pk1Col := &gocql.ColumnMetadata{Name: "pk1", Type: gocql.NewNativeType(0, gocql.TypeInt, "")}
	ck1Col := &gocql.ColumnMetadata{Name: "ck1", Type: gocql.NewNativeType(0, gocql.TypeVarchar, "")}
	col1Col := &gocql.ColumnMetadata{Name: "col1", Type: gocql.NewNativeType(0, gocql.TypeBoolean, "")}
	tables := map[string]*gocql.TableMetadata{
		"test_table": {
			Name:              "test_table",
			PartitionKey:      []*gocql.ColumnMetadata{pk1Col},
			ClusteringColumns: []*gocql.ColumnMetadata{ck1Col},
			Columns:           map[string]*gocql.ColumnMetadata{"pk1": pk1Col, "ck1": ck1Col, "col1": col1Col},
		},
	}
	mockKeyspace.On("Tables").Return(tables)

	isi := InfoSchemaImpl{KeyspaceMetadata: mockKeyspace}
	colDefs, colIds, err := isi.GetColumns(nil, common.SchemaAndName{Name: "test_table"}, nil, nil)

	// Test Case 1: Expect correct columns to be returned
	assert.NoError(t, err)
	assert.Len(t, colIds, 3, "Expected three column IDs to be returned")
	assert.Len(t, colDefs, 3, "Expected three column definitions to be returned")

	// Test Case 2: verify properties of a primary key and a regular column
	var pkFound, colFound bool
	for _, id := range colIds {
		col := colDefs[id]
		if col.Name == "pk1" {
			assert.True(t, col.NotNull, "Primary key columns should be NotNull")
			assert.Equal(t, "int", col.Type.Name)
			pkFound = true
		}
		if col.Name == "col1" {
			assert.False(t, col.NotNull, "Non-primary key columns should be nullable")
			assert.Equal(t, "boolean", col.Type.Name)
			colFound = true
		}
	}
	// Test Case 3: check if columns exist
	assert.True(t, pkFound, "Primary key 'pk1' was not found in the results")
	assert.True(t, colFound, "Regular column 'col1' was not found in the results")
	
	// Test Case 4: Table doesn't exist in keyspace
	colDefs, colIds, err = isi.GetColumns(nil, common.SchemaAndName{Name: "test_table1"}, nil, nil)
	assert.Error(t, err)
	assert.EqualError(t, err, "table 'test_table1' not found in keyspace metadata")
	assert.Nil(t, colDefs)
	assert.Nil(t, colIds)

	// Test Case 5: nil Keyspace
	isi = InfoSchemaImpl{
		KeyspaceMetadata: nil,
	}
	_, _, err = isi.GetColumns(nil, common.SchemaAndName{Name: "test_table"}, nil, nil)
	assert.Error(t, err)
	assert.EqualError(t, err, "keyspace metadata not initialized")
	
	// Test Case 6: Error from getTypeString
	mockKeyspace = &cc.MockKeyspaceMetadata{} 
	errCol := &gocql.ColumnMetadata{
		Name: "col1",
		Type: gocql.NewNativeType(0, gocql.TypeSet, ""), 
	}
	tables = map[string]*gocql.TableMetadata{
		"err_table": {
			Name:              "err_table",
			PartitionKey:      []*gocql.ColumnMetadata{},
			ClusteringColumns: []*gocql.ColumnMetadata{},
			Columns:           map[string]*gocql.ColumnMetadata{"col1": errCol},
		},
	}
	mockKeyspace.On("Tables").Return(tables).Once()

	isi = InfoSchemaImpl{KeyspaceMetadata: mockKeyspace}
	colDefs, colIds, err = isi.GetColumns(nil, common.SchemaAndName{Name: "err_table"}, nil, nil)
	assert.Error(t, err)
	assert.EqualError(t, err, "invalid set type")
	assert.Nil(t, colDefs)
	assert.Nil(t, colIds)

	mockKeyspace.AssertExpectations(t)
}

func TestGetConstraints(t *testing.T) {
	mockKeyspace := &cc.MockKeyspaceMetadata{}
	tables := map[string]*gocql.TableMetadata{
		"test_table": {
			Name:              "test_table",
			PartitionKey:      []*gocql.ColumnMetadata{{Name: "pk1"}, {Name: "pk2"}},
			ClusteringColumns: []*gocql.ColumnMetadata{{Name: "ck1"}},
		},
	}
	mockKeyspace.On("Tables").Return(tables)

	isi := InfoSchemaImpl{KeyspaceMetadata: mockKeyspace}
	pks, cks, fks, err := isi.GetConstraints(nil, common.SchemaAndName{Name: "test_table"})

	assert.NoError(t, err)
	// Test Case 1: verify pk, fk and ck
	assert.Equal(t, []string{"pk1", "pk2", "ck1"}, pks, "Primary keys did not match expected")
	assert.Empty(t, cks, "Check constraints should be empty for Cassandra")
	assert.Empty(t, fks, "Foreign key constraints should be empty for Cassandra")
	// Test Case 2: Table doesn't exist in keyspace
	pks, cks, fks, err = isi.GetConstraints(nil, common.SchemaAndName{Name: "test_table1"})
	assert.Error(t, err)
	assert.EqualError(t, err, "table 'test_table1' not found in keyspace metadata")
	assert.Nil(t, pks)
	assert.Nil(t, cks)
	assert.Nil(t, fks)
	// Test Case 3: nil Keyspace
	isi = InfoSchemaImpl{
		KeyspaceMetadata: nil,
	}
	_, _, _, err = isi.GetConstraints(nil, common.SchemaAndName{Name: "test_table"})
	assert.Error(t, err)
	assert.EqualError(t, err, "keyspace metadata not initialized")
	mockKeyspace.AssertExpectations(t)
}

func TestGetForeignKeys(t *testing.T) {
	isi := InfoSchemaImpl{}
	fks, err := isi.GetForeignKeys(nil, common.SchemaAndName{})
	// Test Case: should return null
	assert.NoError(t, err)
	assert.Nil(t, fks, "GetForeignKeys should always return nil for Cassandra")
}

func TestGetIndexes(t *testing.T) {
	mockKeyspace := &cc.MockKeyspaceMetadata{}
	tables := map[string]*gocql.TableMetadata{
		"test_table": {
			Name: "test_table",
			Columns: map[string]*gocql.ColumnMetadata{
				"col1": {Name: "col1"},
				"col2": {Name: "col2", Index: gocql.ColumnIndexMetadata{Name: "col2_idx"}},
			},
		},
	}
	mockKeyspace.On("Tables").Return(tables)

	isi := InfoSchemaImpl{KeyspaceMetadata: mockKeyspace}
	colNameIdMap := map[string]string{"col1": "id1", "col2": "id2"}
	conv := internal.MakeConv()
	indexes, err := isi.GetIndexes(conv, common.SchemaAndName{Name: "test_table"}, colNameIdMap)

	assert.NoError(t, err)
	// Test Case 1: Expect exactly one index to be found
	assert.Len(t, indexes, 1, "Expected exactly one index to be found")
	idx := indexes[0]
	// Test Case 2: check name and conditions
	assert.Equal(t, "col2_idx", idx.Name)
	assert.False(t, idx.Unique, "Cassandra secondary indexes should be non-unique")
	assert.Len(t, idx.Keys, 1)
	assert.Equal(t, "id2", idx.Keys[0].ColId)
	// Test Case 3: Table doesn't exist in keyspace
	indexes, err = isi.GetIndexes(conv, common.SchemaAndName{Name: "test_table1"}, colNameIdMap)
	assert.Error(t, err)
	assert.EqualError(t, err, "table 'test_table1' not found in keyspace metadata")
	assert.Nil(t, indexes)
	// Test Case 4: nil Keyspace
	isi = InfoSchemaImpl{
		KeyspaceMetadata: nil,
	}
	_, err = isi.GetIndexes(conv, common.SchemaAndName{Name: "test_table"}, colNameIdMap)
	assert.Error(t, err)
	assert.EqualError(t, err, "keyspace metadata not initialized")
	mockKeyspace.AssertExpectations(t)
}

func TestDataMigrationStubs(t *testing.T) {
	isi := InfoSchemaImpl{}
	ctx := context.Background()
	conv := internal.MakeConv()

	_, err := isi.GetRowsFromTable(conv, "table1")
	assert.ErrorIs(t, err, errNotSupported)

	_, err = isi.GetRowCount(common.SchemaAndName{Name: "table1"})
	assert.ErrorIs(t, err, errNotSupported)

	err = isi.ProcessData(conv, "table1", schema.Table{}, nil, ddl.CreateTable{}, internal.AdditionalDataAttributes{})
	assert.ErrorIs(t, err, errNotSupported)

	_, err = isi.StartChangeDataCapture(ctx, conv)
	assert.ErrorIs(t, err, errNotSupported)

	_, err = isi.StartStreamingMigration(ctx, "", nil, conv, nil)
	assert.ErrorIs(t, err, errNotSupported)
}

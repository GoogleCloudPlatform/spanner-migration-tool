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
	"reflect"
	"testing"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/profiles"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/schema"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type MockIter struct {
	mock.Mock
	data      [][]interface{}
	scanPos   int
	errToReturn error
}

func (m *MockIter) Scan(dest ...interface{}) bool {
	if m.scanPos >= len(m.data) {
		return false
	}
	for i, d := range dest {
		if i < len(m.data[m.scanPos]) {
			reflect.ValueOf(d).Elem().Set(reflect.ValueOf(m.data[m.scanPos][i]))
		}
	}
	m.scanPos++
	return true
}

func (m *MockIter) Close() error {
	args := m.Called()
	return args.Error(0)
}

type MockQuery struct { mock.Mock }
func (m *MockQuery) Iter() IterInterface { args := m.Called(); return args.Get(0).(IterInterface) }
type MockSession struct { mock.Mock }
func (m *MockSession) Query(stmt string, values ...interface{}) QueryInterface { args := m.Called(stmt, mock.Anything); return args.Get(0).(QueryInterface) }
func (m *MockSession) Close() { m.Called() }


func TestProcessSchemaCassandra(t *testing.T) {
	logger.InitializeLogger("INFO")
	mockSession := new(MockSession)

	mockQueryTables := new(MockQuery)
	mockIterTables := new(MockIter)
	mockIterTables.data = [][]interface{}{
		{"users"},
		{"transactions"},
	}
	mockQueryTables.On("Iter").Return(mockIterTables)
	mockIterTables.On("Close").Return(nil)
	mockSession.On("Query", "SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?", mock.Anything).Return(mockQueryTables)

	mockQueryUsersConstraints := new(MockQuery)
	mockIterUsersConstraints := new(MockIter)
	mockIterUsersConstraints.data = [][]interface{}{
		{"user_id", "partition_key", -1},
		{"name", "regular", -1},
		{"settings", "regular", -1},
	}
	mockQueryUsersConstraints.On("Iter").Return(mockIterUsersConstraints)
	mockIterUsersConstraints.On("Close").Return(nil)

	mockQueryUsersColumns := new(MockQuery)
	mockIterUsersColumns := new(MockIter)
	mockIterUsersColumns.data = [][]interface{}{
		{"user_id", "uuid", "partition_key"},
		{"name", "text", "regular"},
		{"settings", "map<text, text>", "regular"},
	}
	mockQueryUsersColumns.On("Iter").Return(mockIterUsersColumns)
	mockIterUsersColumns.On("Close").Return(nil)

	mockQueryUsersIndexes := new(MockQuery)
	mockIterUsersIndexes := new(MockIter)
	mockIterUsersIndexes.data = [][]interface{}{
		{"users_name_idx", map[string]string{"target": "\"name\""}},
	}
	mockQueryUsersIndexes.On("Iter").Return(mockIterUsersIndexes)
	mockIterUsersIndexes.On("Close").Return(nil)

	mockQueryTxConstraints := new(MockQuery)
	mockIterTxConstraints := new(MockIter)
	mockIterTxConstraints.data = [][]interface{}{
		{"account_id", "partition_key", -1},
		{"tx_time", "clustering", 0},
		{"tx_id", "clustering", 1},
		{"amount", "regular", -1},
	}
	mockQueryTxConstraints.On("Iter").Return(mockIterTxConstraints)
	mockIterTxConstraints.On("Close").Return(nil)

	mockQueryTxColumns := new(MockQuery)
	mockIterTxColumns := new(MockIter)
	mockIterTxColumns.data = [][]interface{}{
		{"account_id", "text", "partition_key"},
		{"tx_time", "timestamp", "clustering"},
		{"tx_id", "timeuuid", "clustering"},
		{"amount", "decimal", "regular"},
	}
	mockQueryTxColumns.On("Iter").Return(mockIterTxColumns)
	mockIterTxColumns.On("Close").Return(nil)

	mockQueryTxIndexes := new(MockQuery)
	mockIterTxIndexes := new(MockIter) 
	mockQueryTxIndexes.On("Iter").Return(mockIterTxIndexes)
	mockIterTxIndexes.On("Close").Return(nil)

	mockSession.On("Query", "SELECT column_name, kind, position FROM system_schema.columns WHERE keyspace_name = ? AND table_name = ?", mock.Anything).Return(mockQueryUsersConstraints).Once()
	mockSession.On("Query", "SELECT column_name, type, kind FROM system_schema.columns WHERE keyspace_name = ? AND table_name = ?", mock.Anything).Return(mockQueryUsersColumns).Once()
	mockSession.On("Query", "SELECT index_name, options FROM system_schema.indexes WHERE keyspace_name = ? AND table_name = ?", mock.Anything).Return(mockQueryUsersIndexes).Once()

	mockSession.On("Query", "SELECT column_name, kind, position FROM system_schema.columns WHERE keyspace_name = ? AND table_name = ?", mock.Anything).Return(mockQueryTxConstraints).Once()
	mockSession.On("Query", "SELECT column_name, type, kind FROM system_schema.columns WHERE keyspace_name = ? AND table_name = ?", mock.Anything).Return(mockQueryTxColumns).Once()
	mockSession.On("Query", "SELECT index_name, options FROM system_schema.indexes WHERE keyspace_name = ? AND table_name = ?", mock.Anything).Return(mockQueryTxIndexes).Once()


	conv := internal.MakeConv()
	isi := InfoSchemaImpl{
		session: mockSession,
		SourceProfile: profiles.SourceProfile{
			Conn: profiles.SourceProfileConnection{
				Cassandra: profiles.SourceProfileConnectionCassandra{Keyspace: "testks"},
			},
		},
	}
	commonInfoSchema := common.InfoSchemaImpl{}
	_, err := commonInfoSchema.GenerateSrcSchema(conv, isi, 1)
	assert.Nil(t, err)

	actualSchema := make(map[string]schema.Table)
	for _, table := range conv.SrcSchema {
		actualSchema[table.Name] = table
	}

	usersTable, ok := actualSchema["users"]
	assert.True(t, ok, "users table should be present in the parsed schema")
	assert.Equal(t, 1, len(usersTable.PrimaryKeys), "users table should have 1 primary key column")
	assert.Equal(t, usersTable.ColNameIdMap["user_id"], usersTable.PrimaryKeys[0].ColId)
	assert.Equal(t, 3, len(usersTable.ColDefs), "users table should have 3 columns")
	userIdCol, ok := usersTable.ColDefs[usersTable.ColNameIdMap["user_id"]]
	assert.True(t, ok)
	assert.Equal(t, "uuid", userIdCol.Type.Name)
	assert.True(t, userIdCol.NotNull, "'user_id' is a PK and should be NOT NULL")
	nameCol, ok := usersTable.ColDefs[usersTable.ColNameIdMap["name"]]
	assert.True(t, ok)
	assert.Equal(t, "text", nameCol.Type.Name)
	assert.False(t, nameCol.NotNull, "'name' is not a PK and should be NULLABLE")
	settingsCol, ok := usersTable.ColDefs[usersTable.ColNameIdMap["settings"]]
	assert.True(t, ok)
	assert.Equal(t, "map<text, text>", settingsCol.Type.Name)
	assert.False(t, settingsCol.NotNull, "'settings' is not a PK and should be NULLABLE")
	assert.Equal(t, 1, len(usersTable.Indexes), "users table should have 1 index")
	assert.Equal(t, "users_name_idx", usersTable.Indexes[0].Name)
	assert.Equal(t, 1, len(usersTable.Indexes[0].Keys), "users_name_idx should have 1 key column")
	assert.Equal(t, usersTable.ColNameIdMap["name"], usersTable.Indexes[0].Keys[0].ColId)
	assert.False(t, usersTable.Indexes[0].Unique)

	txTable, ok := actualSchema["transactions"]
	assert.True(t, ok, "transactions table should be present in the parsed schema")
	assert.Equal(t, 3, len(txTable.PrimaryKeys), "transactions table should have 3 primary key columns")
	assert.Equal(t, txTable.ColNameIdMap["account_id"], txTable.PrimaryKeys[0].ColId, "First PK should be account_id")
	assert.Equal(t, txTable.ColNameIdMap["tx_time"], txTable.PrimaryKeys[1].ColId, "Second PK should be tx_time")
	assert.Equal(t, txTable.ColNameIdMap["tx_id"], txTable.PrimaryKeys[2].ColId, "Third PK should be tx_id")
	assert.Equal(t, 4, len(txTable.ColDefs), "transactions table should have 4 columns")
	accountIdCol, ok := txTable.ColDefs[txTable.ColNameIdMap["account_id"]]
	assert.True(t, ok)
	assert.Equal(t, "text", accountIdCol.Type.Name)
	assert.True(t, accountIdCol.NotNull, "'account_id' is a PK and should be NOT NULL")
	txTimeCol, ok := txTable.ColDefs[txTable.ColNameIdMap["tx_time"]]
	assert.True(t, ok)
	assert.Equal(t, "timestamp", txTimeCol.Type.Name)
	assert.True(t, txTimeCol.NotNull, "'tx_time' is a PK and should be NOT NULL")
	txIdCol, ok := txTable.ColDefs[txTable.ColNameIdMap["tx_id"]]
	assert.True(t, ok)
	assert.Equal(t, "timeuuid", txIdCol.Type.Name)
	assert.True(t, txIdCol.NotNull, "'tx_id' is a PK and should be NOT NULL")
	amountCol, ok := txTable.ColDefs[txTable.ColNameIdMap["amount"]]
	assert.True(t, ok)
	assert.Equal(t, "decimal", amountCol.Type.Name)
	assert.False(t, amountCol.NotNull, "'amount' is not a PK and should be NULLABLE")
	assert.Equal(t, 0, len(txTable.Indexes), "transactions table should have no indexes")

	assert.Equal(t, int64(0), conv.Unexpecteds())
}
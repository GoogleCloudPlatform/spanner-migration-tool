// Copyright 2024 Google LLC
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
package cassandraclient

import (
	"github.com/gocql/gocql"
	"github.com/stretchr/testify/mock"
)

type MockGocqlSession struct {
	mock.Mock
}

func (m *MockGocqlSession) KeyspaceMetadata(keyspace string) (*gocql.KeyspaceMetadata, error) {
	args := m.Called(keyspace)
	if md, ok := args.Get(0).(*gocql.KeyspaceMetadata); ok {
		return md, args.Error(1)
	}
	return nil, args.Error(1)
}

func (m *MockGocqlSession) Close() {
	m.Called()
}

type MockKeyspaceMetadata struct {
	mock.Mock
	MockTables map[string]*gocql.TableMetadata
}

func (m *MockKeyspaceMetadata) Tables() map[string]*gocql.TableMetadata {
	args := m.Called()
	if tables, ok := args.Get(0).(map[string]*gocql.TableMetadata); ok {
		return tables
	}
	return m.MockTables
}

type MockCassandraCluster struct {
	mock.Mock
}

func (m *MockCassandraCluster) KeyspaceMetadata(keyspace string) (KeyspaceMetadataInterface, error) {
	args := m.Called(keyspace)
	if md, ok := args.Get(0).(KeyspaceMetadataInterface); ok {
		return md, args.Error(1)
	}
	return nil, args.Error(1)
}

func (m *MockCassandraCluster) Close() {
	m.Called()
}

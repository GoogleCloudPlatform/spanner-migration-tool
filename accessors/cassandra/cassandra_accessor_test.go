// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may-obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package cassandraaccessor

import (
	"errors"
	"fmt"
	"testing"

	cc "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/clients/cassandra"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/profiles"
	"github.com/stretchr/testify/assert"
)

func TestNewCassandraAccessor(t *testing.T) {
	originalGetOrCreateClient := GetOrCreateClient
	defer func() { GetOrCreateClient = originalGetOrCreateClient }()

	sourceProfile := profiles.SourceProfile{
		Conn: profiles.SourceProfileConnection{
			Cassandra: profiles.SourceProfileConnectionCassandra{
				Host:       "127.0.0.1",
				Port:       "9042",
				Keyspace:   "test_keyspace",
				DataCenter: "dc1",
				User:       "user",
				Pwd:        "pass",
			},
		},
	}

	t.Run("Success", func(t *testing.T) {
		mockMetadata := new(cc.MockKeyspaceMetadata)
		mockClient := new(cc.MockCassandraCluster)

		mockClient.On("KeyspaceMetadata", "test_keyspace").Return(mockMetadata, nil).Once()

		GetOrCreateClient = func(contactPoints []string, port int, keyspace, datacenter, user, password string) (cc.CassandraClusterInterface, error) {
			assert.Equal(t, []string{"127.0.0.1"}, contactPoints)
			assert.Equal(t, 9042, port)
			assert.Equal(t, "test_keyspace", keyspace)
			assert.Equal(t, "dc1", datacenter)
			assert.Equal(t, "user", user)
			assert.Equal(t, "pass", password)
			return mockClient, nil
		}

		accessor, keyspaceMD, err := NewCassandraAccessor(sourceProfile)

		assert.NoError(t, err)
		assert.NotNil(t, accessor)
		assert.Equal(t, mockMetadata, keyspaceMD)
		assert.Equal(t, mockClient, accessor.client)
		assert.Equal(t, mockMetadata, accessor.keyspaceMetadata)
		mockClient.AssertExpectations(t)
	})

	t.Run("Failure to create client", func(t *testing.T) {
		expectedErr := errors.New("client creation failed")
		GetOrCreateClient = func(contactPoints []string, port int, keyspace, datacenter, user, password string) (cc.CassandraClusterInterface, error) {
			return nil, expectedErr
		}

		accessor, keyspaceMD, err := NewCassandraAccessor(sourceProfile)

		assert.Nil(t, accessor)
		assert.Nil(t, keyspaceMD)
		assert.Error(t, err)
		assert.True(t, errors.Is(err, expectedErr), "The original error should be wrapped")
		assert.EqualError(t, err, fmt.Sprintf("failed to create Cassandra client: %s", expectedErr))
	})

	t.Run("Failure to get keyspace metadata", func(t *testing.T) {
		expectedErr := errors.New("metadata retrieval failed")
		mockClient := new(cc.MockCassandraCluster)

		mockClient.On("KeyspaceMetadata", "test_keyspace").Return(nil, expectedErr).Once()
		mockClient.On("Close").Return().Once()

		GetOrCreateClient = func(contactPoints []string, port int, keyspace, datacenter, user, password string) (cc.CassandraClusterInterface, error) {
			return mockClient, nil
		}

		accessor, keyspaceMD, err := NewCassandraAccessor(sourceProfile)

		assert.Nil(t, accessor)
		assert.Nil(t, keyspaceMD)
		assert.Error(t, err)
		assert.True(t, errors.Is(err, expectedErr), "The original error should be wrapped")
		assert.EqualError(t, err, fmt.Sprintf("failed to retrieve keyspace metadata for 'test_keyspace': %s", expectedErr))
		mockClient.AssertExpectations(t)
	})
}

func TestCassandraAccessor_Close(t *testing.T) {
	t.Run("Closes a non-nil client", func(t *testing.T) {
		mockClient := new(cc.MockCassandraCluster)
		mockClient.On("Close").Return().Once()
		accessor := &CassandraAccessor{client: mockClient}

		accessor.Close()

		mockClient.AssertExpectations(t)
	})

	t.Run("Does not panic with a nil client", func(t *testing.T) {
		accessor := &CassandraAccessor{client: nil}
		
		assert.NotPanics(t, func() {
			accessor.Close()
		})
	})
}
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
	"errors"
	"fmt"
	"testing"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
)

func TestCassandraClusterImpl(t *testing.T) {
	t.Run("KeyspaceMetadata Success", func(t *testing.T) {
		mockSession := new(MockGocqlSession)
		expectedMetadata := &gocql.KeyspaceMetadata{Name: "testks"}
		mockSession.On("KeyspaceMetadata", "testks").Return(expectedMetadata, nil).Once()

		clusterImpl := &CassandraClusterImpl{session: mockSession}
		keyspaceMeta, err := clusterImpl.KeyspaceMetadata("testks")

		assert.NoError(t, err)
		assert.NotNil(t, keyspaceMeta)
		impl, ok := keyspaceMeta.(*CassandraKeyspaceMetadataImpl)
		assert.True(t, ok)
		assert.Equal(t, expectedMetadata, impl.keyspaceMetadata)
		mockSession.AssertExpectations(t)
	})

	t.Run("KeyspaceMetadata Error DB", func(t *testing.T) {
		mockSession := new(MockGocqlSession)
		expectedErr := errors.New("database connection failed")
		mockSession.On("KeyspaceMetadata", "testks").Return(nil, expectedErr).Once()

		clusterImpl := &CassandraClusterImpl{session: mockSession}
		keyspaceMeta, err := clusterImpl.KeyspaceMetadata("testks")

		assert.Nil(t, keyspaceMeta)
		assert.Error(t, err)
		assert.True(t, errors.Is(err, expectedErr))
		assert.EqualError(t, err, fmt.Sprintf("failed to get keyspace metadata for testks: %s", expectedErr))
		mockSession.AssertExpectations(t)
	})

	t.Run("KeyspaceMetadata Error No Keyspace", func(t *testing.T) {
		mockSession := new(MockGocqlSession)
		mockSession.On("KeyspaceMetadata", "testks").Return(nil, nil).Once()

		clusterImpl := &CassandraClusterImpl{session: mockSession}
		meta, err := clusterImpl.KeyspaceMetadata("testks")

		assert.Nil(t, meta)
		assert.Error(t, err)
		assert.EqualError(t, err, "keyspace testks not found in cluster metadata")

		mockSession.AssertExpectations(t)
	})

	t.Run("Close", func(t *testing.T) {
		mockSession := new(MockGocqlSession)
		mockSession.On("Close").Return().Once()
		clusterImpl := &CassandraClusterImpl{session: mockSession}
		clusterImpl.Close()
		mockSession.AssertExpectations(t)
	})
}

func TestCassandraKeyspaceMetadataImpl(t *testing.T) {
	t.Run("Tables", func(t *testing.T) {
		expectedTables := map[string]*gocql.TableMetadata{
			"table1": {Name: "table1"},
		}
		keyspaceMeta := &gocql.KeyspaceMetadata{Tables: expectedTables}
		metaImpl := &CassandraKeyspaceMetadataImpl{keyspaceMetadata: keyspaceMeta}
		actualTables := metaImpl.Tables()
		assert.Equal(t, expectedTables, actualTables)
	})
}

func TestGocqlSessionImpl(t *testing.T) {
	t.Run("NewGocqlSessionImpl", func(t *testing.T) {
		var s *gocql.Session 
		impl := NewGocqlSessionImpl(s)
		assert.NotNil(t, impl)
		assert.Equal(t, s, impl.session)
	})

	t.Run("Close", func(t *testing.T) {
		var mockSession *gocql.Session
		mockSession = &gocql.Session{}
		impl := &GocqlSessionImpl{session: mockSession}
		assert.NotPanics(t, func() {
			impl.Close()
		})
	})
}

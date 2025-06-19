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
	"sync"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
)

func resetGlobals() {
	once = sync.Once{}
	globalClusterConfig = nil
}

func TestGetOrCreateCassandraClusterClient(t *testing.T) {
	originalCreateSession := createSessionFromCluster
	originalNewCluster := newCluster
	defer func() {
		createSessionFromCluster = originalCreateSession
		newCluster = originalNewCluster
		resetGlobals()
	}()

	t.Run("Success on first call with basic config", func(t *testing.T) {
		resetGlobals()
		var capturedClusterConfig *gocql.ClusterConfig

		newCluster = func(contactPoints ...string) *gocql.ClusterConfig {
			cfg := gocql.NewCluster(contactPoints...)
			capturedClusterConfig = cfg
			return cfg
		}

		mockSession := &MockGocqlSession{}
		createSessionFromCluster = func(c *gocql.ClusterConfig) (GocqlSessionInterface, error) {
			return mockSession, nil
		}

		client, err := GetOrCreateCassandraClusterClient([]string{"127.0.0.1"}, 0, "test_keyspace", "", "", "")

		assert.NoError(t, err)
		assert.NotNil(t, client)
		assert.NotNil(t, globalClusterConfig)
		assert.Equal(t, "test_keyspace", capturedClusterConfig.Keyspace)
		assert.Equal(t, gocql.Quorum, capturedClusterConfig.Consistency)
		assert.Equal(t, 10*time.Second, capturedClusterConfig.Timeout)
		assert.IsType(t, &gocql.SimpleRetryPolicy{}, capturedClusterConfig.RetryPolicy)
		assert.Equal(t, 3, capturedClusterConfig.RetryPolicy.(*gocql.SimpleRetryPolicy).NumRetries)
		assert.Nil(t, capturedClusterConfig.Authenticator)
	})

	t.Run("Success on first call with full config", func(t *testing.T) {
		resetGlobals()
		var capturedClusterConfig *gocql.ClusterConfig

		newCluster = func(contactPoints ...string) *gocql.ClusterConfig {
			cfg := gocql.NewCluster(contactPoints...)
			capturedClusterConfig = cfg
			return cfg
		}
		mockSession := &MockGocqlSession{}
		createSessionFromCluster = func(c *gocql.ClusterConfig) (GocqlSessionInterface, error) {
			return mockSession, nil
		}

		client, err := GetOrCreateCassandraClusterClient([]string{"127.0.0.1"}, 9042, "ks", "dc1", "user", "pass")

		assert.NoError(t, err)
		assert.NotNil(t, client)
		assert.Equal(t, 9042, capturedClusterConfig.Port)
		assert.NotNil(t, capturedClusterConfig.PoolConfig.HostSelectionPolicy)
		assert.IsType(t, gocql.DCAwareRoundRobinPolicy(""), capturedClusterConfig.PoolConfig.HostSelectionPolicy)
		expectedAuth := gocql.PasswordAuthenticator{Username: "user", Password: "pass"}
		assert.Equal(t, expectedAuth, capturedClusterConfig.Authenticator)
	})

	t.Run("Failure on session creation", func(t *testing.T) {
		resetGlobals()
		expectedErr := errors.New("session creation failed")
		createSessionFromCluster = func(c *gocql.ClusterConfig) (GocqlSessionInterface, error) {
			return nil, expectedErr
		}

		client, err := GetOrCreateCassandraClusterClient([]string{"127.0.0.1"}, 9042, "ks", "", "", "")

		assert.Nil(t, client)
		assert.Error(t, err)
		assert.True(t, errors.Is(err, expectedErr))
		assert.EqualError(t, err, "failed to create Cassandra session: session creation failed")
	})

}

func TestGetOrCreateCassandraClusterClientSingleton(t *testing.T) {
	var newClusterCallCount int
	resetGlobals()

	originalNewCluster := newCluster
	newCluster = func(cp ...string) *gocql.ClusterConfig {
		newClusterCallCount++
		return gocql.NewCluster(cp...)
	}
	t.Cleanup(func() { newCluster = originalNewCluster })

	_, _ = GetOrCreateCassandraClusterClient([]string{"host1"}, 9042, "keyspace1", "dc1", "user1", "pass1")
	_, _ = GetOrCreateCassandraClusterClient([]string{"host2"}, 9043, "keyspace2", "dc2", "user2", "pass2")
	_, _ = GetOrCreateCassandraClusterClient([]string{"host3"}, 9044, "keyspace3", "dc3", "user3", "pass3")


	assert.Equal(t, 1, newClusterCallCount, "The cluster config should only be initialized once.")
	assert.NotNil(t, globalClusterConfig)
	assert.Equal(t, []string{"host1"}, globalClusterConfig.Hosts)
	assert.Equal(t, 9042, globalClusterConfig.Port)
	assert.Equal(t, "keyspace1", globalClusterConfig.Keyspace)
	assert.Equal(t, gocql.PasswordAuthenticator{Username: "user1", Password: "pass1"}, globalClusterConfig.Authenticator)	
}
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
	"fmt"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
)

func resetGlobals() {
	clusterConfigMux = sync.Mutex{}
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

	t.Run("Concurrent calls", func(t *testing.T) {
		resetGlobals()
		mockSession := &MockGocqlSession{}
		createSessionFromCluster = func(c *gocql.ClusterConfig) (GocqlSessionInterface, error) {
			return mockSession, nil
		}
		newCluster = func(contactPoints ...string) *gocql.ClusterConfig {
			return gocql.NewCluster(contactPoints...)
		}

		var wg sync.WaitGroup
		numGoroutines := 5
		wg.Add(numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			go func(i int) {
				defer wg.Done()
				_, err := GetOrCreateCassandraClusterClient([]string{"127.0.0.1"}, 9042, fmt.Sprintf("keyspace%d", i), "", "", "")
				assert.NoError(t, err)
			}(i)
		}

		wg.Wait()
		assert.NotNil(t, globalClusterConfig, "globalClusterConfig should be set after concurrent calls")
	})
}

func TestCreateSessionFromCluster(t *testing.T) {
	resetGlobals()

	originalNewCluster := newCluster
	newCluster = func(contactPoints ...string) *gocql.ClusterConfig {
		return gocql.NewCluster(contactPoints...)
	}
	t.Cleanup(func() { newCluster = originalNewCluster })

	_, _ = GetOrCreateCassandraClusterClient([]string{"127.0.0.1"}, 9042, "keyspace", "dc", "user", "pass")
	assert.NotNil(t, globalClusterConfig)
}
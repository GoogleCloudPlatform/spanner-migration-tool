// Copyright 2024 Google LLC
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
package cassandraaccessor

import (
	"fmt"
	"strconv"
	"strings"

	cc "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/clients/cassandra"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/profiles"
)

type CassandraAccessor struct {
	client   cc.CassandraClusterInterface
	keyspaceMetadata cc.KeyspaceMetadataInterface
}

var GetOrCreateClient = cc.GetOrCreateCassandraClusterClient

func NewCassandraAccessor(sourceProfile profiles.SourceProfile) (*CassandraAccessor, cc.KeyspaceMetadataInterface, error) {
	cfg := sourceProfile.Conn.Cassandra

	port, _ := strconv.Atoi(cfg.Port)

	client, err := GetOrCreateClient(
		strings.Split(cfg.Host, ","),
		port,
		cfg.Keyspace,
		cfg.DataCenter,
		cfg.User,
		cfg.Pwd,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create Cassandra client: %w", err)
	}

	keyspaceMD, err := client.KeyspaceMetadata(cfg.Keyspace)
	if err != nil {
		client.Close()
		return nil, nil, fmt.Errorf("failed to retrieve keyspace metadata for '%s': %w", cfg.Keyspace, err)
	}

	accessor := &CassandraAccessor{
		client:   client,
		keyspaceMetadata: keyspaceMD,
	}

	return accessor, keyspaceMD, nil
}

func (acc *CassandraAccessor) Close() {
	if acc.client != nil {
		acc.client.Close()
	}
}

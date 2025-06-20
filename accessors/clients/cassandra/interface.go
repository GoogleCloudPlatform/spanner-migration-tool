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
	"fmt"
	"github.com/gocql/gocql"
)

type GocqlSessionInterface interface {
	KeyspaceMetadata(keyspace string) (*gocql.KeyspaceMetadata, error)
	Close()
}

type KeyspaceMetadataInterface interface {
	Tables() map[string]*gocql.TableMetadata
}

type CassandraClusterInterface interface {
	KeyspaceMetadata(keyspace string) (KeyspaceMetadataInterface, error)
	Close() 
}

type GocqlSessionImpl struct {
	session *gocql.Session
}

func NewGocqlSessionImpl(session *gocql.Session) *GocqlSessionImpl {
	return &GocqlSessionImpl{session: session}
}

func (gs *GocqlSessionImpl) KeyspaceMetadata(keyspace string) (*gocql.KeyspaceMetadata, error) {
	ks, err := gs.session.KeyspaceMetadata(keyspace)
	if err != nil {
		return nil, fmt.Errorf("failed to get keyspace metadata for %s: %w", keyspace, err)
	}
	if ks == nil {
		return nil, fmt.Errorf("keyspace %s not found in cluster metadata", keyspace)
	}
	return ks, nil
}

func (gs *GocqlSessionImpl) Close() {
	if gs.session != nil {
		gs.session.Close()
	}
}

type CassandraKeyspaceMetadataImpl struct {
	keyspaceMetadata *gocql.KeyspaceMetadata
}

func (c *CassandraKeyspaceMetadataImpl) Tables() map[string]*gocql.TableMetadata {
	return c.keyspaceMetadata.Tables
}

type CassandraClusterImpl struct {
	session GocqlSessionInterface
}

func (c *CassandraClusterImpl) KeyspaceMetadata(keyspace string) (KeyspaceMetadataInterface, error) {
	ks, err := c.session.KeyspaceMetadata(keyspace)
	if err != nil {
		return nil, fmt.Errorf("failed to get keyspace metadata for %s: %w", keyspace, err)
	}
	if ks == nil {
		return nil, fmt.Errorf("keyspace %s not found in cluster metadata", keyspace)
	}
	return &CassandraKeyspaceMetadataImpl{keyspaceMetadata: ks}, nil
}

func (c *CassandraClusterImpl) Close() {
	c.session.Close()
}



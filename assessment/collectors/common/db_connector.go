/* Copyright 2025 Google LLC
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
// limitations under the License.*/

package common

import (
	"database/sql"
	"fmt"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/conversion"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/profiles"
)

// ConnectionConfigProvider interface for getting database connection configuration
type ConnectionConfigProvider interface {
	GetConnectionConfig(sourceProfile profiles.SourceProfile) (interface{}, error)
}

// DefaultConnectionConfigProvider provides default connection configuration
type DefaultConnectionConfigProvider struct{}

// GetConnectionConfig returns the connection configuration for the given source profile
func (d DefaultConnectionConfigProvider) GetConnectionConfig(sourceProfile profiles.SourceProfile) (interface{}, error) {
	return conversion.ConnectionConfig(sourceProfile)
}

// DBConnector interface for establishing database connections
type DBConnector interface {
	Connect(driver string, connectionConfig interface{}) (*sql.DB, error)
}

// SQLDBConnector provides default SQL database connection functionality
type SQLDBConnector struct{}

// Connect establishes a database connection using the provided configuration
func (d SQLDBConnector) Connect(driver string, connectionConfig interface{}) (*sql.DB, error) {
	connectionStr, ok := connectionConfig.(string)
	if !ok {
		return nil, fmt.Errorf("invalid connection configuration type")
	}

	db, err := sql.Open(driver, connectionStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %w", err)
	}

	// Test the connection
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return db, nil
}

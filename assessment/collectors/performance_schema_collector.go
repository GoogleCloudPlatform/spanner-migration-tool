// Copyright 2025 Google LLC
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

package assessment

import (
	"database/sql"
	_ "embed"
	"fmt"

	collectorCommon "github.com/GoogleCloudPlatform/spanner-migration-tool/assessment/collectors/common"
	sourcesCommon "github.com/GoogleCloudPlatform/spanner-migration-tool/assessment/sources"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/assessment/sources/mysql"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/assessment/utils"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/profiles"
	"go.uber.org/zap"
)

// PerformanceSchemaCollector collects performance schema data from source databases
type PerformanceSchemaCollector struct {
	queries []utils.QueryAssessmentInfo
}

// IsEmpty checks if the collector has any data
func (c PerformanceSchemaCollector) IsEmpty() bool {
	return len(c.queries) == 0
}

// GetDefaultPerformanceSchemaCollector creates a new PerformanceSchemaCollector with default settings
func GetDefaultPerformanceSchemaCollector(sourceProfile profiles.SourceProfile) (PerformanceSchemaCollector, error) {
	return GetPerformanceSchemaCollector(sourceProfile, collectorCommon.SQLDBConnector{}, collectorCommon.DefaultConnectionConfigProvider{}, DefaultPerformanceSchemaProvider{})
}

// GetPerformanceSchemaCollector creates a new PerformanceSchemaCollector with custom dependencies
func GetPerformanceSchemaCollector(sourceProfile profiles.SourceProfile, dbConnector collectorCommon.DBConnector, configProvider collectorCommon.ConnectionConfigProvider, performanceSchemaProvider PerformanceSchemaProvider) (PerformanceSchemaCollector, error) {
	logger.Log.Info("initializing performance schema collector")

	connectionConfig, err := configProvider.GetConnectionConfig(sourceProfile)
	if err != nil {
		return PerformanceSchemaCollector{}, fmt.Errorf("failed to get connection config: %w", err)
	}

	db, err := dbConnector.Connect(sourceProfile.Driver, connectionConfig)
	if err != nil {
		return PerformanceSchemaCollector{}, fmt.Errorf("failed to connect to database: %w", err)
	}

	performanceSchema, err := performanceSchemaProvider.getPerformanceSchema(db, sourceProfile)
	if err != nil {
		return PerformanceSchemaCollector{}, fmt.Errorf("failed to get performance schema: %w", err)
	}

	queries, err := performanceSchema.GetAllQueryAssessments()
	if err != nil {
		return PerformanceSchemaCollector{}, fmt.Errorf("failed to get all queries: %w", err)
	}

	logger.Log.Info("performance schema collector initialized successfully",
		zap.Int("query_count", len(queries)))

	return PerformanceSchemaCollector{
		queries: queries,
	}, nil
}

// DBConnector interface for establishing database connections
type PerformanceSchemaProvider interface {
	getPerformanceSchema(db *sql.DB, sourceProfile profiles.SourceProfile) (sourcesCommon.PerformanceSchema, error)
}

// SQLDBConnector provides default SQL database connection functionality
type DefaultPerformanceSchemaProvider struct{}

// getPerformanceSchema creates a performance schema implementation based on the database driver
func (d DefaultPerformanceSchemaProvider) getPerformanceSchema(db *sql.DB, sourceProfile profiles.SourceProfile) (sourcesCommon.PerformanceSchema, error) {
	driver := sourceProfile.Driver
	switch driver {
	case constants.MYSQL:
		return mysql.PerformanceSchemaImpl{
			Db:     db,
			DbName: sourceProfile.Conn.Mysql.Db,
		}, nil
	default:
		return nil, fmt.Errorf("driver %s not supported for performance schema", driver)
	}
}

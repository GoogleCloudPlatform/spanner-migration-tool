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

package conversion

import (
	"fmt"

	sp "cloud.google.com/go/spanner"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/profiles"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/common"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/writer"
)

type DataFromDatabaseInterface interface {
	dataFromDatabaseForDMSMigration() (*writer.BatchWriter, error)
	dataFromDatabaseForBulkMigration(migrationProjectId string, sourceProfile profiles.SourceProfile, targetProfile profiles.TargetProfile, config writer.BatchWriterConfig, conv *internal.Conv, client *sp.Client, gi GetInfoInterface, sm SnapshotMigrationInterface) (*writer.BatchWriter, error)
}

type DataFromDatabaseImpl struct{}

// TODO: Define the data processing logic for DMS migrations here.
func (dd *DataFromDatabaseImpl) dataFromDatabaseForDMSMigration() (*writer.BatchWriter, error) {
	return nil, fmt.Errorf("dms configType is not implemented yet, please use 'bulk'")
}



// 1. Migrate the data from the data shards, the schema shard needs to be specified here again.
// 2. Create a connection profile object for it
// 3. Perform a snapshot migration for the shard
// 4. Once all shard migrations are complete, return the batch writer object
func (dd *DataFromDatabaseImpl) dataFromDatabaseForBulkMigration(migrationProjectId string, sourceProfile profiles.SourceProfile, targetProfile profiles.TargetProfile, config writer.BatchWriterConfig, conv *internal.Conv, client *sp.Client, gi GetInfoInterface, sm SnapshotMigrationInterface) (*writer.BatchWriter, error) {
	var bw *writer.BatchWriter
	for _, dataShard := range sourceProfile.Config.ShardConfigurationBulk.DataShards {

		logger.Log.Info(fmt.Sprintf("Initiating migration for shard: %v\n", dataShard.DbName))
		infoSchema, err := gi.getInfoSchemaForShard(migrationProjectId, dataShard, sourceProfile.Driver, targetProfile, &profiles.SourceProfileDialectImpl{}, &GetInfoImpl{})
		if err != nil {
			return nil, err
		}
		additionalDataAttributes := internal.AdditionalDataAttributes{
			ShardId: dataShard.DataShardId,
		}
		bw = sm.performSnapshotMigration(config, conv, client, infoSchema, additionalDataAttributes, &common.InfoSchemaImpl{}, &PopulateDataConvImpl{})
	}

	return bw, nil
}



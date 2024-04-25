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
	"context"
	"sync"

	sp "cloud.google.com/go/spanner"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/utils"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/profiles"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/common"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/csv"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/writer"
	"github.com/stretchr/testify/mock"
)

type MockGetInfo struct {
	mock.Mock
}

func (mgi *MockGetInfo) getInfoSchemaForShard(migrationShardId string, shardConnInfo profiles.DirectConnectionConfig, driver string, targetProfile profiles.TargetProfile, s profiles.SourceProfileDialectInterface, g GetInfoInterface) (common.InfoSchema, error) {
	args := mgi.Called(migrationShardId, shardConnInfo, driver, targetProfile, s, g)
	return args.Get(0).(common.InfoSchema), args.Error(1)
}
func (mgi *MockGetInfo) GetInfoSchemaFromCloudSQL(migrationShardId string, sourceProfile profiles.SourceProfile, targetProfile profiles.TargetProfile) (common.InfoSchema, error) {
	args := mgi.Called(migrationShardId, sourceProfile, targetProfile)
	return args.Get(0).(common.InfoSchema), args.Error(1)
}
func (mgi *MockGetInfo) GetInfoSchema(migrationShardId string, sourceProfile profiles.SourceProfile, targetProfile profiles.TargetProfile) (common.InfoSchema, error) {
	args := mgi.Called(migrationShardId, sourceProfile, targetProfile)
	return args.Get(0).(common.InfoSchema), args.Error(1)
}

type MockSchemaFromSource struct {
	mock.Mock
}

func (msads *MockSchemaFromSource) schemaFromDatabase(migrationProjectId string, sourceProfile profiles.SourceProfile, targetProfile profiles.TargetProfile, getInfo GetInfoInterface, processSchema common.ProcessSchemaInterface) (*internal.Conv, error) {
	args := msads.Called(migrationProjectId, sourceProfile, targetProfile, getInfo, processSchema)
	return args.Get(0).(*internal.Conv), args.Error(1)
}
func (msads *MockSchemaFromSource) SchemaFromDump(driver string, spDialect string, ioHelper *utils.IOStreams, processDump ProcessDumpByDialectInterface) (*internal.Conv, error) {
	args := msads.Called(driver, spDialect, ioHelper, processDump)
	return args.Get(0).(*internal.Conv), args.Error(1)
}

type MockDataFromSource struct {
	mock.Mock
}

func (msads *MockDataFromSource) dataFromDatabase(ctx context.Context, migrationProjectId string, sourceProfile profiles.SourceProfile, targetProfile profiles.TargetProfile, config writer.BatchWriterConfig, conv *internal.Conv, client *sp.Client, getInfo GetInfoInterface, dataFromDb DataFromDatabaseInterface, snapshotMigration SnapshotMigrationInterface) (*writer.BatchWriter, error) {
	args := msads.Called(ctx, migrationProjectId, sourceProfile, targetProfile, config, conv, client, getInfo, dataFromDb, snapshotMigration)
	return args.Get(0).(*writer.BatchWriter), args.Error(1)
}
func (msads *MockDataFromSource) dataFromDump(driver string, config writer.BatchWriterConfig, ioHelper *utils.IOStreams, client *sp.Client, conv *internal.Conv, dataOnly bool, processDump ProcessDumpByDialectInterface, populateDataConv PopulateDataConvInterface) (*writer.BatchWriter, error) {
	args := msads.Called(driver, config, ioHelper, client, conv, dataOnly, processDump, populateDataConv)
	return args.Get(0).(*writer.BatchWriter), args.Error(1)
}
func (msads *MockDataFromSource) dataFromCSV(ctx context.Context, sourceProfile profiles.SourceProfile, targetProfile profiles.TargetProfile, config writer.BatchWriterConfig, conv *internal.Conv, client *sp.Client, pdc PopulateDataConvInterface, csv csv.CsvInterface) (*writer.BatchWriter, error) {
	args := msads.Called(ctx, sourceProfile, targetProfile, config, conv, client, pdc, csv)
	return args.Get(0).(*writer.BatchWriter), args.Error(1)
}

type MockValidateOrCreateResources struct {
	mock.Mock
}

func (mcr *MockValidateOrCreateResources) ValidateOrCreateResourcesForShardedMigration(ctx context.Context, projectId string, instanceName string, validateOnly bool, region string, sourceProfile profiles.SourceProfile) error {
	args := mcr.Called(ctx, projectId, instanceName, validateOnly, region, sourceProfile)
	return args.Error(0)
}

type MockResourceGeneration struct {
	mock.Mock
}

func (mrg *MockResourceGeneration) RollbackResourceCreation(ctx context.Context, profiles []*ConnectionProfileReq) error {
	args := mrg.Called(ctx, profiles)
	return args.Error(0)
}
func (mrg *MockResourceGeneration) GetConnectionProfilesForResources(ctx context.Context, projectId string, sourceProfile profiles.SourceProfile, region string, validateOnly bool) ([]*ConnectionProfileReq, []*ConnectionProfileReq, error) {
	args := mrg.Called(ctx, projectId, sourceProfile, region, validateOnly)
	return args.Get(0).([]*ConnectionProfileReq), args.Get(1).([]*ConnectionProfileReq), args.Error(2)
}
func (mrg *MockResourceGeneration) PrepareMinimalDowntimeResources(createResourceData *ConnectionProfileReq, mutex *sync.Mutex) common.TaskResult[*ConnectionProfileReq] {
	args := mrg.Called(createResourceData, mutex)
	return args.Get(0).(common.TaskResult[*ConnectionProfileReq])
}

type MockValidateResources struct {
	mock.Mock
}

func (mvr *MockValidateResources) ValidateResourceGeneration(ctx context.Context, projectId string, instanceId string, sourceProfile profiles.SourceProfile, conv *internal.Conv) error {
	args := mvr.Called(ctx, projectId, instanceId, sourceProfile, conv)
	return args.Error(0)
}
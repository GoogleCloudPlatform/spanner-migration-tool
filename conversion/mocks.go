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

// Package conversion handles initial setup for the command line tool
// and web APIs.

// TODO:(searce) Organize code in go style format to make this file more readable.
//
//	public constants first
//	key public type definitions next (although often it makes sense to put them next to public functions that use them)
//	then public functions (and relevant type definitions)
//	and helper functions and other non-public definitions last (generally in order of importance)
package conversion

import (
	"context"
	"os"

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

func (mgi *MockGetInfo) getInfoSchemaForShard(shardConnInfo profiles.DirectConnectionConfig, driver string, targetProfile profiles.TargetProfile, s profiles.SourceProfileDialectInterface, g GetInfoInterface) (common.InfoSchema, error) {
	args := mgi.Called(shardConnInfo, driver, targetProfile, s, g)
	return args.Get(0).(common.InfoSchema), args.Error(1)
}
func (mgi *MockGetInfo) GetInfoSchemaFromCloudSQL(sourceProfile profiles.SourceProfile, targetProfile profiles.TargetProfile) (common.InfoSchema, error) {
	args := mgi.Called(sourceProfile, targetProfile)
	return args.Get(0).(common.InfoSchema), args.Error(1)
}
func (mgi *MockGetInfo) GetInfoSchema(sourceProfile profiles.SourceProfile, targetProfile profiles.TargetProfile) (common.InfoSchema, error) {
	args := mgi.Called(sourceProfile, targetProfile)
	return args.Get(0).(common.InfoSchema), args.Error(1)
}

type MockSchemaFromSource struct {
    mock.Mock
}
func (msads *MockSchemaFromSource) schemaFromDatabase(sourceProfile profiles.SourceProfile, targetProfile profiles.TargetProfile, getInfo GetInfoInterface, processSchema common.ProcessSchemaInterface) (*internal.Conv, error) {
	args := msads.Called(sourceProfile, targetProfile, getInfo, processSchema)
	return args.Get(0).(*internal.Conv), args.Error(1)
}
func (msads *MockSchemaFromSource) SchemaFromDump(driver string, spDialect string, ioHelper *utils.IOStreams, processDump ProcessDumpByDialectInterface, seekable SeekableInterface) (*internal.Conv, error) {
	args := msads.Called(driver, spDialect, ioHelper, processDump)
	return args.Get(0).(*internal.Conv), args.Error(1)
}

type MockDataFromSource struct {
    mock.Mock
}
func (msads *MockDataFromSource) dataFromDatabase(ctx context.Context, sourceProfile profiles.SourceProfile, targetProfile profiles.TargetProfile, config writer.BatchWriterConfig, conv *internal.Conv, client *sp.Client, getInfo GetInfoInterface, dataFromDb DataFromDatabaseInterface, snapshotMigration SnapshotMigrationInterface) (*writer.BatchWriter, error) {
	args := msads.Called(ctx, sourceProfile, targetProfile, config, conv, client, getInfo, dataFromDb, snapshotMigration)
	return args.Get(0).(*writer.BatchWriter), args.Error(1)
}
func (msads *MockDataFromSource) dataFromDump(driver string, config writer.BatchWriterConfig, ioHelper *utils.IOStreams, client *sp.Client, conv *internal.Conv, dataOnly bool, processDump ProcessDumpByDialectInterface, populateDataConv PopulateDataConvInterface, seekable SeekableInterface) (*writer.BatchWriter, error) {
	args := msads.Called(driver, config, ioHelper, client, conv, dataOnly, processDump, populateDataConv)
	return args.Get(0).(*writer.BatchWriter), args.Error(1)
}
func (msads *MockDataFromSource) dataFromCSV(ctx context.Context, sourceProfile profiles.SourceProfile, targetProfile profiles.TargetProfile, config writer.BatchWriterConfig, conv *internal.Conv, client *sp.Client, pdc PopulateDataConvInterface, csv csv.CsvInterface) (*writer.BatchWriter, error) {
	args := msads.Called(ctx, sourceProfile, targetProfile, config, conv, client, pdc, csv)
	return args.Get(0).(*writer.BatchWriter), args.Error(1)
}

type MockProcessDumpByDialect struct {
	mock.Mock
}
func (mpdd *MockProcessDumpByDialect) ProcessDump(driver string, conv *internal.Conv, r *internal.Reader) error {
	args:= mpdd.Called(driver, conv, r)
	return args.Error(0)
}

type MockSeekable struct {
	mock.Mock
}
func (ms *MockSeekable) getSeekable(f *os.File) (*os.File, int64, error) {
	args:= ms.Called(f)
	return args.Get(0).(*os.File), args.Get(1).(int64), args.Error(2)
}
func (ms *MockSeekable) seek(ioHelper *utils.IOStreams, offset int64, whence int) (int64, error) {
	args:= ms.Called(ioHelper, offset, whence)
	return args.Get(0).(int64), args.Error(1)
}

type MockPopulateDataConv struct {
	mock.Mock
}
func (mpdc *MockPopulateDataConv) populateDataConv(conv *internal.Conv, config writer.BatchWriterConfig, client *sp.Client) *writer.BatchWriter {
	args:= mpdc.Called(conv, config, client)
	return args.Get(0).(*writer.BatchWriter)
}
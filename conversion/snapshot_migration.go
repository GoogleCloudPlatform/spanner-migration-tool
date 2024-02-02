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
	"fmt"

	sp "cloud.google.com/go/spanner"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/profiles"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/common"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/writer"
)

func performSnapshotMigration(config writer.BatchWriterConfig, conv *internal.Conv, client *sp.Client, infoSchema common.InfoSchema, additionalAttributes internal.AdditionalDataAttributes) *writer.BatchWriter {
	common.SetRowStats(conv, infoSchema)
	totalRows := conv.Rows()
	if !conv.Audit.DryRun {
		conv.Audit.Progress = *internal.NewProgress(totalRows, "Writing data to Spanner", internal.Verbose(), false, int(internal.DataWriteInProgress))
	}
	batchWriter := populateDataConv(conv, config, client)
	common.ProcessData(conv, infoSchema, additionalAttributes)
	batchWriter.Flush()
	return batchWriter
}

func snapshotMigrationHandler(sourceProfile profiles.SourceProfile, config writer.BatchWriterConfig, conv *internal.Conv, client *sp.Client, infoSchema common.InfoSchema) (*writer.BatchWriter, error) {
	switch sourceProfile.Driver {
	// Skip snapshot migration via Spanner migration tool for mysql and oracle since dataflow job will job will handle this from backfilled data.
	case constants.MYSQL, constants.ORACLE, constants.POSTGRES:
		return &writer.BatchWriter{}, nil
	case constants.DYNAMODB:
		return performSnapshotMigration(config, conv, client, infoSchema, internal.AdditionalDataAttributes{ShardId: ""}), nil
	default:
		return &writer.BatchWriter{}, fmt.Errorf("streaming migration not supported for driver %s", sourceProfile.Driver)
	}
}
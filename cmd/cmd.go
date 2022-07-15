// Copyright 2020 Google LLC
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

// Package cmd implements command line utility for HarbourBridge.
package cmd

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/cloudspannerecosystem/harbourbridge/common/constants"
	"github.com/cloudspannerecosystem/harbourbridge/common/utils"
	"github.com/cloudspannerecosystem/harbourbridge/conversion"
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/profiles"
	"github.com/cloudspannerecosystem/harbourbridge/proto/migration"
	"github.com/google/uuid"
)

var (
	badDataFile = "dropped.txt"
	reportFile  = "report.txt"
	schemaFile  = "schema.txt"
	sessionFile = "session.json"
)

const defaultWritersLimit = 40

// CommandLine provides the core processing for HarbourBridge when run as a command-line tool.
// It performs the following steps:
// 1. Run schema conversion (if dataOnly is set to false)
// 2. Create database (if schemaOnly is set to false)
// 3. Run data conversion (if schemaOnly is set to false)
// 4. Generate report
func CommandLine(ctx context.Context, driver, targetDb, dbURI string, dataOnly, schemaOnly, skipForeignKeys bool, schemaSampleSize int64, sessionJSON string, ioHelper *utils.IOStreams, outputFilePrefix string, now time.Time) error {
	// Cleanup hb tmp data directory in case residuals remain from prev runs.
	os.RemoveAll(os.TempDir() + constants.HB_TMP_DIR)
	// Legacy mode is only supported for MySQL, PostgreSQL and DynamoDB
	if driver != "" && utils.IsValidDriver(driver) && !utils.IsLegacyModeSupportedDriver(driver) {
		return fmt.Errorf("legacy mode is not supported for drivers other than %s", strings.Join(utils.GetLegacyModeSupportedDrivers(), ", "))
	}

	var conv *internal.Conv
	var err error
	// Creating profiles from legacy flags. We only pass schema-sample-size here because thats the
	// only flag passed through the arguments. Dumpfile params are contained within ioHelper
	// and direct connect params will be fetched from the env variables.
	sourceProfile, _ := profiles.NewSourceProfile(fmt.Sprintf("schema-sample-size=%d", schemaSampleSize), driver)
	sourceProfile.Driver = driver
	targetProfile, _ := profiles.NewTargetProfile("")
	targetProfile.TargetDb = targetDb
	if !dataOnly {
		// We pass an empty string to the sqlConnectionStr parameter as this is the legacy codepath,
		// which reads the environment variables and constructs the string later on.
		conv, err = conversion.SchemaConv(sourceProfile, targetProfile, ioHelper)
		if err != nil {
			return err
		}
		if ioHelper.SeekableIn != nil {
			defer ioHelper.In.Close()
		}

		conversion.WriteSchemaFile(conv, now, outputFilePrefix+schemaFile, ioHelper.Out)
		conversion.WriteSessionFile(conv, outputFilePrefix+sessionFile, ioHelper.Out)
		if schemaOnly {
			conversion.Report(driver, nil, ioHelper.BytesRead, "", conv, outputFilePrefix+reportFile, ioHelper.Out)
			return nil
		}
	} else {
		conv = internal.MakeConv()
		err = conversion.ReadSessionFile(conv, sessionJSON)
		if err != nil {
			return err
		}
	}

	// Populate migration request id and migration type in conv object
	conv.Audit.MigrationRequestId = "HB-" + uuid.New().String()
	if dataOnly {
		conv.Audit.MigrationType = migration.MigrationData_DATA_ONLY.Enum()
	} else {
		conv.Audit.MigrationType = migration.MigrationData_SCHEMA_AND_DATA.Enum()
	}

	adminClient, err := utils.NewDatabaseAdminClient(ctx)
	if err != nil {
		return fmt.Errorf("can't create admin client: %w", utils.AnalyzeError(err, dbURI))
	}
	defer adminClient.Close()
	err = conversion.CreateOrUpdateDatabase(ctx, adminClient, dbURI, driver, targetDb, conv, ioHelper.Out)
	if err != nil {
		return fmt.Errorf("can't create/update database: %v", err)
	}

	client, err := utils.GetClient(ctx, dbURI)
	if err != nil {
		return fmt.Errorf("can't create client for db %s: %v", dbURI, err)
	}

	// We pass an empty string to the sqlConnectionStr parameter as this is the legacy codepath,
	// which reads the environment variables and constructs the string later on.
	bw, err := conversion.DataConv(ctx, sourceProfile, targetProfile, ioHelper, client, conv, dataOnly, defaultWritersLimit)
	if err != nil {
		return fmt.Errorf("can't finish data conversion for db %s: %v", dbURI, err)
	}
	if !skipForeignKeys {
		if err = conversion.UpdateDDLForeignKeys(ctx, adminClient, dbURI, conv, ioHelper.Out); err != nil {
			return fmt.Errorf("can't perform update schema on db %s with foreign keys: %v", dbURI, err)
		}
	}
	banner := utils.GetBanner(now, dbURI)
	conversion.Report(driver, bw.DroppedRowsByTable(), ioHelper.BytesRead, banner, conv, outputFilePrefix+reportFile, ioHelper.Out)
	conversion.WriteBadData(bw, conv, banner, outputFilePrefix+badDataFile, ioHelper.Out)
	// Cleanup hb tmp data directory.
	os.RemoveAll(os.TempDir() + constants.HB_TMP_DIR)
	return nil
}

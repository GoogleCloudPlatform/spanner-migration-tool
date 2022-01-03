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
	"time"

	"github.com/cloudspannerecosystem/harbourbridge/conversion"
	"github.com/cloudspannerecosystem/harbourbridge/internal"
)

var (
	badDataFile = "dropped.txt"
	reportFile  = "report.txt"
	schemaFile  = "schema.txt"
	sessionFile = "session.json"
)

// CommandLine provides the core processing for HarbourBridge when run as a command-line tool.
// It performs the following steps:
// 1. Run schema conversion (if dataOnly is set to false)
// 2. Create database (if schemaOnly is set to false)
// 3. Run data conversion (if schemaOnly is set to false)
// 4. Generate report
func CommandLine(ctx context.Context, driver, targetDb, dbURI string, dataOnly, schemaOnly, skipForeignKeys bool, schemaSampleSize int64, sessionJSON string, ioHelper *conversion.IOStreams, outputFilePrefix string, now time.Time) error {
	var conv *internal.Conv
	var err error
	if !dataOnly {
		// We pass an empty string to the sqlConnectionStr parameter as this is the legacy codepath,
		// which reads the environment variables and constructs the string later on.
		// we are not supporting this codepath for new databases like sqlserver.
		conv, err = conversion.SchemaConv(driver, "", targetDb, ioHelper, schemaSampleSize)
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
	adminClient, err := conversion.NewDatabaseAdminClient(ctx)
	if err != nil {
		return fmt.Errorf("can't create admin client: %w", conversion.AnalyzeError(err, dbURI))
	}
	defer adminClient.Close()
	err = conversion.CreateOrUpdateDatabase(ctx, adminClient, dbURI, conv, ioHelper.Out)
	if err != nil {
		return fmt.Errorf("can't create/update database: %v", err)
	}

	client, err := conversion.GetClient(ctx, dbURI)
	if err != nil {
		return fmt.Errorf("can't create client for db %s: %v", dbURI, err)
	}

	// We pass an empty string to the sqlConnectionStr parameter as this is the legacy codepath,
	// which reads the environment variables and constructs the string later on.
	bw, err := conversion.DataConv(driver, "", ioHelper, client, conv, dataOnly, schemaSampleSize)
	if err != nil {
		return fmt.Errorf("can't finish data conversion for db %s: %v", dbURI, err)
	}
	if !skipForeignKeys {
		if err = conversion.UpdateDDLForeignKeys(ctx, adminClient, dbURI, conv, ioHelper.Out); err != nil {
			return fmt.Errorf("can't perform update schema on db %s with foreign keys: %v", dbURI, err)
		}
	}
	banner := conversion.GetBanner(now, dbURI)
	conversion.Report(driver, bw.DroppedRowsByTable(), ioHelper.BytesRead, banner, conv, outputFilePrefix+reportFile, ioHelper.Out)
	conversion.WriteBadData(bw, conv, banner, outputFilePrefix+badDataFile, ioHelper.Out)
	return nil
}

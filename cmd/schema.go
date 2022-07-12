/* Copyright 2020 Google LLC
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

package cmd

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path"
	"time"

	sp "cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"github.com/cloudspannerecosystem/harbourbridge/common/constants"
	"github.com/cloudspannerecosystem/harbourbridge/common/utils"
	"github.com/cloudspannerecosystem/harbourbridge/conversion"
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/logger"
	"github.com/cloudspannerecosystem/harbourbridge/profiles"
	"github.com/cloudspannerecosystem/harbourbridge/proto/migration"
	"github.com/google/subcommands"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

// SchemaCmd struct with flags.
type SchemaCmd struct {
	source        string
	sourceProfile string
	target        string
	targetProfile string
	filePrefix    string // TODO: move filePrefix to global flags
	logLevel      string
	dryRun        bool
}

// Name returns the name of operation.
func (cmd *SchemaCmd) Name() string {
	return "schema"
}

// Synopsis returns summary of operation.
func (cmd *SchemaCmd) Synopsis() string {
	return "generate schema for target db from source db schema"
}

// Usage returns usage info of the command.
func (cmd *SchemaCmd) Usage() string {
	return fmt.Sprintf(`%v schema -source=[source] -source-profile="key1=value1,key2=value2" ...

Convert schema for source db specified by source and source-profile. Source db
dump file can be specified by either file param in source-profile or piped to
stdin. Connection profile for source database in direct connect mode can be
specified by setting appropriate params in source-profile. The schema flags are:
`, path.Base(os.Args[0]))
}

// SetFlags sets the flags.
func (cmd *SchemaCmd) SetFlags(f *flag.FlagSet) {
	f.StringVar(&cmd.source, "source", "", "Flag for specifying source DB, (e.g., `PostgreSQL`, `MySQL`, `DynamoDB`)")
	f.StringVar(&cmd.sourceProfile, "source-profile", "", "Flag for specifying connection profile for source database e.g., \"file=<path>,format=dump\"")
	f.StringVar(&cmd.target, "target", "Spanner", "Specifies the target DB, defaults to Spanner (accepted values: `Spanner`)")
	f.StringVar(&cmd.targetProfile, "target-profile", "", "Flag for specifying connection profile for target database e.g., \"dialect=postgresql\"")
	f.StringVar(&cmd.filePrefix, "prefix", "", "File prefix for generated files")
	f.StringVar(&cmd.logLevel, "log-level", "INFO", "Configure the logging level for the command (INFO, DEBUG), defaults to INFO")
	f.BoolVar(&cmd.dryRun, "dry-run", false, "Flag for generating DDL and schema conversion report without creating a spanner database")
}

func (cmd *SchemaCmd) Execute(ctx context.Context, f *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	// Cleanup hb tmp data directory in case residuals remain from prev runs.
	os.RemoveAll(os.TempDir() + constants.HB_TMP_DIR)
	var err error
	defer func() {
		if err != nil {
			logger.Log.Fatal("FATAL error", zap.Error(err))
		}
	}()
	err = logger.InitializeLogger(cmd.logLevel)
	if err != nil {
		fmt.Println("Error initialising logger, did you specify a valid log-level? [DEBUG, INFO, WARN, ERROR, FATAL]", err)
		return subcommands.ExitFailure
	}
	defer logger.Log.Sync()

	sourceProfile, err := profiles.NewSourceProfile(cmd.sourceProfile, cmd.source)
	if err != nil {
		return subcommands.ExitUsageError
	}
	sourceProfile.Driver, err = sourceProfile.ToLegacyDriver(cmd.source)
	if err != nil {
		return subcommands.ExitUsageError
	}

	targetProfile, err := profiles.NewTargetProfile(cmd.targetProfile)
	if err != nil {
		return subcommands.ExitUsageError
	}
	targetProfile.TargetDb = targetProfile.ToLegacyTargetDb()

	dumpFilePath := ""
	if sourceProfile.Ty == profiles.SourceProfileTypeFile && (sourceProfile.File.Format == "" || sourceProfile.File.Format == "dump") {
		dumpFilePath = sourceProfile.File.Path
	}
	ioHelper := utils.NewIOStreams(sourceProfile.Driver, dumpFilePath)
	if ioHelper.SeekableIn != nil {
		defer ioHelper.In.Close()
	}

	dbName, err := utils.GetDatabaseName(sourceProfile.Driver, time.Now())
	if err != nil {
		err = fmt.Errorf("can't generate database name for prefix: %v", err)
		return subcommands.ExitFailure
	}

	// If filePrefix not explicitly set, use generated dbName.
	if cmd.filePrefix == "" {
		cmd.filePrefix = dbName + "."
	}

	schemaConversionStartTime := time.Now()
	var conv *internal.Conv
	conv, err = conversion.SchemaConv(sourceProfile, targetProfile, &ioHelper)
	if err != nil {
		return subcommands.ExitFailure
	}

	conversion.WriteSchemaFile(conv, schemaConversionStartTime, cmd.filePrefix+schemaFile, ioHelper.Out)
	conversion.WriteSessionFile(conv, cmd.filePrefix+sessionFile, ioHelper.Out)

	// Populate migration request id and migration type in conv object
	conv.Audit.MigrationRequestId = "HB-" + uuid.New().String()
	conv.Audit.MigrationType = migration.MigrationData_SCHEMA_ONLY.Enum()

	var (
		project, instance string
		adminClient       *database.DatabaseAdminClient
		client            *sp.Client
	)

	if !cmd.dryRun {

		project, instance, dbName, err = targetProfile.GetResourceIds(ctx, schemaConversionStartTime, sourceProfile.Driver, ioHelper.Out)
		if err != nil {
			return subcommands.ExitUsageError
		}
		fmt.Println("Using Google Cloud project:", project)
		fmt.Println("Using Cloud Spanner instance:", instance)
		utils.PrintPermissionsWarning(sourceProfile.Driver, ioHelper.Out)

		dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", project, instance, dbName)

		adminClient, err = utils.NewDatabaseAdminClient(ctx)
		if err != nil {
			err = fmt.Errorf("can't create admin client: %v", utils.AnalyzeError(err, dbURI))
			return subcommands.ExitFailure
		}
		defer adminClient.Close()
		client, err = utils.GetClient(ctx, dbURI)
		if err != nil {
			err = fmt.Errorf("can't create client for db %s: %v", dbURI, err)
			return subcommands.ExitFailure
		}
		defer client.Close()

		err = conversion.CreateOrUpdateDatabase(ctx, adminClient, dbURI, sourceProfile.Driver, targetProfile.TargetDb, conv, ioHelper.Out)
		if err != nil {
			err = fmt.Errorf("can't create/update database: %v", err)
			return subcommands.ExitFailure
		}
	}

	schemaCoversionEndTime := time.Now()
	conv.Audit.SchemaConversionDuration = schemaCoversionEndTime.Sub(schemaConversionStartTime)
	banner := utils.GetBanner(schemaConversionStartTime, dbName)
	conversion.Report(sourceProfile.Driver, nil, ioHelper.BytesRead, banner, conv, cmd.filePrefix+reportFile, ioHelper.Out)
	// Cleanup hb tmp data directory.
	os.RemoveAll(os.TempDir() + constants.HB_TMP_DIR)
	return subcommands.ExitSuccess
}

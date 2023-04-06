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

package cmd

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/cloudspannerecosystem/harbourbridge/common/constants"
	"github.com/cloudspannerecosystem/harbourbridge/common/utils"
	"github.com/cloudspannerecosystem/harbourbridge/conversion"
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/logger"
	"github.com/cloudspannerecosystem/harbourbridge/proto/migration"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/writer"
	"github.com/google/subcommands"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

// SchemaAndDataCmd struct with flags.
type SchemaAndDataCmd struct {
	source          string
	sourceProfile   string
	target          string
	targetProfile   string
	SkipForeignKeys bool
	filePrefix      string // TODO: move filePrefix to global flags
	WriteLimit      int64
	dryRun          bool
	logLevel        string
	verbose         bool
}

// Name returns the name of operation.
func (cmd *SchemaAndDataCmd) Name() string {
	return "schema-and-data"
}

// Synopsis returns summary of operation.
func (cmd *SchemaAndDataCmd) Synopsis() string {
	return "schema and data migration from source db to target db in schema-and-data"
}

// Usage returns usage info of the command.
func (cmd *SchemaAndDataCmd) Usage() string {
	return fmt.Sprintf(`%v schema-and-data -source=[source] -target-profile="instance=my-instance"...

Migrate schema and data from source db to target db in schema-and-data. Source db dump
file can be specified by either file param in source-profile or piped to stdin.
Connection profile for source databases in direct connect mode can be specified
by setting appropriate params in source-profile. The schema-and-data flags are:
`, path.Base(os.Args[0]))
}

// SetFlags sets the flags.
func (cmd *SchemaAndDataCmd) SetFlags(f *flag.FlagSet) {
	f.StringVar(&cmd.source, "source", "", "Flag for specifying source DB, (e.g., `PostgreSQL`, `MySQL`, `DynamoDB`)")
	f.StringVar(&cmd.sourceProfile, "source-profile", "", "Flag for specifying connection profile for source database e.g., \"file=<path>,format=dump\"")
	f.StringVar(&cmd.target, "target", "Spanner", "Specifies the target DB, defaults to Spanner (accepted values: `Spanner`)")
	f.StringVar(&cmd.targetProfile, "target-profile", "", "Flag for specifying connection profile for target database e.g., \"dialect=postgresql\"")
	f.BoolVar(&cmd.SkipForeignKeys, "skip-foreign-keys", false, "Skip creating foreign keys after data migration is complete (ddl statements for foreign keys can still be found in the downloaded schema.ddl.txt file and the same can be applied separately)")
	f.StringVar(&cmd.filePrefix, "prefix", "", "File prefix for generated files")
	f.Int64Var(&cmd.WriteLimit, "write-limit", DefaultWritersLimit, "Write limit for writes to spanner")
	f.BoolVar(&cmd.dryRun, "dry-run", false, "Flag for generating DDL and schema conversion report without creating a spanner database")
	f.StringVar(&cmd.logLevel, "log-level", "INFO", "Configure the logging level for the command (INFO, DEBUG), defaults to INFO")
	f.BoolVar(&cmd.verbose, "v", false, "verbose: print additional output")
	f.BoolVar(&cmd.verbose, "verbose", false, "verbose: print additional output")
}

func (cmd *SchemaAndDataCmd) Execute(ctx context.Context, f *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	// Cleanup hb tmp data directory in case residuals remain from prev runs.
	os.RemoveAll(filepath.Join(os.TempDir(), constants.HB_TMP_DIR))
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
	internal.VerboseInit(cmd.verbose)

	sourceProfile, targetProfile, ioHelper, dbName, err := PrepareMigrationPrerequisites(cmd.sourceProfile, cmd.targetProfile, cmd.source)
	if err != nil {
		err = fmt.Errorf("error while preparing prerequisites for migration: %v", err)
		return subcommands.ExitUsageError
	}
	schemaConversionStartTime := time.Now()

	// If filePrefix not explicitly set, use dbName as prefix.
	if cmd.filePrefix == "" {
		cmd.filePrefix = dbName + "."
	}

	var (
		conv   *internal.Conv
		bw     *writer.BatchWriter
		banner string
		dbURI  string
	)
	conv, err = conversion.SchemaConv(sourceProfile, targetProfile, &ioHelper)
	if err != nil {
		panic(err)
	}
	schemaCoversionEndTime := time.Now()
	conv.Audit.SchemaConversionDuration = schemaCoversionEndTime.Sub(schemaConversionStartTime)

	// Populate migration request id and migration type in conv object.
	conv.Audit.MigrationRequestId = "HB-" + uuid.New().String()
	conv.Audit.MigrationType = migration.MigrationData_SCHEMA_AND_DATA.Enum()

	conversion.WriteSchemaFile(conv, schemaConversionStartTime, cmd.filePrefix+schemaFile, ioHelper.Out)
	conversion.WriteSessionFile(conv, cmd.filePrefix+sessionFile, ioHelper.Out)

	if !cmd.dryRun {
		conversion.Report(sourceProfile.Driver, nil, ioHelper.BytesRead, "", conv, cmd.filePrefix+reportFile, ioHelper.Out)
		bw, err = MigrateDatabase(ctx, targetProfile, sourceProfile, dbName, &ioHelper, cmd, conv, nil)
		if err != nil {
			err = fmt.Errorf("can't finish database migration for db %s: %v", dbName, err)
			return subcommands.ExitFailure
		}
		dataCoversionEndTime := time.Now()
		conv.Audit.DataConversionDuration = dataCoversionEndTime.Sub(schemaCoversionEndTime)
		banner = utils.GetBanner(schemaConversionStartTime, dbURI)

	} else {
		conv.Audit.DryRun = true
		schemaCoversionEndTime := time.Now()
		conv.Audit.SchemaConversionDuration = schemaCoversionEndTime.Sub(schemaConversionStartTime)
		bw, err = conversion.DataConv(ctx, sourceProfile, targetProfile, &ioHelper, nil, conv, true, cmd.WriteLimit)
		if err != nil {
			err = fmt.Errorf("can't finish data conversion for db %s: %v", dbName, err)
			return subcommands.ExitFailure
		}
		dataCoversionEndTime := time.Now()
		conv.Audit.DataConversionDuration = dataCoversionEndTime.Sub(schemaCoversionEndTime)
		banner = utils.GetBanner(schemaConversionStartTime, dbName)
	}
	conversion.Report(sourceProfile.Driver, bw.DroppedRowsByTable(), ioHelper.BytesRead, banner, conv, cmd.filePrefix+reportFile, ioHelper.Out)
	conversion.WriteBadData(bw, conv, banner, cmd.filePrefix+badDataFile, ioHelper.Out)

	// Cleanup hb tmp data directory.
	os.RemoveAll(filepath.Join(os.TempDir(), constants.HB_TMP_DIR))
	return subcommands.ExitSuccess
}

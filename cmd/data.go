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
	"strings"
	"time"

	sp "cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	spanneracc "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/spanner"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/utils"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/conversion"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/proto/migration"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/writer"
	"github.com/google/subcommands"
	"go.uber.org/zap"
)

// DataCmd struct with flags.
type DataCmd struct {
	source          string
	sourceProfile   string
	target          string
	targetProfile   string
	sessionJSON     string
	filePrefix      string // TODO: move filePrefix to global flags
	WriteLimit      int64
	dryRun          bool
	logLevel        string
	SkipForeignKeys bool
	validate        bool
}

// Name returns the name of operation.
func (cmd *DataCmd) Name() string {
	return "data"
}

// Synopsis returns summary of operation.
func (cmd *DataCmd) Synopsis() string {
	return "migrate data from source db to target db"
}

// Usage returns usage info of the command.
func (cmd *DataCmd) Usage() string {
	return fmt.Sprintf(`%v data -session=[session_file] -source=[source] -target-profile="instance=my-instance"...

Migrate data from source db to target db. Source db dump file can be specified
by either file param in source-profile or piped to stdin. Connection profile
for source databases in direct connect mode can be specified by setting
appropriate params in source-profile. The data flags are:
`, path.Base(os.Args[0]))
}

// SetFlags sets the flags.
func (cmd *DataCmd) SetFlags(f *flag.FlagSet) {
	f.StringVar(&cmd.source, "source", "", "Flag for specifying source DB, (e.g., `PostgreSQL`, `MySQL`, `DynamoDB`)")
	f.StringVar(&cmd.sourceProfile, "source-profile", "", "Flag for specifying connection profile for source database e.g., \"file=<path>,format=dump\"")
	f.StringVar(&cmd.sessionJSON, "session", "", "Specifies the file we restore session state from")
	f.StringVar(&cmd.target, "target", "Spanner", "Specifies the target DB, defaults to Spanner (accepted values: `Spanner`)")
	f.StringVar(&cmd.targetProfile, "target-profile", "", "Flag for specifying connection profile for target database e.g., \"dialect=postgresql\"")
	f.StringVar(&cmd.filePrefix, "prefix", "", "File prefix for generated files")
	f.Int64Var(&cmd.WriteLimit, "write-limit", DefaultWritersLimit, "Write limit for writes to spanner")
	f.BoolVar(&cmd.dryRun, "dry-run", false, "Flag for generating DDL and schema conversion report without creating a spanner database")
	f.StringVar(&cmd.logLevel, "log-level", "DEBUG", "Configure the logging level for the command (INFO, DEBUG), defaults to DEBUG")
	f.BoolVar(&cmd.SkipForeignKeys, "skip-foreign-keys", false, "Skip creating foreign keys after data migration is complete (ddl statements for foreign keys can still be found in the downloaded schema.ddl.txt file and the same can be applied separately)")
	f.BoolVar(&cmd.validate, "validate", false, "Flag for validating if all the required input parameters are present")
}

func (cmd *DataCmd) Execute(ctx context.Context, f *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	// Cleanup smt tmp data directory in case residuals remain from prev runs.
	os.RemoveAll(filepath.Join(os.TempDir(), constants.SMT_TMP_DIR))
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

	conv := internal.MakeConv()
	// validate and parse source-profile, target-profile and source
	sourceProfile, targetProfile, ioHelper, dbName, err := PrepareMigrationPrerequisites(cmd.sourceProfile, cmd.targetProfile, cmd.source)
	if err != nil {
		err = fmt.Errorf("error while preparing prerequisites for migration: %v", err)
		return subcommands.ExitUsageError
	}
	var (
		bw     *writer.BatchWriter
		banner string
	)
	// Populate migration request id and migration type in conv object.
	conv.Audit.MigrationRequestId, _ = utils.GenerateName("smt-job")
	conv.Audit.MigrationRequestId = strings.Replace(conv.Audit.MigrationRequestId, "_", "-", -1)
	conv.Audit.MigrationType = migration.MigrationData_DATA_ONLY.Enum()
	conv.Audit.SkipMetricsPopulation = os.Getenv("SKIP_METRICS_POPULATION") == "true"
	dataCoversionStartTime := time.Now()

	if cmd.validate {
		if cmd.sessionJSON == "" {
			err = fmt.Errorf("cannot leave --session flag empty, please specify session file path e.g., --session=./session.json etc")
			return subcommands.ExitUsageError
		}
		return subcommands.ExitSuccess
	}

	if !sourceProfile.UseTargetSchema() {
		err = conversion.ReadSessionFile(conv, cmd.sessionJSON)
		if err != nil {
			return subcommands.ExitUsageError
		}
		if targetProfile.Conn.Sp.Dialect != "" && conv.SpDialect != targetProfile.Conn.Sp.Dialect {
			err = fmt.Errorf("running data migration for Spanner dialect: %v, whereas schema mapping was done for dialect: %v", targetProfile.Conn.Sp.Dialect, conv.SpDialect)
			return subcommands.ExitUsageError
		}
	}

	var (
		dbURI string
	)
	if !cmd.dryRun {
		now := time.Now()
		bw, err = MigrateDatabase(ctx, targetProfile, sourceProfile, dbName, &ioHelper, cmd, conv, nil)
		if err != nil {
			err = fmt.Errorf("can't finish database migration for db %s: %v", dbName, err)
			return subcommands.ExitFailure
		}
		banner = utils.GetBanner(now, dbURI)
	} else {
		conv.Audit.DryRun = true
		bw, err = conversion.DataConv(ctx, sourceProfile, targetProfile, &ioHelper, nil, conv, true, cmd.WriteLimit)
		if err != nil {
			err = fmt.Errorf("can't finish data conversion for db %s: %v", dbName, err)
			return subcommands.ExitFailure
		}
		banner = utils.GetBanner(dataCoversionStartTime, dbName)
	}
	dataCoversionEndTime := time.Now()
	dataCoversionDuration := dataCoversionEndTime.Sub(dataCoversionStartTime)
	conv.Audit.DataConversionDuration = dataCoversionDuration

	// If filePrefix not explicitly set, use dbName as prefix.
	if cmd.filePrefix == "" {
		cmd.filePrefix = targetProfile.Conn.Sp.Dbname
	}
	conversion.Report(sourceProfile.Driver, bw.DroppedRowsByTable(), ioHelper.BytesRead, banner, conv, cmd.filePrefix, dbName, ioHelper.Out)
	conversion.WriteBadData(bw, conv, banner, cmd.filePrefix+badDataFile, ioHelper.Out)
	// Cleanup smt tmp data directory.
	os.RemoveAll(filepath.Join(os.TempDir(), constants.SMT_TMP_DIR))
	return subcommands.ExitSuccess
}

// validateExistingDb validates that the existing spanner schema is in accordance with the one specified in the session file.
func validateExistingDb(ctx context.Context, spDialect, dbURI string, adminClient *database.DatabaseAdminClient, client *sp.Client, conv *internal.Conv) error {
	dbExists, err := spanneracc.CheckExistingDb(ctx, dbURI)
	if err != nil {
		err = fmt.Errorf("can't verify target database: %v", err)
		return err
	}
	if !dbExists {
		err = fmt.Errorf("target database doesn't exist")
		return err
	}
	var nonEmptyTableName string
	nonEmptyTableName, err = conversion.ValidateTables(ctx, client, spDialect)
	if err != nil {
		err = fmt.Errorf("error validating the tables: %v", err)
		return err
	}
	if nonEmptyTableName != "" {
		fmt.Printf("WARNING: Some tables in the database are non-empty e.g %s, overwriting these tables can lead to unintended behaviour. If this is unintended, please reconsider your migration attempt.\n\n", nonEmptyTableName)
	}
	spannerConv := internal.MakeConv()
	spannerConv.SpDialect = spDialect
	err = utils.ReadSpannerSchema(ctx, spannerConv, client)
	if err != nil {
		err = fmt.Errorf("can't read spanner schema: %v", err)
		return err
	}
	err = utils.CompareSchema(conv, spannerConv)
	if err != nil {
		err = fmt.Errorf("error while comparing the schema from session file and existing spanner schema: %v", err)
		return err
	}
	return nil
}

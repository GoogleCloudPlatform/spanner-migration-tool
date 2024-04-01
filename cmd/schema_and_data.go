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

	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/utils"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/conversion"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/profiles"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/proto/migration"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/writer"
	"github.com/google/subcommands"
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
	project         string
	WriteLimit      int64
	dryRun          bool
	logLevel        string
	validate        bool
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
	f.StringVar(&cmd.project, "project", "", "Flag spcifying default project id for all the generated resources for the migration")
	f.Int64Var(&cmd.WriteLimit, "write-limit", DefaultWritersLimit, "Write limit for writes to spanner")
	f.BoolVar(&cmd.dryRun, "dry-run", false, "Flag for generating DDL and schema conversion report without creating a spanner database")
	f.StringVar(&cmd.logLevel, "log-level", "DEBUG", "Configure the logging level for the command (INFO, DEBUG), defaults to DEBUG")
	f.BoolVar(&cmd.validate, "validate", false, "Flag for validating if all the required input parameters are present")
}

func (cmd *SchemaAndDataCmd) Execute(ctx context.Context, f *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
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
	// validate and parse source-profile, target-profile and source
	sourceProfile, targetProfile, ioHelper, dbName, err := PrepareMigrationPrerequisites(cmd.sourceProfile, cmd.targetProfile, cmd.source)
	if err != nil {
		err = fmt.Errorf("error while preparing prerequisites for migration: %v", err)
		return subcommands.ExitUsageError
	}
	if cmd.project == "" {
		getInfo := &utils.GetUtilInfoImpl{}
		cmd.project, err = getInfo.GetProject()
		if err != nil {
			logger.Log.Error("Could not get project id from gcloud environment --project flag. Inferring the migration project id from target profile.", zap.Error(err))
			cmd.project = targetProfile.Conn.Sp.Project
		}
	}
	if cmd.validate {
		return subcommands.ExitSuccess
	}
	schemaConversionStartTime := time.Now()

	// If filePrefix not explicitly set, use dbName as prefix.
	if cmd.filePrefix == "" {
		cmd.filePrefix = dbName
	}

	var (
		conv   *internal.Conv
		bw     *writer.BatchWriter
		banner string
		dbURI  string
	)
	convImpl := &conversion.ConvImpl{}
	conv, err = convImpl.SchemaConv(cmd.project, sourceProfile, targetProfile, &ioHelper, &conversion.SchemaFromSourceImpl{})
	if err != nil {
		panic(err)
	}
	schemaCoversionEndTime := time.Now()
	conv.Audit.SchemaConversionDuration = schemaCoversionEndTime.Sub(schemaConversionStartTime)

	// Populate migration request id and migration type in conv object.
	conv.Audit.MigrationRequestId, _ = utils.GenerateName("smt-job")
	conv.Audit.MigrationRequestId = strings.Replace(conv.Audit.MigrationRequestId, "_", "-", -1)
	conv.Audit.MigrationType = migration.MigrationData_SCHEMA_AND_DATA.Enum()

	conversion.WriteSchemaFile(conv, schemaConversionStartTime, cmd.filePrefix+schemaFile, ioHelper.Out, sourceProfile.Driver)
	conversion.WriteSessionFile(conv, cmd.filePrefix+sessionFile, ioHelper.Out)
	conv.Audit.SkipMetricsPopulation = os.Getenv("SKIP_METRICS_POPULATION") == "true"
	reportImpl := conversion.ReportImpl{}
	if !cmd.dryRun {
		reportImpl.GenerateReport(sourceProfile.Driver, nil, ioHelper.BytesRead, "", conv, cmd.filePrefix, dbName, ioHelper.Out)
		bw, err = MigrateDatabase(ctx, cmd.project, targetProfile, sourceProfile, dbName, &ioHelper, cmd, conv, nil)
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
		// If migration type is Minimal Downtime, validate if required resources can be generated
		if !conv.UI && sourceProfile.Driver == constants.MYSQL && sourceProfile.Ty == profiles.SourceProfileTypeConfig && sourceProfile.Config.ConfigType == constants.DATAFLOW_MIGRATION {
			err := ValidateResourceGenerationHelper(ctx, cmd.project, targetProfile.Conn.Sp.Project, targetProfile.Conn.Sp.Instance, sourceProfile, conv)
			if err != nil {
				logger.Log.Error(err.Error())
				return subcommands.ExitFailure
			}
		}

		bw, err = convImpl.DataConv(ctx, cmd.project, sourceProfile, targetProfile, &ioHelper, nil, conv, true, cmd.WriteLimit, &conversion.DataFromSourceImpl{})
		if err != nil {
			err = fmt.Errorf("can't finish data conversion for db %s: %v", dbName, err)
			return subcommands.ExitFailure
		}
		dataCoversionEndTime := time.Now()
		conv.Audit.DataConversionDuration = dataCoversionEndTime.Sub(schemaCoversionEndTime)
		banner = utils.GetBanner(schemaConversionStartTime, dbName)
	}
	reportImpl.GenerateReport(sourceProfile.Driver, bw.DroppedRowsByTable(), ioHelper.BytesRead, banner, conv, cmd.filePrefix, dbName, ioHelper.Out)
	conversion.WriteBadData(bw, conv, banner, cmd.filePrefix+badDataFile, ioHelper.Out)

	// Cleanup smt tmp data directory.
	os.RemoveAll(filepath.Join(os.TempDir(), constants.SMT_TMP_DIR))
	return subcommands.ExitSuccess
}

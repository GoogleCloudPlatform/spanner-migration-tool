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
	"time"

	"github.com/cloudspannerecosystem/harbourbridge/common/constants"
	"github.com/cloudspannerecosystem/harbourbridge/common/utils"
	"github.com/cloudspannerecosystem/harbourbridge/conversion"
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/profiles"
	"github.com/cloudspannerecosystem/harbourbridge/proto/migration"
	"github.com/google/subcommands"
	"github.com/google/uuid"
)

// DataCmd struct with flags.
type DataCmd struct {
	source          string
	sourceProfile   string
	target          string
	targetProfile   string
	skipForeignKeys bool
	sessionJSON     string
	filePrefix      string // TODO: move filePrefix to global flags
	writeLimit      int64
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
	f.BoolVar(&cmd.skipForeignKeys, "skip-foreign-keys", false, "Skip creating foreign keys after data migration is complete (ddl statements for foreign keys can still be found in the downloaded schema.ddl.txt file and the same can be applied separately)")
	f.StringVar(&cmd.filePrefix, "prefix", "", "File prefix for generated files")
	f.Int64Var(&cmd.writeLimit, "write-limit", defaultWritersLimit, "Write limit for writes to spanner")
}

func (cmd *DataCmd) Execute(ctx context.Context, f *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	// Cleanup hb tmp data directory in case residuals remain from prev runs.
	os.RemoveAll(os.TempDir() + constants.HB_TMP_DIR)
	var err error
	defer func() {
		if err != nil {
			fmt.Printf("FATAL error: %v\n", err)
		}
	}()
	conv := internal.MakeConv()

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

	now := time.Now()
	project, instance, dbName, err := targetProfile.GetResourceIds(ctx, now, sourceProfile.Driver, ioHelper.Out)
	if err != nil {
		return subcommands.ExitUsageError
	}
	fmt.Println("Using Google Cloud project:", project)
	fmt.Println("Using Cloud Spanner instance:", instance)
	utils.PrintPermissionsWarning(sourceProfile.Driver, ioHelper.Out)

	dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", project, instance, dbName)

	// If filePrefix not explicitly set, use dbName as prefix.
	if cmd.filePrefix == "" {
		cmd.filePrefix = dbName + "."
	}

	client, err := utils.GetClient(ctx, dbURI)
	if err != nil {
		err = fmt.Errorf("can't create client for db %s: %v", dbURI, err)
		return subcommands.ExitFailure
	}
	defer client.Close()

	if !sourceProfile.UseTargetSchema() {
		err = conversion.ReadSessionFile(conv, cmd.sessionJSON)
		if err != nil {
			return subcommands.ExitUsageError
		}
		if targetProfile.TargetDb != "" && conv.TargetDb != targetProfile.TargetDb {
			err = fmt.Errorf("running data migration for Spanner dialect: %v, whereas schema mapping was done for dialect: %v", targetProfile.TargetDb, conv.TargetDb)
			return subcommands.ExitUsageError
		}
	}

	adminClient, err := utils.NewDatabaseAdminClient(ctx)
	if err != nil {
		err = fmt.Errorf("can't create admin client: %w", utils.AnalyzeError(err, dbURI))
		return subcommands.ExitFailure
	}
	defer adminClient.Close()

	// Populate migration request id and migration type in conv object
	conv.MigrationRequestId = "HB-" + uuid.New().String()
	conv.MigrationType = migration.MigrationData_DATA_ONLY.Enum()

	if !sourceProfile.UseTargetSchema() {
		err = conversion.CreateOrUpdateDatabase(ctx, adminClient, dbURI, sourceProfile.Driver, targetProfile.TargetDb, conv, ioHelper.Out)
		if err != nil {
			err = fmt.Errorf("can't create/update database: %v", err)
			return subcommands.ExitFailure
		}
	}

	streamingCfg, err := startDatastream(ctx, sourceProfile, targetProfile)
	if err != nil {
		err = fmt.Errorf("error starting datastream: %v", err)
		return subcommands.ExitFailure
	}

	bw, err := performSnapshotMigration(ctx, sourceProfile, targetProfile, ioHelper, client, conv, cmd.writeLimit, dbURI)
	if err != nil {
		err = fmt.Errorf("can't do snapshot migration: %v", err)
		return subcommands.ExitFailure
	}

	err = startDataflow(ctx, sourceProfile, targetProfile, streamingCfg)
	if err != nil {
		err = fmt.Errorf("error starting dataflow: %v", err)
		return subcommands.ExitFailure
	}

	if !cmd.skipForeignKeys {
		if err = conversion.UpdateDDLForeignKeys(ctx, adminClient, dbURI, conv, ioHelper.Out); err != nil {
			err = fmt.Errorf("can't perform update schema on db %s with foreign keys: %v", dbURI, err)
			return subcommands.ExitFailure
		}
	}

	banner := utils.GetBanner(now, dbURI)
	conversion.Report(sourceProfile.Driver, bw.DroppedRowsByTable(), ioHelper.BytesRead, banner, conv, cmd.filePrefix+reportFile, ioHelper.Out)
	conversion.WriteBadData(bw, conv, banner, cmd.filePrefix+badDataFile, ioHelper.Out)
	// Cleanup hb tmp data directory.
	os.RemoveAll(os.TempDir() + constants.HB_TMP_DIR)
	return subcommands.ExitSuccess
}

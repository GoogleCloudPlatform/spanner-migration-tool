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
	"github.com/google/subcommands"
)

// SchemaAndDataCmd struct with flags.
type SchemaAndDataCmd struct {
	source          string
	sourceProfile   string
	target          string
	targetProfile   string
	skipForeignKeys bool
	filePrefix      string // TODO: move filePrefix to global flags
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
	f.BoolVar(&cmd.skipForeignKeys, "skip-foreign-keys", false, "Skip creating foreign keys after data migration is complete (ddl statements for foreign keys can still be found in the downloaded schema.ddl.txt file and the same can be applied separately)")
	f.StringVar(&cmd.filePrefix, "prefix", "", "File prefix for generated files")
}

func (cmd *SchemaAndDataCmd) Execute(ctx context.Context, f *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	// Cleanup hb tmp data directory in case residuals remain from prev runs.
	os.RemoveAll(os.TempDir() + constants.HB_TMP_DIR)
	var err error
	defer func() {
		if err != nil {
			fmt.Printf("FATAL error: %v\n", err)
		}
	}()

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

	// If filePrefix not explicitly set, use dbName as prefix.
	if cmd.filePrefix == "" {
		dbName, err := utils.GetDatabaseName(sourceProfile.Driver, now)
		if err != nil {
			panic(fmt.Errorf("can't generate database name for prefix: %v", err))
		}
		cmd.filePrefix = dbName + "."
	}

	var conv *internal.Conv
	conv, err = conversion.SchemaConv(sourceProfile, targetProfile, &ioHelper)
	if err != nil {
		panic(err)
	}

	conversion.WriteSchemaFile(conv, now, cmd.filePrefix+schemaFile, ioHelper.Out)
	conversion.WriteSessionFile(conv, cmd.filePrefix+sessionFile, ioHelper.Out)
	conversion.Report(sourceProfile.Driver, nil, ioHelper.BytesRead, "", conv, cmd.filePrefix+reportFile, ioHelper.Out)

	project, instance, dbName, err := targetProfile.GetResourceIds(ctx, now, sourceProfile.Driver, ioHelper.Out)
	if err != nil {
		return subcommands.ExitUsageError
	}
	fmt.Println("Using Google Cloud project:", project)
	fmt.Println("Using Cloud Spanner instance:", instance)
	utils.PrintPermissionsWarning(sourceProfile.Driver, ioHelper.Out)

	dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", project, instance, dbName)

	adminClient, err := utils.NewDatabaseAdminClient(ctx)
	if err != nil {
		err = fmt.Errorf("can't create admin client: %w", utils.AnalyzeError(err, dbURI))
		return subcommands.ExitFailure
	}
	defer adminClient.Close()
	client, err := utils.GetClient(ctx, dbURI)
	if err != nil {
		err = fmt.Errorf("can't create client for db %s: %v", dbURI, err)
		return subcommands.ExitFailure
	}
	defer client.Close()

	err = conversion.CreateOrUpdateDatabase(ctx, adminClient, dbURI, conv, ioHelper.Out)
	if err != nil {
		err = fmt.Errorf("can't create/update database: %v", err)
		return subcommands.ExitFailure
	}

	bw, err := conversion.DataConv(ctx, sourceProfile, targetProfile, &ioHelper, client, conv, true)
	if err != nil {
		err = fmt.Errorf("can't finish data conversion for db %s: %v", dbURI, err)
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

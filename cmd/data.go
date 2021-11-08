package cmd

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path"
	"time"

	"github.com/cloudspannerecosystem/harbourbridge/conversion"
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/google/subcommands"
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
appropriate environment variables. The data flags are:
`, path.Base(os.Args[0]))
}

// SetFlags sets the flags.
func (cmd *DataCmd) SetFlags(f *flag.FlagSet) {
	f.StringVar(&cmd.source, "source", "", "Flag for specifying source DB, (e.g., `PostgreSQL`, `MySQL`, `DynamoDB`)")
	f.StringVar(&cmd.sourceProfile, "source-profile", "", "Flag for specifying connection profile for source database e.g., \"file=<path>,format=dump\"")
	f.StringVar(&cmd.sessionJSON, "session", "", "Specifies the file we restore session state from")
	f.StringVar(&cmd.target, "target", "Spanner", "Specifies the target DB, defaults to Spanner (accepted values: `Spanner`)")
	f.StringVar(&cmd.targetProfile, "target-profile", "", "Flag for specifying connection profile for target database e.g., \"dialect=postgresql\"")
	flag.BoolVar(&cmd.skipForeignKeys, "skip-foreign-keys", false, "Skip creating foreign keys after data migration is complete (ddl statements for foreign keys can still be found in the downloaded schema.ddl.txt file and the same can be applied separately)")
	f.StringVar(&cmd.filePrefix, "prefix", "", "File prefix for generated files")
}

func (cmd *DataCmd) Execute(ctx context.Context, f *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	var err error
	defer func() {
		if err != nil {
			fmt.Printf("FATAL error: %v\n", err)
		}
	}()

	sourceProfile, err := NewSourceProfile(cmd.sourceProfile, cmd.source)
	if err != nil {
		return subcommands.ExitUsageError
	}
	driverName, err := sourceProfile.ToLegacyDriver(cmd.source)
	if err != nil {
		return subcommands.ExitUsageError
	}

	targetProfile, err := NewTargetProfile(cmd.targetProfile)
	if err != nil {
		return subcommands.ExitUsageError
	}
	targetDb := targetProfile.ToLegacyTargetDb()

	dumpFilePath := ""
	if sourceProfile.ty == SourceProfileTypeFile && (sourceProfile.file.format == "" || sourceProfile.file.format == "dump") {
		dumpFilePath = sourceProfile.file.path
	}
	ioHelper := conversion.NewIOStreams(driverName, dumpFilePath)
	if ioHelper.SeekableIn != nil {
		defer ioHelper.In.Close()
	}

	now := time.Now()
	project, instance, dbName, err := getResourceIds(ctx, targetProfile, now, driverName, ioHelper.Out)
	if err != nil {
		return subcommands.ExitUsageError
	}
	dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", project, instance, dbName)

	// If filePrefix not explicitly set, use dbName as prefix.
	if cmd.filePrefix == "" {
		cmd.filePrefix = dbName + "."
	}

	conv := internal.MakeConv()
	err = conversion.ReadSessionFile(conv, cmd.sessionJSON)
	if err != nil {
		return subcommands.ExitUsageError
	}
	if targetDb != "" && conv.TargetDb != targetDb {
		err = fmt.Errorf("running data migration for Spanner dialect: %v, whereas schema mapping was done for dialect: %v", targetDb, conv.TargetDb)
		return subcommands.ExitUsageError
	}

	adminClient, err := conversion.NewDatabaseAdminClient(ctx)
	if err != nil {
		err = fmt.Errorf("can't create admin client: %w", conversion.AnalyzeError(err, dbURI))
		return subcommands.ExitFailure
	}
	defer adminClient.Close()
	client, err := conversion.GetClient(ctx, dbURI)
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
	sqlConnectionStr := ""
	if sourceProfile.ty == SourceProfileTypeConnection {
		// For DynamoDB, the SDK only reads credentials from env variables.
		// There is no way to pass them through the code hence, don't need to override
		// sqlConnectionStr for DynamoDB.
		if sourceProfile.conn.ty == SourceProfileConnectionTypeMySQL {
			connParams := sourceProfile.conn.mysql
			sqlConnectionStr = conversion.GetMYSQLConnectionStr(connParams.host, connParams.port, connParams.user, connParams.pwd, connParams.db)
		} else if sourceProfile.conn.ty == SourceProfileConnectionTypePostgreSQL {
			connParams := sourceProfile.conn.pg
			sqlConnectionStr = conversion.GetPGSQLConnectionStr(connParams.host, connParams.port, connParams.user, connParams.pwd, connParams.db)
		}
	}

	bw, err := conversion.DataConv(driverName, sqlConnectionStr, &ioHelper, client, conv, true)
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
	banner := conversion.GetBanner(now, dbURI)
	conversion.Report(driverName, bw.DroppedRowsByTable(), ioHelper.BytesRead, banner, conv, cmd.filePrefix+reportFile, ioHelper.Out)
	conversion.WriteBadData(bw, conv, banner, cmd.filePrefix+badDataFile, ioHelper.Out)
	return subcommands.ExitSuccess
}

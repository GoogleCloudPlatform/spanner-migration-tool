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

// SchemaCmd struct with flags.
type SchemaCmd struct {
	source        string
	sourceProfile string
	target        string
	targetProfile string
	filePrefix    string // TODO: move filePrefix to global flags
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

<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 00e463e (Add data and eval subcommands to harbourbridge command line interface (#212))
Convert schema for source db specified by source and source-profile. Source db
dump file can be specified by either file param in source-profile or piped to
stdin. Connection profile for source databases in direct connect mode can be
specified by setting appropriate environment variables. The schema flags are:
<<<<<<< HEAD
=======
Convert schema for source db specified by driver. Source db dump file can be
specified by either file param in source-profile or piped to stdin. Connection
profile for source databases in direct connect mode can be specified by setting
appropriate environment variables. The schema flags are:
>>>>>>> 6522c9b (Add support for source-profile and target-profile in subcommands. (#208))
=======
>>>>>>> 00e463e (Add data and eval subcommands to harbourbridge command line interface (#212))
`, path.Base(os.Args[0]))
}

// SetFlags sets the flags.
func (cmd *SchemaCmd) SetFlags(f *flag.FlagSet) {
	f.StringVar(&cmd.source, "source", "", "Flag for specifying source DB, (e.g., `PostgreSQL`, `MySQL`, `DynamoDB`)")
	f.StringVar(&cmd.sourceProfile, "source-profile", "", "Flag for specifying connection profile for source database e.g., \"file=<path>,format=dump\"")
	f.StringVar(&cmd.target, "target", "Spanner", "Specifies the target DB, defaults to Spanner (accepted values: `Spanner`)")
	f.StringVar(&cmd.targetProfile, "target-profile", "", "Flag for specifying connection profile for target database e.g., \"dialect=postgresql\"")
	f.StringVar(&cmd.filePrefix, "prefix", "", "File prefix for generated files")
}

func (cmd *SchemaCmd) Execute(ctx context.Context, f *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 00e463e (Add data and eval subcommands to harbourbridge command line interface (#212))
	var err error
	defer func() {
		if err != nil {
			fmt.Printf("FATAL error: %v\n", err)
		}
	}()

<<<<<<< HEAD
	sourceProfile, err := NewSourceProfile(cmd.sourceProfile, cmd.source)
	if err != nil {
		return subcommands.ExitUsageError
	}
	driverName, err := sourceProfile.ToLegacyDriver(cmd.source)
	if err != nil {
		return subcommands.ExitUsageError
=======
=======
>>>>>>> 00e463e (Add data and eval subcommands to harbourbridge command line interface (#212))
	sourceProfile, err := NewSourceProfile(cmd.sourceProfile, cmd.source)
	if err != nil {
		return subcommands.ExitUsageError
	}
	driverName, err := sourceProfile.ToLegacyDriver(cmd.source)
	if err != nil {
<<<<<<< HEAD
		panic(err)
>>>>>>> 6522c9b (Add support for source-profile and target-profile in subcommands. (#208))
=======
		return subcommands.ExitUsageError
>>>>>>> 00e463e (Add data and eval subcommands to harbourbridge command line interface (#212))
	}

	targetProfile, err := NewTargetProfile(cmd.targetProfile)
	if err != nil {
<<<<<<< HEAD
<<<<<<< HEAD
		return subcommands.ExitUsageError
=======
		panic(err)
>>>>>>> 6522c9b (Add support for source-profile and target-profile in subcommands. (#208))
=======
		return subcommands.ExitUsageError
>>>>>>> 00e463e (Add data and eval subcommands to harbourbridge command line interface (#212))
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

	// If filePrefix not explicitly set, use generated dbName.
	if cmd.filePrefix == "" {
		dbName, err := conversion.GetDatabaseName(driverName, time.Now())
		if err != nil {
<<<<<<< HEAD
<<<<<<< HEAD
			err = fmt.Errorf("can't generate database name for prefix: %v", err)
			return subcommands.ExitFailure
=======
			panic(fmt.Errorf("can't generate database name for prefix: %v", err))
>>>>>>> 6522c9b (Add support for source-profile and target-profile in subcommands. (#208))
=======
			err = fmt.Errorf("can't generate database name for prefix: %v", err)
			return subcommands.ExitFailure
>>>>>>> 00e463e (Add data and eval subcommands to harbourbridge command line interface (#212))
		}
		cmd.filePrefix = dbName + "."
	}

	schemaSampleSize := int64(100000)
	if sourceProfile.ty == SourceProfileTypeConnection {
		if sourceProfile.conn.ty == SourceProfileConnectionTypeDynamoDB {
			if sourceProfile.conn.dydb.schemaSampleSize != 0 {
				schemaSampleSize = sourceProfile.conn.dydb.schemaSampleSize
			}
		}
	}
	var conv *internal.Conv
	conv, err = conversion.SchemaConv(driverName, targetDb, &ioHelper, schemaSampleSize)
	if err != nil {
<<<<<<< HEAD
<<<<<<< HEAD
		return subcommands.ExitFailure
=======
		panic(err)
>>>>>>> 6522c9b (Add support for source-profile and target-profile in subcommands. (#208))
=======
		return subcommands.ExitFailure
>>>>>>> 00e463e (Add data and eval subcommands to harbourbridge command line interface (#212))
	}

	now := time.Now()
	conversion.WriteSchemaFile(conv, now, cmd.filePrefix+schemaFile, ioHelper.Out)
	conversion.WriteSessionFile(conv, cmd.filePrefix+sessionFile, ioHelper.Out)
	conversion.Report(driverName, nil, ioHelper.BytesRead, "", conv, cmd.filePrefix+reportFile, ioHelper.Out)
	return subcommands.ExitSuccess
}

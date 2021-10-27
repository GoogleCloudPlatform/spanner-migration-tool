package cmd

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path"
	"time"

	"github.com/cloudspannerecosystem/harbourbridge/conversion"
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/google/subcommands"
)

// SchemaCmd struct with flags.
type SchemaCmd struct {
	source string
	sourceProfile string
	target string
	targetProfile string
	filePrefix       string // TODO: move filePrefix to global flags
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
	return fmt.Sprintf(`%v schema [file]...

Convert schema for source db specified by driver. Source db dump file can be
specified by either dump-file flag or piped to stdin. Connection profile for source
databases in direct connect mode can be specified by setting appropriate
environment variables. The schema flags are:
`, path.Base(os.Args[0]))
}

// SetFlags sets the flags.
func (cmd *SchemaCmd) SetFlags(f *flag.FlagSet) {
	f.StringVar(&cmd.source, "source", "", "Flag for specifying source DB, (accepted values are \"PostgreSQL\", \"MySQL\", and \"DynamoDB\")")
	f.StringVar(&cmd.sourceProfile, "source-profile", "", "Flag for specifying connection profile for source database")
	f.StringVar(&cmd.target, "target", "Spanner", "Specifies the target DB, defaults to Spanner (accepted values: \"Spanner\")")
	f.StringVar(&cmd.targetProfile, "target-profile", "", "Flag for specifying connection profile for target database")
	f.StringVar(&cmd.filePrefix, "prefix", "", "File prefix for generated files")
}

func (cmd *SchemaCmd) Execute(ctx context.Context, f *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	sourceProfile, err := NewSourceProfile(cmd.sourceProfile, cmd.source)
	if err != nil {
		log.Fatal(err)
	}
	driverName, err := sourceProfile.ToLegacyDriver(cmd.source)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("legacy driverName = %+v\n", driverName)

	targetProfile, err := NewTargetProfile(cmd.targetProfile)
	if err != nil {
		log.Fatal(err)
	}
	targetDb := targetProfile.ToLegacyTargetDb()
	fmt.Printf("legacy targetDb = %+v\n", targetDb)

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
			log.Fatalf("can't generate database name for prefix: %v\n", err)
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
		log.Fatal(err)
	}

	now := time.Now()
	conversion.WriteSchemaFile(conv, now, cmd.filePrefix+schemaFile, ioHelper.Out)
	conversion.WriteSessionFile(conv, cmd.filePrefix+sessionFile, ioHelper.Out)
	conversion.Report(driverName, nil, ioHelper.BytesRead, "", conv, cmd.filePrefix+reportFile, ioHelper.Out)
	return subcommands.ExitSuccess
}

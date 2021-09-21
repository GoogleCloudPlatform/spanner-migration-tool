package cli

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
	prompt       bool
	filePrefix       string
	driverName string
	schemaSampleSize  int64
	dumpFilePath string
	targetDb string
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

Convert schema for source db specified by driver. Source db dump files can be
specified by either dump-file flag or from stdin. Connection profile for source
databases in direct connect mode can be specified by setting appropriate
environment variables. The schema flags are:
`, path.Base(os.Args[0]))
}

// SetFlags sets the flags.
func (cmd *SchemaCmd) SetFlags(f *flag.FlagSet) {
	f.BoolVar(&cmd.prompt, "prompt", false, "Prompt before executing")
	f.StringVar(&cmd.filePrefix, "prefix", "", "File prefix for generated files")
	f.StringVar(&cmd.driverName, "driver", "pg_dump", "Flag for specifying source DB or dump files (accepted values are \"pg_dump\", \"postgres\", \"mysqldump\", \"mysql\", and \"dynamodb\")")
	f.Int64Var(&cmd.schemaSampleSize, "schema-sample-size", int64(100000), "Number of rows to use for inferring schema (only for DynamoDB)")
	f.StringVar(&cmd.dumpFilePath, "dump-file", "", "Location of dump file to process")
	f.StringVar(&cmd.targetDb, "target-db", "", "Specifies the target DB, defaults to spanner.")
}

func (cmd *SchemaCmd) Execute(ctx context.Context, f *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	input := loadInput(cmd.dumpFilePath)
	ioHelper := &conversion.IOStreams{In: input, Out: os.Stdout}

	var conv *internal.Conv
	var err error
	conv, err = conversion.SchemaConv(cmd.driverName, cmd.targetDb, ioHelper, cmd.schemaSampleSize)
	if err != nil {
		log.Fatal(err)
	}
	if ioHelper.SeekableIn != nil {
		defer ioHelper.In.Close()
	}

	now := time.Now()
	conversion.WriteSchemaFile(conv, now, cmd.filePrefix+schemaFile, ioHelper.Out)
	conversion.WriteSessionFile(conv, cmd.filePrefix+sessionFile, ioHelper.Out)
	conversion.Report(cmd.driverName, nil, ioHelper.BytesRead, "", conv, cmd.filePrefix+reportFile, ioHelper.Out)
	return subcommands.ExitSuccess
}

// Load the dump file if parameter has been passed by the user.
// If no parameter has been passed, then read from standard input
func loadInput(dumpFile string) *os.File {
	if dumpFile != "" {
		fmt.Printf("\nloading dump file from path: %s\n", dumpFile)
		file, err := os.Open(dumpFile)
		if err != nil {
			fmt.Printf("\nerror reading file: %v err:%v", dumpFile, err)
			panic(err)
		}
		return file
	}
	return os.Stdin
}

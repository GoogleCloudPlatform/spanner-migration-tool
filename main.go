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

// Package main implements HarbourBridge, a stand-alone tool for Cloud Spanner
// evaluation, using data from an existing PostgreSQL/MySQL database. See README.md
// for details.
package main

import (
	"context"
	"embed"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/sijms/go-ora/v2"

	"github.com/cloudspannerecosystem/harbourbridge/cmd"
	"github.com/cloudspannerecosystem/harbourbridge/common/constants"
	"github.com/cloudspannerecosystem/harbourbridge/common/utils"
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/logger"
	"github.com/cloudspannerecosystem/harbourbridge/web"
	"github.com/cloudspannerecosystem/harbourbridge/webv2"
	"github.com/google/subcommands"
)

//go:embed frontend/*
var frontendDir embed.FS

var (
	dbNameOverride   string
	instanceOverride string
	filePrefix       = ""
	driverName       = constants.PGDUMP
	schemaSampleSize = int64(0)
	verbose          bool
	schemaOnly       bool
	dataOnly         bool
	skipForeignKeys  bool
	sessionJSON      string
	webapi           bool
	webapiv2         bool
	dumpFilePath     string
	targetDb         = constants.TargetSpanner
)

func setupGlobalFlags() {
	flag.StringVar(&dbNameOverride, "dbname", "", "dbname: name to use for Spanner DB")
	flag.StringVar(&instanceOverride, "instance", "", "instance: Spanner instance to use")
	flag.StringVar(&filePrefix, "prefix", "", "prefix: file prefix for generated files")
	flag.StringVar(&driverName, "driver", constants.PGDUMP, "driver name: flag for accessing source DB or dump files (accepted values are \"pg_dump\", \"postgres\", \"mysqldump\", and \"mysql\")")
	flag.Int64Var(&schemaSampleSize, "schema-sample-size", int64(100000), "schema-sample-size: the number of rows to use for inferring schema (only for DynamoDB)")
	flag.BoolVar(&verbose, "v", false, "verbose: print additional output")
	flag.BoolVar(&verbose, "verbose", false, "verbose: print additional output")
	flag.BoolVar(&schemaOnly, "schema-only", false, "schema-only: in this mode we do schema conversion, but skip data conversion")
	flag.BoolVar(&dataOnly, "data-only", false, "data-only: in this mode we skip schema conversion and just do data conversion (use the session flag to specify the session file for schema and data mapping)")
	flag.BoolVar(&skipForeignKeys, "skip-foreign-keys", false, "skip-foreign-keys: if true, skip creating foreign keys after data migration is complete (ddl statements for foreign keys can still be found in the downloaded schema.ddl.txt file and the same can be applied separately)")
	flag.StringVar(&sessionJSON, "session", "", "session: specifies the file we restore session state from (used in data-only to provide schema and data mapping)")
	flag.BoolVar(&webapi, "web", false, "web: run the web interface (experimental)")
	flag.BoolVar(&webapiv2, "webv2", false, "web: run the web interface (experimental)")
	flag.StringVar(&dumpFilePath, "dump-file", "", "dump-file: location of dump file to process")
	flag.StringVar(&targetDb, "target-db", constants.TargetSpanner, "target-db: Specifies the target DB. Defaults to spanner")
}

func didSetVerboseTwice() bool {
	numTimesSet := 0
	flag.Visit(func(f *flag.Flag) {
		if f.Name == "v" || f.Name == "verbose" {
			numTimesSet++
		}
	})
	return numTimesSet > 1
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	flag.PrintDefaults()
	fmt.Fprintf(os.Stderr, `Note: input is always read from stdin.
Sample usage:
  pg_dump mydb | %s
  %s < my_pg_dump_file
`, os.Args[0], os.Args[0])
}

func main() {
	ctx := context.Background()
	lf, err := utils.SetupLogFile()
	if err != nil {
		fmt.Printf("\nCan't set up log file: %v\n", err)
		panic(fmt.Errorf("can't set up log file"))
	}
	defer utils.Close(lf)

	// TODO: Remove this check and always run HB in subcommands mode once
	// global command line mode is deprecated. We can also enable support for
	// top-level flags in subcommand then.
	if len(os.Args) > 1 && os.Args[1] != "" && !strings.HasPrefix(os.Args[1], "-") {
		// Using HB CLI in subcommand mode.
		subcommands.Register(subcommands.HelpCommand(), "")
		subcommands.Register(subcommands.CommandsCommand(), "")
		subcommands.Register(&cmd.SchemaCmd{}, "")
		subcommands.Register(&cmd.DataCmd{}, "")
		subcommands.Register(&cmd.SchemaAndDataCmd{}, "")
		flag.Parse()
		os.Exit(int(subcommands.Execute(ctx)))
	}
	fmt.Printf("\nWarning: Found usage of deprecated flags. Support for these " +
		"flags will be discontinued soon.\nIt is recommended to use Harbourbridge " +
		"using connection profiles. Checkout usage here: https://github.com/cloudspannerecosystem/harbourbridge/tree/master/cmd#command-line-flags\n\n")
	err = logger.InitializeLogger("INFO")
	if err != nil {
		panic(fmt.Errorf("error initialising logger"))
	}
	defer logger.Log.Sync()
	
	// Running HB CLI in global command line mode.
	setupGlobalFlags()
	flag.Usage = usage
	flag.Parse()

	// Note: the web interface does not use any commandline flags.
	if webapi {
		web.FrontendDir = frontendDir
		web.App()
		return
	}

	// Note: the web interface does not use any commandline flags.
	if webapiv2 {
		webv2.App()
		return
	}

	if didSetVerboseTwice() {
		panic(fmt.Errorf("cannot set both -v and -verbose flags"))
	}

	internal.VerboseInit(verbose)
	if schemaOnly && dataOnly {
		panic(fmt.Errorf("can't use both schema-only and data-only modes at once"))
	}
	if dataOnly && sessionJSON == "" {
		panic(fmt.Errorf("when using data-only mode, the session must specify the session file to use"))
	}
	if schemaOnly && skipForeignKeys {
		panic(fmt.Errorf("can't use both schema-only and skip-foreign-keys at once, foreign Key creation can only be skipped when data migration takes place"))
	}

	if targetDb == constants.TargetExperimentalPostgres {
		if !(driverName == constants.PGDUMP || driverName == constants.POSTGRES) {
			panic(fmt.Errorf("can only convert to experimental postgres when source %s or %s. (target-db: %s driver: %s)", constants.PGDUMP, constants.POSTGRES, targetDb, driverName))
		}
	} else if targetDb != constants.TargetSpanner {
		panic(fmt.Errorf("unkown target-db %s", targetDb))
	}
	fmt.Printf("Using driver (source DB): %s target-db: %s\n", driverName, targetDb)

	ioHelper := utils.NewIOStreams(driverName, dumpFilePath)

	var project, instance string
	if !schemaOnly {
		project, err = utils.GetProject()
		if err != nil {
			fmt.Printf("\nCan't get project: %v\n", err)
			panic(fmt.Errorf("can't get project"))
		}
		fmt.Println("Using Google Cloud project:", project)

		instance = instanceOverride
		if instance == "" {
			instance, err = utils.GetInstance(ctx, project, ioHelper.Out)
			if err != nil {
				fmt.Printf("\nCan't get instance: %v\n", err)
				panic(fmt.Errorf("can't get instance"))
			}
		}
		fmt.Println("Using Cloud Spanner instance:", instance)
		utils.PrintPermissionsWarning(driverName, ioHelper.Out)
	}

	now := time.Now()
	dbName := dbNameOverride
	if dbName == "" {
		dbName, err = utils.GetDatabaseName(driverName, now)
		if err != nil {
			fmt.Printf("\nCan't get database name: %v\n", err)
			panic(fmt.Errorf("can't get database name"))
		}
	}
	dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", project, instance, dbName)

	// If filePrefix not explicitly set, use dbName.
	if filePrefix == "" {
		filePrefix = dbName + "."
	}

	// TODO (agasheesh@): Collect all the config state in a single struct and pass the same to CommandLine instead of
	// passing multiple parameters. Config state would be populated by parsing the flags and environment variables.
	err = cmd.CommandLine(ctx, driverName, targetDb, dbURI, dataOnly, schemaOnly, skipForeignKeys, schemaSampleSize, sessionJSON, &ioHelper, filePrefix, now)
	if err != nil {
		panic(err)
	}
}

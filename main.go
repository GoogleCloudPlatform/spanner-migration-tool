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
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"

	"github.com/cloudspannerecosystem/harbourbridge/cmd"
	"github.com/cloudspannerecosystem/harbourbridge/conversion"
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/web"
	"github.com/google/subcommands"
)

var (
	dbNameOverride   string
	instanceOverride string
	filePrefix       = ""
	driverName       = conversion.PGDUMP
	schemaSampleSize = int64(0)
	verbose          bool
	schemaOnly       bool
	dataOnly         bool
	skipForeignKeys  bool
	sessionJSON      string
	webapi           bool
	dumpFilePath     string
	targetDb         = conversion.TARGET_SPANNER
)

func setupGlobalFlags() {
	flag.StringVar(&dbNameOverride, "dbname", "", "dbname: name to use for Spanner DB")
	flag.StringVar(&instanceOverride, "instance", "", "instance: Spanner instance to use")
	flag.StringVar(&filePrefix, "prefix", "", "prefix: file prefix for generated files")
	flag.StringVar(&driverName, "driver", "pg_dump", "driver name: flag for accessing source DB or dump files (accepted values are \"pg_dump\", \"postgres\", \"mysqldump\", and \"mysql\")")
	flag.Int64Var(&schemaSampleSize, "schema-sample-size", int64(100000), "schema-sample-size: the number of rows to use for inferring schema (only for DynamoDB)")
	flag.BoolVar(&verbose, "v", false, "verbose: print additional output")
	flag.BoolVar(&schemaOnly, "schema-only", false, "schema-only: in this mode we do schema conversion, but skip data conversion")
	flag.BoolVar(&dataOnly, "data-only", false, "data-only: in this mode we skip schema conversion and just do data conversion (use the session flag to specify the session file for schema and data mapping)")
	flag.BoolVar(&skipForeignKeys, "skip-foreign-keys", false, "skip-foreign-keys: if true, skip creating foreign keys after data migration is complete (ddl statements for foreign keys can still be found in the downloaded schema.ddl.txt file and the same can be applied separately)")
	flag.StringVar(&sessionJSON, "session", "", "session: specifies the file we restore session state from (used in schema-only to provide schema and data mapping)")
	flag.BoolVar(&webapi, "web", false, "web: run the web interface (experimental)")
	flag.StringVar(&dumpFilePath, "dump-file", "", "dump-file: location of dump file to process")
	flag.StringVar(&targetDb, "target-db", conversion.TARGET_SPANNER, "target-db: Specifies the target DB. Defaults to spanner")
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
	lf, err := conversion.SetupLogFile()
	if err != nil {
		fmt.Printf("\nCan't set up log file: %v\n", err)
		panic(fmt.Errorf("can't set up log file"))
	}
	defer conversion.Close(lf)

	// TODO: Remove this check and always run HB in subcommands mode once
	// global command line mode is deprecated. We can also enable support for
	// top-level flags in subcommand then.
	if os.Args[1] != "" && !strings.HasPrefix(os.Args[1], "-") {
		// Using HB CLI in subcommand mode.
		subcommands.Register(subcommands.HelpCommand(), "")
		subcommands.Register(subcommands.CommandsCommand(), "")
		subcommands.Register(&cmd.SchemaCmd{}, "")
		flag.Parse()
		os.Exit(int(subcommands.Execute(ctx)))
	}
	// Running HB CLI in global command line mode.
	setupGlobalFlags()
	flag.Usage = usage
	flag.Parse()

	// Note: the web interface does not use any commandline flags.
	if webapi {
		web.WebApp()
		return
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

	if targetDb == conversion.TARGET_EXPERIMENTAL_POSTGRES {
		if !(driverName == conversion.PGDUMP || driverName == conversion.POSTGRES) {
			panic(fmt.Errorf("can only convert to experimental postgres when source %s or %s. (target-db: %s driver: %s)", conversion.PGDUMP, conversion.POSTGRES, targetDb, driverName))
		}
	} else if targetDb != conversion.TARGET_SPANNER {
		panic(fmt.Errorf("unkown target-db %s", targetDb))
	}
	fmt.Printf("Using driver (source DB): %s target-db: %s\n", driverName, targetDb)

	ioHelper := conversion.NewIOStreams(driverName, dumpFilePath)

	var project, instance string
	if !schemaOnly {
		project, err = conversion.GetProject()
		if err != nil {
			fmt.Printf("\nCan't get project: %v\n", err)
			panic(fmt.Errorf("can't get project"))
		}
		fmt.Println("Using Google Cloud project:", project)

		instance = instanceOverride
		if instance == "" {
			instance, err = conversion.GetInstance(project, ioHelper.Out)
			if err != nil {
				fmt.Printf("\nCan't get instance: %v\n", err)
				panic(fmt.Errorf("can't get instance"))
			}
		}
		fmt.Println("Using Cloud Spanner instance:", instance)
		conversion.PrintPermissionsWarning(driverName, ioHelper.Out)
	}

	now := time.Now()
	dbName := dbNameOverride
	if dbName == "" {
		dbName, err = conversion.GetDatabaseName(driverName, now)
		if err != nil {
			fmt.Printf("\nCan't get database name: %v\n", err)
			panic(fmt.Errorf("can't get database name"))
		}
	}

	// If filePrefix not explicitly set, use dbName.
	if filePrefix == "" {
		filePrefix = dbName + "."
	}

	// TODO (agasheesh@): Collect all the config state in a single struct and pass the same to CommandLine instead of
	// passing multiple parameters. Config state would be populated by parsing the flags and environment variables.
	err = cmd.CommandLine(driverName, targetDb, project, instance, dbName, dataOnly, schemaOnly, skipForeignKeys, schemaSampleSize, sessionJSON, &ioHelper, filePrefix, now)
	if err != nil {
		panic(err)
	}
}

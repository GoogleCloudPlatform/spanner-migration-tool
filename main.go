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

// Package main implements Spanner migration tool, a stand-alone tool for Cloud Spanner
// evaluation, using data from an existing PostgreSQL/MySQL database. See README.md
// for details.
package main

import (
	"context"
	"embed"
	"flag"
	"fmt"
	"os"

	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/sijms/go-ora/v2"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/cmd"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/utils"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/webv2"
	"github.com/google/subcommands"
)

//go:embed ui/dist/ui/*
var distDir embed.FS

func main() {
	ctx := context.Background()
	lf, err := utils.SetupLogFile()
	if err != nil {
		fmt.Printf("\nCan't set up log file: %v\n", err)
		panic(fmt.Errorf("can't set up log file"))
	}
	defer utils.Close(lf)
	// Using SMT CLI in subcommand mode.
	subcommands.Register(subcommands.HelpCommand(), "")
	subcommands.Register(subcommands.CommandsCommand(), "")
	subcommands.Register(&cmd.SchemaCmd{}, "")
	subcommands.Register(&cmd.DataCmd{}, "")
	subcommands.Register(&cmd.SchemaAndDataCmd{}, "")
	subcommands.Register(&cmd.CleanupCmd{}, "")
	subcommands.Register(&webv2.WebCmd{DistDir: distDir}, "")
	flag.Parse()
	os.Exit(int(subcommands.Execute(ctx)))
}

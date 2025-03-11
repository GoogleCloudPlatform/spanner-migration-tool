/* Copyright 2025 Google LLC
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
// limitations under the License.*/

package cmd

import (
	"context"
	"flag"
	"fmt"

	"github.com/google/subcommands"
)

type ImportDataCmd struct {
	instanceId        string
	dbName            string
	tableName         string
	sourceUri         string
	sourceFormat      string
	schemaUri         string
	csvLineDelimiter  string
	csvFieldDelimiter string
	project           string
}

func (cmd *ImportDataCmd) SetFlags(set *flag.FlagSet) {
	set.StringVar(&cmd.instanceId, "instance-id", "", "Spanner instance Id")
	set.StringVar(&cmd.dbName, "db-name", "", "Spanner database name")
	set.StringVar(&cmd.tableName, "table-name", "", "Spanner table name")
	set.StringVar(&cmd.sourceUri, "source-uri", "", "URI of the file to import")
	set.StringVar(&cmd.sourceFormat, "format", "", "Format of the file to import. Valid values {csv}")
	set.StringVar(&cmd.schemaUri, "schema-uri", "", "URI of the file with schema for the csv to import. Only used for csv format.")
	set.StringVar(&cmd.csvLineDelimiter, "csv-line-delimiter", "", "Token to be used as line delimiter for csv format. Defaults to '\\n'. Only used for csv format.")
	set.StringVar(&cmd.csvFieldDelimiter, "csv-field-delimiter", "", "Token to be used as field delimiter for csv format. Defaults to ','. Only used for csv format.")
	set.StringVar(&cmd.project, "project", "", "Project id for all resources related to this import")
}

func (cmd *ImportDataCmd) Execute(ctx context.Context, f *flag.FlagSet, args ...interface{}) subcommands.ExitStatus {
	//TODO implement me
	fmt.Printf("executing dummy import data\n")
	fmt.Printf("instanceId %s, dbName %s, schemaUri %s\n", cmd.instanceId, cmd.dbName, cmd.schemaUri)
	return 0
}

func (cmd *ImportDataCmd) Name() string {
	return "Import Data"
}

// Synopsis returns summary of operation.
func (cmd *ImportDataCmd) Synopsis() string {
	return "Import data from supported source files to spanner"
}

// Usage returns usage info of the command.
func (cmd *ImportDataCmd) Usage() string {
	//TODO implement me
	return fmt.Sprintf("test usage")
}

package cmd

import (
	"context"
	"flag"
	"fmt"

	"github.com/google/subcommands"
)

type ImportDataCmd struct {
	source            string
	sourceProfile     string
	target            string
	targetProfile     string
	assessmentProfile string
	project           string
	logLevel          string
	dryRun            bool
	validate          bool
	sessionJSON       string
}

func (cmd *ImportDataCmd) SetFlags(set *flag.FlagSet) {
	//TODO implement me
	panic("implement me")
}

func (cmd *ImportDataCmd) Execute(ctx context.Context, f *flag.FlagSet, args ...interface{}) subcommands.ExitStatus {
	//TODO implement me
	panic("implement me")
}

func (cmd *ImportDataCmd) Name() string {
	return "import"
}

// Synopsis returns summary of operation.
func (cmd *ImportDataCmd) Synopsis() string {
	return "import data to spanner"
}

// Usage returns usage info of the command.
func (cmd *ImportDataCmd) Usage() string {
	return fmt.Sprintf("test usage")
}

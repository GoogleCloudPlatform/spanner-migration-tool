// Copyright 2023 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package cmd

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/utils"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/profiles"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/streaming"
	"github.com/google/subcommands"
	"go.uber.org/zap"
)

// CleanupCmd is the command for cleaning up the migration resources during a minimal
// downtime migration.
type CleanupCmd struct {
	jobId         string
	dataShardIds  string
	targetProfile string
	datastream    bool
	dataflow      bool
	pubsub        bool
	monitoring    bool
	logLevel      string
	validate      bool
}

// Name returns the name of operation.
func (cmd *CleanupCmd) Name() string {
	return "cleanup"
}

// Synopsis returns summary of operation.
func (cmd *CleanupCmd) Synopsis() string {
	return "cleanup cleans up the generated resources for a provided jobId"
}

// Usage returns usage info of the command.
func (cmd *CleanupCmd) Usage() string {
	return fmt.Sprintf(`%v cleanup --jobId=[jobId] --datastream --dataflow ...

Cleanup GCP resources generated as part of setting up a migration pipeline by providing a 
jobId generated during the job creation.
`, path.Base(os.Args[0]))
}

// SetFlags sets the flags.
func (cmd *CleanupCmd) SetFlags(f *flag.FlagSet) {
	f.StringVar(&cmd.jobId, "jobId", "", "Flag for specifying the migration jobId")
	f.StringVar(&cmd.targetProfile, "target-profile", "", "Flag for specifying project and instance details of Spanner e.g., \"project=XYZ,instance=ABC\"")
	f.StringVar(&cmd.dataShardIds, "dataShardIds", "", "Flag for specifying a comma separated list of dataShardIds to be cleaned up. Defaults to ALL shards. Optional flag, and only valid for a sharded migration.")
	f.BoolVar(&cmd.datastream, "datastream", false, "Flag for specifying if Datastream streams associated with the migration job should be cleaned up or not. Defaults to FALSE.")
	f.BoolVar(&cmd.dataflow, "dataflow", false, "Flag for specifying if Dataflow job associated with the migration job should be cleaned up or not. Defaults to FALSE.")
	f.BoolVar(&cmd.pubsub, "pubsub", false, "Flag for specifying if pubsub associated with the migration job should be cleaned up or not. Defaults to FALSE.")
	f.BoolVar(&cmd.monitoring, "monitoring", false, "Flag for specifying if monitoring dashboards associated with the migration job should be cleaned up or not. Defaults to FALSE.")
	f.StringVar(&cmd.logLevel, "log-level", "DEBUG", "Configure the logging level for the command (INFO, DEBUG), defaults to DEBUG")
	f.BoolVar(&cmd.validate, "validate", false, "Flag for validating if all the required input parameters are present")
}

func (cmd *CleanupCmd) Execute(ctx context.Context, f *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	err := logger.InitializeLogger(cmd.logLevel)
	if err != nil {
		fmt.Println("Error initialising logger, did you specify a valid log-level? [DEBUG, INFO, WARN, ERROR, FATAL]", err)
		return subcommands.ExitFailure
	}
	targetProfile, err := profiles.NewTargetProfile(cmd.targetProfile)
	if err != nil {
		logger.Log.Debug(fmt.Sprintf("Target profile is not properly configured, this is needed for SMT to lookup job details in the metadata database: %v\n", err))
		return subcommands.ExitFailure
	}
	project, instance, err := streaming.GetInstanceDetails(ctx, targetProfile)
	if err != nil {
		logger.Log.Debug(fmt.Sprintf("can't get resource ids: %v\n", err))
		return subcommands.ExitFailure
	}
	dataShardIds, err := profiles.ParseList(cmd.dataShardIds)
	if err != nil {
		logger.Log.Debug(fmt.Sprintf("Could not parse datashardIds: %v\n", err))
		return subcommands.ExitFailure
	}
	if !(cmd.datastream || cmd.dataflow || cmd.pubsub || cmd.monitoring) {
		logger.Log.Error("At least one of datastream, dataflow, pubsub or monitoring must be specified, we recommend cleaning up all resources!\n")
		return subcommands.ExitUsageError
	}
	// all input parameters have been validated
	if cmd.validate {
		logger.Log.Info("All required parameters are present, validated that the command is syntactically correct.\n")
		return subcommands.ExitSuccess
	}
	jobCleanupOptions := streaming.JobCleanupOptions{
		Datastream: cmd.datastream,
		Dataflow:   cmd.dataflow,
		Pubsub:     cmd.pubsub,
		Monitoring: cmd.monitoring,
	}
	getInfo := &utils.GetUtilInfoImpl{}
	migrationProjectId, err := getInfo.GetProject()
	if err != nil {
		logger.Log.Error("Could not get project id from gcloud environment. Inferring migration project id from target profile.", zap.Error(err))
		migrationProjectId = project
	}
	logger.Log.Info(fmt.Sprintf("Initiating job cleanup for jobId: %v \n", cmd.jobId))
	streaming.InitiateJobCleanup(ctx, cmd.jobId, dataShardIds, jobCleanupOptions, migrationProjectId, project, instance)
	return subcommands.ExitSuccess
}

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
	"os"
	"path"
	"path/filepath"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/assessment"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/conversion"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/expressions_api"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/common"
	"github.com/google/subcommands"
	"go.uber.org/zap"
)

// AssessmentCmd struct with flags.
type AssessmentCmd struct {
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

// Name returns the name of operation.
func (cmd *AssessmentCmd) Name() string {
	return "assessment"
}

// Synopsis returns summary of operation.
func (cmd *AssessmentCmd) Synopsis() string {
	return "generate assessment for migration of the current database to Spanner"
}

// Usage returns usage info of the command.
func (cmd *AssessmentCmd) Usage() string {
	return fmt.Sprintf(`%v assessment -source=[source] -source-profile="key1=value1,key2=value2" -assessment-profile="key1=value1" ...

Run an assessment on the existing source db and create a report on the complexity of 
performing a migration to Spanner. The configuration of the assessment collectors is
provided in the assessment-profile
The assessment flags are:
`, path.Base(os.Args[0]))
}

// SetFlags sets the flags.
func (cmd *AssessmentCmd) SetFlags(f *flag.FlagSet) {
	f.StringVar(&cmd.source, "source", "", "Flag for specifying source DB, (e.g., `PostgreSQL`, `MySQL`, `DynamoDB`)")
	f.StringVar(&cmd.sourceProfile, "source-profile", "", "Flag for specifying connection profile for source database e.g., \"file=<path>,format=dump\"")
	f.StringVar(&cmd.target, "target", "Spanner", "Specifies the target DB, defaults to Spanner (accepted values: `Spanner`)")
	f.StringVar(&cmd.targetProfile, "target-profile", "", "Flag for specifying connection profile for target database e.g., \"dialect=postgresql\"")
	f.StringVar(&cmd.assessmentProfile, "assessment-profile", "", "File for specifying configuration to tbe used during assessment. e.g. \"app-code-location=\"<a/b/c>")
	f.StringVar(&cmd.project, "project", "", "Flag spcifying default project id for all the generated resources for the migration")
	f.StringVar(&cmd.logLevel, "log-level", "DEBUG", "Configure the logging level for the command (INFO, DEBUG), defaults to DEBUG")
	f.BoolVar(&cmd.dryRun, "dry-run", false, "Flag for generating DDL and schema conversion report without creating a spanner database")
	f.BoolVar(&cmd.validate, "validate", false, "Flag for validating if all the required input parameters are present")
	f.StringVar(&cmd.sessionJSON, "session", "", "Optional. Specifies the file we restore session state from.")
}

func (cmd *AssessmentCmd) Execute(ctx context.Context, f *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	// Cleanup smt tmp data directory in case residuals remain from prev runs.
	os.RemoveAll(filepath.Join(os.TempDir(), constants.SMT_TMP_DIR))
	var err error
	defer func() {
		if err != nil {
			logger.Log.Fatal("FATAL error", zap.Error(err))
		}
	}()
	err = logger.InitializeLogger(cmd.logLevel)
	if err != nil {
		fmt.Println("Error initialising logger, did you specify a valid log-level? [DEBUG, INFO, WARN, ERROR, FATAL]", err)
		return subcommands.ExitFailure
	}
	defer logger.Log.Sync()
	// Generate source and spanner schema
	// Initialize collectors based on assessment profile
	// Initialize the assessment engine with the collectors and schema
	// Generate assessment report

	if cmd.validate {
		return subcommands.ExitSuccess
	}

	conv, exitStatus := generateConv(cmd)
	if conv == nil {
		return exitStatus
	}

	assessmentOutput, err := assessment.PerformAssessment(conv)
	if err != nil {
		logger.Log.Fatal("could not complete assessment", zap.Error(err))
		return subcommands.ExitFailure
	}
	assessment.GenerateReport(assessmentOutput)

	// Follow up if required - save assessment report
	// Cleanup smt tmp data directory.
	os.RemoveAll(filepath.Join(os.TempDir(), constants.SMT_TMP_DIR))
	return subcommands.ExitSuccess
}

func generateConv(cmd *AssessmentCmd) (*internal.Conv, subcommands.ExitStatus) {
	sourceProfile, targetProfile, ioHelper, _, err := PrepareMigrationPrerequisites(cmd.sourceProfile, cmd.targetProfile, cmd.source)
	if err != nil {
		err = fmt.Errorf("error while preparing prerequisites for migration: %v", err)
		return nil, subcommands.ExitUsageError
	}

	var conv *internal.Conv
	convImpl := &conversion.ConvImpl{}
	if cmd.sessionJSON != "" {
		logger.Log.Info("Loading the conversion context from session file."+
			" The source profile will not be used for the schema conversion.", zap.String("sessionFile", cmd.sessionJSON))
		conv = internal.MakeConv()
		err = conversion.ReadSessionFile(conv, cmd.sessionJSON)
		if err != nil {
			return nil, subcommands.ExitFailure
		}
		expressionVerificationAccessor, _ := expressions_api.NewExpressionVerificationAccessorImpl(context.Background(), targetProfile.Conn.Sp.Project, targetProfile.Conn.Sp.Instance)
		schemaToSpanner := common.SchemaToSpannerImpl{
			ExpressionVerificationAccessor: expressionVerificationAccessor,
		}
		err := schemaToSpanner.VerifyExpressions(conv)

		if err != nil {
			return nil, subcommands.ExitFailure
		}
	} else {
		ctx := context.Background()
		ddlVerifier, err := expressions_api.NewDDLVerifierImpl(ctx, "", "")
		if err != nil {
			logger.Log.Error(fmt.Sprintf("error trying create ddl verifier: %v", err))
			return nil, subcommands.ExitFailure
		}
		sfs := &conversion.SchemaFromSourceImpl{
			DdlVerifier: ddlVerifier,
		}
		conv, err = convImpl.SchemaConv(cmd.project, sourceProfile, targetProfile, &ioHelper, sfs)
		if err != nil {
			return nil, subcommands.ExitFailure
		}
	}
	if conv == nil {
		logger.Log.Error("Could not initialize conversion context")
		return nil, subcommands.ExitFailure
	}

	logger.Log.Info("completed creation on source and spanner schema")
	return conv, 0
}

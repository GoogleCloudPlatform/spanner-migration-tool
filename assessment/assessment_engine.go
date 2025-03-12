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

package assessment

import (
	"context"
	"fmt"

	assessment "github.com/GoogleCloudPlatform/spanner-migration-tool/assessment/collectors"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/assessment/utils"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/profiles"
	"go.uber.org/zap"
)

type assessmentCollectors struct {
	sampleCollector        *assessment.SampleCollector
	infoSchemaCollector    *assessment.InfoSchemaCollector
	appAssessmentCollector *assessment.MigrationSummarizer
}

func PerformAssessment(conv *internal.Conv, sourceProfile profiles.SourceProfile, assessmentConfig map[string]string, projectId string) (utils.AssessmentOutput, error) {

	logger.Log.Info("performing assessment")
	logger.Log.Info(fmt.Sprintf("assessment config %+v", assessmentConfig))
	logger.Log.Info(fmt.Sprintf("project id %+v", projectId))

	ctx := context.Background()

	output := utils.AssessmentOutput{}
	// Initialize collectors
	c, err := initializeCollectors(conv, sourceProfile, assessmentConfig, projectId, ctx)
	if err != nil {
		logger.Log.Error("unable to initialize collectors")
		return output, err
	}

	// perform each type of assessment (in parallel) - cost, schema, app code, query, performance
	// within each type of assessment (in parallel) - invoke each collector to fetch information from relevant rules
	// Iterate over assessment rules and order output by confidence of each element. Merge outputs where required
	// Select the highest confidence output for each attribute
	// Populate assessment struct

	output.SchemaAssessment, err = performSchemaAssessment(ctx, c)

	return output, err
}

// Initilize collectors. Take a decision here on which collectors are mandatory and which are optional
func initializeCollectors(conv *internal.Conv, sourceProfile profiles.SourceProfile, assessmentConfig map[string]string, projectId string, ctx context.Context) (assessmentCollectors, error) {
	c := assessmentCollectors{}
	sampleCollector, err := assessment.CreateSampleCollector()
	if err != nil {
		return c, err
	}
	c.sampleCollector = &sampleCollector
	infoSchemaCollector, err := assessment.CreateInfoSchemaCollector(conv, sourceProfile)
	if infoSchemaCollector.IsEmpty() {
		return c, err
	}
	c.infoSchemaCollector = &infoSchemaCollector

	//Initiialize App Assessment Collector

	codeDirectory, exists := assessmentConfig["codeDirectory"]
	if exists {
		logger.Log.Info("initializing app collector")

		mysqlSchema := ""   // TODO fetch from conv
		spannerSchema := "" // TODO fetch from conv
		mysqlSchemaPath, exists := assessmentConfig["mysqlSchemaPath"]
		if exists {
			logger.Log.Info(fmt.Sprintf("overriding mysql schema from file %s", mysqlSchemaPath))
			mysqlSchema, err := utils.ReadFile(mysqlSchemaPath)
			if err != nil {
				logger.Log.Debug("error reading MySQL schema file:", zap.Error(err))
			}
			logger.Log.Debug(mysqlSchema)
		}

		spannerSchemaPath, exists := assessmentConfig["spannerSchemaPath"]
		if exists {
			logger.Log.Info(fmt.Sprintf("overriding spanner schema from file %s", spannerSchemaPath))
			spannerSchema, err := utils.ReadFile(spannerSchemaPath)
			if err != nil {
				logger.Log.Debug("error reading Spanner schema file:", zap.Error(err))
			}
			logger.Log.Info(spannerSchema)
		}

		summarizer, err := assessment.NewMigrationSummarizer(ctx, nil, projectId, assessmentConfig["location"], mysqlSchema, spannerSchema, codeDirectory)
		if err != nil {
			logger.Log.Error("error initiating migration summarizer")
			return c, err
		}
		c.appAssessmentCollector = summarizer
		logger.Log.Info("initialized app collector")
	} else {
		logger.Log.Info("app code info unavailable")
	}

	return c, err
}

func performSchemaAssessment(ctx context.Context, collectors assessmentCollectors) (utils.SchemaAssessmentOutput, error) {
	schemaOut := utils.SchemaAssessmentOutput{}
	schemaOut.SourceTableDefs, schemaOut.SpannerTableDefs = collectors.infoSchemaCollector.ListTables()
	schemaOut.SourceIndexDef, schemaOut.SpannerIndexDef = collectors.infoSchemaCollector.ListIndexes()
	schemaOut.Triggers = collectors.infoSchemaCollector.ListTriggers()
	schemaOut.SourceColDefs, schemaOut.SpannerColDefs = collectors.infoSchemaCollector.ListColumnDefinitions()
	schemaOut.StoredProcedureAssessmentOutput = collectors.infoSchemaCollector.ListStoredProcedures()

	if collectors.appAssessmentCollector != nil {
		logger.Log.Info("adding app assessment details")
		codeAssessment, err := collectors.appAssessmentCollector.AnalyzeProject(ctx)

		if err != nil {
			logger.Log.Error("error analyzing project", zap.Error(err))
			return schemaOut, err
		}

		logger.Log.Info(fmt.Sprintf("snippets %+v", codeAssessment.Snippets))
		schemaOut.CodeSnippets = &codeAssessment.Snippets
	}
	return schemaOut, nil
}

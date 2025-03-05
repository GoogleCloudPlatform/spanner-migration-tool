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
	assessment "github.com/GoogleCloudPlatform/spanner-migration-tool/assessment/collectors"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/assessment/utils"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/profiles"
)

type assessmentCollectors struct {
	sampleCollector     assessment.SampleCollector
	infoSchemaCollector assessment.InfoSchemaCollector
}

func PerformAssessment(conv *internal.Conv, sourceProfile profiles.SourceProfile) (utils.AssessmentOutput, error) {

	logger.Log.Info("performing assessment")

	output := utils.AssessmentOutput{}
	// Initialize collectors
	c, err := initializeCollectors(conv, sourceProfile)
	if err != nil {
		logger.Log.Error("unable to initialize collectors")
		return output, err
	}

	// perform each type of assessment (in parallel) - cost, schema, app code, query, performance
	// within each type of assessment (in parallel) - invoke each collector to fetch information from relevant rules
	// Iterate over assessment rules and order output by confidence of each element. Merge outputs where required
	// Select the highest confidence output for each attribute
	// Populate assessment struct

	output.SchemaAssessment, err = performSchemaAssessment(c)
	return output, err
}

// Initilize collectors. Take a decision here on which collectors are mandatory and which are optional
func initializeCollectors(conv *internal.Conv, sourceProfile profiles.SourceProfile) (assessmentCollectors, error) {
	c := assessmentCollectors{}
	sampleCollector, err := assessment.CreateSampleCollector()
	if err != nil {
		return c, err
	}
	c.sampleCollector = sampleCollector
	infoSchemaCollector, err := assessment.CreateInfoSchemaCollector(conv, sourceProfile)
	if infoSchemaCollector.IsEmpty() {
		return c, err
	}
	c.infoSchemaCollector = infoSchemaCollector
	return c, err
}

func performSchemaAssessment(collectors assessmentCollectors) (utils.SchemaAssessmentOutput, error) {
	schemaOut := utils.SchemaAssessmentOutput{}
	schemaOut.SourceTableDefs, schemaOut.SpannerTableDefs = collectors.infoSchemaCollector.ListTables()
	schemaOut.SourceIndexDef, schemaOut.SpannerIndexDef = collectors.infoSchemaCollector.ListIndexes()
	schemaOut.Triggers = collectors.infoSchemaCollector.ListTriggers()
	schemaOut.SourceColDefs, schemaOut.SpannerColDefs = collectors.infoSchemaCollector.ListColumnDefinitions()
	schemaOut.StoredProcedureAssessmentOutput = collectors.infoSchemaCollector.ListStoredProcedures()
	return schemaOut, nil
}

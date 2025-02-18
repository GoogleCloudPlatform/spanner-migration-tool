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
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
)

type AssessmentOutput struct {
	costAssessment        CostAssessmentOutput
	schemaAssessment      SchemaAssessmentOutput
	appCodeAssessment     AppCodeAssessmentOutput
	queryAssessment       QueryAssessmentOutput
	performanceAssessment PerformanceAssessmentOutput
}

type CostAssessmentOutput struct {
	//TBD
}

type SchemaAssessmentOutput struct {
	//TBD
	tableNames []string
}

type AppCodeAssessmentOutput struct {
	//TBD
}

type QueryAssessmentOutput struct {
	//TBD
}

type PerformanceAssessmentOutput struct {
	//TBD
}

type assessmentCollectors struct {
	sampleCollector assessment.SampleCollector
}

func PerformAssessment(conv *internal.Conv) (AssessmentOutput, error) {

	logger.Log.Info("performing assessment")

	output := AssessmentOutput{}
	// Initialize collectors
	c, err := initializeCollectors()
	if err != nil {
		logger.Log.Fatal("unable to initiliaze collectors")
		return output, err
	}

	// perform each type of assessment (in parallel) - cost, schema, app code, query, performance
	// within each type of assessment (in parallel) - invoke each collector to fetch information from relevant rules
	// Iterate over assessment rules and order output by confidence of each element. Merge outputs where required
	// Select the highest confidence output for each attribute
	// Populate assessment struct

	output.schemaAssessment, err = performSchemaAssessment(c)
	return output, nil
}

// Initilize collectors. Take a decision here on which collectors are mandatory and which are optional
func initializeCollectors() (assessmentCollectors, error) {
	c := assessmentCollectors{}
	sampleCollector, err := assessment.CreateSampleCollector()
	if err != nil {
		return c, err
	}
	c.sampleCollector = sampleCollector
	return c, nil
}

func performSchemaAssessment(collectors assessmentCollectors) (SchemaAssessmentOutput, error) {
	schemaOut := SchemaAssessmentOutput{}
	tables := collectors.sampleCollector.ListTables()
	schemaOut.tableNames = tables
	return schemaOut, nil
}

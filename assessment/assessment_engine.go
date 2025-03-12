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
	"strings"

	assessment "github.com/GoogleCloudPlatform/spanner-migration-tool/assessment/collectors"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/assessment/sources/mysql"
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

	srcTableDefs, spTableDefs := collectors.infoSchemaCollector.ListTables()
	srcColDefs, spColDefs := collectors.infoSchemaCollector.ListColumnDefinitions() // TODO - move this inside the table info.
	srcIndexes, spIndexes := collectors.infoSchemaCollector.ListIndexes()

	tableAssessments := []utils.TableAssessment{}
	for tableId, srcTableDef := range srcTableDefs {
		spTableDef := spTableDefs[tableId]
		tableSizeDiff := tableSizeDiffBytes(&srcTableDef, &spTableDef)

		columnAssessments := []utils.ColumnAssessment{}

		//Populate column info
		for id, srcColumn := range srcColDefs {
			spColumn := spColDefs[id]
			if srcColumn.TableId != tableId {
				//Column not of current table
				continue
			}
			isTypeCompatible := mysql.SourceSpecificComparisonImpl{}.IsDataTypeCodeCompatible(srcColumn, spColumn) // Make generic when more sources added
			sizeIncreaseInBytes := getSpColSizeBytes(spColumn) - srcColumn.MaxColumnSize
			colAssessment := utils.ColumnAssessment{SourceColDef: &srcColumn, SpannerColDef: &spColumn, CompatibleDataType: isTypeCompatible, SizeIncreaseInBytes: int(sizeIncreaseInBytes)}
			columnAssessments = append(columnAssessments, colAssessment)
		}

		//Populate indexes
		tableSrcIndexes := []utils.SrcIndexDetails{}
		for _, srcIndex := range srcIndexes {
			if srcIndex.TableId == tableId {
				tableSrcIndexes = append(tableSrcIndexes, srcIndex)
			}
		}

		tableSpIndexes := []utils.SpIndexDetails{}
		for _, spIndex := range spIndexes {
			if spIndex.TableId == tableId {
				tableSpIndexes = append(tableSpIndexes, spIndex)
			}
		}

		tableAssessment := utils.TableAssessment{
			SourceTableDef:      &srcTableDef,
			SpannerTableDef:     &spTableDef,
			Columns:             columnAssessments,
			SourceIndexDef:      tableSrcIndexes,
			SpannerIndexDef:     tableSpIndexes,
			CompatibleCharset:   isCharsetCompatible(srcTableDef.Charset),
			SizeIncreaseInBytes: tableSizeDiff,
		}

		tableAssessments = append(tableAssessments, tableAssessment)
	}
	schemaOut.TableAssessment = tableAssessments

	schemaOut.Triggers = collectors.infoSchemaCollector.ListTriggers()
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

func isCharsetCompatible(srcCharset string) bool {
	if !strings.Contains(srcCharset, "utf8") { // TODO add charset level comparisons - per source
		return true
	}
	return false
}

func tableSizeDiffBytes(srcTableDef *utils.TableDetails, spTableDef *utils.TableDetails) int {
	// TODO - if no spanner table exists - return nil
	return 1 //TODO - currently dummy implementation assuming spanner will always be bigger - to calculate based on charset and column size differences
}

// TODO - move to spanner interface?
func getSpColSizeBytes(spCol utils.SpColumnDetails) int64 {
	var size int64
	switch strings.ToUpper(spCol.Datatype) {
	case "ARRAY":
		size = spCol.Len //TODO correct this based on underlying type
	case "BOOL":
		size = 1
	case "BYTES":
		size = spCol.Len
	case "DATE":
		size = 4
	case "FLOAT32":
		size = 4
	case "FLOAT64":
		size = 8
	case "INT64":
		size = 8
	case "JSON":
		size = spCol.Len
	case "NUMERIC":
		size = 22 //TODO - calculate based on precision
	case "STRING":
		size = spCol.Len

	case "STRUCT":
		return 8 // TODO - get sum of parts
	case "TIMESTAMP":
		return 12
	default:
		//TODO - add all types
		return 8
	}
	return 8 + size //Overhead per col plus size
}

// TODO - move to source specific interface. Store in a more scalable structure - maybe a static map
func isDataTypeCodeCompatible(srcColumnDef utils.SrcColumnDetails, spColumnDef utils.SpColumnDetails) bool {

	switch strings.ToUpper(spColumnDef.Datatype) {
	case "BOOL":
		switch srcColumnDef.Datatype {
		case "tinyint":
			return true
		case "bit":
			return true
		default:
			return false
		}
	case "BYTES":
		switch srcColumnDef.Datatype {
		case "binary":
			return true
		case "varbinary":
			return true
		case "blob":
			return true
		default:
			return false
		}
	case "DATE":
		switch srcColumnDef.Datatype {
		case "date":
			return true
		default:
			return false
		}
	case "FLOAT32":
		switch srcColumnDef.Datatype {
		case "float":
			return true
		case "double":
			return true
		default:
			return false
		}
	case "FLOAT64":
		switch srcColumnDef.Datatype {
		case "float":
			return true
		case "double":
			return true
		default:
			return false
		}
	case "INT64":
		switch srcColumnDef.Datatype {
		case "int":
			return true
		case "bigint":
			return true
		default:
			return false
		}
	case "JSON":
		switch srcColumnDef.Datatype {
		case "json":
			return true
		case "varchar":
			return true
		default:
			return false
		}
	case "NUMERIC":
		switch srcColumnDef.Datatype {
		case "float":
			return true
		case "double":
			return true
		default:
			return false
		}
	case "STRING":
		switch srcColumnDef.Datatype {
		case "varchar":
			return true
		case "text":
			return true
		case "mediumtext":
			return true
		case "longtext":
			return true
		default:
			return false
		}
	case "TIMESTAMP":
		switch srcColumnDef.Datatype {
		case "timestamp":
			return true
		case "datetime":
			return true
		default:
			return false
		}
	default:
		//TODO - add all types
		return false
	}

}

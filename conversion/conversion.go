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

// Package conversion handles initial setup for the command line tool
// and web APIs.

// TODO:(searce) Organize code in go style format to make this file more readable.
//
//	public constants first
//	key public type definitions next (although often it makes sense to put them next to public functions that use them)
//	then public functions (and relevant type definitions)
//	and helper functions and other non-public definitions last (generally in order of importance)
package conversion

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"

	datastream "cloud.google.com/go/datastream/apiv1"
	sp "cloud.google.com/go/spanner"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/utils"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal/reports"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/profiles"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/common"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/csv"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/writer"
)

var (
	once             sync.Once
	datastreamClient *datastream.Client
)

type ConnectionProfile struct {
	// Project Id of the resource
	ProjectId string
	// Datashard Id for the resource
	DatashardId string
	// Name of connection profile
	Id string
	// If true, don't create resource, only validate if creation is possible. If false, create resource.
	ValidateOnly bool
	// If true, create source connection profile, else create target connection profile and gcs bucket.
	IsSource bool
	// For source connection profile host of MySql instance
	Host string
	// For source connection profile port of MySql instance
	Port string
	// For source connection profile password of MySql instance
	Password string
	// For source connection profile user name of MySql instance
	User string
	// Region of connection profile to be created
	Region string
	// For target connection profile name of gcs bucket to be created
	BucketName string
}

type ConnectionProfileReq struct {
	ConnectionProfile ConnectionProfile
	Error             error
	Ctx               context.Context
}

type CreateMigrationResources interface {
	multiError(errorMessages []error) error
	prepareMinimalDowntimeResources(createResourceData *ConnectionProfileReq, mutex *sync.Mutex) common.TaskResult[*ConnectionProfileReq]
	getConnProfilesRegion(ctx context.Context, projectId string, region string, dsClient *datastream.Client)
}

func GetDatastreamClient(ctx context.Context) *datastream.Client {
	if datastreamClient == nil {
		once.Do(func() {
			datastreamClient, _ = datastream.NewClient(ctx)
		})
		return datastreamClient
	}
	return datastreamClient
}

type ConvInterface interface {
	SchemaConv(sourceProfile profiles.SourceProfile, targetProfile profiles.TargetProfile, ioHelper *utils.IOStreams, schemaFromSource SchemaFromSourceInterface) (*internal.Conv, error)
	DataConv(ctx context.Context, sourceProfile profiles.SourceProfile, targetProfile profiles.TargetProfile, ioHelper *utils.IOStreams, client *sp.Client, conv *internal.Conv, dataOnly bool, writeLimit int64, dataFromSource DataFromSourceInterface) (*writer.BatchWriter, error) 
}
type ConvImpl struct {}

// SchemaConv performs the schema conversion
// The SourceProfile param provides the connection details to use the go SQL library.
func (ci *ConvImpl) SchemaConv(sourceProfile profiles.SourceProfile, targetProfile profiles.TargetProfile, ioHelper *utils.IOStreams, schemaFromSource SchemaFromSourceInterface) (*internal.Conv, error) {
	switch sourceProfile.Driver {
	case constants.POSTGRES, constants.MYSQL, constants.DYNAMODB, constants.SQLSERVER, constants.ORACLE:
		return schemaFromSource.schemaFromDatabase(sourceProfile, targetProfile, &GetInfoImpl{}, &common.ProcessSchemaImpl{})
	case constants.PGDUMP, constants.MYSQLDUMP:
		return schemaFromSource.SchemaFromDump(sourceProfile.Driver, targetProfile.Conn.Sp.Dialect, ioHelper, &ProcessDumpByDialectImpl{})
	default:
		return nil, fmt.Errorf("schema conversion for driver %s not supported", sourceProfile.Driver)
	}
}

// DataConv performs the data conversion
// The SourceProfile param provides the connection details to use the go SQL library.
func (ci *ConvImpl) DataConv(ctx context.Context, sourceProfile profiles.SourceProfile, targetProfile profiles.TargetProfile, ioHelper *utils.IOStreams, client *sp.Client, conv *internal.Conv, dataOnly bool, writeLimit int64, dataFromSource DataFromSourceInterface) (*writer.BatchWriter, error) {
	config := writer.BatchWriterConfig{
		BytesLimit: 100 * 1000 * 1000,
		WriteLimit: writeLimit,
		RetryLimit: 1000,
		Verbose:    internal.Verbose(),
	}
	switch sourceProfile.Driver {
	case constants.POSTGRES, constants.MYSQL, constants.DYNAMODB, constants.SQLSERVER, constants.ORACLE:
		return dataFromSource.dataFromDatabase(ctx, sourceProfile, targetProfile, config, conv, client, &GetInfoImpl{}, &DataFromDatabaseImpl{}, &SnapshotMigrationImpl{})
	case constants.PGDUMP, constants.MYSQLDUMP:
		if conv.SpSchema.CheckInterleaved() {
			return nil, fmt.Errorf("spanner migration tool does not currently support data conversion from dump files\nif the schema contains interleaved tables. Suggest using direct access to source database\ni.e. using drivers postgres and mysql")
		}
		return dataFromSource.dataFromDump(sourceProfile.Driver, config, ioHelper, client, conv, dataOnly, &ProcessDumpByDialectImpl{}, &PopulateDataConvImpl{})
	case constants.CSV:
		return dataFromSource.dataFromCSV(ctx, sourceProfile, targetProfile, config, conv, client, &PopulateDataConvImpl{}, &csv.CsvImpl{})
	default:
		return nil, fmt.Errorf("data conversion for driver %s not supported", sourceProfile.Driver)
	}
}

type ReportInterface interface {
	GenerateReport(driver string, badWrites map[string]int64, BytesRead int64, banner string, conv *internal.Conv, reportFileName string, dbName string, out *os.File)
}

type ReportImpl struct {}

// Report generates a report of schema and data conversion.
func (r *ReportImpl) GenerateReport(driver string, badWrites map[string]int64, BytesRead int64, banner string, conv *internal.Conv, reportFileName string, dbName string, out *os.File) {

	//Write the structured report file
	structuredReportFileName := fmt.Sprintf("%s.%s", reportFileName, "structured_report.json")
	reportGenerator := reports.ReportImpl{}
	structuredReport := reportGenerator.GenerateStructuredReport(driver, dbName, conv, badWrites, true, true)
	fBytes, _ := json.MarshalIndent(structuredReport, "", " ")
	f, err := os.Create(structuredReportFileName)
	if err != nil {
		fmt.Fprintf(out, "Can't write out structured report file %s: %v\n", reportFileName, err)
		fmt.Fprintf(out, "Writing report to stdout\n")
		f = out
	} else {
		defer f.Close()
	}
	f.Write(fBytes)

	//Write the text report file from the structured report
	textReportFileName := fmt.Sprintf("%s.%s", reportFileName, "report.txt")
	f, err = os.Create(textReportFileName)
	if err != nil {
		fmt.Fprintf(out, "Can't write out report file %s: %v\n", reportFileName, err)
		fmt.Fprintf(out, "Writing report to stdout\n")
		f = out
	} else {
		defer f.Close()
	}
	w := bufio.NewWriter(f)
	w.WriteString(banner)
	reportGenerator.GenerateTextReport(structuredReport, w)
	w.Flush()

	var isDump bool
	if strings.Contains(driver, "dump") {
		isDump = true
	}
	if isDump {
		fmt.Fprintf(out, "Processed %d bytes of %s data (%d statements, %d rows of data, %d errors, %d unexpected conditions).\n",
			BytesRead, driver, conv.Statements(), conv.Rows(), conv.StatementErrors(), conv.Unexpecteds())
	} else {
		fmt.Fprintf(out, "Processed source database via %s driver (%d rows of data, %d unexpected conditions).\n",
			driver, conv.Rows(), conv.Unexpecteds())
	}
	// We've already written summary to f (as part of GenerateReport).
	// In the case where f is stdout, don't write a duplicate copy.
	if f != out {
		fmt.Fprint(out, structuredReport.Summary.Text)
		fmt.Fprintf(out, "See file '%s' for details of the schema and data conversions.\n", reportFileName)
	}
}

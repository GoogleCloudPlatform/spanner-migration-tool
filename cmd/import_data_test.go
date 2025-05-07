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
	"flag"
	"testing"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/stretchr/testify/assert"
)

func TestBasicCsvImport(t *testing.T) {
	importDataCmd := ImportDataCmd{}

	fs := flag.NewFlagSet("testSetFlags", flag.ContinueOnError)
	importDataCmd.SetFlags(fs)

	importDataCmd.instanceId = "testInstance"
	importDataCmd.databaseName = "versionone"
	importDataCmd.tableName = "table2"
	importDataCmd.sourceUri = "../test_data/basic_csv.csv"
	importDataCmd.sourceFormat = "csv"
	importDataCmd.schemaUri = "../test_data/basic_csv_schema.json"
	importDataCmd.csvLineDelimiter = "\n"
	importDataCmd.csvFieldDelimiter = ","
	importDataCmd.project = ""
	//importDataCmd.Execute(context.Background(), fs)
}

func TestImportDataCmd_SetFlags(t *testing.T) {
	cmd := &ImportDataCmd{}
	fs := flag.NewFlagSet("import", flag.ContinueOnError)
	cmd.SetFlags(fs)

	assert.NotNil(t, fs.Lookup("instance-id"))
	assert.NotNil(t, fs.Lookup("database-name"))
	assert.NotNil(t, fs.Lookup("table-name"))
	assert.NotNil(t, fs.Lookup("source-uri"))
	assert.NotNil(t, fs.Lookup("source-format"))
	assert.NotNil(t, fs.Lookup("schema-uri"))
	assert.NotNil(t, fs.Lookup("csv-line-delimiter"))
	assert.NotNil(t, fs.Lookup("csv-field-delimiter"))
	assert.NotNil(t, fs.Lookup("project"))
}

func TestValidateInputLocal_MissingInstanceID(t *testing.T) {
	input := &ImportDataCmd{}
	err := validateInputLocal(input)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Please specify instanceId")
}

func TestValidateInputLocal_MissingDatabaseName(t *testing.T) {
	input := &ImportDataCmd{instanceId: "test-instance"}
	err := validateInputLocal(input)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Please specify databaseName")
}

func TestValidateInputLocal_MissingSourceURI(t *testing.T) {
	input := &ImportDataCmd{instanceId: "test-instance", databaseName: "test-db"}
	err := validateInputLocal(input)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Please specify sourceUri")
}

func TestValidateInputLocal_MissingSourceFormat(t *testing.T) {
	input := &ImportDataCmd{instanceId: "test-instance", databaseName: "test-db", sourceUri: "file:///tmp/data.csv"}
	err := validateInputLocal(input)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Please specify sourceFormat")
}

func TestValidateInputLocal_CSVMissingSchemaURI(t *testing.T) {
	input := &ImportDataCmd{instanceId: "test-instance", databaseName: "test-db", sourceUri: "file:///tmp/data.csv", sourceFormat: constants.CSV}
	err := validateInputLocal(input)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Please specify schemaUri")
}

func TestValidateInputLocal_SuccessCSV(t *testing.T) {
	input := &ImportDataCmd{
		instanceId:   "test-instance",
		databaseName: "test-db",
		sourceUri:    "file:///tmp/data.csv",
		sourceFormat: constants.CSV,
		schemaUri:    "file:///tmp/schema.csv",
	}
	err := validateInputLocal(input)
	assert.NoError(t, err)
}

func TestValidateInputLocal_SuccessNonCSV(t *testing.T) {
	input := &ImportDataCmd{
		instanceId:   "test-instance",
		databaseName: "test-db",
		sourceUri:    "gs://bucket/data.avro",
		sourceFormat: "avro",
	}
	err := validateInputLocal(input)
	assert.NoError(t, err)
}

func TestHandleTableNameDefaults_TableNamePresent(t *testing.T) {
	tableName := "explicit_table"
	sourceUri := "gs://bucket/data.csv"
	result := handleTableNameDefaults(tableName, sourceUri)
	assert.Equal(t, "explicit_table", result)
}

func TestHandleTableNameDefaults_TableNameEmptyFileScheme(t *testing.T) {
	tableName := ""
	sourceUri := "file:///path/to/my_data.csv"
	result := handleTableNameDefaults(tableName, sourceUri)
	assert.Equal(t, "my_data.csv", result)
}

func TestHandleTableNameDefaults_TableNameEmptyGCScheme(t *testing.T) {
	tableName := ""
	sourceUri := "gs://my-bucket/data_file.txt"
	result := handleTableNameDefaults(tableName, sourceUri)
	assert.Equal(t, "data_file.txt", result)
}

func TestHandleTableNameDefaults_TableNameEmptyLocalPathNoScheme(t *testing.T) {
	tableName := ""
	sourceUri := "/tmp/another_file.json"
	result := handleTableNameDefaults(tableName, sourceUri)
	assert.Equal(t, "another_file.json", result)
}

func TestHandleTableNameDefaults_TableNameEmptyRelativePath(t *testing.T) {
	tableName := ""
	sourceUri := "relative/path/some_data.avro"
	result := handleTableNameDefaults(tableName, sourceUri)
	assert.Equal(t, "some_data.avro", result)
}

func TestHandleTableNameDefaults_TableNameEmptyURIWithTrailingSlash(t *testing.T) {
	tableName := ""
	sourceUri := "gs://my-bucket/folder/"
	result := handleTableNameDefaults(tableName, sourceUri)
	assert.Equal(t, "folder", result)
}

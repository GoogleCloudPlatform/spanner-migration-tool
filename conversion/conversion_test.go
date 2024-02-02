// Copyright 2024 Google LLC
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
	"context"
	"testing"

	sp "cloud.google.com/go/spanner"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/utils"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/profiles"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/writer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type MockSchemaFromSource struct {
    mock.Mock
}
func (msads *MockSchemaFromSource) schemaFromDatabase(sourceProfile profiles.SourceProfile, targetProfile profiles.TargetProfile, gi GetInfoInterface) (*internal.Conv, error) {
	args := msads.Called(sourceProfile, targetProfile)
	return args.Get(0).(*internal.Conv), args.Error(1)
}
func (msads *MockSchemaFromSource) SchemaFromDump(driver string, spDialect string, ioHelper *utils.IOStreams) (*internal.Conv, error) {
	args := msads.Called(driver, spDialect, ioHelper)
	return args.Get(0).(*internal.Conv), args.Error(1)
}

type MockDataFromSource struct {
    mock.Mock
}
func (msads *MockDataFromSource) dataFromDatabase(ctx context.Context, sourceProfile profiles.SourceProfile, targetProfile profiles.TargetProfile, config writer.BatchWriterConfig, conv *internal.Conv, client *sp.Client, gi GetInfoInterface) (*writer.BatchWriter, error) {
	args := msads.Called(ctx, sourceProfile, targetProfile, config, conv, client)
	return args.Get(0).(*writer.BatchWriter), args.Error(1)
}
func (msads *MockDataFromSource) dataFromDump(driver string, config writer.BatchWriterConfig, ioHelper *utils.IOStreams, client *sp.Client, conv *internal.Conv, dataOnly bool) (*writer.BatchWriter, error) {
	args := msads.Called(driver, config, ioHelper, client, conv, dataOnly)
	return args.Get(0).(*writer.BatchWriter), args.Error(1)
}
func (msads *MockDataFromSource) dataFromCSV(ctx context.Context, sourceProfile profiles.SourceProfile, targetProfile profiles.TargetProfile, config writer.BatchWriterConfig, conv *internal.Conv, client *sp.Client) (*writer.BatchWriter, error) {
	args := msads.Called(ctx, sourceProfile, targetProfile, config, conv, client)
	return args.Get(0).(*writer.BatchWriter), args.Error(1)
}

func TestSchemaConv(t *testing.T) {
	// Avoid getting/setting env variables in the unit tests.
	testCases := []struct {
		name          				string
		sourceProfileDriver		  	string
		output						interface{}
		function  					string
		errorExpected 				bool
	}{
		{
			name: 					"postgres driver",
			sourceProfileDriver: 	"postgres",
			output: 				&internal.Conv{},
			function: 				"schemaFromDatabase",
			errorExpected: 			false,
		},
		{
			name: 					"mysql driver",
			sourceProfileDriver: 	"mysql",
			output: 				&internal.Conv{},
			function: 				"schemaFromDatabase",
			errorExpected: 			false,
		},
		{
			name: 					"dynamodb driver",
			sourceProfileDriver: 	"dynamodb",
			output: 				&internal.Conv{},
			function: 				"schemaFromDatabase",
			errorExpected: 			false,
		},
		{
			name: 					"sqlserver driver",
			sourceProfileDriver: 	"sqlserver",
			output: 				&internal.Conv{},
			function: 				"schemaFromDatabase",
			errorExpected: 			false,
		},
		{
			name: 					"oracle driver",
			sourceProfileDriver: 	"oracle",
			output: 				&internal.Conv{},
			function: 				"schemaFromDatabase",
			errorExpected: 			false,
		},
		{
			name: 					"pg dump driver",
			sourceProfileDriver: 	"pg_dump",
			output: 				&internal.Conv{},
			function: 				"SchemaFromDump",
			errorExpected: 			false,
		},
		{
			name: 					"mysql dump driver",
			sourceProfileDriver: 	"mysqldump",
			output: 				&internal.Conv{},
			function: 				"SchemaFromDump",
			errorExpected: 			false,
		},
		{
			name: 					"invalid driver",
			sourceProfileDriver: 	"invalid",
			output: 				nil,
			function: 				"",
			errorExpected: 			true,
		},
	}

	for _, tc := range testCases {
		m := MockSchemaFromSource{}
		m.On(tc.function, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(tc.output, nil)
		c := ConvImpl{}
		_, err := c.SchemaConv(profiles.SourceProfile{Driver: tc.sourceProfileDriver}, profiles.TargetProfile{}, &utils.IOStreams{}, &m)
		assert.Equal(t, tc.errorExpected, err != nil, tc.name)
		if err == nil {
			m.AssertExpectations(t) 
		}
	}
}

func TestDataConv(t *testing.T) {
	// Avoid getting/setting env variables in the unit tests.
	testCases := []struct {
		name          				string
		sourceProfileDriver		  	string
		output						interface{}
		function  					string
		errorExpected 				bool
	}{
		{
			name: 					"postgres driver",
			sourceProfileDriver: 	"postgres",
			output: 				&writer.BatchWriter{},
			function: 				"dataFromDatabase",
			errorExpected: 			false,
		},
		{
			name: 					"mysql driver",
			sourceProfileDriver: 	"mysql",
			output: 				&writer.BatchWriter{},
			function: 				"dataFromDatabase",
			errorExpected: 			false,
		},
		{
			name: 					"dynamodb driver",
			sourceProfileDriver: 	"dynamodb",
			output: 				&writer.BatchWriter{},
			function: 				"dataFromDatabase",
			errorExpected: 			false,
		},
		{
			name: 					"sqlserver driver",
			sourceProfileDriver: 	"sqlserver",
			output: 				&writer.BatchWriter{},
			function: 				"dataFromDatabase",
			errorExpected: 			false,
		},
		{
			name: 					"oracle driver",
			sourceProfileDriver: 	"oracle",
			output: 				&writer.BatchWriter{},
			function: 				"dataFromDatabase",
			errorExpected: 			false,
		},
		{
			name: 					"pg dump driver",
			sourceProfileDriver: 	"pg_dump",
			output: 				&writer.BatchWriter{},
			function: 				"dataFromDump",
			errorExpected: 			false,
		},
		{
			name: 					"mysql dump driver",
			sourceProfileDriver: 	"mysqldump",
			output: 				&writer.BatchWriter{},
			function: 				"dataFromDump",
			errorExpected: 			false,
		},
		{
			name: 					"crv driver",
			sourceProfileDriver: 	"csv",
			output: 				&writer.BatchWriter{},
			function: 				"dataFromCSV",
			errorExpected: 			false,
		},
		{
			name: 					"invalid driver",
			sourceProfileDriver: 	"invalid",
			output: 				nil,
			function: 				"",
			errorExpected: 			true,
		},
	}

	ctx:= context.Background()
	for _, tc := range testCases {
		m := MockDataFromSource{}
		m.On(tc.function, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(tc.output, nil)
		c := ConvImpl{}
		_, err := c.DataConv(ctx, profiles.SourceProfile{Driver: tc.sourceProfileDriver}, profiles.TargetProfile{}, &utils.IOStreams{}, &sp.Client{}, &internal.Conv{}, true, int64(5), &m)
		assert.Equal(t, tc.errorExpected, err != nil, tc.name)
		if err == nil {
			m.AssertExpectations(t) 
		}
	}
}
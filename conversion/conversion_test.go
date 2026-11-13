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

package conversion

import (
	"context"
	"fmt"
	"testing"

	sp "cloud.google.com/go/spanner"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/utils"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/profiles"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/writer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

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
		m.On(tc.function, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(tc.output, nil)
		c := ConvImpl{}
		_, err := c.SchemaConv("migration-project-id", profiles.SourceProfile{Driver: tc.sourceProfileDriver}, profiles.TargetProfile{}, &utils.IOStreams{}, &m)
		assert.Equal(t, tc.errorExpected, err != nil, tc.name)
		if err == nil {
			m.AssertExpectations(t) 
		}
	}
}

func TestSchemaConv_DatabaseOptionsAndNilConvErrorGuard(t *testing.T) {
	targetWithOverrides := profiles.TargetProfile{
		Conn: profiles.TargetProfileConnection{
			Sp: profiles.TargetProfileConnectionSpanner{
				Dbname:                      "my-target-db",
				DefaultTimezone:             "America/New_York",
				DefaultTransactionIsolation: "repeatable read",
			},
		},
	}

	t.Run("returns error without nil-pointer panic when schemaFromDatabase fails", func(t *testing.T) {
		m := MockSchemaFromSource{}
		m.On("schemaFromDatabase", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return((*internal.Conv)(nil), fmt.Errorf("db connection failed"))
		c := ConvImpl{}
		assert.NotPanics(t, func() {
			conv, err := c.SchemaConv("migration-project-id", profiles.SourceProfile{Driver: "mysql"}, targetWithOverrides, &utils.IOStreams{}, &m)
			assert.Error(t, err)
			assert.Nil(t, conv)
		})
		m.AssertExpectations(t)
	})

	t.Run("returns error without nil-pointer panic when SchemaFromDump fails", func(t *testing.T) {
		m := MockSchemaFromSource{}
		m.On("SchemaFromDump", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return((*internal.Conv)(nil), fmt.Errorf("failed to parse dump"))
		c := ConvImpl{}
		assert.NotPanics(t, func() {
			conv, err := c.SchemaConv("migration-project-id", profiles.SourceProfile{Driver: "mysqldump"}, targetWithOverrides, &utils.IOStreams{}, &m)
			assert.Error(t, err)
			assert.Nil(t, conv)
		})
		m.AssertExpectations(t)
	})

	t.Run("applies targetProfile DatabaseOptions overrides over existing conv values", func(t *testing.T) {
		m := MockSchemaFromSource{}
		dumpConv := &internal.Conv{
			DatabaseOptions: ddl.DatabaseOptions{
				DefaultTransactionIsolation: "serializable",
			},
		}
		m.On("SchemaFromDump", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(dumpConv, nil)
		c := ConvImpl{}
		conv, err := c.SchemaConv("migration-project-id", profiles.SourceProfile{Driver: "mysqldump"}, targetWithOverrides, &utils.IOStreams{}, &m)
		assert.NoError(t, err)
		assert.Equal(t, "my-target-db", conv.DatabaseOptions.DbName)
		assert.Equal(t, "America/New_York", conv.DatabaseOptions.DefaultTimezone)
		assert.Equal(t, "repeatable read", conv.DatabaseOptions.DefaultTransactionIsolation)
		m.AssertExpectations(t)
	})

	t.Run("preserves existing conv DatabaseOptions when targetProfile options are empty", func(t *testing.T) {
		m := MockSchemaFromSource{}
		dumpConv := &internal.Conv{
			DatabaseOptions: ddl.DatabaseOptions{
				DbName:                      "existing-db",
				DefaultTimezone:             "UTC",
				DefaultTransactionIsolation: "serializable",
			},
		}
		m.On("SchemaFromDump", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(dumpConv, nil)
		c := ConvImpl{}
		conv, err := c.SchemaConv("migration-project-id", profiles.SourceProfile{Driver: "mysqldump"}, profiles.TargetProfile{}, &utils.IOStreams{}, &m)
		assert.NoError(t, err)
		assert.Equal(t, "existing-db", conv.DatabaseOptions.DbName)
		assert.Equal(t, "UTC", conv.DatabaseOptions.DefaultTimezone)
		assert.Equal(t, "serializable", conv.DatabaseOptions.DefaultTransactionIsolation)
		m.AssertExpectations(t)
	})
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
		m.On(tc.function, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(tc.output, nil)
		c := ConvImpl{}
		_, err := c.DataConv(ctx, "migration-project-id", profiles.SourceProfile{Driver: tc.sourceProfileDriver}, profiles.TargetProfile{}, &utils.IOStreams{}, &sp.Client{}, &internal.Conv{}, true, int64(5), &m)
		assert.Equal(t, tc.errorExpected, err != nil, tc.name)
		if err == nil {
			m.AssertExpectations(t) 
		}
	}
}

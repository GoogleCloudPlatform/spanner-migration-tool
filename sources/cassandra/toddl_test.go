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

// Package cassandra handles schema migration from Cassandra.
package cassandra

import (
	"testing"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/schema"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type MockCassandraMappingProvider struct {
	mock.Mock
}

func (m *MockCassandraMappingProvider) GetSpannerType(cassandraTypeName string, spType string) (ddl.Type, []internal.SchemaIssue) {
	args := m.Called(cassandraTypeName, spType)
	return args.Get(0).(ddl.Type), args.Get(1).([]internal.SchemaIssue)
}

func (m *MockCassandraMappingProvider) GetOption(cassandraTypeName string, spType ddl.Type) string {
	args := m.Called(cassandraTypeName, spType)
	return args.String(0)
}

func TestToSpannerType(t *testing.T) {

	mockMapper := new(MockCassandraMappingProvider)
	srcTypeName := "text"
	expectedSpannerType := ddl.Type{Name: ddl.String, Len: ddl.MaxLength}
	expectedIssues := []internal.SchemaIssue{internal.NoGoodType}
	mockMapper.On("GetSpannerType", srcTypeName, "").Return(expectedSpannerType, expectedIssues)
	tdi := &ToDdlImpl{
		typeMapper: mockMapper,
	}
	spType, issues := tdi.ToSpannerType(nil, "", schema.Type{Name: srcTypeName}, false)
	mockMapper.AssertCalled(t, "GetSpannerType", srcTypeName, "")
	assert.Equal(t, expectedSpannerType, spType)
	assert.Equal(t, expectedIssues, issues)
}

func TestGetColumnAutoGen(t *testing.T) {
	tdi := &ToDdlImpl{}
	autoGenCol, err := tdi.GetColumnAutoGen(nil, ddl.AutoGenCol{}, "", "")
	assert.Nil(t, err)
	assert.Nil(t, autoGenCol)
}

func TestGetTypeOption(t *testing.T) {
	mockMapper := new(MockCassandraMappingProvider)
	srcTypeName := "tinyint"
	spannerType := ddl.Type{Name: ddl.String, Len: ddl.MaxLength}
	expectedOption := "text"
	mockMapper.On("GetOption", srcTypeName, spannerType).Return(expectedOption)
	tdi := &ToDdlImpl{
		typeMapper: mockMapper,
	}
	option := tdi.GetTypeOption(srcTypeName, spannerType)
	mockMapper.AssertCalled(t, "GetOption", srcTypeName, spannerType)
	assert.Equal(t, expectedOption, option)
}

func TestCassandraTypeMapper(t *testing.T) {
	mapper := NewCassandraTypeMapper()

	testCases := []struct {
		name                string
		cassandraType       string
		userSpannerType     string
		expectedSpannerType ddl.Type
		expectedOption      string
		expectedIssues      []internal.SchemaIssue
	}{
		{
			name:                "Default tinyint",
			cassandraType:       "tinyint",
			expectedSpannerType: ddl.Type{Name: ddl.Int64},
			expectedOption:      "tinyint",
			expectedIssues:      []internal.SchemaIssue{internal.Widened},
		},
		{
			name:                "Override tinyint to STRING",
			cassandraType:       "tinyint",
			userSpannerType:     ddl.String,
			expectedSpannerType: ddl.Type{Name: ddl.String, Len: ddl.MaxLength},
			expectedOption:      "text",
			expectedIssues:      []internal.SchemaIssue{internal.Widened},
		},
		{
			name:                "Default smallint",
			cassandraType:       "smallint",
			expectedSpannerType: ddl.Type{Name: ddl.Int64},
			expectedOption:      "smallint",
			expectedIssues:      []internal.SchemaIssue{internal.Widened},
		},
		{
			name:                "Override smallint to STRING",
			cassandraType:       "smallint",
			userSpannerType:     ddl.String,
			expectedSpannerType: ddl.Type{Name: ddl.String, Len: ddl.MaxLength},
			expectedOption:      "text",
			expectedIssues:      []internal.SchemaIssue{internal.Widened},
		},
		{
			name:                "Default int",
			cassandraType:       "int",
			expectedSpannerType: ddl.Type{Name: ddl.Int64},
			expectedOption:      "int",
			expectedIssues:      []internal.SchemaIssue{internal.Widened},
		},
		{
			name:                "Override int to STRING",
			cassandraType:       "int",
			userSpannerType:     ddl.String,
			expectedSpannerType: ddl.Type{Name: ddl.String, Len: ddl.MaxLength},
			expectedOption:      "text",
			expectedIssues:      []internal.SchemaIssue{internal.Widened},
		},
		{
			name:                "Default bigint",
			cassandraType:       "bigint",
			expectedSpannerType: ddl.Type{Name: ddl.Int64},
			expectedOption:      "bigint",
		},
		{
			name:                "Override bigint to STRING",
			cassandraType:       "bigint",
			userSpannerType:     ddl.String,
			expectedSpannerType: ddl.Type{Name: ddl.String, Len: ddl.MaxLength},
			expectedOption:      "text",
			expectedIssues:      []internal.SchemaIssue{internal.Widened},
		},
		{
			name:                "Default float",
			cassandraType:       "float",
			expectedSpannerType: ddl.Type{Name: ddl.Float32},
			expectedOption:      "float",
		},
		{
			name:                "Override float to FLOAT64",
			cassandraType:       "float",
			userSpannerType:     ddl.Float64,
			expectedSpannerType: ddl.Type{Name: ddl.Float64},
			expectedOption:      "double",
			expectedIssues:      []internal.SchemaIssue{internal.Widened},
		},
		{
			name:                "Override float to STRING",
			cassandraType:       "float",
			userSpannerType:     ddl.String,
			expectedSpannerType: ddl.Type{Name: ddl.String, Len: ddl.MaxLength},
			expectedOption:      "text",
			expectedIssues:      []internal.SchemaIssue{internal.Widened},
		},
		{
			name:                "Default double",
			cassandraType:       "double",
			expectedSpannerType: ddl.Type{Name: ddl.Float64},
			expectedOption:      "double",
		},
		{
			name:                "Override double to STRING",
			cassandraType:       "double",
			userSpannerType:     ddl.String,
			expectedSpannerType: ddl.Type{Name: ddl.String, Len: ddl.MaxLength},
			expectedOption:      "text",
			expectedIssues:      []internal.SchemaIssue{internal.Widened},
		},
		{
			name:                "Default decimal",
			cassandraType:       "decimal",
			expectedSpannerType: ddl.Type{Name: ddl.Numeric},
			expectedOption:      "decimal",
			expectedIssues:      []internal.SchemaIssue{internal.PrecisionLoss},
		},
		{
			name:                "Default varint",
			cassandraType:       "varint",
			expectedSpannerType: ddl.Type{Name: ddl.Numeric},
			expectedOption:      "varint",
			expectedIssues:      []internal.SchemaIssue{internal.PrecisionLoss},
		},
		{
			name:                "Override varint to STRING",
			cassandraType:       "varint",
			userSpannerType:     ddl.String,
			expectedSpannerType: ddl.Type{Name: ddl.String, Len: ddl.MaxLength},
			expectedOption:      "text",
			expectedIssues:      []internal.SchemaIssue{internal.Widened},
		},
		{
			name:                "Override varint to BYTES",
			cassandraType:       "varint",
			userSpannerType:     ddl.Bytes,
			expectedSpannerType: ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength},
			expectedOption:      "blob",
			expectedIssues:      []internal.SchemaIssue{internal.Widened},
		},
		{
			name:                "Default text",
			cassandraType:       "text",
			expectedSpannerType: ddl.Type{Name: ddl.String, Len: ddl.MaxLength},
			expectedOption:      "text",
		},
		{
			name:                "Override text to BYTES",
			cassandraType:       "text",
			userSpannerType:     ddl.Bytes,
			expectedSpannerType: ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength},
			expectedOption:      "blob",
			expectedIssues:      []internal.SchemaIssue{internal.Widened},
		},
		{
			name:                "Default varchar",
			cassandraType:       "varchar",
			expectedSpannerType: ddl.Type{Name: ddl.String, Len: ddl.MaxLength},
			expectedOption:      "varchar",
		},
		{
			name:                "Override varchar to BYTES",
			cassandraType:       "varchar",
			userSpannerType:     ddl.Bytes,
			expectedSpannerType: ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength},
			expectedOption:      "blob",
			expectedIssues:      []internal.SchemaIssue{internal.Widened},
		},
		{
			name:                "Default ascii",
			cassandraType:       "ascii",
			expectedSpannerType: ddl.Type{Name: ddl.String, Len: ddl.MaxLength},
			expectedOption:      "ascii",
		},
		{
			name:                "Override ascii to BYTES",
			cassandraType:       "ascii",
			userSpannerType:     ddl.Bytes,
			expectedSpannerType: ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength},
			expectedOption:      "blob",
			expectedIssues:      []internal.SchemaIssue{internal.Widened},
		},
		{
			name:                "Default uuid",
			cassandraType:       "uuid",
			expectedSpannerType: ddl.Type{Name: ddl.String, Len: ddl.MaxLength},
			expectedOption:      "uuid",
		},
		{
			name:                "Override uuid to BYTES",
			cassandraType:       "uuid",
			userSpannerType:     ddl.Bytes,
			expectedSpannerType: ddl.Type{Name: ddl.Bytes, Len: 16},
			expectedOption:      "uuid",
			expectedIssues:      []internal.SchemaIssue{internal.CassandraUUID},
		},
		{
			name:                "Default timeuuid",
			cassandraType:       "timeuuid",
			expectedSpannerType: ddl.Type{Name: ddl.String, Len: ddl.MaxLength},
			expectedOption:      "timeuuid",
		},
		{
			name:                "Override timeuuid to BYTES",
			cassandraType:       "timeuuid",
			userSpannerType:     ddl.Bytes,
			expectedSpannerType: ddl.Type{Name: ddl.Bytes, Len: 16},
			expectedOption:      "timeuuid",
			expectedIssues:      []internal.SchemaIssue{internal.CassandraTIMEUUID},
		},
		{
			name:                "Default inet",
			cassandraType:       "inet",
			expectedSpannerType: ddl.Type{Name: ddl.String, Len: ddl.MaxLength},
			expectedOption:      "inet",
		},
		{
			name:                "Default blob",
			cassandraType:       "blob",
			expectedSpannerType: ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength},
			expectedOption:      "blob",
		},
		{
			name:                "Override blob to STRING",
			cassandraType:       "blob",
			userSpannerType:     ddl.String,
			expectedSpannerType: ddl.Type{Name: ddl.String, Len: ddl.MaxLength},
			expectedOption:      "text",
			expectedIssues:      []internal.SchemaIssue{internal.Widened},
		},
		{
			name:                "Default date",
			cassandraType:       "date",
			expectedSpannerType: ddl.Type{Name: ddl.Date},
			expectedOption:      "date",
		},
		{
			name:                "Override date to STRING",
			cassandraType:       "date",
			userSpannerType:     ddl.String,
			expectedSpannerType: ddl.Type{Name: ddl.String, Len: ddl.MaxLength},
			expectedOption:      "text",
			expectedIssues:      []internal.SchemaIssue{internal.Widened},
		},
		{
			name:                "Default timestamp",
			cassandraType:       "timestamp",
			expectedSpannerType: ddl.Type{Name: ddl.Timestamp},
			expectedOption:      "timestamp",
		},
		{
			name:                "Override timestamp to STRING",
			cassandraType:       "timestamp",
			userSpannerType:     ddl.String,
			expectedSpannerType: ddl.Type{Name: ddl.String, Len: ddl.MaxLength},
			expectedOption:      "text",
			expectedIssues:      []internal.SchemaIssue{internal.Widened},
		},
		{
			name:                "Default time",
			cassandraType:       "time",
			expectedSpannerType: ddl.Type{Name: ddl.Int64},
			expectedOption:      "time",
			expectedIssues:      []internal.SchemaIssue{internal.Time},
		},
		{
			name:                "Override time to STRING",
			cassandraType:       "time",
			userSpannerType:     ddl.String,
			expectedSpannerType: ddl.Type{Name: ddl.String, Len: ddl.MaxLength},
			expectedOption:      "text",
			expectedIssues:      []internal.SchemaIssue{internal.Widened},
		},
		{
			name:                "Default duration",
			cassandraType:       "duration",
			expectedSpannerType: ddl.Type{Name: ddl.String, Len: ddl.MaxLength},
			expectedOption:      "text",
			expectedIssues:      []internal.SchemaIssue{internal.NoGoodType},
		},
		{
			name:                "Default boolean",
			cassandraType:       "boolean",
			expectedSpannerType: ddl.Type{Name: ddl.Bool},
			expectedOption:      "boolean",
		},
		{
			name:                "Override boolean to INT64",
			cassandraType:       "boolean",
			userSpannerType:     ddl.Int64,
			expectedSpannerType: ddl.Type{Name: ddl.Int64},
			expectedOption:      "int",
			expectedIssues:      []internal.SchemaIssue{internal.Widened},
		},
		{
			name:                "Override boolean to STRING",
			cassandraType:       "boolean",
			userSpannerType:     ddl.String,
			expectedSpannerType: ddl.Type{Name: ddl.String, Len: ddl.MaxLength},
			expectedOption:      "text",
			expectedIssues:      []internal.SchemaIssue{internal.Widened},
		},
		{
			name:                "Default counter",
			cassandraType:       "counter",
			expectedSpannerType: ddl.Type{Name: ddl.Int64},
			expectedOption:      "counter",
			expectedIssues:      []internal.SchemaIssue{internal.NoGoodType},
		},
		{
			name:                "List Type",
			cassandraType:       "list<text>",
			expectedSpannerType: ddl.Type{Name: ddl.String, Len: ddl.MaxLength, IsArray: true},
			expectedOption:      "list<text>",
		},
		{
			name:                "Set Type",
			cassandraType:       "set<tinyint>",
			expectedSpannerType: ddl.Type{Name: ddl.Int64, IsArray: true},
			expectedOption:      "set<tinyint>",
			expectedIssues:      []internal.SchemaIssue{internal.Widened},
		},
		{
			name:                "Map Type",
			cassandraType:       "map<text,int>",
			expectedSpannerType: ddl.Type{Name: ddl.JSON},
			expectedOption:      "map<text,int>",
			expectedIssues:      []internal.SchemaIssue{internal.CassandraMAP},
		},
		{
			name:                "Unsupported types in map",
			cassandraType:       "map<udt, duration>",
			expectedSpannerType: ddl.Type{Name: ddl.JSON},
			expectedOption:      "map<text,text>",
			expectedIssues:      []internal.SchemaIssue{internal.CassandraMAP, internal.NoGoodType},
		},
		{
			name:                "Unsupported types in map",
			cassandraType:       "map<duration, udt>",
			expectedSpannerType: ddl.Type{Name: ddl.JSON},
			expectedOption:      "map<text,text>",
			expectedIssues:      []internal.SchemaIssue{internal.CassandraMAP, internal.NoGoodType},
		},
		{
			name:                "Fallback Nested List",
			cassandraType:       "list<map<map<int,text>,list<int>>>",
			expectedSpannerType: ddl.Type{Name: ddl.String, Len: ddl.MaxLength},
			expectedOption:      "text",
			expectedIssues:      []internal.SchemaIssue{internal.NoGoodType},
		},
		{
			name:                "Fallback Nested Map",
			cassandraType:       "map<list<text>,map<text,text>>",
			expectedSpannerType: ddl.Type{Name: ddl.String, Len: ddl.MaxLength},
			expectedOption:      "text",
			expectedIssues:      []internal.SchemaIssue{internal.NoGoodType},
		},
		{
			name:                "Fallback unknown type",
			cassandraType:       "some_udt",
			expectedSpannerType: ddl.Type{Name: ddl.String, Len: ddl.MaxLength},
			expectedOption:      "text",
			expectedIssues:      []internal.SchemaIssue{internal.NoGoodType},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Test GetSpannerType
			spType, issues := mapper.GetSpannerType(tc.cassandraType, tc.userSpannerType)
			assert.Equal(t, tc.expectedSpannerType, spType)
			assert.ElementsMatch(t, tc.expectedIssues, issues)

			// Test GetOption
			option := mapper.GetOption(tc.cassandraType, spType)
			assert.Equal(t, tc.expectedOption, option)
		})
	}
}

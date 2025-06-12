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
	"strings"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/schema"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
)

// ToDdlImpl Cassandra specific implementation for the ToDdl.
type ToDdlImpl struct {
	typeMapper CassandraMappingProvider
}

func (tdi ToDdlImpl) ToSpannerType(conv *internal.Conv, spType string, srcType schema.Type, isPk bool) (ddl.Type, []internal.SchemaIssue) {
	return tdi.typeMapper.GetSpannerType(srcType.Name, spType)
}

func (tdi ToDdlImpl) GetColumnAutoGen(conv *internal.Conv, autoGenCol ddl.AutoGenCol, colId string, tableId string) (*ddl.AutoGenCol, error) {
	return &ddl.AutoGenCol{}, nil
}

func (tdi ToDdlImpl) GetTypeOption(srcTypeName string, spType ddl.Type) string {
	return tdi.typeMapper.GetOption(srcTypeName, spType)
}

// CassandraDdlInfo encapsulates info about the ddl type, cassandra_type and issue
type CassandraDdlInfo struct {
	SpannerType ddl.Type
	Option      string
	Issues      []internal.SchemaIssue
}

// Static initialisation of base mapping
var typeMappings = map[string][]CassandraDdlInfo{
	"TINYINT": {
		{
			SpannerType: ddl.Type{Name: ddl.Int64},
			Option:      "tinyint",
			Issues:      []internal.SchemaIssue{internal.Widened},
		},
		{
			SpannerType: ddl.Type{Name: ddl.String, Len: ddl.MaxLength},
			Option:      "text",
			Issues:      []internal.SchemaIssue{internal.Widened},
		},
	},
	"SMALLINT": {
		{
			SpannerType: ddl.Type{Name: ddl.Int64},
			Option:      "smallint",
			Issues:      []internal.SchemaIssue{internal.Widened},
		},
		{
			SpannerType: ddl.Type{Name: ddl.String, Len: ddl.MaxLength},
			Option:      "text",
			Issues:      []internal.SchemaIssue{internal.Widened},
		},
	},
	"INT": {
		{
			SpannerType: ddl.Type{Name: ddl.Int64},
			Option:      "int",
			Issues:      []internal.SchemaIssue{internal.Widened},
		},
		{
			SpannerType: ddl.Type{Name: ddl.String, Len: ddl.MaxLength},
			Option:      "text",
			Issues:      []internal.SchemaIssue{internal.Widened},
		},
	},
	"BIGINT": {
		{
			SpannerType: ddl.Type{Name: ddl.Int64},
			Option:      "bigint",
			Issues:      nil,
		},
		{
			SpannerType: ddl.Type{Name: ddl.String, Len: ddl.MaxLength},
			Option:      "text",
			Issues:      []internal.SchemaIssue{internal.Widened},
		},
	},
	"FLOAT": {
		{
			SpannerType: ddl.Type{Name: ddl.Float32},
			Option:      "float",
			Issues:      nil,
		},
		{
			SpannerType: ddl.Type{Name: ddl.Float64},
			Option:      "double",
			Issues:      []internal.SchemaIssue{internal.Widened},
		},
		{
			SpannerType: ddl.Type{Name: ddl.String, Len: ddl.MaxLength},
			Option:      "text",
			Issues:      []internal.SchemaIssue{internal.Widened},
		},
	},
	"DOUBLE": {
		{
			SpannerType: ddl.Type{Name: ddl.Float64},
			Option:      "double",
			Issues:      nil,
		},
		{
			SpannerType: ddl.Type{Name: ddl.String, Len: ddl.MaxLength},
			Option:      "text",
			Issues:      []internal.SchemaIssue{internal.Widened},
		},
	},
	"DECIMAL": {
		// TODO: Generate appropriate SchemaIssue to warn of potential data loss
		{
			SpannerType: ddl.Type{Name: ddl.Numeric},
			Option:      "decimal",
			Issues:      nil,
		},
	},
	"VARINT": {
		// TODO: Generate appropriate SchemaIssue to warn of potential data loss
		{
			SpannerType: ddl.Type{Name: ddl.Numeric},
			Option:      "decimal",
			Issues:      nil,
		},
		{
			SpannerType: ddl.Type{Name: ddl.String, Len: ddl.MaxLength},
			Option:      "text",
			Issues:      []internal.SchemaIssue{internal.Widened},
		},
		{
			SpannerType: ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength},
			Option:      "blob",
			Issues:      []internal.SchemaIssue{internal.Widened},
		},
	},
	"TEXT": {
		{
			SpannerType: ddl.Type{Name: ddl.String, Len: ddl.MaxLength},
			Option:      "text",
			Issues:      nil,
		},
		{
			SpannerType: ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength},
			Option:      "blob",
			Issues:      []internal.SchemaIssue{internal.Widened},
		},
	},
	"VARCHAR": {
		{
			SpannerType: ddl.Type{Name: ddl.String, Len: ddl.MaxLength},
			Option:      "varchar",
			Issues:      nil,
		},
		{
			SpannerType: ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength},
			Option:      "blob",
			Issues:      []internal.SchemaIssue{internal.Widened},
		},
	},
	"ASCII": {
		{
			SpannerType: ddl.Type{Name: ddl.String, Len: ddl.MaxLength},
			Option:      "ascii",
			Issues:      nil,
		},
		{
			SpannerType: ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength},
			Option:      "blob",
			Issues:      []internal.SchemaIssue{internal.Widened},
		},
	},
	"UUID": {
		{
			SpannerType: ddl.Type{Name: ddl.String, Len: ddl.MaxLength},
			Option:      "uuid",
			Issues:      nil,
		},
		{
			SpannerType: ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength},
			Option:      "uuid",
			Issues:      []internal.SchemaIssue{internal.Widened},
		},
	},
	"TIMEUUID": {
		{
			SpannerType: ddl.Type{Name: ddl.String, Len: ddl.MaxLength},
			Option:      "timeuuid",
			Issues:      nil,
		},
		{
			SpannerType: ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength},
			Option:      "timeuuid",
			Issues:      []internal.SchemaIssue{internal.Widened},
		},
	},
	"INET": {
		{
			SpannerType: ddl.Type{Name: ddl.String, Len: ddl.MaxLength},
			Option:      "inet",
			Issues:      nil,
		},
	},
	"BLOB": {
		{
			SpannerType: ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength},
			Option:      "blob",
			Issues:      nil,
		},
		{
			SpannerType: ddl.Type{Name: ddl.String, Len: ddl.MaxLength},
			Option:      "text",
			Issues:      []internal.SchemaIssue{internal.Widened},
		},
	},
	"DATE": {
		{
			SpannerType: ddl.Type{Name: ddl.Date},
			Option:      "date",
			Issues:      nil,
		},
		{
			SpannerType: ddl.Type{Name: ddl.String, Len: ddl.MaxLength},
			Option:      "text",
			Issues:      []internal.SchemaIssue{internal.Widened},
		},
	},
	"TIMESTAMP": {
		{
			SpannerType: ddl.Type{Name: ddl.Timestamp},
			Option:      "timestamp",
			Issues:      nil,
		},
		{
			SpannerType: ddl.Type{Name: ddl.String, Len: ddl.MaxLength},
			Option:      "text",
			Issues:      []internal.SchemaIssue{internal.Widened},
		},
	},
	"TIME": {
		{
			SpannerType: ddl.Type{Name: ddl.Int64},
			Option:      "time",
			Issues:      []internal.SchemaIssue{internal.Time},
		},
		{
			SpannerType: ddl.Type{Name: ddl.String, Len: ddl.MaxLength},
			Option:      "text",
			Issues:      []internal.SchemaIssue{internal.Widened},
		},
	},
	"DURATION": {
		// TODO: Generate appropriate SchemaIssue to warn about adapter not supporting duration
		{
			SpannerType: ddl.Type{Name: ddl.String, Len: ddl.MaxLength},
			Option:      "duration",
			Issues:      nil,
		},
	},
	"BOOLEAN": {
		{
			SpannerType: ddl.Type{Name: ddl.Bool},
			Option:      "boolean",
			Issues:      nil,
		},
		{
			SpannerType: ddl.Type{Name: ddl.Int64},
			Option:      "int",
			Issues:      []internal.SchemaIssue{internal.Widened},
		},
		{
			SpannerType: ddl.Type{Name: ddl.String, Len: ddl.MaxLength},
			Option:      "text",
			Issues:      []internal.SchemaIssue{internal.Widened},
		},
	},
	"COUNTER": {
		// TODO: Generate appropriate SchemaIssue to warn about adapter not supporting counter
		{
			SpannerType: ddl.Type{Name: ddl.Int64},
			Option:      "counter",
			Issues:      nil,
		},
	},
}

// CassandraMappingProvider defines an interface for type mapping.
type CassandraMappingProvider interface {
	GetSpannerType(cassandraTypeName string, spType string) (ddl.Type, []internal.SchemaIssue)
	GetOption(cassandraTypeName string, spType ddl.Type) string
}

// CassandraTypeMapper implements CassandraMappingProvider.
type CassandraTypeMapper struct{}

func NewCassandraTypeMapper() *CassandraTypeMapper {
	return &CassandraTypeMapper{}
}

// getMapping retrieves a Spanner DDL mapping rule for a given Cassandra type and Spanner Type(if non-default).
func (m *CassandraTypeMapper) getMapping(cassandraTypeName string, spTypeName string) (CassandraDdlInfo, bool) {
	s := strings.ToUpper(strings.ReplaceAll(cassandraTypeName, " ", ""))
	if mappings, ok := typeMappings[s]; ok && len(mappings) > 0 {
		if spTypeName != "" {
			for _, mapping := range mappings {
				if mapping.SpannerType.Name == spTypeName {
					return mapping, true
				}
			}
		}
		return mappings[0], true
	}
	return CassandraDdlInfo{}, false
}

// GetSpannerType finds the correct mapping for the Spanner type and issues
func (m *CassandraTypeMapper) GetSpannerType(cassandraTypeName string, spType string) (ddl.Type, []internal.SchemaIssue) {
	if mapping, ok := m.getMapping(cassandraTypeName, spType); ok {
		return mapping.SpannerType, mapping.Issues
	}
	return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.NoGoodType}
}

// GetOption finds the correct Option string for a given mapping
func (m *CassandraTypeMapper) GetOption(cassandraTypeName string, spType ddl.Type) string {
	if mapping, ok := m.getMapping(cassandraTypeName, spType.Name); ok {
		return mapping.Option
	}
	return cassandraTypeName
}

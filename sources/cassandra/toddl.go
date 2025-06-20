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

// TODO: Make CassandraTypeOption an array of strings
// This might be needed for future work of supporting maps as interleaved tables.
// CassandraDdlInfo encapsulates info about the ddl type, cassandra_type and issue
type CassandraDdlInfo struct {
	SpannerType              ddl.Type
	CassandraTypeOption      string
	Issues                   []internal.SchemaIssue
}

// Static initialisation of base map
// This maps a Cassandra primitive type to a list of options for Spanner DDL type. 
// The first option is the default option. 
// The other options are non-default that a user can select.
var typeMappings = map[string][]CassandraDdlInfo{
	"TINYINT": {
		{
			SpannerType:         ddl.Type{Name: ddl.Int64},
			CassandraTypeOption: "tinyint",
			Issues:              []internal.SchemaIssue{internal.Widened},
		},
		{
			SpannerType:         ddl.Type{Name: ddl.String, Len: ddl.MaxLength},
			CassandraTypeOption: "text",
			Issues:              []internal.SchemaIssue{internal.Widened},
		},
	},
	"SMALLINT": {
		{
			SpannerType:         ddl.Type{Name: ddl.Int64},
			CassandraTypeOption: "smallint",
			Issues:              []internal.SchemaIssue{internal.Widened},
		},
		{
			SpannerType:         ddl.Type{Name: ddl.String, Len: ddl.MaxLength},
			CassandraTypeOption: "text",
			Issues:              []internal.SchemaIssue{internal.Widened},
		},
	},
	"INT": {
		{
			SpannerType:         ddl.Type{Name: ddl.Int64},
			CassandraTypeOption: "int",
			Issues:              []internal.SchemaIssue{internal.Widened},
		},
		{
			SpannerType:         ddl.Type{Name: ddl.String, Len: ddl.MaxLength},
			CassandraTypeOption: "text",
			Issues:              []internal.SchemaIssue{internal.Widened},
		},
	},
	"BIGINT": {
		{
			SpannerType:         ddl.Type{Name: ddl.Int64},
			CassandraTypeOption: "bigint",
			Issues:              nil,
		},
		{
			SpannerType:         ddl.Type{Name: ddl.String, Len: ddl.MaxLength},
			CassandraTypeOption: "text",
			Issues:              []internal.SchemaIssue{internal.Widened},
		},
	},
	"FLOAT": {
		{
			SpannerType:         ddl.Type{Name: ddl.Float32},
			CassandraTypeOption: "float",
			Issues:              nil,
		},
		{
			SpannerType:         ddl.Type{Name: ddl.Float64},
			CassandraTypeOption: "double",
			Issues:              []internal.SchemaIssue{internal.Widened},
		},
		{
			SpannerType:         ddl.Type{Name: ddl.String, Len: ddl.MaxLength},
			CassandraTypeOption: "text",
			Issues:              []internal.SchemaIssue{internal.Widened},
		},
	},
	"DOUBLE": {
		{
			SpannerType:         ddl.Type{Name: ddl.Float64},
			CassandraTypeOption: "double",
			Issues:              nil,
		},
		{
			SpannerType:         ddl.Type{Name: ddl.String, Len: ddl.MaxLength},
			CassandraTypeOption: "text",
			Issues:              []internal.SchemaIssue{internal.Widened},
		},
	},
	"DECIMAL": {
		// TODO: Generate appropriate SchemaIssue to warn of potential data loss
		{
			SpannerType:         ddl.Type{Name: ddl.Numeric},
			CassandraTypeOption: "decimal",
			Issues:              nil,
		},
	},
	"VARINT": {
		// TODO: Generate appropriate SchemaIssue to warn of potential data loss
		{
			SpannerType:         ddl.Type{Name: ddl.Numeric},
			CassandraTypeOption: "varint",
			Issues:              nil,
		},
		{
			SpannerType:         ddl.Type{Name: ddl.String, Len: ddl.MaxLength},
			CassandraTypeOption: "text",
			Issues:              []internal.SchemaIssue{internal.Widened},
		},
		{
			SpannerType:         ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength},
			CassandraTypeOption: "blob",
			Issues:              []internal.SchemaIssue{internal.Widened},
		},
	},
	"TEXT": {
		{
			SpannerType:         ddl.Type{Name: ddl.String, Len: ddl.MaxLength},
			CassandraTypeOption: "text",
			Issues:              nil,
		},
		{
			SpannerType:         ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength},
			CassandraTypeOption: "blob",
			Issues:              []internal.SchemaIssue{internal.Widened},
		},
	},
	"VARCHAR": {
		{
			SpannerType:         ddl.Type{Name: ddl.String, Len: ddl.MaxLength},
			CassandraTypeOption: "varchar",
			Issues:              nil,
		},
		{
			SpannerType:         ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength},
			CassandraTypeOption: "blob",
			Issues:              []internal.SchemaIssue{internal.Widened},
		},
	},
	"ASCII": {
		{
			SpannerType:         ddl.Type{Name: ddl.String, Len: ddl.MaxLength},
			CassandraTypeOption: "ascii",
			Issues:              nil,
		},
		{
			SpannerType:         ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength},
			CassandraTypeOption: "blob",
			Issues:              []internal.SchemaIssue{internal.Widened},
		},
	},
	// TODO: Generate appropriate SchemaIssue to warn 
	// that the field might acceept UUID of unsupported versions as compared to Cassandra.
	"UUID": {
		{
			SpannerType:         ddl.Type{Name: ddl.String, Len: ddl.MaxLength},
			CassandraTypeOption: "uuid",
			Issues:              nil,
		},
		{
			SpannerType:         ddl.Type{Name: ddl.Bytes, Len: 16},
			CassandraTypeOption: "uuid",
			Issues:              []internal.SchemaIssue{internal.Widened},
		},
	},
	// TODO: Generate appropriate SchemaIssue to warn 
	// that the field might acceept TIMEUUID of unsupported versions as compared to Cassandra.
	"TIMEUUID": {
		{
			SpannerType:         ddl.Type{Name: ddl.String, Len: ddl.MaxLength},
			CassandraTypeOption: "timeuuid",
			Issues:              nil,
		},
		{
			SpannerType:         ddl.Type{Name: ddl.Bytes, Len: 16},
			CassandraTypeOption: "timeuuid",
			Issues:              []internal.SchemaIssue{internal.Widened},
		},
	},
	"INET": {
		{
			SpannerType:         ddl.Type{Name: ddl.String, Len: ddl.MaxLength},
			CassandraTypeOption: "inet",
			Issues:              nil,
		},
	},
	"BLOB": {
		{
			SpannerType:         ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength},
			CassandraTypeOption: "blob",
			Issues:              nil,
		},
		{
			SpannerType:         ddl.Type{Name: ddl.String, Len: ddl.MaxLength},
			CassandraTypeOption: "text",
			Issues:              []internal.SchemaIssue{internal.Widened},
		},
	},
	"DATE": {
		{
			SpannerType:         ddl.Type{Name: ddl.Date},
			CassandraTypeOption: "date",
			Issues:              nil,
		},
		{
			SpannerType:         ddl.Type{Name: ddl.String, Len: ddl.MaxLength},
			CassandraTypeOption: "text",
			Issues:              []internal.SchemaIssue{internal.Widened},
		},
	},
	"TIMESTAMP": {
		{
			SpannerType:         ddl.Type{Name: ddl.Timestamp},
			CassandraTypeOption: "timestamp",
			Issues:              nil,
		},
		{
			SpannerType:         ddl.Type{Name: ddl.String, Len: ddl.MaxLength},
			CassandraTypeOption: "text",
			Issues:              []internal.SchemaIssue{internal.Widened},
		},
	},
	"TIME": {
		{
			SpannerType:         ddl.Type{Name: ddl.Int64},
			CassandraTypeOption: "time",
			Issues:              []internal.SchemaIssue{internal.Time},
		},
		{
			SpannerType:         ddl.Type{Name: ddl.String, Len: ddl.MaxLength},
			CassandraTypeOption: "text",
			Issues:              []internal.SchemaIssue{internal.Widened},
		},
	},
	"DURATION": {
		// TODO: Generate appropriate SchemaIssue to warn about adapter not supporting duration
		{
			SpannerType:         ddl.Type{Name: ddl.String, Len: ddl.MaxLength},
			CassandraTypeOption: "text",
			Issues:              []internal.SchemaIssue{internal.NoGoodType},
		},
	},
	"BOOLEAN": {
		{
			SpannerType:         ddl.Type{Name: ddl.Bool},
			CassandraTypeOption: "boolean",
			Issues:              nil,
		},
		{
			SpannerType:         ddl.Type{Name: ddl.Int64},
			CassandraTypeOption: "int",
			Issues:              []internal.SchemaIssue{internal.Widened},
		},
		{
			SpannerType:         ddl.Type{Name: ddl.String, Len: ddl.MaxLength},
			CassandraTypeOption: "text",
			Issues:              []internal.SchemaIssue{internal.Widened},
		},
	},
	"COUNTER": {
		// TODO: Generate appropriate SchemaIssue to warn about adapter not supporting counter
		{
			SpannerType:         ddl.Type{Name: ddl.Int64},
			CassandraTypeOption: "counter",
			Issues:              nil,
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
	// TODO: Generate appropriate SchemaIssue to warn about conversion from map to JSON
    // Handles map collection type
    if strings.HasPrefix(s, "MAP<") {
        startIndex := strings.Index(s, "<")
        midIndex   := strings.Index(s, ",")
        endIndex   := strings.Index(s, ">")

        KeyTypeName := strings.TrimSpace(s[startIndex+1 : midIndex])
        ValueTypeName := strings.TrimSpace(s[midIndex+1 : endIndex])

        var KeyTypeOption string
        var ValueTypeOption string
        var hasIssue bool

        keyMapping, foundKey := m.getMapping(KeyTypeName, spTypeName)
        if !foundKey {
            KeyTypeOption = "text"
            hasIssue = true
        } else {
            KeyTypeOption = keyMapping.CassandraTypeOption
            for _, keyIssue := range keyMapping.Issues {
                if keyIssue == internal.NoGoodType {
                    hasIssue = true
                }
            }
        }

        valueMapping, foundValue := m.getMapping(ValueTypeName, spTypeName)
        if !foundValue {
            ValueTypeOption = "text"
            hasIssue = true
        } else {
            ValueTypeOption = valueMapping.CassandraTypeOption
            for _, valueIssue := range valueMapping.Issues {
                if valueIssue == internal.NoGoodType {
                    hasIssue = true
                }
            }
        }

        newCassandraTypeOption := "map<" + KeyTypeOption + "," + ValueTypeOption + ">"

        issues := []internal.SchemaIssue{}
        if hasIssue {
            issues = append(issues, internal.NoGoodType)
        }
        
        return CassandraDdlInfo{
            SpannerType:         ddl.Type{Name: ddl.JSON},
            CassandraTypeOption: newCassandraTypeOption,
            Issues:              issues,
        }, true
    }
    // Handles list and set collection type
    if strings.HasPrefix(s, "LIST<") || strings.HasPrefix(s, "SET<") {
        startIndex := strings.Index(s, "<")
        endIndex   := strings.Index(s, ">")

        innerCassandraTypeName := strings.TrimSpace(s[startIndex+1 : endIndex])

        var newCassandraTypeOption string

        if mapping, ok := m.getMapping(innerCassandraTypeName, spTypeName); ok {
            mapping.SpannerType.IsArray = true
            if strings.HasPrefix(s, "LIST<") { 
                newCassandraTypeOption = "list<" + mapping.CassandraTypeOption + ">"
            } else {
                newCassandraTypeOption = "set<" + mapping.CassandraTypeOption + ">"
            }
            mapping.CassandraTypeOption = newCassandraTypeOption
            return mapping, true
        }
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

// GetOption finds the correct CassandraTypeOption string for a given mapping
func (m *CassandraTypeMapper) GetOption(cassandraTypeName string, spType ddl.Type) string {
	if mapping, ok := m.getMapping(cassandraTypeName, spType.Name); ok {
		return mapping.CassandraTypeOption
	}
	return "text"
}

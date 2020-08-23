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

package dynamodb

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/schema"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
	"github.com/stretchr/testify/assert"
)

type mockDynamoClient struct {
	listTableCallCount     int
	listTableOutputs       []dynamodb.ListTablesOutput
	describeTableCallCount int
	describeTableOutputs   []dynamodb.DescribeTableOutput
	scanCallCount          int
	scanOutputs            []dynamodb.ScanOutput
}

func (m *mockDynamoClient) ListTables(input *dynamodb.ListTablesInput) (*dynamodb.ListTablesOutput, error) {
	if m.listTableCallCount >= len(m.listTableOutputs) {
		return nil, fmt.Errorf("unexpected call to ListTables: %v", input)
	}
	m.listTableCallCount++
	return &m.listTableOutputs[m.listTableCallCount-1], nil
}

func (m *mockDynamoClient) DescribeTable(input *dynamodb.DescribeTableInput) (*dynamodb.DescribeTableOutput, error) {
	if m.describeTableCallCount >= len(m.describeTableOutputs) {
		return nil, fmt.Errorf("unexpected call to DescribeTable: %v", input)
	}
	m.describeTableCallCount++
	return &m.describeTableOutputs[m.describeTableCallCount-1], nil
}

func (m *mockDynamoClient) Scan(input *dynamodb.ScanInput) (*dynamodb.ScanOutput, error) {
	if m.scanCallCount >= len(m.scanOutputs) {
		return nil, fmt.Errorf("unexpected call to Scan: %v", input)
	}
	m.scanCallCount++
	return &m.scanOutputs[m.scanCallCount-1], nil
}

func TestProcessSchema(t *testing.T) {
	tableNameA := "test_a"
	tableNameB := "test_b"
	attrNameA := "a"
	attrNameB := "b"
	hashKeyType := "HASH"
	sortKeyType := "RANGE"
	str := "str"
	strA := "str-1"
	strB := "str-2"
	strC := "str-3"
	strD := "str-4"
	numStr := "10"

	listTableOutputs := []dynamodb.ListTablesOutput{
		{TableNames: []*string{&tableNameA}, LastEvaluatedTableName: &tableNameA},
		{TableNames: []*string{&tableNameB}},
	}
	describeTableOutputs := []dynamodb.DescribeTableOutput{
		{
			Table: &dynamodb.TableDescription{
				TableName: &tableNameA,
				KeySchema: []*dynamodb.KeySchemaElement{
					{AttributeName: &attrNameA, KeyType: &hashKeyType},
				},
			},
		},
		{
			Table: &dynamodb.TableDescription{
				TableName: &tableNameB,
				KeySchema: []*dynamodb.KeySchemaElement{
					{AttributeName: &attrNameA, KeyType: &hashKeyType},
					{AttributeName: &attrNameB, KeyType: &sortKeyType},
				},
			},
		},
	}
	scanOutputs := []dynamodb.ScanOutput{
		{
			Items: []map[string]*dynamodb.AttributeValue{
				{
					"a": &dynamodb.AttributeValue{S: &strA},
					"b": &dynamodb.AttributeValue{S: &str},
				},
				{
					"a": &dynamodb.AttributeValue{S: &strB},
					"b": &dynamodb.AttributeValue{S: &str},
				},
			},
			LastEvaluatedKey: map[string]*dynamodb.AttributeValue{
				"a": {S: &strB},
			},
		},
		{
			Items: []map[string]*dynamodb.AttributeValue{
				{
					"a": &dynamodb.AttributeValue{S: &strC},
					"b": &dynamodb.AttributeValue{N: &numStr},
				},
				{
					"a": &dynamodb.AttributeValue{S: &strD},
				},
			},
		},
		{
			Items: []map[string]*dynamodb.AttributeValue{
				{
					"a": &dynamodb.AttributeValue{S: &strA},
					"b": &dynamodb.AttributeValue{S: &str},
					"c": &dynamodb.AttributeValue{S: &strC},
				},
				{
					"a": &dynamodb.AttributeValue{S: &strB},
					"b": &dynamodb.AttributeValue{S: &str},
					"d": &dynamodb.AttributeValue{S: &strD},
				},
			},
		},
	}

	client := &mockDynamoClient{
		listTableOutputs:     listTableOutputs,
		describeTableOutputs: describeTableOutputs,
		scanOutputs:          scanOutputs,
	}
	tables := []string{}
	sampleSize := int64(10000)

	conv := internal.MakeConv()
	err := ProcessSchema(conv, client, tables, sampleSize)

	assert.Nil(t, err)
	expectedSchema := map[string]ddl.CreateTable{
		"test_a": {
			Name:     "test_a",
			ColNames: []string{"a", "b"},
			ColDefs: map[string]ddl.ColumnDef{
				"a": {Name: "a", T: ddl.String{Len: ddl.MaxLength{}}, NotNull: true},
				"b": {Name: "b", T: ddl.String{Len: ddl.MaxLength{}}},
			},
			Pks: []ddl.IndexKey{{Col: "a"}},
		},
		"test_b": {
			Name:     "test_b",
			ColNames: []string{"a", "b", "c", "d"},
			ColDefs: map[string]ddl.ColumnDef{
				"a": {Name: "a", T: ddl.String{Len: ddl.MaxLength{}}, NotNull: true},
				"b": {Name: "b", T: ddl.String{Len: ddl.MaxLength{}}, NotNull: true},
				"c": {Name: "c", T: ddl.String{Len: ddl.MaxLength{}}},
				"d": {Name: "d", T: ddl.String{Len: ddl.MaxLength{}}},
			},
			Pks: []ddl.IndexKey{{Col: "a"}, {Col: "b"}},
		},
	}
	assert.Equal(t, expectedSchema, stripSchemaComments(conv.SpSchema))
	assert.Equal(t, int64(0), conv.Unexpecteds())
}

func TestProcessSchema_FullDataTypes(t *testing.T) {
	tableNameA := "test_a"
	attrNameA := "a"
	attrNameB := "b"
	hashKeyType := "HASH"
	sortKeyType := "RANGE"

	str := "str"
	numIntStr := "10"
	numFloatStr := "11.0"
	boolVal := true
	binaryVal := []byte("ABC")
	listVal := []*dynamodb.AttributeValue{
		{S: &str},
		{N: &numIntStr},
	}
	mapVal := map[string]*dynamodb.AttributeValue{
		"list": {L: listVal},
	}

	listTableOutputs := []dynamodb.ListTablesOutput{
		{TableNames: []*string{&tableNameA}},
	}
	describeTableOutputs := []dynamodb.DescribeTableOutput{
		{
			Table: &dynamodb.TableDescription{
				TableName: &tableNameA,
				KeySchema: []*dynamodb.KeySchemaElement{
					{AttributeName: &attrNameA, KeyType: &hashKeyType},
					{AttributeName: &attrNameB, KeyType: &sortKeyType},
				},
			},
		},
	}
	scanOutputs := []dynamodb.ScanOutput{
		{
			Items: []map[string]*dynamodb.AttributeValue{
				{
					"a": &dynamodb.AttributeValue{S: &str},
					"b": &dynamodb.AttributeValue{N: &numIntStr},
					"c": &dynamodb.AttributeValue{N: &numFloatStr},
					"d": &dynamodb.AttributeValue{BOOL: &boolVal},
					"e": &dynamodb.AttributeValue{B: binaryVal},
					"f": &dynamodb.AttributeValue{L: listVal},
					"g": &dynamodb.AttributeValue{M: mapVal},
					"h": &dynamodb.AttributeValue{SS: []*string{&str}},
					"i": &dynamodb.AttributeValue{BS: [][]byte{binaryVal}},
					"j": &dynamodb.AttributeValue{NS: []*string{&numIntStr}},
					"k": &dynamodb.AttributeValue{NS: []*string{&numFloatStr}},
				},
				// The following empty row is needed to make all optional
				// columns nullable.
				{},
			},
		},
	}

	client := &mockDynamoClient{
		listTableOutputs:     listTableOutputs,
		describeTableOutputs: describeTableOutputs,
		scanOutputs:          scanOutputs,
	}
	tables := []string{}
	sampleSize := int64(10000)

	conv := internal.MakeConv()
	err := ProcessSchema(conv, client, tables, sampleSize)

	assert.Nil(t, err)
	expectedSchema := map[string]ddl.CreateTable{
		"test_a": {
			Name:     "test_a",
			ColNames: []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k"},
			ColDefs: map[string]ddl.ColumnDef{
				"a": {Name: "a", T: ddl.String{Len: ddl.MaxLength{}}, NotNull: true},
				"b": {Name: "b", T: ddl.Int64{}, NotNull: true},
				"c": {Name: "c", T: ddl.String{Len: ddl.MaxLength{}}},
				"d": {Name: "d", T: ddl.Bool{}},
				"e": {Name: "e", T: ddl.Bytes{Len: ddl.MaxLength{}}},
				"f": {Name: "f", T: ddl.String{Len: ddl.MaxLength{}}},
				"g": {Name: "g", T: ddl.String{Len: ddl.MaxLength{}}},
				"h": {Name: "h", T: ddl.String{Len: ddl.MaxLength{}}, IsArray: true},
				"i": {Name: "i", T: ddl.Bytes{Len: ddl.MaxLength{}}, IsArray: true},
				"j": {Name: "j", T: ddl.Int64{}, IsArray: true},
				"k": {Name: "k", T: ddl.String{Len: ddl.MaxLength{}}, IsArray: true},
			},
			Pks: []ddl.IndexKey{{Col: "a"}, {Col: "b"}},
		},
	}
	assert.Equal(t, expectedSchema, stripSchemaComments(conv.SpSchema))
	assert.Equal(t, int64(0), conv.Unexpecteds())
}

func TestGenericSchema(t *testing.T) {
	dySchema := dynamoDBSchema{TableName: "test"}
	dySchema.ColumnNames = []string{"a", "b", "c"}
	dySchema.ColumnTypes = map[string]colType{
		"a": {Name: "String", Nullable: false},
		"b": {Name: "NumberInt", Nullable: false},
		"c": {Name: "Bool", Nullable: true},
	}
	dySchema.PrimaryKeys = []string{"b", "a"}
	dySchema.SecIndexes = []index{{Name: "index_c_b", Keys: []string{"c", "b"}}}
	s := dySchema.genericSchema()
	expectedSchema := schema.Table{
		Name:     "test",
		ColNames: []string{"a", "b", "c"},
		ColDefs: map[string]schema.Column{
			"a": {Name: "a", Type: schema.Type{Name: "String"}, NotNull: true},
			"b": {Name: "b", Type: schema.Type{Name: "NumberInt"}, NotNull: true},
			"c": {Name: "c", Type: schema.Type{Name: "Bool"}},
		},
		PrimaryKeys: []schema.Key{{Column: "b"}, {Column: "a"}},
		Indexes: []schema.Index{
			{Name: "index_c_b", Keys: []schema.Key{{Column: "c"}, {Column: "b"}}},
		},
	}
	assert.Equal(t, expectedSchema, s)
}

func TestInferDataTypes(t *testing.T) {
	dySchema := dynamoDBSchema{TableName: "test"}
	stats := map[string]map[string]int64{
		"all_rows_not_null": {
			"NumberInt": 1000,
		},
		"err_row": {
			"NumberFloat": 1,
			"NumberInt":   999,
		},
		"err_null_row": {
			"NumberInt": 999,
		},
		"enough_null_row": {
			"NumberInt": 900,
		},
		"not_conflict_row": {
			"String":    50,
			"NumberInt": 950,
		},
		"conflict_row": {
			"String":    51,
			"NumberInt": 949,
		},
		"equal_conflict_rows": {
			"String":    500,
			"NumberInt": 500,
		},
		"not_conflict_row_after_norm": {
			"String":    40,
			"NumberInt": 760,
		},
		"conflict_row_after_norm": {
			"String":    41,
			"NumberInt": 759,
		},
		"equal_conflict_row_after_norm": {
			"String":    400,
			"NumberInt": 400,
		},
		"empty_records": {
			"String": 0,
		},
		"empty_stats": {},
	}
	dySchema.inferDataTypes(stats, 1000)
	expectColNames := []string{
		"all_rows_not_null", "err_row", "err_null_row", "enough_null_row",
		"not_conflict_row", "conflict_row", "equal_conflict_rows",
		"not_conflict_row_after_norm", "conflict_row_after_norm",
		"equal_conflict_row_after_norm",
	}
	assert.ElementsMatch(t, expectColNames, dySchema.ColumnNames)
	assert.Equal(t, map[string]colType{
		"all_rows_not_null":             {Name: "NumberInt", Nullable: false},
		"err_row":                       {Name: "NumberInt", Nullable: false},
		"err_null_row":                  {Name: "NumberInt", Nullable: false},
		"enough_null_row":               {Name: "NumberInt", Nullable: true},
		"not_conflict_row":              {Name: "NumberInt", Nullable: false},
		"conflict_row":                  {Name: "String", Nullable: false},
		"equal_conflict_rows":           {Name: "String", Nullable: false},
		"not_conflict_row_after_norm":   {Name: "NumberInt", Nullable: true},
		"conflict_row_after_norm":       {Name: "String", Nullable: true},
		"equal_conflict_row_after_norm": {Name: "String", Nullable: true},
	}, dySchema.ColumnTypes)
}

func TestScanSampleData(t *testing.T) {
	strA := "str-1"
	strB := "str-2"
	numStr := "10"
	scanOutputs := []dynamodb.ScanOutput{
		{
			Items: []map[string]*dynamodb.AttributeValue{
				{
					"a": {S: &strA},
				},
				{
					"a": {N: &numStr},
				},
			},
			LastEvaluatedKey: map[string]*dynamodb.AttributeValue{
				"a": {S: &strA},
			},
		},
		{
			Items: []map[string]*dynamodb.AttributeValue{
				{
					"a": {S: &strB},
					"b": {N: &numStr},
				},
				{
					// This will not be scaned due to the sample size.
					"a": {N: &numStr},
					"b": {S: &strB},
				},
			},
		},
	}

	client := &mockDynamoClient{
		scanOutputs: scanOutputs,
	}

	dySchema := dynamoDBSchema{TableName: "test"}
	stats, _, err := dySchema.scanSampleData(client, 3)
	assert.Nil(t, err)

	expectedStats := map[string]map[string]int64{
		"a": {
			"String":    2,
			"NumberInt": 1,
		},
		"b": {
			"NumberInt": 1,
		},
	}
	assert.Equal(t, expectedStats, stats)
}

func TestParseIndexes(t *testing.T) {
	tableName := "test"
	attrNameA := "a"
	attrNameB := "b"
	attrNameC := "c"
	hashKeyType := "HASH"
	sortKeyType := "RANGE"
	indexName := "secondary_index_c"
	describeTableOutputs := []dynamodb.DescribeTableOutput{
		{
			Table: &dynamodb.TableDescription{
				TableName: &tableName,
				KeySchema: []*dynamodb.KeySchemaElement{
					{AttributeName: &attrNameA, KeyType: &hashKeyType},
					{AttributeName: &attrNameB, KeyType: &sortKeyType},
				},
				GlobalSecondaryIndexes: []*dynamodb.GlobalSecondaryIndexDescription{
					{
						IndexName: &indexName,
						KeySchema: []*dynamodb.KeySchemaElement{
							{AttributeName: &attrNameC, KeyType: &hashKeyType},
						},
					},
				},
			},
		},
	}

	client := &mockDynamoClient{
		describeTableOutputs: describeTableOutputs,
	}

	dySchema := dynamoDBSchema{TableName: "test"}

	err := dySchema.parseIndexes(client)
	assert.Nil(t, err)

	pKeys := []string{"a", "b"}
	assert.Equal(t, pKeys, dySchema.PrimaryKeys)
	secIndexes := []index{{Name: "secondary_index_c", Keys: []string{"c"}}}
	assert.Equal(t, secIndexes, dySchema.SecIndexes)
}

func TestListTables(t *testing.T) {
	tableNameA := "table-a"
	tableNameB := "table-b"

	listTableOutputs := []dynamodb.ListTablesOutput{
		{TableNames: []*string{&tableNameA}, LastEvaluatedTableName: &tableNameA},
		{TableNames: []*string{&tableNameB}},
	}

	client := &mockDynamoClient{
		listTableOutputs: listTableOutputs,
	}

	tables, err := listTables(client)
	assert.Nil(t, err)
	assert.Equal(t, []string{"table-a", "table-b"}, tables)
}

func stripSchemaComments(spSchema map[string]ddl.CreateTable) map[string]ddl.CreateTable {
	for t, ct := range spSchema {
		for c, cd := range ct.ColDefs {
			cd.Comment = ""
			ct.ColDefs[c] = cd
		}
		ct.Comment = ""
		spSchema[t] = ct
	}
	return spSchema
}

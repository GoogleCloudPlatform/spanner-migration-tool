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
				"a": {Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
				"b": {Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
			},
			Pks: []ddl.IndexKey{{Col: "a"}},
		},
		"test_b": {
			Name:     "test_b",
			ColNames: []string{"a", "b", "c", "d"},
			ColDefs: map[string]ddl.ColumnDef{
				"a": {Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
				"b": {Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
				"c": {Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
				"d": {Name: "d", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
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
	numStr := "1234.56789"
	invalidNumStr := "199999999999999999999999999999.999999999"
	boolVal := true
	binaryVal := []byte("ABC")
	listVal := []*dynamodb.AttributeValue{
		{S: &str},
		{N: &numStr},
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
					"b": &dynamodb.AttributeValue{N: &numStr},
					"c": &dynamodb.AttributeValue{N: &invalidNumStr},
					"d": &dynamodb.AttributeValue{BOOL: &boolVal},
					"e": &dynamodb.AttributeValue{B: binaryVal},
					"f": &dynamodb.AttributeValue{L: listVal},
					"g": &dynamodb.AttributeValue{M: mapVal},
					"h": &dynamodb.AttributeValue{SS: []*string{&str}},
					"i": &dynamodb.AttributeValue{BS: [][]byte{binaryVal}},
					"j": &dynamodb.AttributeValue{NS: []*string{&numStr}},
					"k": &dynamodb.AttributeValue{NS: []*string{&invalidNumStr}},
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
				"a": {Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
				"b": {Name: "b", T: ddl.Type{Name: ddl.Numeric}, NotNull: true},
				"c": {Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
				"d": {Name: "d", T: ddl.Type{Name: ddl.Bool}},
				"e": {Name: "e", T: ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}},
				"f": {Name: "f", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
				"g": {Name: "g", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
				"h": {Name: "h", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength, IsArray: true}},
				"i": {Name: "i", T: ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength, IsArray: true}},
				"j": {Name: "j", T: ddl.Type{Name: ddl.Numeric, IsArray: true}},
				"k": {Name: "k", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength, IsArray: true}},
			},
			Pks: []ddl.IndexKey{{Col: "a"}, {Col: "b"}},
		},
	}
	assert.Equal(t, expectedSchema, stripSchemaComments(conv.SpSchema))
	assert.Equal(t, int64(0), conv.Unexpecteds())
}

func TestInferDataTypes(t *testing.T) {
	dySchema := schema.Table{Name: "test"}
	stats := map[string]map[string]int64{
		"all_rows_not_null": {
			"Number": 1000,
		},
		"err_row": {
			"NumberString": 1,
			"Number":       999,
		},
		"err_null_row": {
			"Number": 999,
		},
		"enough_null_row": {
			"Number": 900,
		},
		"not_conflict_row": {
			"String": 50,
			"Number": 950,
		},
		"conflict_row": {
			"String": 51,
			"Number": 949,
		},
		"equal_conflict_rows": {
			"String": 500,
			"Number": 500,
		},
		"not_conflict_row_with_noise": {
			"String":       40,
			"Number":       760,
			"NumberString": 10,
		},
		"conflict_row_with_noise": {
			"String":       41,
			"Number":       759,
			"NumberString": 10,
		},
		"equal_conflict_row_with_noise": {
			"String":       400,
			"Number":       400,
			"NumberString": 10,
		},
		"empty_records": {
			"String": 0,
		},
		"empty_stats": {},
	}
	inferDataTypes(stats, 1000, &dySchema)
	expectColNames := []string{
		"all_rows_not_null", "err_row", "err_null_row", "enough_null_row",
		"not_conflict_row", "conflict_row", "equal_conflict_rows",
		"not_conflict_row_with_noise", "conflict_row_with_noise",
		"equal_conflict_row_with_noise",
	}
	assert.ElementsMatch(t, expectColNames, dySchema.ColNames)
	assert.Equal(t, map[string]schema.Column{
		"all_rows_not_null":             {Name: "all_rows_not_null", Type: schema.Type{Name: "Number"}, NotNull: true},
		"err_row":                       {Name: "err_row", Type: schema.Type{Name: "Number"}, NotNull: true},
		"err_null_row":                  {Name: "err_null_row", Type: schema.Type{Name: "Number"}, NotNull: true},
		"enough_null_row":               {Name: "enough_null_row", Type: schema.Type{Name: "Number"}, NotNull: false},
		"not_conflict_row":              {Name: "not_conflict_row", Type: schema.Type{Name: "Number"}, NotNull: true},
		"conflict_row":                  {Name: "conflict_row", Type: schema.Type{Name: "String"}, NotNull: true},
		"equal_conflict_rows":           {Name: "equal_conflict_rows", Type: schema.Type{Name: "String"}, NotNull: true},
		"not_conflict_row_with_noise":   {Name: "not_conflict_row_with_noise", Type: schema.Type{Name: "Number"}, NotNull: false},
		"conflict_row_with_noise":       {Name: "conflict_row_with_noise", Type: schema.Type{Name: "String"}, NotNull: false},
		"equal_conflict_row_with_noise": {Name: "equal_conflict_row_with_noise", Type: schema.Type{Name: "String"}, NotNull: false},
	}, dySchema.ColDefs)
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

	stats, _, err := scanSampleData(client, 3, "test")
	assert.Nil(t, err)

	expectedStats := map[string]map[string]int64{
		"a": {
			typeString: 2,
			typeNumber: 1,
		},
		"b": {
			typeNumber: 1,
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

	dySchema := schema.Table{Name: "test"}

	err := analyzeMetadata(client, &dySchema)
	assert.Nil(t, err)

	pKeys := []schema.Key{{Column: "a"}, {Column: "b"}}
	assert.Equal(t, pKeys, dySchema.PrimaryKeys)
	secIndexes := []schema.Index{{Name: "secondary_index_c", Keys: []schema.Key{{Column: "c"}}}}
	assert.Equal(t, secIndexes, dySchema.Indexes)
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

func TestIncTypeCount_Numeric(t *testing.T) {
	for _, test := range []struct {
		desc     string
		in       string
		wantType string
	}{
		{desc: "a positive case", in: "1234.56789", wantType: typeNumber},
		{desc: "a negative case", in: "-1234.56789", wantType: typeNumber},
		{desc: "min of numeric", in: "-99999999999999999999999999999.999999999", wantType: typeNumber},
		{desc: "max of numeric", in: "99999999999999999999999999999.999999999", wantType: typeNumber},
		{desc: "larger precision", in: "199999999999999999999999999999.999999999", wantType: typeNumberString},
		{desc: "larger scale", in: "99999999999999999999999999999.9999999991", wantType: typeNumberString},
		{desc: "smaller precision", in: "-199999999999999999999999999999.999999999", wantType: typeNumberString},
		{desc: "smaller scale", in: "-99999999999999999999999999999.9999999991", wantType: typeNumberString},
	} {
		s := make(map[string]int64)
		attr := &dynamodb.AttributeValue{N: &test.in}
		incTypeCount("Revenue", attr, s)
		assert.Equal(t, s[test.wantType], int64(1))
	}
}

func TestIncTypeCount_NumericArray(t *testing.T) {
	num1 := "1234.56789"
	num2 := "-1234.56789"
	num3 := "199999999999999999999999999999.999999999"
	num4 := "-199999999999999999999999999999.999999999"
	for _, test := range []struct {
		desc     string
		in       []*string
		wantType string
	}{
		{desc: "all valid", in: []*string{&num1, &num2}, wantType: typeNumberSet},
		{desc: "not valid", in: []*string{&num3, &num4}, wantType: typeNumberStringSet},
		{desc: "one of the elments is invalid", in: []*string{&num1, &num2, &num3}, wantType: typeNumberStringSet},
	} {
		s := make(map[string]int64)
		attr := &dynamodb.AttributeValue{NS: test.in}
		incTypeCount("Revenue", attr, s)
		assert.Equal(t, s[test.wantType], int64(1))
	}
}

func TestSetRowStats(t *testing.T) {
	tableNameA := "test_a"
	tableNameB := "test_b"
	tableItemCountA := int64(10)
	tableItemCountB := int64(20)

	listTableOutputs := []dynamodb.ListTablesOutput{
		{TableNames: []*string{&tableNameA, &tableNameB}},
	}
	describeTableOutputs := []dynamodb.DescribeTableOutput{
		{
			Table: &dynamodb.TableDescription{
				TableName: &tableNameA,
				ItemCount: &tableItemCountA,
			},
		},
		{
			Table: &dynamodb.TableDescription{
				TableName: &tableNameB,
				ItemCount: &tableItemCountB,
			},
		},
	}

	conv := internal.MakeConv()
	client := &mockDynamoClient{
		listTableOutputs:     listTableOutputs,
		describeTableOutputs: describeTableOutputs,
	}

	SetRowStats(conv, client)

	assert.Equal(t, tableItemCountA, conv.Stats.Rows[tableNameA])
	assert.Equal(t, tableItemCountB, conv.Stats.Rows[tableNameB])
}

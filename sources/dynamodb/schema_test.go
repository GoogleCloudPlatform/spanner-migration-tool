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
	"context"
	"fmt"
	"math/big"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/schema"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/common"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
)

func init() {
	logger.Log = zap.NewNop()
}

type mockDynamoClient struct {
	listTableCallCount     int
	listTableOutputs       []dynamodb.ListTablesOutput
	describeTableCallCount int
	describeTableOutputs   []dynamodb.DescribeTableOutput
	scanCallCount          int
	scanOutputs            []dynamodb.ScanOutput
	updateTableCallCount   int
	updateTableOutputs     []dynamodb.UpdateTableOutput
	dynamodbiface.DynamoDBAPI
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

func (m *mockDynamoClient) UpdateTable(input *dynamodb.UpdateTableInput) (*dynamodb.UpdateTableOutput, error) {
	if m.updateTableCallCount >= len(m.updateTableOutputs) {
		return nil, fmt.Errorf("unexpected call to UpdateTable: %v", input)
	}
	m.updateTableCallCount++
	return &m.updateTableOutputs[m.updateTableCallCount-1], nil
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
	sampleSize := int64(10000)

	conv := internal.MakeConv()
	processSchema := common.ProcessSchemaImpl{}
	err := processSchema.ProcessSchema(conv, InfoSchemaImpl{client, nil, sampleSize}, 1, internal.AdditionalSchemaAttributes{}, &common.SchemaToSpannerImpl{}, &common.UtilsOrderImpl{}, &common.InfoSchemaImpl{})

	assert.Nil(t, err)
	expectedSchema := map[string]ddl.CreateTable{
		"test_a": {
			Name:   "test_a",
			ColIds: []string{"a", "b"},
			ColDefs: map[string]ddl.ColumnDef{
				"a": {Name: "a", T: ddl.Type{Name: "STRING", Len: ddl.MaxLength, IsArray: false}, NotNull: true, Comment: "", AutoGen: ddl.AutoGenCol{Name: "None", Type: "None"}},
				"b": {Name: "b", T: ddl.Type{Name: "STRING", Len: ddl.MaxLength, IsArray: false}, NotNull: false, Comment: "", AutoGen: ddl.AutoGenCol{Name: "None", Type: "None"}}},
			PrimaryKeys: []ddl.IndexKey{{ColId: "a", Desc: false, Order: 1}},
		},
		"test_b": {
			Name:   "test_b",
			ColIds: []string{"a", "b", "c", "d"},
			ColDefs: map[string]ddl.ColumnDef{
				"a": {Name: "a", T: ddl.Type{Name: "STRING", Len: ddl.MaxLength, IsArray: false}, NotNull: true, Comment: "", AutoGen: ddl.AutoGenCol{Name: "None", Type: "None"}},
				"b": {Name: "b", T: ddl.Type{Name: "STRING", Len: ddl.MaxLength, IsArray: false}, NotNull: true, Comment: "", AutoGen: ddl.AutoGenCol{Name: "None", Type: "None"}},
				"c": {Name: "c", T: ddl.Type{Name: "STRING", Len: ddl.MaxLength, IsArray: false}, NotNull: false, Comment: "", AutoGen: ddl.AutoGenCol{Name: "None", Type: "None"}},
				"d": {Name: "d", T: ddl.Type{Name: "STRING", Len: ddl.MaxLength, IsArray: false}, NotNull: false, Comment: "", AutoGen: ddl.AutoGenCol{Name: "None", Type: "None"}}},
			PrimaryKeys: []ddl.IndexKey{{ColId: "a", Desc: false, Order: 1}, {ColId: "b", Desc: false, Order: 2}},
		}}
	internal.AssertSpSchema(conv, t, expectedSchema, stripSchemaComments(conv.SpSchema))
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
	sampleSize := int64(10000)

	conv := internal.MakeConv()
	processSchema := common.ProcessSchemaImpl{}
	err := processSchema.ProcessSchema(conv, InfoSchemaImpl{client, nil, sampleSize}, 1, internal.AdditionalSchemaAttributes{}, &common.SchemaToSpannerImpl{}, &common.UtilsOrderImpl{}, &common.InfoSchemaImpl{})

	assert.Nil(t, err)
	expectedSchema := map[string]ddl.CreateTable{
		"test_a": {
			Name:   "test_a",
			ColIds: []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k"},
			ColDefs: map[string]ddl.ColumnDef{
				"a": {Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true, AutoGen: ddl.AutoGenCol{Name: "None", Type: "None"}},
				"b": {Name: "b", T: ddl.Type{Name: ddl.Numeric}, NotNull: true, AutoGen: ddl.AutoGenCol{Name: "None", Type: "None"}},
				"c": {Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, AutoGen: ddl.AutoGenCol{Name: "None", Type: "None"}},
				"d": {Name: "d", T: ddl.Type{Name: ddl.Bool}, AutoGen: ddl.AutoGenCol{Name: "None", Type: "None"}},
				"e": {Name: "e", T: ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}, AutoGen: ddl.AutoGenCol{Name: "None", Type: "None"}},
				"f": {Name: "f", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, AutoGen: ddl.AutoGenCol{Name: "None", Type: "None"}},
				"g": {Name: "g", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, AutoGen: ddl.AutoGenCol{Name: "None", Type: "None"}},
				"h": {Name: "h", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength, IsArray: true}, AutoGen: ddl.AutoGenCol{Name: "None", Type: "None"}},
				"i": {Name: "i", T: ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength, IsArray: true}, AutoGen: ddl.AutoGenCol{Name: "None", Type: "None"}},
				"j": {Name: "j", T: ddl.Type{Name: ddl.Numeric, IsArray: true}, AutoGen: ddl.AutoGenCol{Name: "None", Type: "None"}},
				"k": {Name: "k", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength, IsArray: true}, AutoGen: ddl.AutoGenCol{Name: "None", Type: "None"}},
			},
			PrimaryKeys: []ddl.IndexKey{{ColId: "a", Order: 1}, {ColId: "b", Order: 2}},
		},
	}
	internal.AssertSpSchema(conv, t, expectedSchema, stripSchemaComments(conv.SpSchema))
	assert.Equal(t, int64(0), conv.Unexpecteds())
}

func TestProcessData(t *testing.T) {
	strA := "str-1"
	numStr1 := "10.1"
	numStr2 := "12.34"
	numVal1 := big.NewRat(101, 10)

	boolVal := true
	scanOutputs := []dynamodb.ScanOutput{
		{
			Items: []map[string]*dynamodb.AttributeValue{
				{
					"a": {S: &strA},
					"b": {N: &numStr1},
					"c": {N: &numStr2},
					"d": {BOOL: &boolVal},
				},
			},
		},
	}

	client := &mockDynamoClient{
		scanOutputs: scanOutputs,
	}

	tableName := "testtable"
	cols := []string{"a", "b", "c", "d"}
	conv := buildConv(
		ddl.CreateTable{
			Name:   tableName,
			ColIds: cols,
			ColDefs: map[string]ddl.ColumnDef{
				"a": {Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
				"b": {Name: "b", T: ddl.Type{Name: ddl.Numeric}},
				"c": {Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
				"d": {Name: "d", T: ddl.Type{Name: ddl.Bool}},
			},
			PrimaryKeys: []ddl.IndexKey{{ColId: "a", Order: 1}},
		},
		schema.Table{
			Name:   tableName,
			ColIds: cols,
			ColDefs: map[string]schema.Column{
				"a": {Name: "a", Type: schema.Type{Name: typeString}},
				"b": {Name: "b", Type: schema.Type{Name: typeNumber}},
				"c": {Name: "c", Type: schema.Type{Name: typeNumberString}},
				"d": {Name: "d", Type: schema.Type{Name: typeBool}},
			},
			PrimaryKeys: []schema.Key{{ColId: "a", Order: 1}},
		},
	)
	conv.SetDataMode()
	var rows []spannerData
	conv.SetDataSink(
		func(table string, cols []string, vals []interface{}) {
			rows = append(rows, spannerData{table: table, cols: cols, vals: vals})
		})
	commonInfoSchema := common.InfoSchemaImpl{}
	commonInfoSchema.ProcessData(conv, InfoSchemaImpl{client, nil, 10}, internal.AdditionalDataAttributes{})
	assert.Equal(t,
		[]spannerData{
			{
				table: tableName,
				cols:  cols,
				vals:  []interface{}{"str-1", *numVal1, "12.34", true},
			},
		},
		rows,
	)
}

func TestInferDataTypes(t *testing.T) {
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
	colDefs, _, err := inferDataTypes(stats, 1000, make([]string, 0))
	assert.Nil(t, err)
	expectColNames := []string{
		"all_rows_not_null", "err_row", "err_null_row", "enough_null_row",
		"not_conflict_row", "conflict_row", "equal_conflict_rows",
		"not_conflict_row_with_noise", "conflict_row_with_noise",
		"equal_conflict_row_with_noise",
	}
	cnidMap := getSrcColNameIdMap(colDefs)
	for _, ecn := range expectColNames {
		acn := cnidMap[ecn]
		assert.NotEqual(t, "", acn)
	}

	expColDefs := map[string]schema.Column{
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
	}

	for key, ecd := range expColDefs {
		acd := colDefs[cnidMap[key]]
		acd.Id = ""
		assert.Equal(t, ecd, acd)
	}
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

func getSrcColNameIdMap(cols map[string]schema.Column) map[string]string {
	r := make(map[string]string)
	for _, v := range cols {
		r[v.Name] = v.Id
	}
	return r
}

func TestInfoSchemaImpl_GetIndexes(t *testing.T) {
	tableName := "test"
	attrNameA := "a"
	attrNameB := "b"
	attrNameC := "c"
	attrNameD := "d"
	hashKeyType := "HASH"
	sortKeyType := "RANGE"
	globalIndexName := "secondary_index_c"
	localIndexName := "secondary_index_d"
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
						IndexName: &globalIndexName,
						KeySchema: []*dynamodb.KeySchemaElement{
							{AttributeName: &attrNameC, KeyType: &hashKeyType},
						},
					},
				},
				LocalSecondaryIndexes: []*dynamodb.LocalSecondaryIndexDescription{
					{
						IndexName: &localIndexName,
						KeySchema: []*dynamodb.KeySchemaElement{
							{AttributeName: &attrNameD, KeyType: &hashKeyType},
						},
					},
				},
			},
		},
	}

	client := &mockDynamoClient{
		describeTableOutputs: describeTableOutputs,
	}

	dySchema := common.SchemaAndName{Name: "test"}
	conv := internal.MakeConv()
	isi := InfoSchemaImpl{client, nil, 10}
	colNameToId := map[string]string{attrNameC: "c1", attrNameD: "c2"}
	indexes, err := isi.GetIndexes(conv, dySchema, colNameToId)
	assert.Nil(t, err)

	secIndexes := []schema.Index{
		{Name: "secondary_index_c", Keys: []schema.Key{{ColId: "c1"}}},
		{Name: "secondary_index_d", Keys: []schema.Key{{ColId: "c2"}}},
	}
	for i := range indexes {
		indexes[i].Id = ""
	}

	assert.Equal(t, secIndexes, indexes)
}

func TestInfoSchemaImpl_GetConstraints(t *testing.T) {
	tableName := "test"
	attrNameA := "a"
	attrNameB := "b"
	attrNameC := "c"
	attrNameD := "d"
	hashKeyType := "HASH"
	sortKeyType := "RANGE"
	globalIndexName := "secondary_index_c"
	localIndexName := "secondary_index_d"
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
						IndexName: &globalIndexName,
						KeySchema: []*dynamodb.KeySchemaElement{
							{AttributeName: &attrNameC, KeyType: &hashKeyType},
						},
					},
				},
				LocalSecondaryIndexes: []*dynamodb.LocalSecondaryIndexDescription{
					{
						IndexName: &localIndexName,
						KeySchema: []*dynamodb.KeySchemaElement{
							{AttributeName: &attrNameD, KeyType: &hashKeyType},
						},
					},
				},
			},
		},
	}

	client := &mockDynamoClient{
		describeTableOutputs: describeTableOutputs,
	}

	dySchema := common.SchemaAndName{Name: "test"}
	conv := internal.MakeConv()
	isi := InfoSchemaImpl{client, nil, 10}
	primaryKeys, constraints, err := isi.GetConstraints(conv, dySchema)
	assert.Nil(t, err)

	pKeys := []string{"a", "b"}
	assert.Equal(t, pKeys, primaryKeys)
	assert.Empty(t, constraints)
}

func TestInfoSchemaImpl_GetTables(t *testing.T) {
	tableNameA := "table-a"
	tableNameB := "table-b"

	listTableOutputs := []dynamodb.ListTablesOutput{
		{TableNames: []*string{&tableNameA}, LastEvaluatedTableName: &tableNameA},
		{TableNames: []*string{&tableNameB}},
	}

	client := &mockDynamoClient{
		listTableOutputs: listTableOutputs,
	}
	isi := InfoSchemaImpl{client, nil, 10}
	tables, err := isi.GetTables()
	assert.Nil(t, err)
	assert.Equal(t, []common.SchemaAndName{{"", "table-a"}, {"", "table-b"}}, tables)
}

func TestInfoSchemaImpl_GetTableName(t *testing.T) {
	tableNameA := "table-a"

	client := &mockDynamoClient{}
	isi := InfoSchemaImpl{client, nil, 10}
	table := isi.GetTableName("", tableNameA)
	assert.Equal(t, tableNameA, table)
}

func TestInfoSchemaImpl_GetColumns(t *testing.T) {
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

	conv := internal.MakeConv()
	client := &mockDynamoClient{
		scanOutputs: scanOutputs,
	}
	dySchema := common.SchemaAndName{Name: "test"}

	isi := InfoSchemaImpl{client, nil, 10}

	colDefs, _, err := isi.GetColumns(conv, dySchema, nil, nil)
	assert.Nil(t, err)
	expectColNames := []string{
		"a", "b",
	}
	expColDefs := map[string]schema.Column{
		"a": {Name: "a", Type: schema.Type{Name: "String", Mods: []int64(nil), ArrayBounds: []int64(nil)}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}},
		"b": {Name: "b", Type: schema.Type{Name: "String", Mods: []int64(nil), ArrayBounds: []int64(nil)}, NotNull: false, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}}}

	cnidMap := getSrcColNameIdMap(colDefs)
	for _, ecn := range expectColNames {
		acn := cnidMap[ecn]
		assert.NotEqual(t, "", acn)
	}

	for key, ecd := range expColDefs {
		acd := colDefs[cnidMap[key]]
		acd.Id = ""
		assert.Equal(t, ecd, acd)
	}
}

func TestInfoSchemaImpl_GetForeignKeys(t *testing.T) {
	dySchema := common.SchemaAndName{Name: "test"}
	conv := internal.MakeConv()
	client := &mockDynamoClient{}
	isi := InfoSchemaImpl{client, nil, 10}
	fk, err := isi.GetForeignKeys(conv, dySchema)
	assert.Nil(t, err)
	assert.Nil(t, fk)
}

func TestInfoSchemaImpl_GetRowCount(t *testing.T) {
	tableNameA := "test_a"
	tableItemCountA := int64(10)

	describeTableOutputs := []dynamodb.DescribeTableOutput{
		{
			Table: &dynamodb.TableDescription{
				TableName: &tableNameA,
				ItemCount: &tableItemCountA,
			},
		},
	}

	client := &mockDynamoClient{
		describeTableOutputs: describeTableOutputs,
	}

	isi := InfoSchemaImpl{client, nil, 10}
	dySchema := common.SchemaAndName{Name: tableNameA}

	rowCount, err := isi.GetRowCount(dySchema)
	assert.Nil(t, err)
	assert.Equal(t, tableItemCountA, rowCount)
}

func TestInfoSchemaImpl_GetRowsFromTable(t *testing.T) {
	strA := "str-1"
	numStr1 := "10.1"
	numStr2 := "12.34"

	boolVal := true
	scanOutputs := []dynamodb.ScanOutput{
		{
			Items: []map[string]*dynamodb.AttributeValue{
				{
					"a": {S: &strA},
					"b": {N: &numStr1},
					"c": {N: &numStr2},
					"d": {BOOL: &boolVal},
				},
			},
		},
	}

	conv := internal.MakeConv()
	client := &mockDynamoClient{
		scanOutputs: scanOutputs,
	}
	tableName := "testtable"
	isi := InfoSchemaImpl{client, nil, 10}

	rows, err := isi.GetRowsFromTable(conv, tableName)
	assert.Nil(t, err)
	assert.Equal(t, []map[string]*dynamodb.AttributeValue{{
		"a": {S: &strA},
		"b": {N: &numStr1},
		"c": {N: &numStr2},
		"d": {BOOL: &boolVal}}},
		rows,
	)
}

func TestInfoSchemaImpl_ProcessData(t *testing.T) {
	strA := "str-1"
	numStr1 := "10.1"
	numStr2 := "12.34"
	numVal1 := big.NewRat(101, 10)

	boolVal := true
	scanOutputs := []dynamodb.ScanOutput{
		{
			Items: []map[string]*dynamodb.AttributeValue{
				{
					"a": {S: &strA},
					"b": {N: &numStr1},
					"c": {N: &numStr2},
					"d": {BOOL: &boolVal},
				},
			},
		},
	}

	client := &mockDynamoClient{
		scanOutputs: scanOutputs,
	}
	isi := InfoSchemaImpl{client, nil, 10}

	tableName := "cart"
	tableId := "t1"
	cols := []string{"a", "b", "c", "d"}
	colIds := []string{"c1", "c2", "c3", "c4"}
	spSchema := ddl.CreateTable{
		Name:   tableName,
		Id:     tableId,
		ColIds: colIds,
		ColDefs: map[string]ddl.ColumnDef{
			"c1": {Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
			"c2": {Name: "b", T: ddl.Type{Name: ddl.Numeric}},
			"c3": {Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
			"c4": {Name: "d", T: ddl.Type{Name: ddl.Bool}},
		},
		PrimaryKeys: []ddl.IndexKey{{ColId: "c1"}},
	}
	conv := buildConv(
		spSchema,
		schema.Table{
			Name:   tableName,
			Id:     tableId,
			ColIds: colIds,
			ColDefs: map[string]schema.Column{
				"c1": {Name: "a", Type: schema.Type{Name: typeString}},
				"c2": {Name: "b", Type: schema.Type{Name: typeNumber}},
				"c3": {Name: "c", Type: schema.Type{Name: typeNumberString}},
				"c4": {Name: "d", Type: schema.Type{Name: typeBool}},
			},
			PrimaryKeys: []schema.Key{{ColId: "c1"}},
		},
	)

	var rows []spannerData
	conv.SetDataSink(
		func(table string, cols []string, vals []interface{}) {
			rows = append(rows, spannerData{table: table, cols: cols, vals: vals})
		})
	err := isi.ProcessData(conv, tableId, conv.SrcSchema[tableId],
		colIds, spSchema, internal.AdditionalDataAttributes{})
	assert.Nil(t, err)
	assert.Equal(t,
		[]spannerData{
			{
				table: tableName,
				cols:  cols,
				vals:  []interface{}{"str-1", *numVal1, "12.34", true},
			},
		},
		rows,
	)
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

	commonInfoSchema := common.InfoSchemaImpl{}
	commonInfoSchema.SetRowStats(conv, InfoSchemaImpl{client, nil, 10})

	assert.Equal(t, tableItemCountA, conv.Stats.Rows[tableNameA])
	assert.Equal(t, tableItemCountB, conv.Stats.Rows[tableNameB])
}

func TestInfoSchemaImpl_StartChangeDataCapture(t *testing.T) {
	tableName := "testtable"
	attrNameA := "a"
	latestStreamArn := "arn:aws:dynamodb:dydb_endpoint:test_stream"

	cols := []string{attrNameA}
	spSchema := ddl.CreateTable{
		Name:   tableName,
		ColIds: cols,
		ColDefs: map[string]ddl.ColumnDef{
			attrNameA: {Name: attrNameA, T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
		},
		PrimaryKeys: []ddl.IndexKey{{ColId: attrNameA}},
	}
	srcTable := schema.Table{
		Name:   tableName,
		ColIds: cols,
		ColDefs: map[string]schema.Column{
			attrNameA: {Name: attrNameA, Type: schema.Type{Name: typeString}},
		},
		PrimaryKeys: []schema.Key{{ColId: attrNameA}},
	}
	conv := buildConv(spSchema, srcTable)
	type fields struct {
		DynamoClient        dynamodbiface.DynamoDBAPI
		DynamoStreamsClient *dynamodbstreams.DynamoDBStreams
		SampleSize          int64
	}
	type args struct {
		ctx  context.Context
		conv *internal.Conv
	}
	arguments := args{
		ctx:  context.Background(),
		conv: conv,
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    map[string]interface{}
		wantErr bool
	}{
		{
			name: "test for checking correctness of output when stream exists already",
			fields: fields{
				DynamoClient: &mockDynamoClient{
					describeTableOutputs: []dynamodb.DescribeTableOutput{
						{
							Table: &dynamodb.TableDescription{
								TableName: &tableName,
								StreamSpecification: &dynamodb.StreamSpecification{
									StreamEnabled:  aws.Bool(true),
									StreamViewType: aws.String(dynamodb.StreamViewTypeNewImage),
								},
								LatestStreamArn: &latestStreamArn,
							},
						},
					},
				},
			},
			args: arguments,
			want: map[string]interface{}{
				tableName: latestStreamArn,
			},
			wantErr: false,
		},
		{
			name: "test for checking correctness of output when a new stream is created",
			fields: fields{
				DynamoClient: &mockDynamoClient{
					describeTableOutputs: []dynamodb.DescribeTableOutput{
						{
							Table: &dynamodb.TableDescription{
								TableName: &tableName,
							},
						},
					},
					updateTableOutputs: []dynamodb.UpdateTableOutput{
						{
							TableDescription: &dynamodb.TableDescription{
								LatestStreamArn: &latestStreamArn,
								StreamSpecification: &dynamodb.StreamSpecification{
									StreamEnabled:  aws.Bool(true),
									StreamViewType: aws.String(dynamodb.StreamViewTypeNewAndOldImages),
								},
							},
						},
					},
				},
			},
			args: arguments,
			want: map[string]interface{}{
				tableName: latestStreamArn,
			},
			wantErr: false,
		},
		{
			name: "test for handling api calls failure",
			fields: fields{
				DynamoClient: &mockDynamoClient{
					describeTableOutputs: []dynamodb.DescribeTableOutput{
						{
							Table: &dynamodb.TableDescription{
								TableName: &tableName,
							},
						},
					},
				},
			},
			args:    arguments,
			want:    map[string]interface{}{},
			wantErr: false,
		},
	}
	totalUnexpecteds := int64(0)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isi := InfoSchemaImpl{
				DynamoClient:        tt.fields.DynamoClient,
				DynamoStreamsClient: tt.fields.DynamoStreamsClient,
				SampleSize:          tt.fields.SampleSize,
			}
			got, err := isi.StartChangeDataCapture(tt.args.ctx, tt.args.conv)
			if (err != nil) != tt.wantErr {
				t.Errorf("InfoSchemaImpl.StartChangeDataCapture() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("InfoSchemaImpl.StartChangeDataCapture() = %v, want %v", got, tt.want)
			}
			totalUnexpecteds += conv.Unexpecteds()
		})
	}
	assert.Equal(t, int64(1), totalUnexpecteds)
}

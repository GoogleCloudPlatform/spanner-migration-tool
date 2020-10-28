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
	"math/big"
	"testing"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/schema"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
	"github.com/stretchr/testify/assert"
)

type spannerData struct {
	table string
	cols  []string
	vals  []interface{}
}

func TestProcessData(t *testing.T) {
	strA := "str-1"
	strB := "str-2"
	numStr1 := "10.1"
	numStr2 := "12.34"
	numStr3 := "89.0"
	numVal1 := big.NewRat(101, 10)
	numVal2 := big.NewRat(89, 1)

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
			LastEvaluatedKey: map[string]*dynamodb.AttributeValue{
				"a": {S: &strA},
			},
		},
		{
			Items: []map[string]*dynamodb.AttributeValue{
				{
					"a": {S: &strB},
					"b": {N: &numStr3},
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
			Name:     tableName,
			ColNames: cols,
			ColDefs: map[string]ddl.ColumnDef{
				"a": {Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
				"b": {Name: "b", T: ddl.Type{Name: ddl.Numeric}},
				"c": {Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
				"d": {Name: "d", T: ddl.Type{Name: ddl.Bool}},
			},
			Pks: []ddl.IndexKey{{Col: "a"}},
		},
		schema.Table{
			Name:     tableName,
			ColNames: cols,
			ColDefs: map[string]schema.Column{
				"a": {Name: "a", Type: schema.Type{Name: typeString}},
				"b": {Name: "b", Type: schema.Type{Name: typeNumber}},
				"c": {Name: "c", Type: schema.Type{Name: typeNumberString}},
				"d": {Name: "d", Type: schema.Type{Name: typeBool}},
			},
			PrimaryKeys: []schema.Key{{Column: "a"}},
		},
	)
	conv.SetDataMode()
	var rows []spannerData
	conv.SetDataSink(
		func(table string, cols []string, vals []interface{}) {
			rows = append(rows, spannerData{table: table, cols: cols, vals: vals})
		})
	ProcessData(conv, client, false)
	assert.Equal(t,
		[]spannerData{
			{
				table: tableName,
				cols:  cols,
				vals:  []interface{}{"str-1", *numVal1, "12.34", true},
			},
			{
				table: tableName,
				cols:  cols,
				vals:  []interface{}{"str-2", *numVal2, nil, nil},
			},
		},
		rows,
	)
}

func TestCvtColValue(t *testing.T) {
	str := "str-1"
	numStr := "1234.56789"
	boolVal := true
	binaryVal := []byte("ABC")
	binarySetVal := [][]byte{binaryVal}
	listVal := []*dynamodb.AttributeValue{
		{S: &str},
		{N: &numStr},
	}
	mapVal := map[string]*dynamodb.AttributeValue{
		"list": {L: listVal},
	}
	stringSetVal := []*string{&str}
	numVal := big.NewRat(123456789, 100000)

	testcases := []struct {
		name    string
		srcType string                   // Source DB type.
		spType  string                   // Spanner DB type.
		in      *dynamodb.AttributeValue // Input value for conversion.
		want    interface{}              // Expected result.
	}{
		{"bool", typeBool, ddl.Bool, &dynamodb.AttributeValue{BOOL: &boolVal}, true},
		{"binary", typeBinary, ddl.Bytes, &dynamodb.AttributeValue{B: binaryVal}, binaryVal},
		{"binary set", typeBinarySet, ddl.Bytes, &dynamodb.AttributeValue{BS: binarySetVal}, binarySetVal},
		{"map", typeMap, ddl.String, &dynamodb.AttributeValue{M: mapVal}, "{\"B\":null,\"BOOL\":null,\"BS\":null,\"L\":null,\"M\":{\"list\":{\"B\":null,\"BOOL\":null,\"BS\":null,\"L\":[{\"B\":null,\"BOOL\":null,\"BS\":null,\"L\":null,\"M\":null,\"N\":null,\"NS\":null,\"NULL\":null,\"S\":\"str-1\",\"SS\":null},{\"B\":null,\"BOOL\":null,\"BS\":null,\"L\":null,\"M\":null,\"N\":\"1234.56789\",\"NS\":null,\"NULL\":null,\"S\":null,\"SS\":null}],\"M\":null,\"N\":null,\"NS\":null,\"NULL\":null,\"S\":null,\"SS\":null}},\"N\":null,\"NS\":null,\"NULL\":null,\"S\":null,\"SS\":null}"},
		{"list", typeList, ddl.String, &dynamodb.AttributeValue{L: listVal}, "{\"B\":null,\"BOOL\":null,\"BS\":null,\"L\":[{\"B\":null,\"BOOL\":null,\"BS\":null,\"L\":null,\"M\":null,\"N\":null,\"NS\":null,\"NULL\":null,\"S\":\"str-1\",\"SS\":null},{\"B\":null,\"BOOL\":null,\"BS\":null,\"L\":null,\"M\":null,\"N\":\"1234.56789\",\"NS\":null,\"NULL\":null,\"S\":null,\"SS\":null}],\"M\":null,\"N\":null,\"NS\":null,\"NULL\":null,\"S\":null,\"SS\":null}"},
		{"string", typeString, ddl.String, &dynamodb.AttributeValue{S: &str}, str},
		{"string set", typeStringSet, ddl.String, &dynamodb.AttributeValue{SS: stringSetVal}, []string{str}},
		{"number string", typeNumberString, ddl.String, &dynamodb.AttributeValue{N: &numStr}, numStr},
		{"number string set", typeNumberStringSet, ddl.String, &dynamodb.AttributeValue{NS: []*string{&numStr}}, []string{numStr}},
		{"number", typeNumber, ddl.Numeric, &dynamodb.AttributeValue{N: &numStr}, *numVal},
		{"number set", typeNumberSet, ddl.Numeric, &dynamodb.AttributeValue{NS: []*string{&numStr}}, []big.Rat{*numVal}},
	}

	for _, tc := range testcases {
		cvtVal, err := cvtColValue(tc.in, tc.srcType, tc.spType)
		assert.Nil(t, err, fmt.Sprintf("Failed to convert %v from %s to %s", tc.in, typeString, ddl.String))
		assert.Equal(t, tc.want, cvtVal, tc.name)
	}
}

func buildConv(spTable ddl.CreateTable, srcTable schema.Table) *internal.Conv {
	conv := internal.MakeConv()
	conv.SpSchema[spTable.Name] = spTable
	conv.SrcSchema[srcTable.Name] = srcTable
	conv.ToSource[spTable.Name] = internal.NameAndCols{Name: srcTable.Name, Cols: make(map[string]string)}
	conv.ToSpanner[srcTable.Name] = internal.NameAndCols{Name: spTable.Name, Cols: make(map[string]string)}
	for i := range spTable.ColNames {
		conv.ToSource[spTable.Name].Cols[spTable.ColNames[i]] = srcTable.ColNames[i]
		conv.ToSpanner[srcTable.Name].Cols[srcTable.ColNames[i]] = spTable.ColNames[i]
	}
	return conv
}

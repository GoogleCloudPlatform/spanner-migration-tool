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
	"encoding/json"
	"fmt"
	"math/big"
	"testing"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/schema"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
	"github.com/stretchr/testify/assert"
)

type spannerData struct {
	table string
	cols  []string
	vals  []interface{}
}

func TestProcessDataRow(t *testing.T) {
	strA := "str-1"
	numStr1 := "10.1"
	numStr2 := "12.34"
	numVal1 := big.NewRat(101, 10)

	boolVal := true
	items := []map[string]*dynamodb.AttributeValue{
		{
			"a": {S: &strA},
			"b": {N: &numStr1},
			"c": {N: &numStr2},
			"d": {BOOL: &boolVal},
		},
	}

	tableName := "testtable"
	tableId := "t1"
	cols := []string{"a", "b", "c", "d"}
	colIds := []string{"c1", "c2", "c3", "c4"}
	spSchema := ddl.CreateTable{
		Name:   tableName,
		Id:     tableId,
		ColIds: colIds,
		ColDefs: map[string]ddl.ColumnDef{
			"c1": {Name: "a", Id: "c1", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
			"c2": {Name: "b", Id: "c2", T: ddl.Type{Name: ddl.Numeric}},
			"c3": {Name: "c", Id: "c3", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
			"c4": {Name: "d", Id: "c4", T: ddl.Type{Name: ddl.Bool}},
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
				"c1": {Name: "a", Id: "c1", Type: schema.Type{Name: typeString}},
				"c2": {Name: "b", Id: "c2", Type: schema.Type{Name: typeNumber}},
				"c3": {Name: "c", Id: "c3", Type: schema.Type{Name: typeNumberString}},
				"c4": {Name: "d", Id: "c4", Type: schema.Type{Name: typeBool}},
			},
			PrimaryKeys: []schema.Key{{ColId: "c1"}},
		},
	)
	conv.SetDataMode()
	var rows []spannerData
	conv.SetDataSink(
		func(table string, cols []string, vals []interface{}) {
			rows = append(rows, spannerData{table: table, cols: cols, vals: vals})
		})
	for _, attrsMap := range items {
		ProcessDataRow(attrsMap, conv, tableId, conv.SrcSchema[tableId], colIds, spSchema)
	}
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

func TestCvtRowWithError(t *testing.T) {
	tableName := "testtable"
	colIds := []string{"c1"}
	spSchema := ddl.CreateTable{
		Name:   tableName,
		ColIds: colIds,
		ColDefs: map[string]ddl.ColumnDef{
			// Give a wrong target type.
			"c1": {Name: "a", T: ddl.Type{Name: ddl.Float64}},
		},
	}
	srcSchema := schema.Table{
		Name:   tableName,
		ColIds: colIds,
		ColDefs: map[string]schema.Column{
			"c1": {Name: "a", Type: schema.Type{Name: typeString}},
		},
	}
	strA := "str-1"
	attrs := map[string]*dynamodb.AttributeValue{
		"a": {S: &strA},
	}
	_, badCols, srcStrVals := cvtRow(attrs, srcSchema, spSchema, colIds)

	assert.Equal(t, []string{"a"}, badCols)
	assert.Equal(t, []string{attrs["a"].GoString()}, srcStrVals)
}

func TestConvArray(t *testing.T) {
	str := "str-1"
	stringSetVal := []*string{&str}
	binarySetVal := [][]byte{[]byte("ABC")}
	numStr := "1234.56789"
	numVal := big.NewRat(123456789, 100000)

	testcases := []struct {
		name    string
		srcType string                   // Source DB type.
		spType  string                   // Spanner DB type.
		in      *dynamodb.AttributeValue // Input value for conversion.
		want    interface{}              // Expected result.
	}{
		{"binary set", typeBinarySet, ddl.Bytes, &dynamodb.AttributeValue{BS: binarySetVal}, binarySetVal},
		{"string set", typeStringSet, ddl.String, &dynamodb.AttributeValue{SS: stringSetVal}, []string{str}},
		{"number string set", typeNumberStringSet, ddl.String, &dynamodb.AttributeValue{NS: []*string{&numStr}}, []string{numStr}},
		{"number set", typeNumberSet, ddl.Numeric, &dynamodb.AttributeValue{NS: []*string{&numStr}}, []big.Rat{*numVal}},
	}

	for _, tc := range testcases {
		cvtVal, err := convArray(tc.in, tc.srcType, tc.spType)
		assert.Nil(t, err, fmt.Sprintf("Failed to convert %v from %s to %s", tc.in, typeString, ddl.String))
		assert.Equal(t, tc.want, cvtVal, tc.name)
	}
}

func TestConvScalar(t *testing.T) {
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
		{"binary set", typeBinarySet, ddl.String, &dynamodb.AttributeValue{BS: binarySetVal}, "[\"ABC\"]"},
		{"map", typeMap, ddl.String, &dynamodb.AttributeValue{M: mapVal}, "{\"list\":[\"str-1\",\"1234.56789\"]}"},
		{"list", typeList, ddl.String, &dynamodb.AttributeValue{L: listVal}, "[\"str-1\",\"1234.56789\"]"},
		{"string", typeString, ddl.String, &dynamodb.AttributeValue{S: &str}, str},
		{"string set", typeStringSet, ddl.String, &dynamodb.AttributeValue{SS: stringSetVal}, "[\"str-1\"]"},
		{"number string", typeNumberString, ddl.String, &dynamodb.AttributeValue{N: &numStr}, numStr},
		{"number string set", typeNumberStringSet, ddl.String, &dynamodb.AttributeValue{NS: []*string{&numStr}}, "[\"1234.56789\"]"},
		{"number", typeNumber, ddl.Numeric, &dynamodb.AttributeValue{N: &numStr}, *numVal},
		{"number set", typeNumberSet, ddl.String, &dynamodb.AttributeValue{NS: []*string{&numStr}}, "[\"1234.56789\"]"},
	}

	for _, tc := range testcases {
		cvtVal, err := convScalar(tc.in, tc.srcType, tc.spType)
		assert.Nil(t, err, fmt.Sprintf("Failed to convert %v from %s to %s", tc.in, typeString, ddl.String))
		assert.Equal(t, tc.want, cvtVal, tc.name)
	}
}

func TestStripNull(t *testing.T) {
	str := "str-1"
	numStr := "1234.56789"
	boolTrue := true
	boolFalse := false
	binaryVal := []byte("ABC")
	binarySetVal := [][]byte{binaryVal}
	stringSetVal := []*string{&str}
	numberSetVal := []*string{&numStr}
	listVal1 := []*dynamodb.AttributeValue{
		{S: &str},
	}
	mapVal1 := map[string]*dynamodb.AttributeValue{
		"list": {L: listVal1},
	}
	listVal2 := []*dynamodb.AttributeValue{
		{B: binaryVal},
		{S: &str},
		{N: &numStr},
		{BOOL: &boolTrue},
		{SS: stringSetVal},
		{BS: binarySetVal},
		{NS: numberSetVal},
		{NULL: &boolFalse},
		{L: listVal1},
		{M: mapVal1},
	}
	mapVal2 := map[string]*dynamodb.AttributeValue{
		"list": {L: listVal2},
	}

	testcases := []struct {
		name string
		in   *dynamodb.AttributeValue // Input value for conversion.
		want string                   // Expected result.
	}{
		{"binary", &dynamodb.AttributeValue{B: binaryVal}, "\"ABC\""},
		{"string", &dynamodb.AttributeValue{S: &str}, "\"str-1\""},
		{"number", &dynamodb.AttributeValue{N: &numStr}, "\"1234.56789\""},
		{"bool", &dynamodb.AttributeValue{BOOL: &boolTrue}, "true"},
		{"string set", &dynamodb.AttributeValue{SS: stringSetVal}, "[\"str-1\"]"},
		{"binary set", &dynamodb.AttributeValue{BS: binarySetVal}, "[\"ABC\"]"},
		{"number set", &dynamodb.AttributeValue{NS: []*string{&numStr}}, "[\"1234.56789\"]"},
		{"null", &dynamodb.AttributeValue{NULL: &boolFalse}, "false"},
		{"list", &dynamodb.AttributeValue{L: listVal2}, "[\"ABC\",\"str-1\",\"1234.56789\",true,[\"str-1\"],[\"ABC\"],[\"1234.56789\"],false,[\"str-1\"],{\"list\":[\"str-1\"]}]"},
		{"map", &dynamodb.AttributeValue{M: mapVal2}, "{\"list\":[\"ABC\",\"str-1\",\"1234.56789\",true,[\"str-1\"],[\"ABC\"],[\"1234.56789\"],false,[\"str-1\"],{\"list\":[\"str-1\"]}]}"},
	}
	for _, tc := range testcases {
		s, err := stripNull(tc.in)
		assert.Nil(t, err, fmt.Sprintf("Failed to marshal an attribute value: %v", tc.in))
		b, err := json.Marshal(s)
		assert.Nil(t, err, fmt.Sprintf("Failed to marshal to a json string: %v", s))
		assert.Equal(t, tc.want, string(b), tc.name)
	}
}

func buildConv(spTable ddl.CreateTable, srcTable schema.Table) *internal.Conv {
	conv := internal.MakeConv()
	conv.SpSchema[spTable.Id] = spTable
	conv.SrcSchema[srcTable.Id] = srcTable
	return conv
}

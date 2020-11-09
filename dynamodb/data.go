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

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/schema"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
)

// ProcessData performs data conversion for DynamoDB database. For each table,
// we extract data using Scan requests, convert the data to Spanner data (based
// on the source and Spanner schemas), and write it to Spanner. If we can't
// get/process data for a table, we skip that table and process the remaining
// tables.
func ProcessData(conv *internal.Conv, client dynamoClient) error {
	for srcTable, srcSchema := range conv.SrcSchema {
		spTable, err1 := internal.GetSpannerTable(conv, srcTable)
		spCols, err2 := internal.GetSpannerCols(conv, srcTable, srcSchema.ColNames)
		spSchema, ok := conv.SpSchema[spTable]
		if err1 != nil || err2 != nil || !ok {
			conv.Stats.BadRows[srcTable] += conv.Stats.Rows[srcTable]
			conv.Unexpected(fmt.Sprintf("Can't get cols and schemas for table %s: err1=%s, err2=%s, ok=%t",
				srcTable, err1, err2, ok))
			continue
		}

		err := scan(srcTable, client, func(m map[string]*dynamodb.AttributeValue) {
			spVals, badCols, srcStrVals := cvtRow(m, srcSchema, spSchema, spCols)
			if len(badCols) == 0 {
				conv.WriteRow(srcTable, spTable, spCols, spVals)
			} else {
				conv.Unexpected(fmt.Sprintf("Data conversion error for table %s in column(s) %s\n", srcTable, badCols))
				conv.StatsAddBadRow(srcTable, conv.DataMode())
				conv.CollectBadRow(srcTable, srcSchema.ColNames, srcStrVals)
			}
		})
		if err != nil {
			conv.Stats.BadRows[srcTable] += conv.Stats.Rows[srcTable]
			conv.Unexpected(fmt.Sprintf("Can't scan the data for table %s: %s", srcTable, err))
		}
	}
	return nil
}

func scan(table string, client dynamoClient, f func(map[string]*dynamodb.AttributeValue)) error {
	var lastEvaluatedKey map[string]*dynamodb.AttributeValue
	for {
		// Build the query input parameters.
		params := &dynamodb.ScanInput{
			TableName: aws.String(table),
		}
		if lastEvaluatedKey != nil {
			params.ExclusiveStartKey = lastEvaluatedKey
		}

		// Make the DynamoDB Query API call.
		result, err := client.Scan(params)
		if err != nil {
			return fmt.Errorf("failed to make Query API call for table %v: %v", table, err)
		}

		// Iterate the items returned.
		for _, attrsMap := range result.Items {
			f(attrsMap)
		}
		if result.LastEvaluatedKey == nil {
			return nil
		}
		// If there are more rows, then continue.
		lastEvaluatedKey = result.LastEvaluatedKey
	}
}

func cvtRow(attrsMap map[string]*dynamodb.AttributeValue, srcSchema schema.Table, spSchema ddl.CreateTable, spCols []string) ([]interface{}, []string, []string) {
	var err error
	var srcStrVals []string
	var spVals []interface{}
	var badCols []string
	for i, srcCol := range srcSchema.ColNames {
		var spVal interface{}
		var srcStrVal string
		if attrsMap[srcCol] == nil {
			spVal = nil
			srcStrVal = "null"
		} else {
			// Convert data to the target type.
			spVal, err = cvtColValue(attrsMap[srcCol], srcSchema.ColDefs[srcCol].Type.Name, spSchema.ColDefs[spCols[i]].T.Name)
			if err != nil {
				badCols = append(badCols, srcCol)
			}
			srcStrVal = attrsMap[srcCol].GoString()
		}
		srcStrVals = append(srcStrVals, srcStrVal)
		spVals = append(spVals, spVal)
	}
	return spVals, badCols, srcStrVals
}

func cvtColValue(attrVal *dynamodb.AttributeValue, srcType string, spType string) (interface{}, error) {
	switch spType {
	case ddl.Bool:
		switch srcType {
		case typeBool:
			return *attrVal.BOOL, nil
		}
	case ddl.Bytes:
		switch srcType {
		case typeBinary:
			return attrVal.B, nil
		case typeBinarySet:
			return attrVal.BS, nil
		}
	case ddl.String:
		switch srcType {
		case typeMap, typeList:
			// For typeMap and typeList, attrVal is a very verbose data
			// structure that contains null entries for unused type cases. We
			// strip these out using stripNull. If it is important that the
			// Spanner values can be easily unmarshalled back to
			// dynamodb.AttributeValue types, then replace the following five
			// lines with just:
			// b, err := json.Marshal(attrVal)
			// but note that this will consume extra Spanner storage.
			val, err := stripNull(attrVal)
			if err != nil {
				return nil, fmt.Errorf("failed to convert %v to a go struct", attrVal.GoString())
			}
			b, err := json.Marshal(val)
			if err != nil {
				return nil, fmt.Errorf("failed to convert %v to a json string", attrVal.GoString())
			}
			return string(b), nil
		case typeString:
			return *attrVal.S, nil
		case typeStringSet:
			var strArr []string
			for _, s := range attrVal.SS {
				strArr = append(strArr, *s)
			}
			return strArr, nil
		case typeNumberString:
			return *attrVal.N, nil
		case typeNumberStringSet:
			var strArr []string
			for _, s := range attrVal.NS {
				strArr = append(strArr, *s)
			}
			return strArr, nil
		}
	case ddl.Numeric:
		switch srcType {
		case typeNumber:
			s := *attrVal.N
			val, ok := (&big.Rat{}).SetString(s)
			if !ok {
				return nil, fmt.Errorf("failed to convert '%v' to an NUMERIC type", s)
			}
			return *val, nil
		case typeNumberSet:
			var numArr []big.Rat
			for _, s := range attrVal.NS {
				val, ok := (&big.Rat{}).SetString(*s)
				if !ok {
					return nil, fmt.Errorf("failed to convert '%v' to an NUMERIC array", attrVal.NS)
				}
				numArr = append(numArr, *val)
			}
			return numArr, nil
		}
	}
	return nil, fmt.Errorf("can't convert value of type %s to Spanner type %s", attrVal.GoString(), spType)
}

// stripNull converts a dynamodb.AttributeValue to a Go struct which can
// be easily encoded to a json string. If we use the normal json encoder, it
// will have many null values. The purpose of this function is to remove the
// null values in the json string.
func stripNull(a *dynamodb.AttributeValue) (interface{}, error) {
	var err error
	switch {
	case a.M != nil:
		cvtMap := make(map[string]interface{})
		for k, v := range a.M {
			cvtMap[k], err = stripNull(v)
			if err != nil {
				return nil, err
			}
		}
		return cvtMap, nil
	case a.L != nil:
		var cvtList []interface{}
		for _, v := range a.L {
			c, err := stripNull(v)
			if err != nil {
				return nil, err
			}
			cvtList = append(cvtList, c)
		}
		return cvtList, nil
	case a.B != nil:
		return string(a.B), nil
	case a.BOOL != nil:
		return a.BOOL, nil
	case a.BS != nil:
		var bs []string
		for _, b := range a.BS {
			bs = append(bs, string(b))
		}
		return bs, nil
	case a.N != nil:
		return *a.N, nil
	case a.NS != nil:
		return a.NS, nil
	case a.NULL != nil:
		return a.NULL, nil
	case a.S != nil:
		return *a.S, nil
	case a.SS != nil:
		return a.SS, nil
	default:
		return nil, fmt.Errorf("unknown type of AttributeValue: %v", a)
	}
}

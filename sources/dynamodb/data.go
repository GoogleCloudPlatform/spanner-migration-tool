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

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/schema"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
)

func ProcessDataRow(m map[string]*dynamodb.AttributeValue, conv *internal.Conv, tableId string, srcSchema schema.Table, spCols []string, spSchema ddl.CreateTable) {
	spVals, badCols, srcStrVals := cvtRow(m, srcSchema, spSchema, spCols)
	srcTableName := conv.SrcSchema[tableId].Name
	spTableName := conv.SpSchema[tableId].Name
	spColNames := spCols
	srcColNames := []string{}
	for _, colId := range conv.SrcSchema[tableId].ColIds {
		srcColNames = append(srcColNames, conv.SrcSchema[tableId].ColDefs[colId].Name)
	}
	if len(badCols) == 0 {
		conv.WriteRow(srcTableName, spTableName, spColNames, spVals)
	} else {
		conv.Unexpected(fmt.Sprintf("Data conversion error for table %s in column(s) %s\n", srcTableName, badCols))
		conv.StatsAddBadRow(srcTableName, conv.DataMode())
		conv.CollectBadRow(srcTableName, srcColNames, srcStrVals)
	}
}

func cvtRow(attrsMap map[string]*dynamodb.AttributeValue, srcSchema schema.Table, spSchema ddl.CreateTable, spCols []string) ([]interface{}, []string, []string) {
	var err error
	var srcStrVals []string
	var spVals []interface{}
	var badCols []string
	for _, colId := range srcSchema.ColIds {
		srcColName := srcSchema.ColDefs[colId].Name
		var spVal interface{}
		var srcStrVal string
		if attrsMap[srcColName] == nil {
			spVal = nil
			srcStrVal = "null"
		} else {
			// Convert data to the target type.
			// spCols := spCols[i]
			spColDef := spSchema.ColDefs[colId]
			srcColDef := srcSchema.ColDefs[colId]
			if spColDef.T.IsArray {
				spVal, err = convArray(attrsMap[srcColName], srcColDef.Type.Name, spColDef.T.Name)
			} else {
				spVal, err = convScalar(attrsMap[srcColName], srcColDef.Type.Name, spColDef.T.Name)
			}
			if err != nil {
				badCols = append(badCols, srcColName)
			}
			srcStrVal = attrsMap[srcColName].GoString()
		}
		srcStrVals = append(srcStrVals, srcStrVal)
		spVals = append(spVals, spVal)
	}
	return spVals, badCols, srcStrVals
}

func convArray(attrVal *dynamodb.AttributeValue, srcType string, spType string) (interface{}, error) {
	switch spType {
	case ddl.Bytes:
		switch srcType {
		case typeBinarySet:
			return attrVal.BS, nil
		}
	case ddl.String:
		switch srcType {
		case typeStringSet:
			var strArr []string
			for _, s := range attrVal.SS {
				strArr = append(strArr, *s)
			}
			return strArr, nil
		case typeNumberStringSet:
			var strArr []string
			for _, s := range attrVal.NS {
				strArr = append(strArr, *s)
			}
			return strArr, nil
		}
	case ddl.Numeric:
		switch srcType {
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

func convScalar(attrVal *dynamodb.AttributeValue, srcType string, spType string) (interface{}, error) {
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
		}
	case ddl.String:
		switch srcType {
		case typeString:
			return *attrVal.S, nil
		case typeNumberString:
			return *attrVal.N, nil
		case typeMap, typeList, typeStringSet, typeNumberStringSet, typeNumberSet, typeBinarySet:
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

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

		var lastEvaluatedKey map[string]*dynamodb.AttributeValue
		var count int64

		sampleSize := int64(1000)
		conv.Stats.Rows[srcTable] += sampleSize

		for {
			// Build the query input parameters
			params := &dynamodb.ScanInput{
				TableName: aws.String(srcTable),
			}
			if lastEvaluatedKey != nil {
				params.ExclusiveStartKey = lastEvaluatedKey
			}

			// Make the DynamoDB Query API call
			result, err := client.Scan(params)
			if err != nil {
				return fmt.Errorf("failed to make Query API call for table %v: %v", srcTable, err)
			}

			// Iterate the items returned
			for _, attrsMap := range result.Items {
				if count >= sampleSize {
					return nil
				}

				var cvtVals []interface{}
				for i, srcColName := range srcSchema.ColNames {
					// Convert data to the target type
					cvtVal, err := cvtColValue(attrsMap[srcColName], srcSchema.ColDefs[srcColName], spSchema.ColDefs[spCols[i]])
					if err != nil {
						return fmt.Errorf("failed to convert column: %v to %v", attrsMap[srcColName], spSchema.ColDefs[spCols[i]])
					}
					cvtVals = append(cvtVals, cvtVal)
				}

				conv.WriteRow(srcTable, spTable, spCols, cvtVals)
				count++
			}
			if result.LastEvaluatedKey == nil {
				break
			}
			// If there are more rows, then continue.
			lastEvaluatedKey = result.LastEvaluatedKey
		}
	}

	return nil
}

func cvtColValue(attrVal *dynamodb.AttributeValue, srcCd schema.Column, spCd ddl.ColumnDef) (interface{}, error) {
	switch spCd.T.Name {
	case ddl.Bool:
		switch srcCd.Type.Name {
		case typeBool:
			return *attrVal.BOOL, nil
		}
	case ddl.Bytes:
		switch srcCd.Type.Name {
		case typeBinary:
			return attrVal.B, nil
		case typeBinarySet:
			return attrVal.BS, nil
		}
	case ddl.String:
		switch srcCd.Type.Name {
		case typeMap:
			b, err := json.Marshal(attrVal.M)
			if err != nil {
				return nil, fmt.Errorf("failed to encode a map object: %v to a json string", attrVal.GoString())
			}
			return string(b), nil
		case typeList:
			b, err := json.Marshal(attrVal.L)
			if err != nil {
				return nil, fmt.Errorf("failed to encode a list object: %v to a json string", attrVal.GoString())
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
		switch srcCd.Type.Name {
		case typeNumber:
			s := *attrVal.N
			val, ok := (&big.Rat{}).SetString(s)
			if !ok {
				return nil, fmt.Errorf("failed to convert '%v' to an NUMERIC type", s)
			}
			return val, nil
		case typeNumberSet:
			var numArr []*big.Rat
			for _, s := range attrVal.NS {
				val, ok := (&big.Rat{}).SetString(*s)
				if !ok {
					return nil, fmt.Errorf("failed to convert '%v' to an NUMERIC array", attrVal.NS)
				}
				numArr = append(numArr, val)
			}
			return numArr, nil
		}
	}
	return nil, fmt.Errorf("can't convert value of type %s to Spanner type %s", attrVal.GoString(), spCd.T.Name)
}

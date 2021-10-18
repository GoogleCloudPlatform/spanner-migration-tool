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
	"log"
	"math/big"
	"sort"

	sp "cloud.google.com/go/spanner"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/schema"
	"github.com/cloudspannerecosystem/harbourbridge/sources/common"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
)

const (
	typeString          = "String"
	typeBool            = "Bool"
	typeNumber          = "Number"
	typeNumberString    = "NumberString"
	typeBinary          = "Binary"
	typeList            = "List"
	typeMap             = "Map"
	typeStringSet       = "StringSet"
	typeNumberSet       = "NumberSet"
	typeNumberStringSet = "NumberStringSet"
	typeBinarySet       = "BinarySet"

	errThreshold      = float64(0.001)
	conflictThreshold = float64(0.05)
)

type InfoSchemaImpl struct {
	DynamoClient dynamodbiface.DynamoDBAPI
	SampleSize   int64
}

func (isi InfoSchemaImpl) GetToDdl() common.ToDdl {
	return ToDdlImpl{}
}
func (isi InfoSchemaImpl) GetTableName(schema string, tableName string) string {
	return *aws.String(tableName)
}

func (isi InfoSchemaImpl) GetTables() ([]common.SchemaAndName, error) {
	var tables []common.SchemaAndName
	input := &dynamodb.ListTablesInput{}
	for {
		result, err := isi.DynamoClient.ListTables(input)
		if err != nil {
			return nil, err
		}
		for _, t := range result.TableNames {
			tables = append(tables, common.SchemaAndName{Name: *t})
		}

		if result.LastEvaluatedTableName == nil {
			return tables, nil
		}
		input.ExclusiveStartTableName = result.LastEvaluatedTableName
	}
}
func (isi InfoSchemaImpl) GetColumns(conv *internal.Conv, table common.SchemaAndName, constraints map[string][]string, primaryKeys []string) (map[string]schema.Column, []string, error) {
	stats, count, err := scanSampleData(isi.DynamoClient, isi.SampleSize, table.Name)
	if err != nil {
		return nil, nil, err
	}
	return inferDataTypes(stats, count, primaryKeys)
}
func (isi InfoSchemaImpl) GetRowsFromTable(conv *internal.Conv, srcTable string) (interface{}, error) {
	var lastEvaluatedKey map[string]*dynamodb.AttributeValue
	for {
		// Build the query input parameters.
		params := &dynamodb.ScanInput{
			TableName: aws.String(srcTable),
		}
		if lastEvaluatedKey != nil {
			params.ExclusiveStartKey = lastEvaluatedKey
		}

		// Make the DynamoDB Query API call.
		result, err := isi.DynamoClient.Scan(params)
		if err != nil {
			return nil, fmt.Errorf("failed to make Query API call for table %v: %v", srcTable, err)
		}

		if result.LastEvaluatedKey == nil {
			return result.Items, nil
		}
		// If there are more rows, then continue.
		lastEvaluatedKey = result.LastEvaluatedKey
	}
}
func (isi InfoSchemaImpl) GetRowCount(table common.SchemaAndName) (int64, error) {
	input := &dynamodb.DescribeTableInput{
		TableName: aws.String(table.Name),
	}
	result, err := isi.DynamoClient.DescribeTable(input)
	if err != nil {
		return 0, err
	}
	return *result.Table.ItemCount, err
}
func (isi InfoSchemaImpl) GetConstraints(conv *internal.Conv, table common.SchemaAndName) (primaryKeys []string, constraints map[string][]string, err error) {
	input := &dynamodb.DescribeTableInput{
		TableName: aws.String(table.Name),
	}
	result, err := isi.DynamoClient.DescribeTable(input)
	if err != nil {
		return primaryKeys, constraints, fmt.Errorf("failed to make a DescribeTable API call for table %v: %v", table.Name, err)
	}

	// Primary keys.
	for _, i := range result.Table.KeySchema {
		primaryKeys = append(primaryKeys, *i.AttributeName)
	}
	return primaryKeys, constraints, nil
}
func (isi InfoSchemaImpl) GetForeignKeys(conv *internal.Conv, table common.SchemaAndName) (foreignKeys []schema.ForeignKey, err error) {
	return foreignKeys, err
}
func (isi InfoSchemaImpl) GetIndexes(conv *internal.Conv, table common.SchemaAndName) (indexes []schema.Index, err error) {
	input := &dynamodb.DescribeTableInput{
		TableName: aws.String(table.Name),
	}

	result, err := isi.DynamoClient.DescribeTable(input)
	if err != nil {
		return nil, fmt.Errorf("failed to make a DescribeTable API call for table %v: %v", table.Name, err)
	}
	// DynamoDB supports 2 types of indexes: Global Secondary Indexes (GSI) and Local Secondary Indexes (LSI).
	// In GSI, dydb creates another global table for indexes that scales seperately.
	// As for LSI, every partition in dydb maintains its own local index for that partition.
	// For spanner, we should convert both these types as how dydb implements them is irrelevant.
	// For more details, checkout https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/LSI.html

	// Convert secondary indexes from GlobalSecondaryIndexes.
	for _, i := range result.Table.GlobalSecondaryIndexes {
		indexes = append(indexes, getSchemaIndexStruct(*i.IndexName, i.KeySchema))
	}

	// Convert secondary indexes from LocalSecondaryIndexes.
	for _, i := range result.Table.LocalSecondaryIndexes {
		indexes = append(indexes, getSchemaIndexStruct(*i.IndexName, i.KeySchema))
	}
	return indexes, nil
}

// ProcessData performs data conversion for DynamoDB database. For each table,
// we extract data using Scan requests, convert the data to Spanner data (based
// on the source and Spanner schemas), and write it to Spanner. If we can't
// get/process data for a table, we skip that table and process the remaining
// tables.
func (isi InfoSchemaImpl) ProcessData(conv *internal.Conv, srcTable string, srcSchema schema.Table, spTable string, spCols []string, spSchema ddl.CreateTable) {
	rows, err := isi.GetRowsFromTable(conv, srcTable)
	// Iterate the items returned.
	for _, attrsMap := range rows.([]map[string]*dynamodb.AttributeValue) {
		ProcessDataRow(attrsMap, conv, srcTable, srcSchema, spTable, spCols, spSchema)
	}
	if err != nil {
		conv.Stats.BadRows[srcTable] += conv.Stats.Rows[srcTable]
		conv.Unexpected(fmt.Sprintf("Can't scan the data for table %s: %s", srcTable, err))
	}
}

func getSchemaIndexStruct(indexName string, keySchema []*dynamodb.KeySchemaElement) schema.Index {
	var keys []schema.Key
	for _, j := range keySchema {
		keys = append(keys, schema.Key{Column: *j.AttributeName})
	}
	return schema.Index{Name: indexName, Keys: keys}
}

func scanSampleData(client dynamodbiface.DynamoDBAPI, sampleSize int64, table string) (map[string]map[string]int64, int64, error) {
	// A map from column name to a count map of possible data types.
	stats := make(map[string]map[string]int64)
	var count int64
	// Build the query input parameters.
	params := &dynamodb.ScanInput{
		TableName: aws.String(table),
	}

	for {
		// Make the DynamoDB Query API call.
		result, err := client.Scan(params)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to make Query API call for table %v: %v", table, err)
		}

		// Iterate the items returned.
		for _, attrsMap := range result.Items {
			for attrName, attr := range attrsMap {
				if _, ok := stats[attrName]; !ok {
					stats[attrName] = make(map[string]int64)
				}
				incTypeCount(attrName, attr, stats[attrName])
			}

			count++
			if count >= sampleSize {
				return stats, count, nil
			}
		}
		if result.LastEvaluatedKey == nil {
			break
		}
		// If there are more rows, then continue.
		params.ExclusiveStartKey = result.LastEvaluatedKey
	}
	return stats, count, nil
}

func incTypeCount(attrName string, attr *dynamodb.AttributeValue, s map[string]int64) {
	switch {
	case attr.S != nil:
		s[typeString]++
	case attr.BOOL != nil:
		s[typeBool]++
	case attr.N != nil:
		// We map the DynamoDB Number type into Spanner's NUMERIC type
		// if it fits and STRING otherwise. Note that DyanamoDB's Number
		// type has more precision/range than Spanner's NUMERIC.
		// We could potentially do a more detailed analysis and see if
		// the number fits in an INT64 or FLOAT64, but we've chosen to
		// keep the analysis simple for the moment.
		if numericParsable(*attr.N) {
			s[typeNumber]++
		} else {
			s[typeNumberString]++
		}
	case len(attr.B) != 0:
		s[typeBinary]++
	case attr.NULL != nil:
		// Skip, if not present, it means nullable.
	case len(attr.L) != 0:
		s[typeList]++
	case len(attr.M) != 0:
		s[typeMap]++
	case len(attr.SS) != 0:
		s[typeStringSet]++
	case len(attr.NS) != 0:
		parsable := true
		for _, n := range attr.NS {
			if !numericParsable(*n) {
				parsable = false
				break
			}
		}
		if parsable {
			s[typeNumberSet]++
		} else {
			s[typeNumberStringSet]++
		}
	case len(attr.BS) != 0:
		s[typeBinarySet]++
	default:
		log.Printf("Invalid DynamoDB data type: %v - %v", attrName, attr)
	}
}

type statItem struct {
	Type  string
	Count int64
}

func inferDataTypes(stats map[string]map[string]int64, rows int64, primaryKeys []string) (map[string]schema.Column, []string, error) {
	colDefs := make(map[string]schema.Column)
	var colNames []string

	for col, countMap := range stats {
		var statItems, candidates []statItem
		var presentRows int64
		for k, v := range countMap {
			presentRows += v
			if float64(v)/float64(rows) <= errThreshold {
				// If the percentage is less than the error threshold, then
				// this data type has a high chance to be mistakenly inserted
				// and we should discard it.
				continue
			}
			statItems = append(statItems, statItem{Type: k, Count: v})
		}
		if len(statItems) == 0 {
			log.Printf("Skip column %v with no data records", col)
			continue
		}

		// Check if the column is a part of a primary key.
		isPKey := false
		for _, pk := range primaryKeys {
			if pk == col {
				isPKey = true
				break
			}
		}

		// If this column is in the primary key, then it cannot be null.
		nullable := false
		if !isPKey {
			nullable = float64(rows-presentRows)/float64(rows) > errThreshold
		}

		for _, si := range statItems {
			if float64(si.Count)/float64(presentRows) > conflictThreshold {
				// If the normalized percentage is greater than the conflicting
				// threshold, we should consider this data type as a candidate.
				candidates = append(candidates, si)
			}
		}

		colNames = append(colNames, col)
		if len(candidates) == 1 {
			colDefs[col] = schema.Column{Name: col, Type: schema.Type{Name: candidates[0].Type}, NotNull: !nullable}
		} else {
			// If there is no any candidate or more than a single candidate,
			// this column has a significant conflict on data types and then
			// defaults to a String type.
			colDefs[col] = schema.Column{Name: col, Type: schema.Type{Name: typeString}, NotNull: !nullable}
		}
	}
	// Sort column names in increasing order, because the server may return them
	// in a random order.
	sort.Strings(colNames)
	return colDefs, colNames, nil
}

// numericParsable determines whether its argument is a valid Spanner numeric
// values. This is based on the definition of the NUMERIC type in Cloud Spanner:
// a NUMERIC type with 38 digits of precision and 9 digits of scale. It can
// support 29 digits before the decimal point and 9 digits after that.
func numericParsable(n string) bool {
	y, ok := (&big.Rat{}).SetString(n)
	if !ok {
		return false
	}
	// Get the length of numerator in text (base-10).
	numLen := len(y.Num().Text(10))
	// Remove the sign `-` if it exists.
	if y.Num().Sign() == -1 {
		numLen--
	}
	if numLen > sp.NumericPrecisionDigits {
		return false
	}

	// Get the length of denominator in text (base-10). Remove a digit because
	// the length of denominator would have one mor digit than the expected
	// scale. E.g., 0.999 will become 999/1000 and the length of denominator is
	// 4 instead of 3.
	denomLen := len(y.Denom().Text(10)) - 1
	// Remove the sign `-` if it exists.
	if y.Denom().Sign() == -1 {
		denomLen--
	}
	if denomLen > sp.NumericScaleDigits {
		return false
	}

	return true
}

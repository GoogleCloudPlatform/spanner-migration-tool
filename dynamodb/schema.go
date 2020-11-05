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
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/schema"
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

type dynamoClient interface {
	ListTables(input *dynamodb.ListTablesInput) (*dynamodb.ListTablesOutput, error)
	DescribeTable(input *dynamodb.DescribeTableInput) (*dynamodb.DescribeTableOutput, error)
	Scan(input *dynamodb.ScanInput) (*dynamodb.ScanOutput, error)
}

// ProcessSchema performs schema conversion for source tables in a DynamoDB
// database. Since DynamoDB is a schemaless database, this process is imprecise.
// We obtain schema information from two sources: from the table's metadata,
// and from analyzing a sample of the table's rows.
func ProcessSchema(conv *internal.Conv, client dynamoClient, tables []string, sampleSize int64) error {
	if len(tables) == 0 {
		var err error
		tables, err = listTables(client)
		if err != nil {
			return err
		}
		if len(tables) == 0 {
			return fmt.Errorf("no DynamoDB table exists under this account")
		}
	}
	for _, t := range tables {
		if err := processTable(conv, client, t, sampleSize); err != nil {
			return err
		}
	}
	schemaToDDL(conv)
	conv.AddPrimaryKeys()
	return nil
}

func listTables(client dynamoClient) ([]string, error) {
	var tables []string
	input := &dynamodb.ListTablesInput{}
	for {
		result, err := client.ListTables(input)
		if err != nil {
			return nil, err
		}
		for _, t := range result.TableNames {
			tables = append(tables, *t)
		}

		if result.LastEvaluatedTableName == nil {
			return tables, nil
		}
		input.ExclusiveStartTableName = result.LastEvaluatedTableName
	}
}

func processTable(conv *internal.Conv, client dynamoClient, table string, sampleSize int64) error {
	dySchema := schema.Table{Name: table}
	err := analyzeMetadata(client, &dySchema)
	if err != nil {
		return err
	}
	stats, count, err := scanSampleData(client, sampleSize, dySchema.Name)
	if err != nil {
		return err
	}
	inferDataTypes(stats, count, &dySchema)

	// Sort column names in increasing order, because the server may return them
	// in a random order.
	sort.Strings(dySchema.ColNames)
	conv.SrcSchema[table] = dySchema
	return nil
}

func analyzeMetadata(client dynamoClient, s *schema.Table) error {
	input := &dynamodb.DescribeTableInput{
		TableName: aws.String(s.Name),
	}

	result, err := client.DescribeTable(input)
	if err != nil {
		return fmt.Errorf("failed to make a DescribeTable API call for table %v: %v", s.Name, err)
	}

	// Primary keys
	for _, i := range result.Table.KeySchema {
		s.PrimaryKeys = append(s.PrimaryKeys, schema.Key{Column: *i.AttributeName})
	}

	// Secondary indexes
	for _, i := range result.Table.GlobalSecondaryIndexes {
		var keys []schema.Key
		for _, j := range i.KeySchema {
			keys = append(keys, schema.Key{Column: *j.AttributeName})
		}
		// s.SecIndexes = append(s.SecIndexes, index{Name: *i.IndexName, Keys: keys})
		s.Indexes = append(s.Indexes, schema.Index{Name: *i.IndexName, Keys: keys})
	}

	return nil
}

func scanSampleData(client dynamoClient, sampleSize int64, table string) (map[string]map[string]int64, int64, error) {
	// A map from column name to a count map of possible data types.
	stats := make(map[string]map[string]int64)
	var count int64
	// Build the query input parameters
	params := &dynamodb.ScanInput{
		TableName: aws.String(table),
	}

	for {
		// Make the DynamoDB Query API call
		result, err := client.Scan(params)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to make Query API call for table %v: %v", table, err)
		}

		// Iterate the items returned
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

func inferDataTypes(stats map[string]map[string]int64, rows int64, s *schema.Table) {
	if s.ColDefs == nil {
		s.ColDefs = make(map[string]schema.Column)
	}

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
		for _, pk := range s.PrimaryKeys {
			if pk.Column == col {
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

		s.ColNames = append(s.ColNames, col)
		if len(candidates) == 1 {
			s.ColDefs[col] = schema.Column{Name: col, Type: schema.Type{Name: candidates[0].Type}, NotNull: !nullable}
		} else {
			// If there is no any candidate or more than a single candidate,
			// this column has a significant conflict on data types and then
			// defaults to a String type.
			s.ColDefs[col] = schema.Column{Name: col, Type: schema.Type{Name: typeString}, NotNull: !nullable}
		}
	}
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

// SetRowStats populates conv with the number of rows in each table. In
// DynamoDB, we use describe_table api to get the number of total rows, but this
// number is updated approximately every six hours. This means that our row
// count could be out of date by up to six hours. One of the primary uses of
// row count is for calculating progress during data conversion. As a result, if
// there have been huge changes in the number of rows in a table over the last
// six hours, the progress calculation could be inaccurate.
func SetRowStats(conv *internal.Conv, client dynamoClient) {
	tables, err := listTables(client)
	if err != nil {
		conv.Unexpected(fmt.Sprintf("Couldn't get list of table: %s", err))
		return
	}
	if len(tables) == 0 {
		conv.Unexpected("no DynamoDB table exists under this account")
		return
	}
	for _, t := range tables {
		input := &dynamodb.DescribeTableInput{
			TableName: aws.String(t),
		}
		result, err := client.DescribeTable(input)
		if err != nil {
			conv.Unexpected(fmt.Sprintf("failed to make a DescribeTable API call for table %v: %v", t, err))
			return
		}
		conv.Stats.Rows[t] = *result.Table.ItemCount
	}
}

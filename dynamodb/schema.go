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
	"sort"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/schema"
)

const (
	typeString         = "String"
	typeBool           = "Bool"
	typeNumberInt      = "NumberInt"
	typeNumberFloat    = "NumberFloat"
	typeBinary         = "Binary"
	typeList           = "List"
	typeMap            = "Map"
	typeStringSet      = "StringSet"
	typeNumberIntSet   = "NumberIntSet"
	typeNumberFloatSet = "NumberFloatSet"
	typeBinarySet      = "BinarySet"

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
	dySchema := dynamoDBSchema{TableName: table}
	err := dySchema.analyzeMetadata(client)
	if err != nil {
		return err
	}
	stats, count, err := dySchema.scanSampleData(client, sampleSize)
	if err != nil {
		return err
	}
	dySchema.inferDataTypes(stats, count)
	conv.SrcSchema[table] = dySchema.genericSchema()
	return nil
}

type dynamoDBSchema struct {
	TableName   string
	ColumnNames []string
	ColumnTypes map[string]colType
	PrimaryKeys []string
	SecIndexes  []index
}

type colType struct {
	Name     string
	Nullable bool
}

type index struct {
	Name string
	Keys []string
}

func (s *dynamoDBSchema) analyzeMetadata(client dynamoClient) error {
	input := &dynamodb.DescribeTableInput{
		TableName: aws.String(s.TableName),
	}

	result, err := client.DescribeTable(input)
	if err != nil {
		return fmt.Errorf("failed to make a DescribeTable API call for table %v: %v", s.TableName, err)
	}

	// Primary keys
	for _, i := range result.Table.KeySchema {
		s.PrimaryKeys = append(s.PrimaryKeys, *i.AttributeName)
	}

	// Secondary indexes
	for _, i := range result.Table.GlobalSecondaryIndexes {
		var keys []string
		for _, j := range i.KeySchema {
			keys = append(keys, *j.AttributeName)
		}
		s.SecIndexes = append(s.SecIndexes, index{Name: *i.IndexName, Keys: keys})
	}

	return nil
}

func (s *dynamoDBSchema) scanSampleData(client dynamoClient, sampleSize int64) (map[string]map[string]int64, int64, error) {
	// A map from column name to a count map of possible data types.
	stats := make(map[string]map[string]int64)
	// var lastEvaluatedKey map[string]*dynamodb.AttributeValue
	var count int64
	// Build the query input parameters
	params := &dynamodb.ScanInput{
		TableName: aws.String(s.TableName),
	}

	for {
		// Make the DynamoDB Query API call
		result, err := client.Scan(params)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to make Query API call for table %v: %v", s.TableName, err)
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
		if int64Parsable(*attr.N) {
			s[typeNumberInt]++
		} else {
			s[typeNumberFloat]++
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
		if int64Parsable(*attr.NS[0]) {
			s[typeNumberIntSet]++
		} else {
			s[typeNumberFloatSet]++
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

func (s *dynamoDBSchema) inferDataTypes(stats map[string]map[string]int64, rows int64) {
	if s.ColumnTypes == nil {
		s.ColumnTypes = make(map[string]colType)
	}

	for col, countMap := range stats {
		var statItems, candidates []statItem
		var presentRows, normRows int64
		for k, v := range countMap {
			presentRows += v
			if float64(v)/float64(rows) <= errThreshold {
				// If the percentage is less than the error threshold, then
				// this data type has a high chance to be mistakenly inserted
				// and we should discard it.
				continue
			}
			statItems = append(statItems, statItem{Type: k, Count: v})
			normRows += v
		}
		if normRows == 0 {
			log.Printf("Skip column %v with no data records", col)
			continue
		}

		nullable := float64(rows-presentRows)/float64(rows) > errThreshold

		for _, si := range statItems {
			if float64(si.Count)/float64(normRows) > conflictThreshold {
				// If the normalized percentage is greater than the conflicting
				// threshold, we should consider this data type as a candidate.
				candidates = append(candidates, si)
			}
		}

		s.ColumnNames = append(s.ColumnNames, col)
		if len(candidates) == 1 {
			s.ColumnTypes[col] = colType{Name: candidates[0].Type, Nullable: nullable}
		} else {
			// If there is no any candidate or more than a single candidate,
			// this column has a significant conflict on data types and then
			// defaults to a String type.
			s.ColumnTypes[col] = colType{Name: typeString, Nullable: nullable}
		}
	}
}

func (s *dynamoDBSchema) genericSchema() schema.Table {
	colDefs := make(map[string]schema.Column)

	for _, colName := range s.ColumnNames {
		colType := s.ColumnTypes[colName]
		colDef := schema.Column{
			Name:    colName,
			Type:    schema.Type{Name: colType.Name},
			NotNull: !colType.Nullable,
		}
		colDefs[colName] = colDef
	}

	// Sort column names in increasing order.
	colNames := make([]string, len(s.ColumnNames))
	copy(colNames, s.ColumnNames)
	sort.Strings(colNames)

	// The order of primary keys is important.
	var schemaPKeys []schema.Key
	for _, colName := range s.PrimaryKeys {
		schemaPKeys = append(schemaPKeys, schema.Key{Column: colName})
		colDef := colDefs[colName]
		colDefs[colName] = schema.Column{
			Name:    colName,
			Type:    colDef.Type,
			NotNull: true,
		}
	}

	// Record secondary indexes.
	var indexes []schema.Index
	for _, ind := range s.SecIndexes {
		var keys []schema.Key
		for _, k := range ind.Keys {
			keys = append(keys, schema.Key{Column: k})
		}
		index := schema.Index{
			Name: ind.Name,
			Keys: keys,
		}
		indexes = append(indexes, index)
	}

	return schema.Table{
		Name:        s.TableName,
		ColNames:    colNames,
		ColDefs:     colDefs,
		PrimaryKeys: schemaPKeys,
		Indexes:     indexes,
	}
}

func int64Parsable(n string) bool {
	if _, err := strconv.ParseInt(n, 10, 64); err == nil {
		return true
	}
	return false
}

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
)

type dynamoClient interface {
	ListTables(input *dynamodb.ListTablesInput) (*dynamodb.ListTablesOutput, error)
	DescribeTable(input *dynamodb.DescribeTableInput) (*dynamodb.DescribeTableOutput, error)
	Scan(input *dynamodb.ScanInput) (*dynamodb.ScanOutput, error)
}

// ProcessSchema performs schema conversion for source tables in a DynamoDB
// database. We use the standard APIs to obtain source table's schema
// information, including primary keys and secondary indexes. DynamoDB is a
// schemaless database that some optional attributes can be missed or has
// different data types in some rows. Therefore, we have to sample a number of
// rows to infer the schema for optional attributes.
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
	var lastEvaluatedTableName *string

	for {
		input := &dynamodb.ListTablesInput{}
		if lastEvaluatedTableName != nil {
			input.ExclusiveStartTableName = lastEvaluatedTableName
		}

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
		lastEvaluatedTableName = result.LastEvaluatedTableName
	}
}

func processTable(conv *internal.Conv, client dynamoClient, table string, sampleSize int64) error {
	dySchema := dynamoDBSchema{TableName: table}
	err := dySchema.parseIndexes(client)
	if err != nil {
		return err
	}
	stats, err := dySchema.scanSampleData(client, sampleSize)
	if err != nil {
		return err
	}
	dySchema.inferDataTypes(stats)
	conv.SrcSchema[table] = dySchema.genericSchema()
	return nil
}

type dynamoDBSchema struct {
	TableName   string
	ColumnNames []string
	ColumnTypes map[string]string
	PrimaryKeys []string
	SecIndexes  []index
}

type index struct {
	Name string
	Keys []string
}

func (s *dynamoDBSchema) parseIndexes(client dynamoClient) error {
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

func (s *dynamoDBSchema) scanSampleData(client dynamoClient, sampleSize int64) (map[string]map[string]int64, error) {
	// A map from column name to a count map of possible data types.
	stats := make(map[string]map[string]int64)
	var lastEvaluatedKey map[string]*dynamodb.AttributeValue
	var count int64

	for {
		// Build the query input parameters
		params := &dynamodb.ScanInput{
			TableName: aws.String(s.TableName),
		}
		if lastEvaluatedKey != nil {
			params.ExclusiveStartKey = lastEvaluatedKey
		}

		// Make the DynamoDB Query API call
		result, err := client.Scan(params)
		if err != nil {
			return nil, fmt.Errorf("failed to make Query API call for table %v: %v", s.TableName, err)
		}

		// Iterate the items returned
		for _, attrsMap := range result.Items {
			for attrName, attr := range attrsMap {
				if _, ok := stats[attrName]; !ok {
					stats[attrName] = make(map[string]int64)
				}
				incDyDataTypeCount(attrName, attr, stats[attrName])
			}

			count++
			if count >= sampleSize {
				return stats, nil
			}
		}
		if result.LastEvaluatedKey == nil {
			break
		}
		// If there are more rows, then continue.
		lastEvaluatedKey = result.LastEvaluatedKey
	}
	return stats, nil
}

func incDyDataTypeCount(attrName string, attr *dynamodb.AttributeValue, s map[string]int64) {
	if attr.S != nil {
		incCount(s, typeString)
	} else if attr.BOOL != nil {
		incCount(s, typeBool)
	} else if attr.N != nil {
		if int64Parsable(*attr.N) {
			incCount(s, typeNumberInt)
		} else {
			incCount(s, typeNumberFloat)
		}
	} else if len(attr.B) != 0 {
		incCount(s, typeBinary)
	} else if attr.NULL != nil {
		// Skip because all optional attributes are nullable.
	} else if len(attr.L) != 0 {
		incCount(s, typeList)
	} else if len(attr.M) != 0 {
		incCount(s, typeMap)
	} else if len(attr.SS) != 0 {
		incCount(s, typeStringSet)
	} else if len(attr.NS) != 0 {
		if int64Parsable(*attr.NS[0]) {
			incCount(s, typeNumberIntSet)
		} else {
			incCount(s, typeNumberFloatSet)
		}
	} else if len(attr.BS) != 0 {
		incCount(s, typeBinarySet)
	} else {
		log.Printf("Invalid DynamoDB data type: %v - %v", attrName, attr)
	}
}

func (s *dynamoDBSchema) inferDataTypes(stats map[string]map[string]int64) {
	type statItem struct {
		Type  string
		Count int64
	}

	if s.ColumnTypes == nil {
		s.ColumnTypes = make(map[string]string)
	}

	for col, countMap := range stats {
		var statItems []statItem
		for k, v := range countMap {
			statItems = append(statItems, statItem{Type: k, Count: v})
		}

		if len(statItems) == 0 {
			log.Printf("Skip empty column %v", col)
			continue
		}

		// Sort the slice reversely so the most frequent data type will be
		// placed first.
		sort.Slice(statItems, func(i, j int) bool {
			// If counts are equal, then sort by names in alphabetical order.
			if statItems[i].Count == statItems[j].Count {
				return statItems[i].Type < statItems[j].Type
			}
			return statItems[i].Count > statItems[j].Count
		})

		if statItems[0].Count == 0 {
			log.Printf("Skip column %v with no data records", col)
			continue
		}

		s.ColumnNames = append(s.ColumnNames, col)
		s.ColumnTypes[col] = statItems[0].Type
	}
}

func (s *dynamoDBSchema) genericSchema() schema.Table {
	colDefs := make(map[string]schema.Column)

	for _, colName := range s.ColumnNames {
		colType := s.ColumnTypes[colName]
		colDef := schema.Column{
			Name: colName,
			Type: schema.Type{Name: colType},
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

func incCount(m map[string]int64, key string) {
	if _, ok := m[key]; !ok {
		m[key] = 0
	}
	m[key]++
}

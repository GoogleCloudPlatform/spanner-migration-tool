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
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/schema"
)

type statItem struct {
	Type  string
	Count int64
}

const (
	dyTypeString         = "String"
	dyTypeBool           = "Bool"
	dyTypeNumberInt      = "NumberInt"
	dyTypeNumberFloat    = "NumberFloat"
	dyTypeBinary         = "Binary"
	dyTypeList           = "List"
	dyTypeMap            = "Map"
	dyTypeStringSet      = "StringSet"
	dyTypeNumberIntSet   = "NumberIntSet"
	dyTypeNumberFloatSet = "NumberFloatSet"
	dyTypeBinarySet      = "BinarySet"
)

// ProcessSchema performs schema conversion for source tables in a DynamoDB
// database. We use the standard APIs to obtain source table's schema
// information, including primary keys and secondary indexes. DynamoDB is a
// schemaless database that some optional attributes can be missed or has
// different data types in some rows. Therefore, we have to sample a number of
// rows to infer the schema for optional attributes.
func ProcessSchema(conv *internal.Conv, tables []string, sampleSize int64) error {
	mySession := session.Must(session.NewSession())
	svc := dynamodb.New(mySession)

	if len(tables) == 0 {
		var err error
		tables, err = listTables(svc)
		if err != nil {
			return err
		}
	}

	for _, t := range tables {
		if err := processTable(conv, svc, t); err != nil {
			return err
		}
	}
	schemaToDDL(conv)
	conv.AddPrimaryKeys()
	return nil
}

func listTables(client *dynamodb.DynamoDB) ([]string, error) {
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

func processTable(conv *internal.Conv, client *dynamodb.DynamoDB, table string) error {
	dySchema := dynamoDBSchema{TableName: table}
	err := dySchema.parsePKeySecIndex(client)
	if err != nil {
		return err
	}
	err = dySchema.inferSchema(client)
	if err != nil {
		return err
	}
	conv.SrcSchema[table] = dySchema.genericSchema()
	return nil
}

type dynamoDBSchema struct {
	TableName              string
	ColumnNames            []string
	ColumnTypes            []string
	PrimaryKeys            []string
	GlobalSecondaryIndexes [][]string
}

func (s *dynamoDBSchema) parsePKeySecIndex(client *dynamodb.DynamoDB) error {
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
		var secIndex []string
		for _, j := range i.KeySchema {
			secIndex = append(secIndex, *j.AttributeName)
		}
		s.GlobalSecondaryIndexes = append(s.GlobalSecondaryIndexes, secIndex)
	}

	return nil
}

func (s *dynamoDBSchema) inferSchema(client *dynamodb.DynamoDB) error {
	// A map from column name to a count map of possible data types.
	stats := make(map[string]map[string]int64)
	var lastEvaluatedKey map[string]*dynamodb.AttributeValue

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
			return fmt.Errorf("failed to make Query API call for table %v: %v", s.TableName, err)
		}

		// Iterate the items returned
		for _, attrsMap := range result.Items {
			for attrName, attr := range attrsMap {
				if _, ok := stats[attrName]; !ok {
					stats[attrName] = make(map[string]int64)
				}
				incDyDataTypeCount(attrName, attr, stats[attrName])
			}
		}
		if result.LastEvaluatedKey == nil {
			break
		}
		// If there are more rows, then continue.
		lastEvaluatedKey = result.LastEvaluatedKey
	}

	s.inferDataTypes(stats)
	return nil
}

func incDyDataTypeCount(attrName string, attr *dynamodb.AttributeValue, s map[string]int64) {
	if attr.S != nil {
		incCount(s, dyTypeString)
	} else if attr.BOOL != nil {
		incCount(s, dyTypeBool)
	} else if attr.N != nil {
		if int64Parsable(*attr.N) {
			incCount(s, dyTypeNumberInt)
		} else {
			incCount(s, dyTypeNumberFloat)
		}
	} else if len(attr.B) != 0 {
		incCount(s, dyTypeBinary)
	} else if attr.NULL != nil {
		// Skip because all optional attributes are nullable.
	} else if len(attr.L) != 0 {
		incCount(s, dyTypeList)
	} else if len(attr.M) != 0 {
		incCount(s, dyTypeMap)
	} else if len(attr.SS) != 0 {
		incCount(s, dyTypeStringSet)
	} else if len(attr.NS) != 0 {
		if int64Parsable(*attr.NS[0]) {
			incCount(s, dyTypeNumberIntSet)
		} else {
			incCount(s, dyTypeNumberFloatSet)
		}
	} else if len(attr.BS) != 0 {
		incCount(s, dyTypeBinarySet)
	} else {
		log.Printf("Invalid DynamoDB data type: %v - %v", attrName, attr)
	}
}

func (s *dynamoDBSchema) inferDataTypes(stats map[string]map[string]int64) {
	for col, countMap := range stats {
		var statItems []statItem
		for k, v := range countMap {
			statItems = append(statItems, statItem{Type: k, Count: v})
		}

		if len(statItems) == 0 {
			log.Printf("Skip column: %v", col)
			continue
		}

		// Sort the slice reversely so the most frequent data type will be
		// placed first.
		sort.Slice(statItems, func(i, j int) bool {
			return statItems[i].Count > statItems[j].Count
		})

		s.ColumnNames = append(s.ColumnNames, col)
		s.ColumnTypes = append(s.ColumnTypes, statItems[0].Type)
	}
}

func (s *dynamoDBSchema) genericSchema() schema.Table {
	var schemaPKeys []schema.Key
	colDefs := make(map[string]schema.Column)

	for i, colType := range s.ColumnTypes {
		colName := s.ColumnNames[i]
		isNullable := true
		for _, pKey := range s.PrimaryKeys {
			if colName == pKey {
				isNullable = false
				schemaPKeys = append(schemaPKeys, schema.Key{Column: colName})
				break
			}
		}
		colDef := schema.Column{
			Name:    colName,
			Type:    schema.Type{Name: colType},
			NotNull: !isNullable,
		}
		colDefs[colName] = colDef
	}

	return schema.Table{
		Name:        s.TableName,
		ColNames:    s.ColumnNames,
		ColDefs:     colDefs,
		PrimaryKeys: schemaPKeys,
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

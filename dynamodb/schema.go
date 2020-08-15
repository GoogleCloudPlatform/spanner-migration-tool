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

type dynamoDBSchema struct {
	ColumnNames            []string
	ColumnTypes            []string
	PrimaryKeys            []string
	GlobalSecondaryIndexes [][]string
}

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
	input := &dynamodb.DescribeTableInput{
		TableName: aws.String(table),
	}

	result, err := client.DescribeTable(input)
	if err != nil {
		return fmt.Errorf("failed to make a DescribeTable API call for table %v: %v", table, err)
	}

	dySchema := dynamoDBSchema{}

	// Primary keys
	for _, i := range result.Table.KeySchema {
		dySchema.PrimaryKeys = append(dySchema.PrimaryKeys, *i.AttributeName)
	}

	// Secondary indexes
	for _, i := range result.Table.GlobalSecondaryIndexes {
		var secIndex []string
		for _, j := range i.KeySchema {
			secIndex = append(secIndex, *j.AttributeName)
		}
		dySchema.GlobalSecondaryIndexes = append(dySchema.GlobalSecondaryIndexes, secIndex)
	}

	stats := make(map[string]map[string]int64)
	var lastEvaluatedKey map[string]*dynamodb.AttributeValue

	for {
		// Build the query input parameters
		params := &dynamodb.ScanInput{
			TableName: aws.String(table),
		}
		if lastEvaluatedKey != nil {
			params.ExclusiveStartKey = lastEvaluatedKey
		}

		// Make the DynamoDB Query API call
		rlt, err := client.Scan(params)
		if err != nil {
			return fmt.Errorf("failed to make Query API call for table %v: %v", table, err)
		}

		// Print out the items returned
		for _, i := range rlt.Items {
			for k, v := range i {
				if _, ok := stats[k]; !ok {
					stats[k] = map[string]int64{}
				}
				if v.S != nil {
					incCount(stats[k], dyTypeString)
				} else if v.BOOL != nil {
					incCount(stats[k], dyTypeBool)
				} else if v.N != nil {
					if int64Parsable(*v.N) {
						incCount(stats[k], dyTypeNumberInt)
					} else {
						incCount(stats[k], dyTypeNumberFloat)
					}
				} else if len(v.B) != 0 {
					incCount(stats[k], dyTypeBinary)
				} else if v.NULL != nil {
					// Skip because all optional attributes are nullable.
				} else if len(v.L) != 0 {
					incCount(stats[k], dyTypeList)
				} else if len(v.M) != 0 {
					incCount(stats[k], dyTypeMap)
				} else if len(v.SS) != 0 {
					incCount(stats[k], dyTypeStringSet)
				} else if len(v.NS) != 0 {
					if int64Parsable(*v.NS[0]) {
						incCount(stats[k], dyTypeNumberIntSet)
					} else {
						incCount(stats[k], dyTypeNumberFloatSet)
					}
				} else if len(v.BS) != 0 {
					incCount(stats[k], dyTypeBinarySet)
				} else {
					log.Printf("Unrecognized type: %v - %v", k, v)
				}
			}
		}
		if rlt.LastEvaluatedKey == nil {
			break
		}
		lastEvaluatedKey = rlt.LastEvaluatedKey
	}

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

		dySchema.ColumnNames = append(dySchema.ColumnNames, col)
		dySchema.ColumnTypes = append(dySchema.ColumnTypes, statItems[0].Type)
	}

	var schemaPKeys []schema.Key
	colDefs := make(map[string]schema.Column)

	for i, colType := range dySchema.ColumnTypes {
		colName := dySchema.ColumnNames[i]
		isNullable := true
		for _, pKey := range dySchema.PrimaryKeys {
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

	conv.SrcSchema[table] = schema.Table{
		Name:        table,
		ColNames:    dySchema.ColumnNames,
		ColDefs:     colDefs,
		PrimaryKeys: schemaPKeys,
	}
	return nil
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

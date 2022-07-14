// Copyright 2022 Google LLC
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
	"errors"
	"fmt"
	"math/big"
	"reflect"
	"sync"
	"testing"
	"time"

	sp "cloud.google.com/go/spanner"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams/dynamodbstreamsiface"
	"github.com/stretchr/testify/assert"

	"github.com/cloudspannerecosystem/harbourbridge/schema"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
)

type mockDynamoStreamsClient struct {
	describeStreamOutputs                []dynamodbstreams.DescribeStreamOutput
	describeStreamCallCount              int
	listStreamsOutputs                   []dynamodbstreams.ListStreamsOutput
	listStreamsCallCount                 int
	getShardIteratorOutputsTrimHorizon   []dynamodbstreams.GetShardIteratorOutput
	getShardIteratorCallCountTrimHorizon int
	getShardIteratorOutputsSeqNum        []dynamodbstreams.GetShardIteratorOutput
	getShardIteratorCallCountSeqNum      int
	getRecordsOutputs                    []dynamodbstreams.GetRecordsOutput
	getRecordsCallCount                  int
	dynamodbstreamsiface.DynamoDBStreamsAPI
}

func (m *mockDynamoStreamsClient) DescribeStream(input *dynamodbstreams.DescribeStreamInput) (*dynamodbstreams.DescribeStreamOutput, error) {
	if m.describeStreamCallCount >= len(m.describeStreamOutputs) {
		return nil, fmt.Errorf("unexpected call to DescribeStream: %v", input)
	}
	m.describeStreamCallCount++
	return &m.describeStreamOutputs[m.describeStreamCallCount-1], nil
}

func (m *mockDynamoStreamsClient) ListStreams(input *dynamodbstreams.ListStreamsInput) (*dynamodbstreams.ListStreamsOutput, error) {
	if m.listStreamsCallCount >= len(m.listStreamsOutputs) {
		return nil, fmt.Errorf("unexpected call to ListStreams: %v", input)
	}
	m.listStreamsCallCount++
	return &m.listStreamsOutputs[m.listStreamsCallCount-1], nil
}

func (m *mockDynamoStreamsClient) GetShardIterator(input *dynamodbstreams.GetShardIteratorInput) (*dynamodbstreams.GetShardIteratorOutput, error) {
	if *input.ShardIteratorType == "TRIM_HORIZON" {
		if m.getShardIteratorCallCountTrimHorizon >= len(m.getShardIteratorOutputsTrimHorizon) {
			return nil, fmt.Errorf("unexpected call to GetShardIterator: %v", input)
		}
		m.getShardIteratorCallCountTrimHorizon++
		return &m.getShardIteratorOutputsTrimHorizon[m.getShardIteratorCallCountTrimHorizon-1], nil
	} else {
		if m.getShardIteratorCallCountSeqNum >= len(m.getShardIteratorOutputsSeqNum) {
			return nil, fmt.Errorf("unexpected call to GetShardIterator: %v", input)
		}
		m.getShardIteratorCallCountSeqNum++
		return &m.getShardIteratorOutputsSeqNum[m.getShardIteratorCallCountSeqNum-1], nil
	}
}

func (m *mockDynamoStreamsClient) GetRecords(input *dynamodbstreams.GetRecordsInput) (*dynamodbstreams.GetRecordsOutput, error) {
	if m.getRecordsCallCount >= len(m.getRecordsOutputs) {
		return nil, fmt.Errorf("unexpected call to GetShardIterator: %v", input)
	}
	m.getRecordsCallCount++
	return &m.getRecordsOutputs[m.getRecordsCallCount-1], nil
}

func TestProcessStream(t *testing.T) {
	streamInfo := MakeStreamingInfo()
	streamInfo.UserExit = true
	wgStream := &sync.WaitGroup{}

	streamArn := [2]string{"streamArn1", "streamArn2"}
	srcTableName := [2]string{"table1", "table2"}

	describeStreamOutputs := []dynamodbstreams.DescribeStreamOutput{
		{
			StreamDescription: &dynamodbstreams.StreamDescription{
				LastEvaluatedShardId: nil,
				Shards:               []*dynamodbstreams.Shard{},
				StreamArn:            &streamArn[0],
			},
		},
		{
			StreamDescription: &dynamodbstreams.StreamDescription{
				LastEvaluatedShardId: nil,
				Shards:               []*dynamodbstreams.Shard{},
				StreamArn:            &streamArn[0],
			},
		},
		{
			StreamDescription: &dynamodbstreams.StreamDescription{
				LastEvaluatedShardId: nil,
				Shards:               []*dynamodbstreams.Shard{},
				StreamArn:            &streamArn[1],
			},
		},
	}
	streamsClient := &mockDynamoStreamsClient{
		describeStreamOutputs: describeStreamOutputs,
	}

	wgStream.Add(2)
	for i := 0; i < 2; i++ {
		ProcessStream(wgStream, streamsClient, streamInfo, nil, streamArn[i], srcTableName[i])
	}
	wgStream.Wait()
	assert.Equal(t, int64(1), streamInfo.TotalUnexpecteds())
}

func Test_fetchShards(t *testing.T) {
	streamArn := "testStreamArn"
	tableName := "testTable"
	type args struct {
		streamClient         dynamodbstreamsiface.DynamoDBStreamsAPI
		lastEvaluatedShardId *string
		streamArn            string
	}
	streamDescription := dynamodbstreams.StreamDescription{
		LastEvaluatedShardId: aws.String("shard2"),
		Shards: []*dynamodbstreams.Shard{
			{
				ShardId: aws.String("shard1"),
			},
			{
				ShardId: aws.String("shard2"),
			},
		},
		StreamArn: &streamArn,
		TableName: &tableName,
	}
	tests := []struct {
		name    string
		args    args
		want    *dynamodbstreams.StreamDescription
		wantErr bool
	}{
		{
			name: "test for correctness of output",
			args: args{
				streamClient: &mockDynamoStreamsClient{
					describeStreamOutputs: []dynamodbstreams.DescribeStreamOutput{
						{
							StreamDescription: &streamDescription,
						},
					},
				},
				streamArn: streamArn,
			},
			want:    &streamDescription,
			wantErr: false,
		},
		{
			name: "test for checking api failures",
			args: args{
				streamClient: &mockDynamoStreamsClient{},
				streamArn:    streamArn,
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := fetchShards(tt.args.streamClient, tt.args.lastEvaluatedShardId, tt.args.streamArn)
			if (err != nil) != tt.wantErr {
				t.Errorf("fetchShards() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("fetchShards() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getShardIterator(t *testing.T) {
	shardIteratorTrimHorizon := "testShardIteratorTrimHorizon"
	shardIteratorSeqNum := "testShardIteratorSeqNum"
	lastEvaluatedSequenceNumber := "0000000000000001"

	mockStreamsClient := &mockDynamoStreamsClient{
		getShardIteratorOutputsTrimHorizon: []dynamodbstreams.GetShardIteratorOutput{
			{
				ShardIterator: &shardIteratorTrimHorizon,
			},
		},
		getShardIteratorOutputsSeqNum: []dynamodbstreams.GetShardIteratorOutput{
			{
				ShardIterator: &shardIteratorSeqNum,
			},
		},
	}

	shardIterator, err := getShardIterator(mockStreamsClient, nil, "", "")
	assert.Nil(t, err)
	assert.Equal(t, shardIteratorTrimHorizon, *shardIterator)

	shardIterator, err = getShardIterator(mockStreamsClient, &lastEvaluatedSequenceNumber, "", "")
	assert.Nil(t, err)
	assert.Equal(t, shardIteratorSeqNum, *shardIterator)

	_, err = getShardIterator(mockStreamsClient, nil, "", "")
	assert.NotNil(t, err)
}

func Test_getRecords(t *testing.T) {
	mockStreamsClient := &mockDynamoStreamsClient{
		getRecordsOutputs: []dynamodbstreams.GetRecordsOutput{
			{
				NextShardIterator: nil,
				Records: []*dynamodbstreams.Record{
					{
						EventName: aws.String("INSERT"),
					},
					{
						EventName: aws.String("REMOVE"),
					},
					{
						EventName: aws.String("MODIFY"),
					},
					{
						EventName: aws.String("INSERT"),
					},
				},
			},
		},
	}
	shardIterator := "testShardIterator"
	result, err := getRecords(mockStreamsClient, &shardIterator)
	assert.Nil(t, err)
	assert.Equal(t, int(4), len(result.Records))
	mp := make(map[string]int)
	for i := 0; i < len(result.Records); i++ {
		mp[*result.Records[i].EventName]++
	}
	assert.Equal(t, int(1), mp["REMOVE"])
	assert.Equal(t, int(2), mp["INSERT"])
	assert.Equal(t, int(1), mp["MODIFY"])

	_, err = getRecords(mockStreamsClient, &shardIterator)
	assert.NotNil(t, err)
}

func TestProcessShard(t *testing.T) {
	wgShard := &sync.WaitGroup{}
	streamInfo := MakeStreamingInfo()
	streamInfo.UserExit = true
	shardIterator_TrimHorizon := "testShardIteratorTrimHorizon"

	mockStreamClient := &mockDynamoStreamsClient{
		getShardIteratorOutputsTrimHorizon: []dynamodbstreams.GetShardIteratorOutput{
			{
				ShardIterator: &shardIterator_TrimHorizon,
			},
			{
				ShardIterator: &shardIterator_TrimHorizon,
			},
		},
		getRecordsOutputs: []dynamodbstreams.GetRecordsOutput{
			{
				NextShardIterator: nil,
				Records:           []*dynamodbstreams.Record{},
			},
		},
	}
	shardId := "testShardId"
	shard := &dynamodbstreams.Shard{
		SequenceNumberRange: &dynamodbstreams.SequenceNumberRange{
			EndingSequenceNumber:   aws.String("10"),
			StartingSequenceNumber: aws.String("10"),
		},
		ShardId: &shardId,
	}
	streamArn := "testStreamArn"
	srcTable := "testSrcTable"

	wgShard.Add(1)
	ProcessShard(wgShard, streamInfo, nil, mockStreamClient, shard, streamArn, srcTable)
	assert.Equal(t, true, streamInfo.ShardProcessed[*shard.ShardId])

	wgShard.Add(1)
	ProcessShard(wgShard, streamInfo, nil, mockStreamClient, shard, streamArn, srcTable)
	assert.Equal(t, int64(1), streamInfo.TotalUnexpecteds())
	assert.Equal(t, true, streamInfo.ShardProcessed[*shard.ShardId])
}

func TestProcessRecord(t *testing.T) {
	valA := "strA"
	numStr := "10.1"
	numVal := big.NewRat(101, 10)

	tableName := "testtable"
	cols := []string{"a", "b"}
	spSchema := ddl.CreateTable{
		Name:     tableName,
		ColNames: cols,
		ColDefs: map[string]ddl.ColumnDef{
			"a": {Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
			"b": {Name: "b", T: ddl.Type{Name: ddl.Numeric}},
		},
		Pks: []ddl.IndexKey{{Col: "a"}},
	}
	conv := buildConv(
		spSchema,
		schema.Table{
			Name:     tableName,
			ColNames: cols,
			ColDefs: map[string]schema.Column{
				"a": {Name: "a", Type: schema.Type{Name: typeString}},
				"b": {Name: "b", Type: schema.Type{Name: typeNumber}},
			},
			PrimaryKeys: []schema.Key{{Column: "a"}},
		},
	)

	record := &dynamodbstreams.Record{
		Dynamodb: &dynamodbstreams.StreamRecord{
			NewImage: map[string]*dynamodb.AttributeValue{
				"a": {S: &valA},
				"b": {N: &numStr},
			},
		},
		EventName: aws.String("INSERT"),
	}

	streamInfo := MakeStreamingInfo()
	streamInfo.Records[tableName] = make(map[string]int64)
	writes := 0
	streamInfo.write = func(m *sp.Mutation) error {
		writes++
		assert.Equal(t, m, sp.Insert(tableName, []string{"a", "b"}, []interface{}{valA, *numVal}))
		return nil
	}
	ProcessRecord(conv, streamInfo, record, tableName)

	// Check if call was successful.
	assert.Equal(t, 1, writes)
}

func Test_getMutation(t *testing.T) {
	srcTable := "testtable_src"
	spTable := "testtable_sp"
	spCols := []string{"a", "b", "c", "d"}
	srcSchema := schema.Table{
		Name:     srcTable,
		ColNames: spCols,
		ColDefs: map[string]schema.Column{
			"a": {Name: "a", Type: schema.Type{Name: typeNumber}},
			"b": {Name: "b", Type: schema.Type{Name: typeString}},
			"c": {Name: "c", Type: schema.Type{Name: typeBool}},
			"d": {Name: "d", Type: schema.Type{Name: typeString}},
		},
		PrimaryKeys: []schema.Key{schema.Key{Column: "d"}, schema.Key{Column: "b"}},
	}

	type args struct {
		eventName string
		srcTable  string
		spTable   string
		spCols    []string
		spVals    []interface{}
		srcSchema schema.Table
	}
	tests := []struct {
		name  string
		args  args
		wantM *sp.Mutation
	}{
		{
			name: "test for checking insert/update mutations",
			args: args{
				eventName: "INSERT",
				srcTable:  srcTable,
				spTable:   spTable,
				spCols:    spCols,
				spVals:    []interface{}{25, "key1", true, "key2", 3},
				srcSchema: srcSchema,
			},
			wantM: sp.Insert(spTable, spCols, []interface{}{25, "key1", true, "key2", 3}),
		},
		{
			name: "test for checking delete mutations",
			args: args{
				eventName: "REMOVE",
				srcTable:  srcTable,
				spTable:   spTable,
				spCols:    spCols,
				spVals:    []interface{}{nil, "key1", nil, "key2", nil},
				srcSchema: srcSchema,
			},
			wantM: sp.Delete(spTable, sp.Key{"key2", "key1"}),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotM := getMutation(tt.args.eventName, tt.args.srcTable, tt.args.spTable, tt.args.spCols, tt.args.spVals, tt.args.srcSchema); !reflect.DeepEqual(gotM, tt.wantM) {
				t.Errorf("CreateMutation() = %v, want %v", gotM, tt.wantM)
			}
		})
	}
}

func Test_writeRecord(t *testing.T) {
	streamInfo := MakeStreamingInfo()

	table := "testTable"
	streamInfo.Records[table] = make(map[string]int64)
	streamInfo.BadRecords[table] = make(map[string]int64)
	streamInfo.DroppedRecords[table] = make(map[string]int64)

	srcSchema := schema.Table{
		Name:     table,
		ColNames: []string{"e", "f"},
		ColDefs: map[string]schema.Column{
			"e": {Name: "e", Type: schema.Type{Name: typeString}},
			"f": {Name: "f", Type: schema.Type{Name: typeNumber}},
		},
		PrimaryKeys: []schema.Key{schema.Key{Column: "f"}, schema.Key{Column: "e"}},
	}
	tests := []struct {
		srcTable  string
		spTable   string
		eventName string
		spCols    []string
		spVals    []interface{}
		srcSchema schema.Table
	}{
		{
			srcTable:  table,
			spTable:   table,
			eventName: "INSERT",
			spCols:    []string{"a", "b"},
			spVals:    []interface{}{23, true},
		},
		{
			srcTable:  table,
			spTable:   table,
			eventName: "MODIFY",
			spCols:    []string{"c", "d"},
			spVals:    []interface{}{"goodTesting", 45},
		},
		{
			srcTable:  table,
			spTable:   table,
			eventName: "INSERT",
			spCols:    []string{"a", "b"},
			spVals:    []interface{}{27, false},
		},
		{
			srcTable:  table,
			spTable:   table,
			eventName: "MODIFY",
			spCols:    []string{"c", "d"},
			spVals:    []interface{}{"badTesting", 49},
		},
		{
			srcTable:  table,
			spTable:   table,
			eventName: "REMOVE",
			spCols:    []string{"e", "f"},
			spVals:    []interface{}{"goodTesting", 45},
			srcSchema: srcSchema,
		},
		{
			srcTable:  table,
			spTable:   table,
			eventName: "REMOVE",
			spCols:    []string{"e", "f"},
			spVals:    []interface{}{"badTesting", 55},
			srcSchema: srcSchema,
		},
	}
	goodMutations := []*sp.Mutation{
		sp.Insert(table, []string{"a", "b"}, []interface{}{23, true}),
		sp.InsertOrUpdate(table, []string{"c", "d"}, []interface{}{"goodTesting", 45}),
		sp.Delete(table, sp.Key{45, "goodTesting"}),
	}
	badMutations := []*sp.Mutation{
		sp.Insert(table, []string{"a", "b"}, []interface{}{27, false}),
		sp.InsertOrUpdate(table, []string{"c", "d"}, []interface{}{"badTesting", 49}),
		sp.Delete(table, sp.Key{55, "badTesting"}),
	}

	writeCount := int64(0)
	var mutationsWritten []*sp.Mutation
	var mutationsFailed []*sp.Mutation

	streamInfo.write = func(m *sp.Mutation) error {
		var err error
		writeCount++
		if intersect(m, badMutations) {
			err = errors.New("record not processed")
			mutationsFailed = append(mutationsFailed, m)
		} else {
			mutationsWritten = append(mutationsWritten, m)
		}
		time.Sleep(20 * time.Millisecond)
		return err
	}

	for _, data := range tests {
		writeRecord(streamInfo, data.srcTable, data.spTable, data.eventName, data.spCols, data.spVals, data.srcSchema)
	}

	// Check data written.
	assert.Equal(t, true, reflect.DeepEqual(mutationsWritten, goodMutations))

	// Check data rejected.
	assert.Equal(t, true, reflect.DeepEqual(mutationsFailed, badMutations))

	// Check total write calls.
	assert.Equal(t, int64(6), writeCount)

	// Check total dropped records.
	assert.Equal(t, int64(3), sumNestedMapValues(streamInfo.DroppedRecords))

	// Check dropped insert record.
	assert.Equal(t, "type=INSERT table=testTable cols=[a b] data=[27 false] error=record not processed", streamInfo.SampleBadWrites[0])
}

func intersect(m *sp.Mutation, mutationSet []*sp.Mutation) bool {
	for _, mutation := range mutationSet {
		if reflect.DeepEqual(m, mutation) {
			return true
		}
	}
	return false
}

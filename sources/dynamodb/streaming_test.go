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
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams/dynamodbstreamsiface"
	"github.com/stretchr/testify/assert"
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
	streamInfo := MakeInfo()
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
	streamInfo := MakeInfo()
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
	assert.Equal(t, true, streamInfo.ShardStatus[*shard.ShardId])

	wgShard.Add(1)
	ProcessShard(wgShard, streamInfo, nil, mockStreamClient, shard, streamArn, srcTable)
	assert.Equal(t, int64(1), streamInfo.TotalUnexpecteds())
	assert.Equal(t, true, streamInfo.ShardStatus[*shard.ShardId])
}

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
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams/dynamodbstreamsiface"

	"github.com/cloudspannerecosystem/harbourbridge/internal"
)

// NewDynamoDBStream initializes a new DynamoDB Stream for a table with NEW_AND_OLD_IMAGES
// StreamViewType. If there exists a stream for a given table then it must be of type
// NEW_IMAGE or NEW_AND_OLD_IMAGES otherwise streaming changes for this table won't be captured.
// It returns latest Stream Arn for the table along with any error if encountered.
func NewDynamoDBStream(client dynamodbiface.DynamoDBAPI, srcTable string) (string, error) {
	describeTableInput := &dynamodb.DescribeTableInput{
		TableName: aws.String(srcTable),
	}
	result, err := client.DescribeTable(describeTableInput)
	if err != nil {
		return "", fmt.Errorf("unexpected call to DescribeTable: %v", err)
	}
	if result.Table.StreamSpecification != nil {
		switch *result.Table.StreamSpecification.StreamViewType {
		case dynamodb.StreamViewTypeKeysOnly:
			return "", fmt.Errorf("error! there exists a stream with KEYS_ONLY StreamViewType")
		case dynamodb.StreamViewTypeOldImage:
			return "", fmt.Errorf("error! there exists a stream with OLD_IMAGE StreamViewType")
		default:
			return *result.Table.LatestStreamArn, nil
		}
	} else {
		streamSpecification := &dynamodb.StreamSpecification{
			StreamEnabled:  aws.Bool(true),
			StreamViewType: aws.String(dynamodb.StreamViewTypeNewAndOldImages),
		}
		updateTableInput := &dynamodb.UpdateTableInput{
			StreamSpecification: streamSpecification,
			TableName:           aws.String(srcTable),
		}
		res, err := client.UpdateTable(updateTableInput)
		if err != nil {
			return "", fmt.Errorf("unexpected call to UpdateTable: %v", err)
		}
		return *res.TableDescription.LatestStreamArn, nil
	}
}

// catchCtrlC catches the Ctrl+C signal if customer wants to exit.
func catchCtrlC(wg *sync.WaitGroup, streamInfo *StreamingInfo) {
	defer wg.Done()
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		streamInfo.UserExit = true
	}()
}

// ProcessStream processes the latest enabled DynamoDB Stream for a table. It searches
// for shards within stream and for each shard it creates a seperate working thread to
// process records within it.
func ProcessStream(wgStream *sync.WaitGroup, streamClient dynamodbstreamsiface.DynamoDBStreamsAPI, streamInfo *StreamingInfo, conv *internal.Conv, streamArn, srcTable string) {
	defer wgStream.Done()
	wgShard := &sync.WaitGroup{}

	var lastProcessedShardId *string = nil
	passAfterUserExit := false
	for {
		result, err := fetchShards(streamClient, lastProcessedShardId, streamArn)
		if err != nil {
			streamInfo.Unexpected(fmt.Sprintf("Couldn't fetch shards for table %s: %s", srcTable, err))
			break
		}
		for _, shard := range result.Shards {
			lastProcessedShardId = shard.ShardId

			wgShard.Add(1)
			go ProcessShard(wgShard, streamInfo, conv, streamClient, shard, streamArn, srcTable)
		}

		if result.LastEvaluatedShardId == nil && passAfterUserExit {
			break
		}
		if streamInfo.UserExit {
			passAfterUserExit = true
		} else if len(result.Shards) == 0 {
			time.Sleep(10 * time.Second)
		}
	}
	wgShard.Wait()
}

// fetchShards fetches latest unprocessed shards from a given DynamoDB Stream after the lastProcessedShardId.
func fetchShards(streamClient dynamodbstreamsiface.DynamoDBStreamsAPI, lastProcessedShardId *string, streamArn string) (*dynamodbstreams.StreamDescription, error) {
	describeStreamInput := &dynamodbstreams.DescribeStreamInput{
		ExclusiveStartShardId: lastProcessedShardId,
		StreamArn:             &streamArn,
	}
	result, err := streamClient.DescribeStream(describeStreamInput)
	if err != nil {
		return nil, fmt.Errorf("unexpected call to DescribeStream: %v", err)
	}
	return result.StreamDescription, nil
}

// checkTrimmedDataError checks if the error is an TrimmedDataAccessException.
func checkTrimmedDataError(err error) bool {
	return strings.Contains(err.Error(), "TrimmedDataAccessException")
}

// ProcessShard processes records within a shard starting from the first unexpired record. It
// doesn't start processing unless parent shard is processed. For closed shards this process is
// completed after processing all records but for open shards it keeps searching for new records
// until shards gets closed or customer calls for a exit.
func ProcessShard(wgShard *sync.WaitGroup, streamInfo *StreamingInfo, conv *internal.Conv, streamClient dynamodbstreamsiface.DynamoDBStreamsAPI, shard *dynamodbstreams.Shard, streamArn, srcTable string) {
	defer wgShard.Done()

	waitForParentShard(streamInfo, shard.ParentShardId)

	shardId := *shard.ShardId
	streamInfo.SetShardStatus(shardId, false)

	var lastEvaluatedSequenceNumber *string = nil
	passAfterUserExit := false
	retryCount := 0
	for {
		shardIterator, err := getShardIterator(streamClient, lastEvaluatedSequenceNumber, shardId, streamArn)
		if err != nil {
			if checkTrimmedDataError(err) {
				lastEvaluatedSequenceNumber = nil
				continue
			} else {
				streamInfo.Unexpected(fmt.Sprintf("Couldn't get shardIterator for table %s: %s", srcTable, err))
				break
			}
		}

		getRecordsOutput, err := getRecords(streamClient, shardIterator)
		if err != nil {
			// In case of closed shards, after all data records get expired it still returns a non-nil
			// shardIterator for GetShardIterator query. Using this shardIterator for GetRecords
			// API call results in TrimmedDataAccessException. This will result in same steps being
			// followed again and again. To handle this a retry limit of 5 is set.
			if checkTrimmedDataError(err) && retryCount < 5 {
				lastEvaluatedSequenceNumber = nil
				retryCount++
				continue
			} else {
				streamInfo.Unexpected(fmt.Sprintf("Couldn't fetch records for table %s: %s", srcTable, err))
				break
			}
		} else {
			retryCount = 0
		}

		records := getRecordsOutput.Records
		for _, record := range records {
			ProcessRecord(conv, streamInfo, record, srcTable)
			lastEvaluatedSequenceNumber = record.Dynamodb.SequenceNumber
		}

		if getRecordsOutput.NextShardIterator == nil || passAfterUserExit {
			break
		}
		if streamInfo.UserExit {
			passAfterUserExit = true
		} else if len(records) == 0 {
			time.Sleep(5 * time.Second)
		}
	}
	streamInfo.SetShardStatus(shardId, true)
}

// waitForParentShard checks every 6 seconds if parentShard is processed or
// not and waits as long as parent shard is not processed.
func waitForParentShard(streamInfo *StreamingInfo, parentShard *string) {
	if parentShard != nil {
		for {
			streamInfo.lock.Lock()
			done := streamInfo.ShardProcessed[*parentShard]
			streamInfo.lock.Unlock()
			if done {
				return
			} else {
				time.Sleep(6 * time.Second)
			}
		}
	}
}

// getShardIterator returns an iterator to find records based on the lastEvaluatedSequence number.
// If lastEvaluatedSequenceNumber is nil then it uses TrimHorizon as shardIterator type to point to first
// non-expired record otherwise it finds the first unprocessed record after lastEvaluatedSequence number using
// AfterSequenceNumber shardIterator type.
func getShardIterator(streamClient dynamodbstreamsiface.DynamoDBStreamsAPI, lastEvaluatedSequenceNumber *string, shardId, streamArn string) (*string, error) {
	var getShardIteratorInput *dynamodbstreams.GetShardIteratorInput
	if lastEvaluatedSequenceNumber == nil {
		getShardIteratorInput = &dynamodbstreams.GetShardIteratorInput{
			ShardId:           &shardId,
			ShardIteratorType: aws.String(dynamodbstreams.ShardIteratorTypeTrimHorizon),
			StreamArn:         &streamArn,
		}
	} else {
		getShardIteratorInput = &dynamodbstreams.GetShardIteratorInput{
			SequenceNumber:    lastEvaluatedSequenceNumber,
			ShardId:           &shardId,
			ShardIteratorType: aws.String(dynamodbstreams.ShardIteratorTypeAfterSequenceNumber),
			StreamArn:         &streamArn,
		}
	}
	result, err := streamClient.GetShardIterator(getShardIteratorInput)
	if err != nil {
		err = fmt.Errorf("unexpected call to GetShardIterator: %v", err)
		return nil, err
	}
	return result.ShardIterator, nil
}

// getRecords fetches the records from DynamoDB Streams by using the shardIterator.
func getRecords(streamClient dynamodbstreamsiface.DynamoDBStreamsAPI, shardIterator *string) (*dynamodbstreams.GetRecordsOutput, error) {
	getRecordsInput := &dynamodbstreams.GetRecordsInput{
		ShardIterator: shardIterator,
	}
	result, err := streamClient.GetRecords(getRecordsInput)
	if err != nil {
		err = fmt.Errorf("unexpected call to GetRecords: %v", err)
		return result, err
	}
	return result, nil
}

// ProcessRecord processes records retrieved from shards.
func ProcessRecord(conv *internal.Conv, streamInfo *StreamingInfo, record *dynamodbstreams.Record, srcTable string) {
	streamInfo.StatsAddRecord(srcTable, *record.EventName)
	// TODO(nareshz): work in progress
	streamInfo.StatsAddRecordProcessed()
}

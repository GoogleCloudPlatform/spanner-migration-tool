// Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package dynamodb

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	sp "cloud.google.com/go/spanner"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams/dynamodbstreamsiface"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	"github.com/cloudspannerecosystem/harbourbridge/common/constants"
	"github.com/cloudspannerecosystem/harbourbridge/common/metrics"
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/schema"
)

const (
	ESC        = 27
	retryLimit = 100
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

// clear erases the last printed line on the output file.
var clear = fmt.Sprintf("%c[%dA%c[2K", ESC, 1, ESC)

// updateProgress updates the customer every minute with number of records processed
// and if the current moment is an optimum condition for cutover or not.
func updateProgress(optimumCondition, firstCall bool, totalRecordsProcessed int64) {
	if !firstCall {
		fmt.Print(strings.Repeat(clear, 2))
	}
	fmt.Printf("Optimum time for switching to Cloud Spanner: %s\n", strconv.FormatBool(optimumCondition))
	fmt.Printf("Count of records processed: %s\n", strconv.FormatInt(totalRecordsProcessed, 10))
}

// cutoverHelper analyzes the records processed and makes a decision if current moment is
// optimum for switching to Cloud Spanner or not.
func cutoverHelper(wg *sync.WaitGroup, streamInfo *StreamingInfo) {
	defer wg.Done()

	updateProgress(false, true, streamInfo.recordsProcessed)

	timer := int64(0)
	firstFiveMin := int64(0)
	lastFiveMin := int64(0)
	tillLastMin := int64(0)
	arr := [5]int64{0, 0, 0, 0, 0}

	for {
		time.Sleep(60 * time.Second)
		if streamInfo.UserExit {
			break
		}
		counter := timer % 5

		lastFiveMin -= arr[counter]

		arr[counter] = streamInfo.recordsProcessed - tillLastMin
		tillLastMin += arr[counter]

		lastFiveMin += arr[counter]

		if timer < 5 {
			firstFiveMin += arr[counter]
		}

		lastMin := arr[counter]
		optimumCondition := ((lastFiveMin*100 <= 5*firstFiveMin) || (lastMin == 0))
		updateProgress(optimumCondition, false, tillLastMin)
		timer++
	}
}

// ProcessStream processes the latest enabled DynamoDB Stream for a table. It searches
// for shards within stream and for each shard it creates a seperate working thread to
// process records within it.
func ProcessStream(wgStream *sync.WaitGroup, streamClient dynamodbstreamsiface.DynamoDBStreamsAPI, streamInfo *StreamingInfo, conv *internal.Conv, streamArn, srcTable string) {
	defer wgStream.Done()
	wgShard := &sync.WaitGroup{}

	processingStarted := make(map[string]bool)

	passAfterUserExit := false
	for {
		shards, err := scanShards(streamClient, streamArn)
		if err != nil {
			streamInfo.Unexpected(fmt.Sprintf("Couldn't scan shards for table %s: %s", srcTable, err))
			break
		}
		for _, shard := range shards {
			shardId := *shard.ShardId
			if _, ok := processingStarted[shardId]; !ok {
				processingStarted[shardId] = false
				streamInfo.SetShardStatus(shardId, false)
			}
		}
		for _, shard := range shards {
			shardId := *shard.ShardId
			if !processingStarted[shardId] {
				processingStarted[shardId] = true

				wgShard.Add(1)
				go ProcessShard(wgShard, streamInfo, conv, streamClient, shard, streamArn, srcTable)
			}
		}

		if passAfterUserExit {
			break
		} else if streamInfo.UserExit {
			passAfterUserExit = true
		} else {
			time.Sleep(20 * time.Second)
		}
	}
	wgShard.Wait()
}

// scanShards fetches all the shards from a given DynamoDB Stream.
func scanShards(streamClient dynamodbstreamsiface.DynamoDBStreamsAPI, streamArn string) ([]*dynamodbstreams.Shard, error) {
	describeStreamInput := &dynamodbstreams.DescribeStreamInput{
		ExclusiveStartShardId: nil,
		StreamArn:             &streamArn,
	}
	var scanResult []*dynamodbstreams.Shard
	for {
		result, err := streamClient.DescribeStream(describeStreamInput)
		if err != nil {
			return nil, fmt.Errorf("unexpected call to DescribeStream: %v", err)
		}
		scanResult = append(scanResult, result.StreamDescription.Shards...)

		if result.StreamDescription.LastEvaluatedShardId == nil {
			break
		} else {
			describeStreamInput.ExclusiveStartShardId = result.StreamDescription.LastEvaluatedShardId
		}
	}
	return scanResult, nil
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
			done, ok := streamInfo.ShardProcessed[*parentShard]
			streamInfo.lock.Unlock()
			if !ok || done {
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

// ProcessRecord processes records retrieved from shards. It first converts the data
// to Spanner data (based on the source and Spanner schemas), and then writes that data
// to Cloud Spanner.
func ProcessRecord(conv *internal.Conv, streamInfo *StreamingInfo, record *dynamodbstreams.Record, srcTable string) {
	eventName := *record.EventName
	streamInfo.StatsAddRecord(srcTable, eventName)

	// todo - write a function that will compute schemas and colums and return
	tableId, err := internal.GetTableIdFromName(conv, srcTable)
	srcSchema, ok1 := conv.SrcSchema[tableId]
	spSchema, ok2 := conv.SpSchema[tableId]
	if err != nil || !ok1 || !ok2 {
		streamInfo.Unexpected(fmt.Sprintf("Can't get tableId and schemas for table %s: %v", srcTable, err))
		return
	}
	spTable := spSchema.Name
	spCols := []string{}
	srcCols := []string{}
	for _, colId := range spSchema.ColIds {
		spCols = append(spCols, spSchema.ColDefs[colId].Name)
		srcCols = append(srcCols, srcSchema.ColDefs[colId].Name)
	}

	var srcImage map[string]*dynamodb.AttributeValue
	if eventName == "REMOVE" {
		srcImage = record.Dynamodb.Keys
	} else {
		srcImage = record.Dynamodb.NewImage
	}

	spVals, badCols, srcStrVals := cvtRow(srcImage, srcSchema, spSchema, spCols)
	if len(badCols) == 0 {
		writeRecord(streamInfo, srcTable, spTable, eventName, spCols, spVals, srcSchema)
	} else {
		streamInfo.StatsAddBadRecord(srcTable, eventName)
		streamInfo.CollectBadRecord(eventName, srcTable, srcCols, srcStrVals)
	}
	streamInfo.StatsAddRecordProcessed()
}

// writeRecord handles creation and processing of mutation from the converted data to Cloud Spanner.
// If the writer which writes mutations to Cloud Spanner is not configured then it treats the record
// as a bad record.
func writeRecord(streamInfo *StreamingInfo, srcTable, spTable, eventName string, spCols []string, spVals []interface{}, srcSchema schema.Table) {
	if streamInfo.write == nil {
		msg := "Internal error: writeRecord called but writer not configured"
		streamInfo.StatsAddBadRecord(srcTable, eventName)
		streamInfo.Unexpected(msg)
	} else {
		m := getMutation(eventName, srcTable, spTable, spCols, spVals, srcSchema)
		err := writeMutation(m, streamInfo)
		if err != nil {
			streamInfo.StatsAddDroppedRecord(srcTable, eventName)
			streamInfo.CollectDroppedRecord(eventName, spTable, spCols, spVals, err)
		}
	}
}

// getMutation creates a mutation for writing to Cloud Spanner from the converted data.
func getMutation(eventName, srcTable, spTable string, spCols []string, spVals []interface{}, srcSchema schema.Table) (m *sp.Mutation) {
	if eventName == "INSERT" {
		m = sp.Insert(spTable, spCols, spVals)
	} else if eventName == "MODIFY" {
		m = sp.InsertOrUpdate(spTable, spCols, spVals)
	} else {
		m = removeMutation(srcSchema, spTable, srcTable, spVals)
	}
	return m
}

// removeMutation create a mutation from converted data for records of type 'REMOVE'.
// It ensures that when keyset is created the order for primary keys passed is same
// as the original database i.e. HASH Key, Partition Key.
func removeMutation(srcSchema schema.Table, spTable, srcTable string, spVals []interface{}) (m *sp.Mutation) {
	var srcKeys []string
	var reqSpVals []interface{}
	for i := 0; i < len(spVals); i++ {
		if spVals[i] == nil {
			continue
		}
		srcKeys = append(srcKeys, srcSchema.ColIds[i])
		reqSpVals = append(reqSpVals, spVals[i])
	}
	primaryKeys := srcSchema.PrimaryKeys
	if primaryKeys[0].ColId != srcKeys[0] {
		reqSpVals[0], reqSpVals[1] = reqSpVals[1], reqSpVals[0]
	}
	if len(reqSpVals) == 1 {
		m = sp.Delete(spTable, sp.Key{reqSpVals[0]})
	} else {
		m = sp.Delete(spTable, sp.Key{reqSpVals[0], reqSpVals[1]})
	}
	return m
}

// parentDataMissingError is used to track errors where insertions fail because of missing parent data.
//
// Note: If error code and description for parent row missing error is changed in future, then this
// function is subject to change.
func parentDataMissingError(err error) bool {
	return strings.Contains(err.Error(), "NotFound") && strings.Contains(err.Error(), "Parent row") && strings.Contains(err.Error(), "is missing")
}

// writeMutation handles writing of a mutation to Cloud Spanner. To handle insertions failing
// because of missing parent data, a retryLimit is set.
func writeMutation(m *sp.Mutation, streamInfo *StreamingInfo) error {
	var err error
	tryNum := 0
	for tryNum < retryLimit {
		err = streamInfo.write(m)
		if err == nil || !parentDataMissingError(err) {
			break
		}
		time.Sleep(4 * time.Second)
		tryNum++
	}
	return err
}

// setWriter initializes the write function used to write mutations to Cloud Spanner.
func setWriter(streamInfo *StreamingInfo, client *sp.Client, conv *internal.Conv) {
	streamInfo.write = func(m *sp.Mutation) error {
		migrationData := metrics.GetMigrationData(conv, "", "", constants.DataConv)
		serializedMigrationData, _ := proto.Marshal(migrationData)
		migrationMetadataValue := base64.StdEncoding.EncodeToString(serializedMigrationData)
		_, err := client.Apply(metadata.AppendToOutgoingContext(context.Background(), constants.MigrationMetadataKey, migrationMetadataValue), []*sp.Mutation{m})
		return err
	}
}

// fillConvWithStreamingStats passes the information related to processing of DynamoDB Streams
// to conv object for report and bad data file.
func fillConvWithStreamingStats(streamInfo *StreamingInfo, conv *internal.Conv) {
	// Pass Unexpected Conditions
	for unexpectedCondition, count := range streamInfo.Unexpecteds {
		conv.Unexpected(unexpectedCondition)
		if _, ok := conv.Stats.Unexpected[unexpectedCondition]; ok {
			conv.Stats.Unexpected[unexpectedCondition] += (count - 1)
		}
	}
	conv.Audit.StreamingStats.Streaming = true

	// Pass count stats to conv
	conv.Audit.StreamingStats.TotalRecords = streamInfo.Records
	conv.Audit.StreamingStats.BadRecords = streamInfo.BadRecords
	conv.Audit.StreamingStats.DroppedRecords = streamInfo.DroppedRecords

	// Pass badRecords and droppedRecords
	conv.Audit.StreamingStats.SampleBadRecords = streamInfo.SampleBadRecords
	conv.Audit.StreamingStats.SampleBadWrites = streamInfo.SampleBadWrites
}

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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInfo_TotalUnexpecteds(t *testing.T) {
	streamInfo := MakeStreamingInfo()
	streamInfo.Unexpected("testing-unexpecteds-faced-1")
	streamInfo.Unexpected("testing-unexpecteds-faced-2")
	assert.Equal(t, int64(2), streamInfo.TotalUnexpecteds())
}

func TestStreamingInfo_SetShardStatus(t *testing.T) {
	streamInfo := MakeStreamingInfo()
	shardId := "testShardId"
	streamInfo.SetShardStatus(shardId, true)
	assert.Equal(t, true, streamInfo.ShardProcessed[shardId])
}

func sumNestedMapValues(mp map[string]map[string]int64) int64 {
	n := int64(0)
	for _, x := range mp {
		for _, y := range x {
			n += y
		}
	}
	return n
}

func TestStreamingInfo_StatsAddRecord(t *testing.T) {
	streamInfo := MakeStreamingInfo()

	streamInfo.makeRecordMaps("testtable1")
	streamInfo.makeRecordMaps("testtable2")
	assert.NotNil(t, streamInfo.Records["testtable1"])

	streamInfo.StatsAddRecord("testtable1", "INSERT")
	streamInfo.StatsAddRecord("testtable2", "REMOVE")

	assert.Equal(t, int64(2), sumNestedMapValues(streamInfo.Records))
}

func TestStreamingInfo_StatsAddBadRecord(t *testing.T) {
	streamInfo := MakeStreamingInfo()
	tableName := "testtable"
	streamInfo.makeRecordMaps(tableName)

	streamInfo.StatsAddBadRecord(tableName, "INSERT")
	streamInfo.StatsAddBadRecord(tableName, "REMOVE")

	assert.Equal(t, int64(2), sumNestedMapValues(streamInfo.BadRecords))
}

func TestStreamingInfo_StatsAddDroppedRecord(t *testing.T) {
	streamInfo := MakeStreamingInfo()
	tableName := "testtable"
	streamInfo.makeRecordMaps(tableName)

	streamInfo.StatsAddDroppedRecord(tableName, "REMOVE")
	streamInfo.StatsAddDroppedRecord(tableName, "MODIFY")

	assert.Equal(t, int64(2), sumNestedMapValues(streamInfo.DroppedRecords))
}

func TestStreamingInfo_StatsAddRecordProcessed(t *testing.T) {
	streamInfo := MakeStreamingInfo()
	for i := 0; i < 3; i++ {
		streamInfo.StatsAddRecordProcessed()
	}
	assert.Equal(t, int64(3), streamInfo.recordsProcessed)
}

func TestStreamingInfo_makeRecordMaps(t *testing.T) {
	streamInfo := MakeStreamingInfo()
	table := "testtable"

	assert.Nil(t, streamInfo.Records[table])
	streamInfo.makeRecordMaps(table)
	assert.NotNil(t, streamInfo.Records[table])
}

func TestInfo_CollectBadRecord(t *testing.T) {
	streamInfo := MakeStreamingInfo()
	recordType := "REMOVE"
	tableName := "testtable"
	srcCols := []string{"a", "b", "c"}
	srcVals := []string{"231", "34", "null"}
	streamInfo.CollectBadRecord(recordType, tableName, srcCols, srcVals)

	expectedBadRecord := fmt.Sprintf("type=%s table=%s cols=%v data=%v", recordType, tableName, srcCols, srcVals)
	actualBadRecord := streamInfo.SampleBadRecords[0]

	assert.Equal(t, expectedBadRecord, actualBadRecord)
}

func TestInfo_CollectDroppedRecord(t *testing.T) {
	streamInfo := MakeStreamingInfo()
	recordType := "MODIFY"
	strVal := "1234"
	tableName := "testtable"
	cols := []string{"a", "b", "c"}
	vals := []interface{}{strVal, 23, "null"}
	err := errors.New("code:NotFound desc: data accessed not found")

	streamInfo.CollectDroppedRecord(recordType, tableName, cols, vals, err)
	expectedDroppedRecord := fmt.Sprintf("type=%s table=%s cols=%v data=%v error=%v", recordType, tableName, cols, vals, err)
	actualDroppedRecord := streamInfo.SampleBadWrites[0]

	assert.Equal(t, expectedDroppedRecord, actualDroppedRecord)
}

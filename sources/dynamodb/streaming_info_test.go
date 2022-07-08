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

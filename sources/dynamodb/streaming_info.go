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
	"sync"

	sp "cloud.google.com/go/spanner"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
)

// StreamingInfo contains information related to processing of DynamoDB Streams.
type StreamingInfo struct {
	Records          map[string]map[string]int64 // Tablewise count of records received from DynamoDB Streams, broken down by record type i.e. INSERT, MODIFY & REMOVE.
	BadRecords       map[string]map[string]int64 // Tablewise count of records not converted successfully, broken down by record type.
	DroppedRecords   map[string]map[string]int64 // Tablewise count of records successfully converted but failed to written on Spanner, broken down by record type.
	recordsProcessed int64                       // Count of total records processed to Cloud Spanner(includes records which generated error as well).
	ShardProcessed   map[string]bool             // Processing status of a shard, (default false i.e. unprocessed).
	UserExit         bool                        // Flag confirming if customer wants to exit or not, (false until user presses Ctrl+C).
	Unexpecteds      map[string]int64            // Count of unexpected conditions, broken down by condition description.
	write            func(m *sp.Mutation) error  // Writes a given mutation to Cloud Spanner.
	SampleBadRecords []string                    // Records that generated errors during conversion.
	SampleBadWrites  []string                    // Records that faced errors while writing to Cloud Spanner.
	lock             sync.Mutex
}

func MakeStreamingInfo() *StreamingInfo {
	return &StreamingInfo{
		Records:          make(map[string]map[string]int64),
		BadRecords:       make(map[string]map[string]int64),
		DroppedRecords:   make(map[string]map[string]int64),
		recordsProcessed: int64(0),
		ShardProcessed:   make(map[string]bool),
		Unexpecteds:      make(map[string]int64),
		UserExit:         false,
		lock:             sync.Mutex{},
	}
}

// makeRecordMaps initializes maps used to stores record count for
// a given table.
func (info *StreamingInfo) makeRecordMaps(srcTable string) {
	info.Records[srcTable] = make(map[string]int64)
	info.BadRecords[srcTable] = make(map[string]int64)
	info.DroppedRecords[srcTable] = make(map[string]int64)
}

// SetShardStatus changes the processing status of a shard.
//
// true -> shard processed and vice versa.
func (info *StreamingInfo) SetShardStatus(shardId string, status bool) {
	info.lock.Lock()
	info.ShardProcessed[shardId] = status
	info.lock.Unlock()
}

// StatsAddRecord increases the count of records read from DynamoDB Streams
// based on the table name and record type.
func (info *StreamingInfo) StatsAddRecord(srcTable, recordType string) {
	info.lock.Lock()
	info.Records[srcTable][recordType]++
	info.lock.Unlock()
}

// StatsAddBadRecord increases the count of records which are not successfully converted to
// Cloud Spanner supported data types based on the table name and record type.
func (info *StreamingInfo) StatsAddBadRecord(srcTable, recordType string) {
	info.lock.Lock()
	info.BadRecords[srcTable][recordType]++
	info.lock.Unlock()
}

// StatsAddDroppedRecord increases the count of records which failed while writing to Cloud Spanner
// based on the table name and record type.
func (info *StreamingInfo) StatsAddDroppedRecord(srcTable, recordType string) {
	info.lock.Lock()
	info.DroppedRecords[srcTable][recordType]++
	info.lock.Unlock()
}

// StatsAddRecordProcessed increases the count of total records processed to Cloud Spanner.
func (info *StreamingInfo) StatsAddRecordProcessed() {
	info.lock.Lock()
	info.recordsProcessed++
	info.lock.Unlock()
}

// Unexpected records stats about corner-cases and conditions
// that were not expected.
func (info *StreamingInfo) Unexpected(u string) {
	info.lock.Lock()
	internal.VerbosePrintf("Unexpected condition: %s\n", u)
	// Limit size of unexpected map. If over limit, then only
	// update existing entries.
	if _, ok := info.Unexpecteds[u]; ok || len(info.Unexpecteds) < 1000 {
		info.Unexpecteds[u]++
	}
	info.lock.Unlock()
}

// TotalUnexpecteds returns the total number of distinct unexpected conditions
// encountered during processing of DynamoDB Streams.
func (info *StreamingInfo) TotalUnexpecteds() int64 {
	return int64(len(info.Unexpecteds))
}

// CollectBadRecord collects a record if record is not successfully converted to Cloud Spanner
// supported data types.
func (info *StreamingInfo) CollectBadRecord(recordType, srcTable string, srcCols []string, vals []string) {
	info.lock.Lock()
	badRecord := fmt.Sprintf("type=%s table=%s cols=%v data=%v", recordType, srcTable, srcCols, vals)
	// Cap storage used by sampleBadRecords. Keep at least one bad record and at max 100.
	if len(info.SampleBadRecords) < 100 {
		info.SampleBadRecords = append(info.SampleBadRecords, badRecord)
	}
	info.lock.Unlock()
}

// CollectDroppedRecord collects a record if record faces an error while writing to Cloud Spanner.
func (info *StreamingInfo) CollectDroppedRecord(recordType, spTable string, spCols []string, spVals []interface{}, err error) {
	info.lock.Lock()
	droppedRecord := fmt.Sprintf("type=%s table=%s cols=%v data=%v error=%v", recordType, spTable, spCols, spVals, err)
	// Cap storage used by sampleBadWrites. Keep at least one dropped record and at max 100.
	if len(info.SampleBadWrites) < 100 {
		info.SampleBadWrites = append(info.SampleBadWrites, droppedRecord)
	}
	info.lock.Unlock()
}

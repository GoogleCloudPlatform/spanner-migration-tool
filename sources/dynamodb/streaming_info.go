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

	"github.com/cloudspannerecosystem/harbourbridge/internal"
)

// Info contains information related to processing of DynamoDB Streams.
type Info struct {
	Records          map[string]map[string]int64 // Tablewise count of records received from DynamoDB Streams, broken down by record type i.e. INSERT, MODIFY & REMOVE.
	BadRecords       map[string]map[string]int64 // Tablewise count of records not converted successfully, broken down by record type.
	DroppedRecords   map[string]map[string]int64 // Tablewise count of records successfully converted but failed to written on Spanner, broken down by record type.
	recordsProcessed int64                       // Count of total records processed to Cloud Spanner(includes records which generated error as well).
	ShardStatus      map[string]bool             // Processing status of a shard, (default false i.e. unprocessed).
	UserExit         bool                        // flag confirming if customer wants to exit or not, (false until user presses Ctrl+C).
	Unexpecteds      map[string]int64            // Count of unexpected conditions, broken down by condition description.
	write            func(m *sp.Mutation) error  // Writes a given mutation to Cloud Spanner.
	sampleBadRows    []badRow                    // Records of type INSERT that generated errors during conversion.
	sampleBadWrites  []string                    // Records of type INSERT that returned errors while writing to Cloud Spanner.
	lock             sync.Mutex
}

type badRow struct {
	srcTable string
	srcCols  []string
	vals     []string
}

func MakeInfo() *Info {
	return &Info{
		Records:          make(map[string]map[string]int64),
		BadRecords:       make(map[string]map[string]int64),
		DroppedRecords:   make(map[string]map[string]int64),
		recordsProcessed: int64(0),
		ShardStatus:      make(map[string]bool),
		Unexpecteds:      make(map[string]int64),
		UserExit:         false,
		lock:             sync.Mutex{},
	}
}

// SetShardStatus changes the status of a shard.
//
// true -> shard processed and vice versa.
func (info *Info) SetShardStatus(shardId string, status bool) {
	info.lock.Lock()
	info.ShardStatus[shardId] = status
	info.lock.Unlock()
}

// StatsAddRecord increases the count of records read from DynamoDB Streams
// based on the table name and record type.
func (info *Info) StatsAddRecord(srcTable, recordType string) {
	info.lock.Lock()
	info.Records[srcTable][recordType]++
	info.lock.Unlock()
}

// StatsAddRecordProcessed increases the count of total records processed to Cloud Spanner.
func (info *Info) StatsAddRecordProcessed() {
	info.lock.Lock()
	info.recordsProcessed++
	info.lock.Unlock()
}

// Unexpected records stats about corner-cases and conditions
// that were not expected.
func (info *Info) Unexpected(u string) {
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
func (info *Info) TotalUnexpecteds() int64 {
	return int64(len(info.Unexpecteds))
}

// TotalRecords returns the total number of records read from DynamoDB Streams.
func (info *Info) TotalRecords() int64 {
	n := int64(0)
	for _, c := range info.Records {
		for _, x := range c {
			n += x
		}
	}
	return n
}

// TotalDroppedRecords returns the total number of records read from DynamoDB Streams.
func (info *Info) TotalDroppedRecords() int64 {
	n := int64(0)
	for _, c := range info.DroppedRecords {
		for _, x := range c {
			n += x
		}
	}
	return n
}

// StatsAddBadRecord increases the count of records read from DynamoDB Streams
// based on the table name and record type.
func (info *Info) StatsAddBadRecord(srcTable, recordType string) {
	info.lock.Lock()
	info.BadRecords[srcTable][recordType]++
	info.lock.Unlock()
}

// StatsAddRecord increases the count of records read from DynamoDB Streams
// based on the table name and record type.
func (info *Info) StatsAddDroppedRecord(srcTable, recordType string) {
	info.lock.Lock()
	info.DroppedRecords[srcTable][recordType]++
	info.lock.Unlock()
}

func (info *Info) WriteBadRow(srcTable string, srcCols []string, vals []string) {
	info.lock.Lock()
	r := badRow{srcTable: srcTable, srcCols: srcCols, vals: vals}
	// Cap storage used by badRows. Keep at least one bad row and at max 100.
	if len(info.sampleBadRows) == 0 || len(info.sampleBadRows) < 100 {
		info.sampleBadRows = append(info.sampleBadRows, r)
	}
	info.lock.Unlock()
}

func (info *Info) WriteDroppedRow(spTable string, spCols []string, spVals []interface{}) {
	info.lock.Lock()
	droppedRow := fmt.Sprintf("table=%s cols=%v data=%v", spTable, spCols, spVals)
	// Cap storage used by badWrites. Keep at least one bad row and at max 100.
	if len(info.sampleBadWrites) == 0 || len(info.sampleBadWrites) < 100 {
		info.sampleBadWrites = append(info.sampleBadWrites, droppedRow)
	}
	info.lock.Unlock()
}

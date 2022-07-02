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
	"sync"

	"github.com/cloudspannerecosystem/harbourbridge/internal"
)

// Info contains information related to processing of DynamoDB Streams.
type Info struct {
	Records          map[string]map[string]int64 // Tablewise count of records received from DynamoDB Streams, broken down by record type i.e. INSERT, MODIFY & REMOVE.
	recordsProcessed int64                       // Count of total records processed to Cloud Spanner(includes records which generated error as well).
	ShardStatus      map[string]bool             // Processing status of a shard, (default false i.e. unprocessed).
	UserExit         bool                        // flag confirming if customer wants to exit or not, (false until user presses Ctrl+C).
	Unexpecteds      map[string]int64            // Count of unexpected conditions, broken down by condition description.
	lock             sync.Mutex
}

func MakeInfo() *Info {
	return &Info{
		Records:          make(map[string]map[string]int64),
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

// Records returns the total number of records read from DynamoDB Streams.
func (info *Info) TotalRecords() int64 {
	n := int64(0)
	for _, c := range info.Records {
		for _, x := range c {
			n += x
		}
	}
	return n
}

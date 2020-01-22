// Copyright 2019 Google LLC
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

// Package spanner provides high-level abstractions for working with
// Cloud Spanner that are not available from the core Cloud Spanner
// libraries.
package spanner

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	sp "cloud.google.com/go/spanner"
)

// Parameters used to control building batches to write to Spanner.
// Batches are built by adding rows until we hit one of the thresholds.
// Bigger batches are usually more efficient, but we need to be careful
// not to exceed Spanner's limits. Also, sending huge RPCs is potentially
// unreliable.
const (
	countThreshold = 10 * 1000    // Spanner per-operation limit is 20,000.
	byteThreshold  = 20 * 1 << 20 // Spanner per-operation limit is 100MB.
)

// BatchWriter accumulates rows of data (via AddRow) and assembles them
// into batches that it asynchronously writes to Spanner.  Rows are
// written to Spanner using insert semantics i.e. if a row already exists
// in the database, the row will fail with error 'AlreadyExists'.  If
// Spanner returns an error for a batch, BatchWriter splits the batch
// into smaller chunks to retry, as it attempts to isolate which row(s)
// in a batch is bad.  BatchWriter respects Spanner's limits on byte size
// and mutation count and has configurable limits on the number of
// in-progress writes, amount of data buffered and retry behavior.
// BatchWriter is not threadsafe: only one call to AddRow or Flush should
// be active at any time.  See ExampleBatchWriter (batchwriter_test.go)
// for sample usage code.
type BatchWriter struct {
	rows       []*row                     // Buffered rows.
	rBytes     int64                      // Estimate of bytes for buffered rows.
	rCount     int64                      // Mutation count for buffered rows.
	write      func([]*sp.Mutation) error // Typically a closure that calls client.Apply, but structured this way for testing.
	wg         sync.WaitGroup             // Tracks in-progress writes.
	writeLimit int64                      // Limit on number of in-progress writes.
	bytesLimit int64                      // Limit on bytes buffered. AddRow blocks if rBytes exceeded this value.
	retryLimit int64                      // Limit on retries.
	verbose    bool                       // If true, print out messages about each write batch.
	a          asyncState
}

type row struct {
	table string
	cols  []string
	vals  []interface{}
}

// Fields in this struct are modified asynchronously e.g. by go routines writing
// data to Spanner. Either hold a lock or use atomics, as detailed below.
//
// Note on terminology. A bad row is a row that generated an error. A dropped
// row is a row that we didn't write to Spanner (either because it generated
// an error, or because it was part of a batch that generate errors and we'd
// exhausted our retry budget and didn't split the batch and try again).
type asyncState struct {
	writes             int64            // Number of in-progress writes; access using atomic.
	retries            int64            // Number of retries; access using atomic.
	lock               sync.Mutex       // Protects errors and badRows
	errors             map[string]int64 // Errors encountered; protected by lock.
	sampleBadRows      []*row           // A sample of rows that generated errors; protected by lock.
	sampleBadRowsBytes int64            // Estimate of bytes for sampleBadRows; protected by lock.
	droppedRows        map[string]int64 // Count of dropped rows, broken down by table.
}

// BatchWriterConfig specifies parameters for configuring BatchWriter.
type BatchWriterConfig struct {
	WriteLimit int64                      // Limit on number of in-progress writes.
	BytesLimit int64                      // Limit on bytes buffered.
	RetryLimit int64                      // Limit on retries.
	Write      func([]*sp.Mutation) error // Function to call to write to Spanner (typically a closure that calls client.Apply).
	Verbose    bool                       // If true, print out messages about each write batch.
}

// NewBatchWriter returns a new BatchWriter with parameters defined by config.
func NewBatchWriter(config BatchWriterConfig) *BatchWriter {
	return &BatchWriter{
		write:      config.Write,
		writeLimit: config.WriteLimit,
		bytesLimit: config.BytesLimit,
		retryLimit: config.RetryLimit,
		verbose:    config.Verbose,
		a: asyncState{
			errors:      make(map[string]int64),
			droppedRows: make(map[string]int64),
		},
	}
}

// AddRow appends a new row of data to bw's buffer of rows. Depending on the
// state of BatchWriter, AddRow may immediately return, or it may initiate writes,
// or it may block (waiting for some of the writes already in progress to
// complete) and then initiate writes.
func (bw *BatchWriter) AddRow(table string, cols []string, vals []interface{}) {
	r := &row{table, cols, vals}
	bw.rows = append(bw.rows, r)
	bw.rBytes += byteSize(r)
	bw.rCount += int64(len(r.cols))
	bw.writeData()
}

// Flush initiates writes to Spanner of all buffered rows of data, and waits
// for them to complete.
func (bw *BatchWriter) Flush() {
	for len(bw.rows) > 0 {
		if atomic.LoadInt64(&bw.a.writes) < bw.writeLimit {
			m, count, bytes := bw.getBatch()
			if bw.verbose {
				fmt.Printf("Starting write of %d rows to Spanner (%d bytes, %d mutations) [%d in progress]\n",
					len(m), bytes, count, atomic.LoadInt64(&bw.a.writes))
			}
			bw.startWrite(m)
		} else {
			time.Sleep(10 * time.Millisecond)
		}
	}
	bw.wg.Wait()
}

// DroppedRowsByTable returns a map of tables to counts of dropped rows.
// Dropped rows are rows that were not written to Spanner.
func (bw *BatchWriter) DroppedRowsByTable() map[string]int64 {
	// Make a copy of bw.a.droppedRows since it is not thread-safe.
	m := make(map[string]int64)
	bw.a.lock.Lock()
	defer bw.a.lock.Unlock()

	for t, n := range bw.a.droppedRows {
		m[t] = n
	}
	return m
}

// SampleBadRows returns a string-formatted list of sample rows that
// generated errors. Returns at most n rows.
// Note that we split up batches to isolate errors. Each row returned
// by SampleBadRows generated an error when sent to Spanner as a
// single-row batch.
func (bw *BatchWriter) SampleBadRows(n int) []string {
	var l []string
	bw.a.lock.Lock()
	defer bw.a.lock.Unlock()
	for _, x := range bw.a.sampleBadRows {
		if len(l) >= n {
			break
		}
		l = append(l, fmt.Sprintf("table=%s cols=%v data=%v", x.table, x.cols, x.vals))
	}
	return l
}

// Errors returns a map summarizing errors encountered. Keys are error
// strings, and values are the count of that error.
func (bw *BatchWriter) Errors() map[string]int64 {
	// Make a copy of bw.a.errors since it is not thread-safe.
	m := make(map[string]int64)
	bw.a.lock.Lock()
	defer bw.a.lock.Unlock()
	for k, v := range bw.a.errors {
		m[k] = v
	}
	return m
}

func (bw *BatchWriter) getBadRowsForTest() []*row {
	return bw.a.sampleBadRows
}

// getBatch returns a slice of data from the front of bw.rows.  The slice
// returned is the largest one not exceeding countThreshold and byteThreshold.
func (bw *BatchWriter) getBatch() (rows []*row, count int64, bytes int64) {
	for i, _ := range bw.rows {
		c := count + int64(len(bw.rows[i].cols))
		b := bytes + byteSize(bw.rows[i])
		// If next row puts us over the thresholds, then stop. But make sure
		// we have at least one row. If a single row puts us over the
		// thresholds, there's not much we can do: we just try sending it to Spanner
		// (it might succeed, since our thresholds are conservative).
		if (c >= countThreshold || b >= byteThreshold) && len(rows) >= 1 {
			bw.rCount -= count
			bw.rBytes -= bytes
			bw.rows = bw.rows[i:]
			return rows, count, bytes
		}
		count = c
		bytes = b
		rows = append(rows, bw.rows[i])
	}
	bw.rCount = 0
	bw.rBytes = 0
	bw.rows = nil
	return rows, count, bytes
}

func (bw *BatchWriter) errorStats(rows []*row, err error, retry bool) {
	if bw.verbose {
		fmt.Printf("Error while writing %d rows to Spanner: %v\n", len(rows), err)
	}

	bw.a.lock.Lock()
	defer bw.a.lock.Unlock()

	bw.a.errors[err.Error()]++
	if retry {
		return
	}
	// All rows in r will be dropped.
	if len(rows) == 1 {
		// This is a confirmed bad row: add it to the badRows list.
		r := rows[0]
		n := byteSize(r)
		// Use bw.bytesLimit to cap storage used by badRows. Keep at least one bad row.
		if bw.a.sampleBadRowsBytes+n < bw.bytesLimit || len(bw.a.sampleBadRows) == 0 {
			bw.a.sampleBadRows = append(bw.a.sampleBadRows, r)
			bw.a.sampleBadRowsBytes += n
		}
	}
	for _, x := range rows {
		bw.a.droppedRows[x.table]++
	}
	return
}

// Note: doWriteAndHandleErrors must be thread-safe.
func (bw *BatchWriter) doWriteAndHandleErrors(rows []*row) {
	var m []*sp.Mutation
	for _, x := range rows {
		m = append(m, sp.Insert(x.table, x.cols, x.vals))
	}
	if err := bw.write(m); err != nil {
		hitRetryLimit := atomic.LoadInt64(&bw.a.retries) >= bw.retryLimit
		retry := len(rows) > 1 && !hitRetryLimit
		bw.errorStats(rows, err, retry)
		if !retry {
			if hitRetryLimit && bw.verbose {
				fmt.Printf("Have hit %d retries: will not do any more\n", atomic.LoadInt64(&bw.a.retries))
			}
			return
		}
		// Split into 10 pieces and retry. This is useful
		// if a batch contains a bad data row (Spanner
		// will fail the entire batch). In effect we attempt
		// to narrow down which row (or rows) are bad, and
		// write the 'good' rows to Spanner.
		k := 1 + len(rows)/10
		min := func(i, j int) int {
			if i <= j {
				return i
			}
			return j
		}
		for i := 0; i < len(rows); i += k {
			atomic.AddInt64(&bw.a.retries, 1)
			bw.doWriteAndHandleErrors(rows[i:min(i+k, len(rows))])
		}
	}
}

// Note: backgroundWrite must be thread-safe.
func (bw *BatchWriter) backgroundWrite(rows []*row) {
	defer bw.wg.Done()
	defer atomic.AddInt64(&bw.a.writes, -1)
	bw.doWriteAndHandleErrors(rows)
}

// startWrite initiates an asynchronous write of rows to Spanner.
func (bw *BatchWriter) startWrite(rows []*row) {
	bw.wg.Add(1)
	atomic.AddInt64(&bw.a.writes, 1)
	go bw.backgroundWrite(rows)
}

// writeData initiates writes to Spanner until either:
// a) we have less than a 'batch' to write, or
// b) we've hit writeLimit and we're under bytesLimit.
// It will block and re-try till either (a) or (b) holds.
func (bw *BatchWriter) writeData() {
	for bw.rCount > countThreshold || bw.rBytes > byteThreshold {
		if atomic.LoadInt64(&bw.a.writes) < bw.writeLimit {
			m, count, bytes := bw.getBatch()
			if bw.verbose {
				fmt.Printf("Starting write of %d rows to Spanner (%d bytes, %d mutations) [%d in progress]\n",
					len(m), bytes, count, atomic.LoadInt64(&bw.a.writes))
			}
			bw.startWrite(m)
		} else {
			if bw.rBytes < bw.bytesLimit {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func byteSize(r *row) int64 {
	n := int64(len(r.table))
	for _, c := range r.cols {
		n += int64(len(c))
	}
	for _, v := range r.vals {
		switch x := v.(type) {
		case string:
			n += int64(len(x))
		default:
			n += int64(unsafe.Sizeof(v))
		}
	}
	return n
}

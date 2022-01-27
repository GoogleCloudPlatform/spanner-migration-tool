// Copyright 2020 Google LLC
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

package writer

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	sp "cloud.google.com/go/spanner"
	"github.com/stretchr/testify/assert"
)

// TestFlush tests NewBatchWriter, AddRow and Flush.
func TestFlush(t *testing.T) {
	tests := []struct {
		name        string
		count       int
		rowSize     int
		writeLimit  int64
		badRowIndex map[int]bool // Identifies which rows are bad (by index).
	}{
		{name: "One write", count: 1, rowSize: 5, writeLimit: 40},
		{name: "Many writes", count: 50000, rowSize: 5, writeLimit: 40},      // Forces split based on mutation count.
		{name: "Large writes", count: 100, rowSize: 1 << 20, writeLimit: 40}, // Forces split based on byte size.
		{name: "Write limit", count: 50000, rowSize: 5, writeLimit: 5},       // Forces write-limiting.
		{name: "Bad rows", count: 50, rowSize: 5, writeLimit: 40, badRowIndex: map[int]bool{6: true, 17: true}},
	}
	config := BatchWriterConfig{
		BytesLimit: 100 << 20,
		Verbose:    false,
		RetryLimit: 1000,
	}
	for _, tc := range tests {
		data, limit := generateRows(tc.count, tc.rowSize)
		goodRows, badRows := partitionRows(tc.badRowIndex, data)
		badMutations := toMutations(badRows)
		mutex := &sync.Mutex{}
		var writeCount int64
		var rowsWritten []*sp.Mutation
		config.WriteLimit = tc.writeLimit
		config.Write = func(m []*sp.Mutation) error {
			var err error
			assert.LessOrEqual(t, len(m), limit, fmt.Sprintf("%s: Too many rows in write", tc.name))
			mutex.Lock()
			writeCount++
			// intersect could be slow if badMutations is more than a few rows.
			if intersect(m, badMutations) {
				err = errors.New("bad data")
			} else {
				rowsWritten = append(rowsWritten, m...)
			}
			mutex.Unlock()
			time.Sleep(20 * time.Millisecond) // Mimic a Spanner write.
			mutex.Lock()
			assert.LessOrEqual(t, writeCount, tc.writeLimit, fmt.Sprintf("%s: Too many pending writes", tc.name))
			writeCount--
			mutex.Unlock()
			return err
		}
		bw := NewBatchWriter(config)
		for _, x := range data {
			bw.AddRow(x.table, x.cols, x.vals)
		}
		bw.Flush()

		// Check data written.
		expected := toMutations(goodRows)
		actual := rowsWritten
		equalMutations(t, expected, actual, tc.name+" (good data)")

		// Check data rejected.
		expected = toMutations(badRows)
		actual = toMutations(bw.getBadRowsForTest()) // All go routines are done, so we can access bw internals.
		equalMutations(t, expected, actual, tc.name+" (bad data)")
	}
}

func TestDroppedRowsByTable(t *testing.T) {
	bw := NewBatchWriter(BatchWriterConfig{})
	bw.async.lock.Lock()
	bw.async.droppedRows["test1"] = 22
	bw.async.droppedRows["test2"] = 42
	bw.async.lock.Unlock()
	m := bw.DroppedRowsByTable()
	assert.Equal(t, 2, len(m))
	assert.Equal(t, int64(22), m["test1"])
	assert.Equal(t, int64(42), m["test2"])
}

func TestSampleBadRows(t *testing.T) {
	bw := NewBatchWriter(BatchWriterConfig{})
	bw.async.lock.Lock()
	bw.async.sampleBadRows = []*row{
		&row{"test", []string{"col1", "col2"}, []interface{}{"a", int64(42)}},
		&row{"test", []string{"col1", "col2"}, []interface{}{"b", int64(6)}},
	}
	bw.async.lock.Unlock()
	l := bw.SampleBadRows(1)
	assert.Equal(t, l, []string{"table=test cols=[col1 col2] data=[a 42]"})
}

func TestErrors(t *testing.T) {
	bw := NewBatchWriter(BatchWriterConfig{})
	bw.async.lock.Lock()
	bw.async.errors["error string 1"] = 22
	bw.async.errors["error string 2"] = 42
	bw.async.lock.Unlock()
	m := bw.Errors()
	assert.Equal(t, 2, len(m))
	assert.Equal(t, int64(22), m["error string 1"])
	assert.Equal(t, int64(42), m["error string 2"])
}

func ExampleBatchWriter() {
	write := func(m []*sp.Mutation) error {
		var err error
		// Code to write to Spanner e.g.
		// client := sp.NewClient(...)
		// _, err = client.Apply(context.Background(), m)
		return err
	}
	config := BatchWriterConfig{
		WriteLimit: 40,            // Limit on number of in-progress writes; 40 is a good default.
		BytesLimit: 100 * 1 << 20, // Limit on bytes buffered; 100MB is a good default.
		RetryLimit: 1000,          // Limit on retries (if a large set of mutations fails, we split it into smaller pieces and re-try).
		Write:      write,
		Verbose:    false, // Whether to print messages about each write.
	}
	writer := NewBatchWriter(config)

	// Code to generate rows of data to write to Spanner.
	cols := []string{"id", "quote"}
	vals := []interface{}{42, "The answer to life the universe and everything."}
	writer.AddRow("mytable", cols, vals)
	vals = []interface{}{6, "I am not a number."}
	writer.AddRow("mytable", cols, vals)
	// End of code to generate rows.

	writer.Flush() // Flush out remaining writes and wait for them to complete.
}

func generateRows(count int, size int) ([]*row, int) {
	var r []*row
	cols := []string{"a", "b"}
	val := strings.Repeat("x", size)
	for i := 0; i < count; i++ {
		// vals[0] serves as a unique id for each row.
		vals := []interface{}{i, val}
		r = append(r, &row{"table", cols, vals})
	}
	// Find the max number of rows in a write for the (fixed sized)
	// rows generated in this test data.
	limitCount := countThreshold / len(cols) // Compute limit based on mutation count.
	limitBytes := byteThreshold / (8 + size) // Compute limit based on mutation bytes.
	if limitCount <= limitBytes {
		return r, limitCount
	}
	return r, limitBytes
}

// equalMutations is a fast way to check that two slices of mutations are
// the same (reflect.deepEquals) except for re-ordering.
// Note that we should use assert.ElementsMatch, but it is very, very slow
// for the sizes of slices we use here (up to 50K). We convert the mutations
// to strings and sort them, and then use assert.Equal (ElementsMatch
// probably can't do this).
func equalMutations(t *testing.T, expected []*sp.Mutation, actual []*sp.Mutation, name string) {
	toStrings := func(m []*sp.Mutation) []string {
		var s []string
		for _, x := range m {
			s = append(s, fmt.Sprintf("%+v", x))
		}
		return s
	}
	e := toStrings(expected)
	a := toStrings(actual)
	sort.Slice(e, func(i, j int) bool { return e[i] < e[j] })
	sort.Slice(a, func(i, j int) bool { return a[i] < a[j] })
	assert.Equal(t, a, e, name)
}

// intersect returns true if m1 and m2 have a common element.
func intersect(m1 []*sp.Mutation, m2 []*sp.Mutation) bool {
	if m1 == nil || m2 == nil {
		return false
	}
	for _, x := range m1 {
		for _, y := range m2 {
			if reflect.DeepEqual(x, y) {
				return true
			}
		}
	}
	return false
}

func toMutations(r []*row) []*sp.Mutation {
	var m []*sp.Mutation
	for _, x := range r {
		m = append(m, sp.Insert(x.table, x.cols, x.vals))
	}
	return m
}

// partitionRows splits data into goodRows and badRows based on badRowIndex,
// which specifies the indices of bad rows.
func partitionRows(badRowIndex map[int]bool, data []*row) (goodRows []*row, badRows []*row) {
	for i := range data {
		if badRowIndex[i] {
			badRows = append(badRows, data[i])
		} else {
			goodRows = append(goodRows, data[i])
		}
	}
	return goodRows, badRows
}

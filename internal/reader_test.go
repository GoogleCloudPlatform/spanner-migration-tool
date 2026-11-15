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

package internal

import (
	"errors"
	"bufio"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestReadLine tests both NewReader and ReadLine.
func TestReadLine(t *testing.T) {
	type expected struct {
		data   string
		eof    bool
		line   int
		offset int
	}
	tests := []struct {
		name     string
		in       string
		expected []expected
	}{
		{name: "basic", in: "12345\n123\n", expected: []expected{
			{"12345\n", false, 2, 7},
			{"123\n", false, 3, 11},
			{"", true, 3, 11},
		}},
		{name: "no newline", in: "123456789\n12", expected: []expected{
			{"123456789\n", false, 2, 11},
			{"12", true, 2, 13},
			{"", true, 2, 13},
		}},
	}
	for _, tc := range tests {
		r := NewReader(bufio.NewReader(strings.NewReader(tc.in)), nil)
		assert.Equal(t, 1, r.LineNumber, tc.name)
		assert.Equal(t, 1, r.Offset, tc.name)
		for _, x := range tc.expected {
			data := string(r.ReadLine())
			assert.Equal(t, x.data, data, tc.name)
			assert.Equal(t, x.eof, r.EOF, tc.name)
			assert.Equal(t, x.line, r.LineNumber, tc.name)
			assert.Equal(t, x.offset, r.Offset, tc.name)
		}
	}
}

// errReader returns a non-EOF error on every read.
type errReader struct{}

func (errReader) Read(p []byte) (int, error) { return 0, errors.New("boom") }

// A non-EOF read error must set EOF so consumers looping until EOF terminate
// (the type treats read errors as end-of-input).
func TestReadLine_NonEOFErrorSetsEOF(t *testing.T) {
	r := NewReader(bufio.NewReader(errReader{}), nil)
	got := r.ReadLine()
	if len(got) != 0 {
		t.Fatalf("ReadLine() on error = %q, want empty", got)
	}
	if !r.EOF {
		t.Fatalf("r.EOF = false after a read error, want true")
	}
	// A consumer loop must terminate rather than spin forever.
	iters := 0
	for !r.EOF {
		r.ReadLine()
		if iters++; iters > 1000 {
			t.Fatal("ReadLine loop did not terminate after a read error")
		}
	}
}

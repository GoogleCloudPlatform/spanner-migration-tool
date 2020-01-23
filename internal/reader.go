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

package internal

import (
	"bufio"
	"fmt"
	"io"
)

// Reader is a simple line-reader wrapper around bufio.Reader
// that provides line number, file offset, and cached eof state.
// Errors are printed (via fmt.Print) and then treated as eof.
type Reader struct {
	LineNumber int // Starting at line 1
	Offset     int // Character offset from start of input. Starts with character 1.
	EOF        bool
	r          *bufio.Reader
	progress   *Progress
}

// NewReader builds and returns an instance of Reader.
func NewReader(r *bufio.Reader, progress *Progress) *Reader {
	return &Reader{LineNumber: 1, Offset: 1, EOF: false, r: r, progress: progress}
}

// ReadLine returns a line of input.
func (r *Reader) ReadLine() []byte {
	if r.EOF {
		return []byte{}
	}
	b, err := r.r.ReadBytes('\n')
	if err == io.EOF {
		r.EOF = true
	} else if err != nil {
		fmt.Printf("Error reading input data: %v\n", err)
		return []byte{}
	}
	r.Offset += len(b)
	// If ReadBytes returns eof, then we didn't get a new line.
	if !r.EOF {
		r.LineNumber++
	}
	if r.progress != nil {
		r.progress.MaybeReport(int64(r.Offset - 1))
	}
	return b
}

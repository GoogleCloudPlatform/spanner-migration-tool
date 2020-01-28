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

// Package postgres implements PostgreSQL-specific schema conversion
// and data conversion for HarbourBridge.
package postgres

import (
	"fmt"
	"strings"

	pg_query "github.com/lfittl/pg_query_go"
	nodes "github.com/lfittl/pg_query_go/nodes"

	"harbourbridge/internal"
)

// ProcessPgDump reads pg_dump data from r and does schema or data conversion,
// depending on whether conv is configured for schema mode or data mode.
// In schema mode, ProcessPgDump incrementally builds a schema (updating conv).
// In data mode, ProcessPgDump uses this schema to convert PostgreSQL data
// and writes it to Spanner, using the data sink specified in conv.
func ProcessPgDump(conv *Conv, r *internal.Reader) {
	for {
		startLine := r.LineNumber
		startOffset := r.Offset
		b, stmts := readAndParseChunk(conv, r)
		ci := processStatements(conv, stmts)
		internal.VerbosePrintf("Parsed SQL command at line=%d/fpos=%d: %d stmts (%d lines, %d bytes) ci=%v\n", startLine, startOffset, len(stmts), r.LineNumber-startLine, len(b), ci != nil)
		if ci != nil {
			switch ci.stmt {
			case copyFrom:
				processCopyBlock(conv, ci, r)
			case insert:
				ProcessRow(conv, ci.spTable, ci.pgTable, ci.cols, ci.vals)
			}
		}
		if r.EOF {
			break
		}
	}
	if conv.schemaMode() {
		conv.AddPrimaryKeys()
	}
}

// ProcessRow converts a row of data and writes it out to Spanner.
// spTable and pgTable are the Spanner and PostgreSQL table names respectively
// (typically they are the same), cols are Spanner cols, and vals contains
// string data to be converted to appropriate types to send to Spanner.
// ProcessRow is only called in dataMode.
func ProcessRow(conv *Conv, spTable, pgTable string, cols, vals []string) {
	c, v, err := ConvertData(conv, spTable, pgTable, cols, vals)
	if err != nil {
		conv.unexpected(fmt.Sprintf("Error while converting data: %s\n", err))
		conv.statsAddBadRow(spTable, conv.dataMode())
		r := &row{table: spTable, cols: cols, vals: vals}
		bytes := byteSize(r)
		// Cap storage used by badRows. Keep at least one bad row.
		if len(conv.sampleBadRows.rows) == 0 || bytes+conv.sampleBadRows.bytes < conv.sampleBadRows.bytesLimit {
			conv.sampleBadRows.rows = append(conv.sampleBadRows.rows, r)
			conv.sampleBadRows.bytes += bytes
		}
	} else {
		if conv.dataSink == nil {
			msg := "Internal error: ProcessRow called but dataSink not configured"
			internal.VerbosePrintf("%s\n", msg)
			conv.unexpected(msg)
			conv.statsAddBadRow(spTable, conv.dataMode())
		} else {
			conv.dataSink(spTable, c, v)
			conv.statsAddGoodRow(spTable, conv.dataMode())
		}
	}
}

func byteSize(r *row) int64 {
	n := int64(len(r.table))
	for _, c := range r.cols {
		n += int64(len(c))
	}
	for _, v := range r.vals {
		n += int64(len(v))
	}
	return n
}

// readAndParseChunk parses a chunk of pg_dump data, returning the bytes read,
// the parsed AST (nil if nothing read), and whether we've hit end-of-file.
func readAndParseChunk(conv *Conv, r *internal.Reader) ([]byte, []nodes.Node) {
	var l [][]byte
	for {
		b := r.ReadLine()
		l = append(l, b)
		// If we see a semicolon or eof, we're likely to have a command, so try to parse it.
		// Note: we could just parse every iteration, but that would mean more attempts at parsing.
		if strings.Contains(string(b), ";") || r.EOF {
			n := 0
			for i := range l {
				n += len(l[i])
			}
			s := make([]byte, n)
			n = 0
			for i := range l {
				n += copy(s[n:], l[i])
			}
			tree, err := pg_query.Parse(string(s))
			if err == nil {
				return s, tree.Statements
			}
			// Likely causes of failing to parse:
			// a) complex statements with embedded semicolons e.g. 'CREATE FUNCTION'
			// b) a semicolon embedded in a multi-line comment, or
			// c) a semicolon embedded a string constant or column/table name.
			// We deal with this case by reading another line and trying again.
			conv.stats.reparsed++
		}
		if r.EOF {
			if len(l) != 0 {
				fmt.Printf("Error parsing last %d line(s) of input\n", len(l))
			}
			return nil, nil
		}
	}
}

func processCopyBlock(conv *Conv, c *copyOrInsert, r *internal.Reader) {
	internal.VerbosePrintf("Parsing COPY-FROM stdin block starting at line=%d/fpos=%d\n", r.LineNumber, r.Offset)
	for {
		b := r.ReadLine()
		if string(b) == "\\.\n" {
			internal.VerbosePrintf("Parsed COPY-FROM stdin block ending at line=%d/fpos=%d\n", r.LineNumber, r.Offset)
			return
		}
		if r.EOF {
			conv.unexpected("Reached eof while parsing copy-block")
			return
		}
		conv.statsAddRow(c.spTable, conv.schemaMode())
		// We have to read the copy-block data so that we can process the remaining
		// pg_dump content. However, if we don't want the data, stop here.
		// In particular, avoid the strings.Split and ProcessRow calls below, which
		// weill be expensive for huge datasets.
		if !conv.dataMode() {
			continue
		}
		// COPY-FROM blocks use tabs to separate data items. Note that space within data
		// items is significant e.g. if a table row contains data items "a ", " b "
		// it will be show in the COPY-FROM block as "a \t b ".
		ProcessRow(conv, c.spTable, c.pgTable, c.cols, strings.Split(strings.Trim(string(b), "\n"), "\t"))
	}
}

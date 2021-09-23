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

package mysql

import (
	"bufio"
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/stretchr/testify/assert"
)

func TestReport(t *testing.T) {
	s := `
   CREATE TABLE bad_schema (
      a float,
      b integer NOT NULL);
  CREATE TABLE default_value (
      a text,
      b bigint DEFAULT 42,
      PRIMARY KEY (a)
      );
  CREATE TABLE excellent_schema (
      a text,
      b bigint,
      PRIMARY KEY (a)
      );
  CREATE TABLE foreign_key (
      a text,
      b bigint,
      PRIMARY KEY (a),
      FOREIGN KEY (a) REFERENCES excellent_schema(a));
  CREATE TABLE no_pk (
      a bigint,
      b integer NOT NULL,
      c text);`
	conv := internal.MakeConv()
	conv.SetSchemaMode()
	ProcessMySQLDump(conv, internal.NewReader(bufio.NewReader(strings.NewReader(s)), nil))
	conv.SetDataMode()
	conv.Stats.Rows = map[string]int64{"bad_schema": 1000, "no_pk": 5000}
	conv.Stats.GoodRows = map[string]int64{"bad_schema": 990, "no_pk": 3000}
	conv.Stats.BadRows = map[string]int64{"bad_schema": 10, "no_pk": 2000}
	badWrites := map[string]int64{"bad_schema": 50, "no_pk": 0}
	conv.Stats.Unexpected["Testing unexpected messages"] = 5
	buf := new(bytes.Buffer)
	w := bufio.NewWriter(buf)
	internal.GenerateReport("mysqldump", conv, w, badWrites, true, true)
	w.Flush()
	// Print copy of report to stdout (shows up when running go test -v).
	fmt.Print(buf.String())
	// Do a dumb comparison with a static 'expected' string.
	// If 'expected' is painful to maintain, delete it and just
	// use this test as a smoke test and a way to see report output
	// for some canned/sample data.
	expected :=
		`----------------------------
Summary of Conversion
----------------------------
Schema conversion: GOOD (most columns mapped cleanly, but some missing primary keys).
Data conversion: POOR (66% of 6000 rows written to Spanner).

The remainder of this report provides stats on the mysqldump statements
processed, followed by a table-by-table listing of schema and data conversion
details. For background on the schema and data conversion process used, and
explanations of the terms and notes used in this report, see HarbourBridge's
README.

----------------------------
Statements Processed
----------------------------
Analysis of statements in mysqldump output, broken down by statement type.
  schema: statements successfully processed for Spanner schema information.
    data: statements successfully processed for data.
    skip: statements not relevant for Spanner schema or data.
   error: statements that could not be processed.
  --------------------------------------
  schema   data   skip  error  statement
  --------------------------------------
       5      0      0      0  CreateTableStmt
See https://github.com/pingcap/parser for definitions of statement types
(pingcap/parser is the library we use for parsing mysqldump output).

----------------------------
Table bad_schema
----------------------------
Schema conversion: GOOD (all columns mapped cleanly, but missing primary key).
Data conversion: OK (94% of 1000 rows written to Spanner).

Warning
1) Column 'synth_id' was added because this table didn't have a primary key.
   Spanner requires a primary key for every table.

Note
1) Some columns will consume more storage in Spanner e.g. for column 'a', source
   DB type float is mapped to Spanner type float64.

----------------------------
Table default_value
----------------------------
Schema conversion: POOR (many columns did not map cleanly).
Data conversion: NONE (no data rows found).

Warning
1) Some columns have default values which Spanner does not support e.g. column
   'b'.

----------------------------
Table excellent_schema
----------------------------
Schema conversion: EXCELLENT (all columns mapped cleanly).
Data conversion: NONE (no data rows found).

----------------------------
Table foreign_key
----------------------------
Schema conversion: EXCELLENT (all columns mapped cleanly).
Data conversion: NONE (no data rows found).

----------------------------
Table no_pk
----------------------------
Schema conversion: GOOD (all columns mapped cleanly, but missing primary key).
Data conversion: POOR (60% of 5000 rows written to Spanner).

Warning
1) Column 'synth_id' was added because this table didn't have a primary key.
   Spanner requires a primary key for every table.

Note
1) Some columns will consume more storage in Spanner e.g. for column 'b', source
   DB type int(11) is mapped to Spanner type int64.

----------------------------
Unexpected Conditions
----------------------------
For debugging only. This section provides details of unexpected conditions
encountered as we processed the mysqldump data. In particular, the AST node
representation used by the pingcap/parser library used for parsing
mysqldump output is highly permissive: almost any construct can appear at
any node in the AST tree. The list details all unexpected nodes and
conditions.
  --------------------------------------
   count  condition
  --------------------------------------
       5  Testing unexpected messages

`
	assert.Equal(t, expected, buf.String())
}

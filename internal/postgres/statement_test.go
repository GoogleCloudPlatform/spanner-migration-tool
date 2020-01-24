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

package postgres

import (
	"harbourbridge/spanner/ddl"
	"testing"

	pg_query "github.com/lfittl/pg_query_go"
	"github.com/stretchr/testify/assert"
)

// This is just a very basic smoke-test for processStatements.
// The real testing of processStatements happens in process_test.go
// via the public API ProcessPgDump (see TestProcessPgDump).
func TestProcessStatements(t *testing.T) {
	conv := MakeConv()
	conv.SetSchemaMode()
	s := "CREATE TABLE cart (productid text, userid text, quantity bigint);\n" +
		"ALTER TABLE ONLY cart ADD CONSTRAINT cart_pkey PRIMARY KEY (productid, userid);\n"
	tree, err := pg_query.Parse(s)
	assert.Nil(t, err, "Failed to parse")
	ci := processStatements(conv, tree.Statements)
	assert.Nil(t, ci, "Unexpected COPY-FROM or INSERT")
	assert.Equal(t, []string{"productid", "userid", "quantity"}, conv.spSchema["cart"].Cols)
	assert.Equal(t, 3, len(conv.spSchema["cart"].Cds))
	assert.Equal(t, []ddl.IndexKey{ddl.IndexKey{Col: "productid"}, ddl.IndexKey{Col: "userid"}}, conv.spSchema["cart"].Pks)
}

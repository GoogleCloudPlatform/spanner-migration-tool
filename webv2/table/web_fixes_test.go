// Copyright 2026 Google LLC
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

package table

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/schema"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/session"
	"github.com/stretchr/testify/assert"
)

func renameTestConv(srcNames, spNames map[string]string, checks []string) *internal.Conv {
	return renameTestConvDialect(constants.DIALECT_GOOGLESQL, srcNames, spNames, checks)
}

func renameTestConvDialect(dialect string, srcNames, spNames map[string]string, checks []string) *internal.Conv {
	srcCols := map[string]schema.Column{}
	spCols := map[string]ddl.ColumnDef{}
	colIds := []string{}
	for id, name := range srcNames {
		srcCols[id] = schema.Column{Id: id, Name: name}
	}
	for id, name := range spNames {
		spCols[id] = ddl.ColumnDef{Id: id, Name: name, T: ddl.Type{Name: ddl.Int64}}
		colIds = append(colIds, id)
	}
	var ccs []ddl.CheckConstraint
	for i, expr := range checks {
		ccs = append(ccs, ddl.CheckConstraint{Id: "ck" + string(rune('1'+i)), Name: "CK" + string(rune('1'+i)), Expr: expr})
	}
	return &internal.Conv{
		SpDialect: dialect,
		SrcSchema: map[string]schema.Table{"t1": {Id: "t1", Name: "ITEMS", ColDefs: srcCols}},
		SpSchema:  map[string]ddl.CreateTable{"t1": {Id: "t1", Name: "ITEMS", ColIds: colIds, ColDefs: spCols, CheckConstraints: ccs}},
	}
}

func TestRenameColumnInCheckConstraints(t *testing.T) {
	gsql, pg := constants.DIALECT_GOOGLESQL, constants.DIALECT_POSTGRESQL
	same := func(names ...string) map[string]string {
		m := map[string]string{}
		for i, n := range names {
			m["c"+string(rune('1'+i))] = n
		}
		return m
	}
	tests := []struct {
		name     string
		dialect  string
		srcNames map[string]string
		spNames  map[string]string
		check    string
		colId    string
		newName  string
		expected string
	}{
		// --- Basic references (GoogleSQL) ---
		// #1553: Oracle keeps the case the user typed in SEARCH_CONDITION.
		{"Oracle lowercase reference to uppercase column", gsql, same("ID", "QTY"), same("ID", "QTY"),
			"(qty > 0 AND QTY < 100 AND ID > 0)", "c2", "QUANTITY", "(QUANTITY > 0 AND QUANTITY < 100 AND ID > 0)"},
		{"Second rename matches the current Spanner name", gsql, same("ID", "qty"), same("ID", "quantity"),
			"(quantity > 0)", "c2", "item_qty", "(item_qty > 0)"},
		{"Similar column names are not touched", gsql, same("qty", "qty_max"), same("qty", "qty_max"),
			"(qty < qty_max)", "c1", "quantity", "(quantity < qty_max)"},
		{"Keyword-like column name next to IN list", gsql, same("status"), same("status"),
			"(STATUS IN ('A', 'B'))", "c1", "state", "(state IN ('A', 'B'))"},
		{"Qualified column reference", gsql, same("QTY"), same("QTY"),
			"(ITEMS.QTY > 0)", "c1", "QUANTITY", "(ITEMS.QUANTITY > 0)"},
		{"MySQL backtick-quoted identifier", gsql, same("qty"), same("qty"),
			"(`qty` > 0 AND `qty` < 10)", "c1", "quantity", "(`quantity` > 0 AND `quantity` < 10)"},
		{"Number literal is not a reference", gsql, same("e3"), same("e3"),
			"(e3 > 1e3)", "c1", "exp3", "(exp3 > 1e3)"},

		// --- String literals (GoogleSQL: '...' and "..." with backslash escapes) ---
		{"Single-quoted literal with doubled quote", gsql, same("STATUS", "ACTIVE"), same("STATUS", "ACTIVE"),
			"(STATUS IN ('active', 'inactive', 'it''s active') AND ACTIVE IN (0, 1))", "c2", "IS_ACTIVE",
			"(STATUS IN ('active', 'inactive', 'it''s active') AND IS_ACTIVE IN (0, 1))"},
		{"Backslash-escaped quote does not end the literal (review)", gsql, same("status", "qty", "note"), same("status", "qty", "note"),
			`(status <> 'don\'t' AND qty > 0 AND note <> 'ok')`, "c2", "quantity",
			`(status <> 'don\'t' AND quantity > 0 AND note <> 'ok')`},
		{"Column name inside a backslash-escaped literal is kept (review)", gsql, same("note", "qty"), same("note", "qty"),
			`(note <> 'don\'t use qty' AND qty > 0)`, "c2", "quantity",
			`(note <> 'don\'t use qty' AND quantity > 0)`},
		{"Escaped backslash before the closing quote", gsql, same("path", "qty"), same("path", "qty"),
			`(path <> 'C:\\' AND qty > 0)`, "c2", "quantity", `(path <> 'C:\\' AND quantity > 0)`},
		{"Double-quoted string in GoogleSQL is a value (review)", gsql, same("status", "active"), same("status", "active"),
			`(status IN ("active", "inactive") AND active IN (0, 1))`, "c2", "is_active",
			`(status IN ("active", "inactive") AND is_active IN (0, 1))`},

		// --- Other names ---
		{"Non-ASCII column name", gsql, same("größe"), same("größe"),
			"(größe > 0 AND GRÖSSE_X > 0)", "c1", "size", "(size > 0 AND GRÖSSE_X > 0)"},

		// --- Names with non-word characters (review) ---
		{"Source name ending with $", gsql, same("PRICE$", "PRICE$X"), same("PRICE_", "PRICE_X"),
			"(PRICE$ > 0 AND PRICE$X > 0)", "c1", "PRICE_AMT", "(PRICE_AMT > 0 AND PRICE$X > 0)"},
		{"Source name ending with #", gsql, same("COL#"), same("COL_"),
			"(COL# <> 0)", "c1", "COL_NUM", "(COL_NUM <> 0)"},
		{"Source name with $ is longer than the Spanner name", gsql, same("PRICE$"), same("PRICE"),
			"(PRICE$ > 0 AND PRICE > 1)", "c1", "AMT", "(AMT > 0 AND AMT > 1)"},

		// --- PostgreSQL dialect: "..." is an identifier, '' and E'...' literals ---
		{"PostgreSQL dialect quoted reference", pg, same("ID", "QTY"), same("ID", "QTY"),
			`("QTY" > 0)`, "c2", "QUANTITY", `("QUANTITY" > 0)`},
		{"PostgreSQL doubled quote inside literal", pg, same("note", "qty"), same("note", "qty"),
			`(note <> 'it''s qty' AND qty > 0)`, "c2", "quantity", `(note <> 'it''s qty' AND quantity > 0)`},
		{"PostgreSQL backslash is a plain character in standard strings", pg, same("path", "qty"), same("path", "qty"),
			`(path <> 'C:\' AND qty > 0)`, "c2", "quantity", `(path <> 'C:\' AND quantity > 0)`},
		{"PostgreSQL E-string with backslash escape", pg, same("note", "qty"), same("note", "qty"),
			`(note <> E'it\'s qty' AND qty > 0)`, "c2", "quantity", `(note <> E'it\'s qty' AND quantity > 0)`},
		{"PostgreSQL quoted source name starting with $ (review)", pg, same("$amount"), same("Aamount"),
			`("$amount" > 0)`, "c1", "amount", `("amount" > 0)`},
		{"PostgreSQL cast followed by a column reference", pg, same("t", "qty"), same("t", "qty"),
			`(t::time AND qty > 0)`, "c2", "quantity", `(t::time AND quantity > 0)`},
		{"PostgreSQL date_trunc second argument is the column", pg, same("d"), same("d"),
			`(date_trunc('day', d) = d)`, "c1", "day_col", `(date_trunc('day', day_col) = day_col)`},
		{"PostgreSQL INTERVAL without unit, then a column named day", pg, same("day", "ts"), same("day", "ts"),
			`((ts + INTERVAL '1 day') > ts AND ts + INTERVAL '1 day' > ts AND day > 0)`, "c1", "day_num",
			`((ts + INTERVAL '1 day') > ts AND ts + INTERVAL '1 day' > ts AND day_num > 0)`},

		// --- Columns that differ only by case: match case-sensitively ---
		{"Case-sensitive when another column differs only by case", pg, same("Code", "CODE"), same("Code", "CODE_2"),
			`("Code" <> '' AND "CODE" <> '')`, "c1", "code_a", `("code_a" <> '' AND "CODE" <> '')`},
		{"Source name kept when it differs only by case from another Spanner name (review)", pg,
			same("Code", "CODE"), same("Code_1", "CODE"),
			`("Code" <> '' AND "CODE" <> '')`, "c1", "code_a", `("code_a" <> '' AND "CODE" <> '')`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			conv := renameTestConvDialect(tc.dialect, tc.srcNames, tc.spNames, []string{tc.check})
			renameColumnInCheckConstraints(conv, "t1", tc.colId, tc.newName)
			assert.Equal(t, tc.expected, conv.SpSchema["t1"].CheckConstraints[0].Expr)
		})
	}
}

// Every CHECK constraint of the table is rewritten, and only the renamed column.
func TestRenameColumnInCheckConstraints_MultipleConstraints(t *testing.T) {
	conv := renameTestConv(map[string]string{"c1": "QTY", "c2": "PRICE"}, map[string]string{"c1": "QTY", "c2": "PRICE"},
		[]string{"(qty > 0)", "(PRICE > 0)", "(QTY < PRICE AND note <> 'qty')"})
	renameColumnInCheckConstraints(conv, "t1", "c1", "QUANTITY")
	var got []string
	for _, cc := range conv.SpSchema["t1"].CheckConstraints {
		got = append(got, cc.Expr)
	}
	assert.Equal(t, []string{"(QUANTITY > 0)", "(PRICE > 0)", "(QUANTITY < PRICE AND note <> 'qty')"}, got)
}

// #1553 review: swapping column names through a temporary name must keep each
// CHECK reference on its own column (the source name of c1 becomes c2's Spanner name).
func TestRenameColumnInCheckConstraints_SwapThroughTemporaryName(t *testing.T) {
	conv := renameTestConv(map[string]string{"c1": "a", "c2": "b"}, map[string]string{"c1": "a", "c2": "b"}, []string{"(a > 0 AND b < 10)"})
	rename := func(colId, newName string) {
		renameColumnInCheckConstraints(conv, "t1", colId, newName)
		sp := conv.SpSchema["t1"]
		col := sp.ColDefs[colId]
		col.Name = newName
		sp.ColDefs[colId] = col
		conv.SpSchema["t1"] = sp
	}
	rename("c1", "tmp")
	rename("c2", "a")
	rename("c1", "b")
	assert.Equal(t, "(b > 0 AND a < 10)", conv.SpSchema["t1"].CheckConstraints[0].Expr)
}

// #1555: In the PostgreSQL dialect the UI sends PostgreSQL type names; the new
// column must be stored with the standard type name so its length is printed.
func TestAddNewColumn_PostgreSQLDialectMapsTypeName(t *testing.T) {
	tests := []struct {
		name         string
		dialect      string
		payload      string
		expectedType ddl.Type
	}{
		{"PG VARCHAR keeps length", constants.DIALECT_POSTGRESQL, `{"Name": "NOTE", "Datatype": "VARCHAR", "Length": 20, "IsNullable": true}`, ddl.Type{Name: ddl.String, Len: 20}},
		{"PG INT8", constants.DIALECT_POSTGRESQL, `{"Name": "NOTE", "Datatype": "INT8", "IsNullable": true}`, ddl.Type{Name: ddl.Int64}},
		{"PG BOOL unchanged", constants.DIALECT_POSTGRESQL, `{"Name": "NOTE", "Datatype": "BOOL", "IsNullable": true}`, ddl.Type{Name: ddl.Bool}},
		{"GoogleSQL STRING unchanged", constants.DIALECT_GOOGLESQL, `{"Name": "NOTE", "Datatype": "STRING", "Length": 20, "IsNullable": true}`, ddl.Type{Name: ddl.String, Len: 20}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sessionState := session.GetSessionState()
			sessionState.Conv = &internal.Conv{
				Source:    constants.ORACLE,
				SpDialect: tc.dialect,
				SpSchema: map[string]ddl.CreateTable{
					"t1": {Id: "t1", Name: "ITEMS", ColIds: []string{"c1"}, ColDefs: map[string]ddl.ColumnDef{"c1": {Id: "c1", Name: "ID", T: ddl.Type{Name: ddl.Int64}}}},
				},
			}
			req, err := http.NewRequest("POST", "/AddColumn?table=t1", strings.NewReader(tc.payload))
			assert.NoError(t, err)
			rr := httptest.NewRecorder()
			http.HandlerFunc(AddNewColumn).ServeHTTP(rr, req)
			assert.Equal(t, http.StatusOK, rr.Code)

			sp := sessionState.Conv.SpSchema["t1"]
			newColId := sp.ColIds[len(sp.ColIds)-1]
			assert.Equal(t, tc.expectedType, sp.ColDefs[newColId].T)
			if tc.dialect == constants.DIALECT_POSTGRESQL && tc.expectedType.Name == ddl.String {
				assert.Equal(t, "VARCHAR(20)", sp.ColDefs[newColId].T.PGPrintColumnDefType(false))
			}
		})
	}
}

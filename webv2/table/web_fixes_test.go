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
		SrcSchema: map[string]schema.Table{"t1": {Id: "t1", Name: "ITEMS", ColDefs: srcCols}},
		SpSchema:  map[string]ddl.CreateTable{"t1": {Id: "t1", Name: "ITEMS", ColIds: colIds, ColDefs: spCols, CheckConstraints: ccs}},
	}
}

func TestRenameColumnInCheckConstraints(t *testing.T) {
	tests := []struct {
		name     string
		srcNames map[string]string
		spNames  map[string]string
		checks   []string
		colId    string
		newName  string
		expected []string
	}{
		{
			// #1553: Oracle keeps the case the user typed in SEARCH_CONDITION.
			name:     "Oracle lowercase reference to uppercase column",
			srcNames: map[string]string{"c1": "ID", "c2": "QTY"},
			spNames:  map[string]string{"c1": "ID", "c2": "QTY"},
			checks:   []string{"(qty > 0)", "(QTY < 100 AND ID > 0)"},
			colId:    "c2",
			newName:  "QUANTITY",
			expected: []string{"(QUANTITY > 0)", "(QUANTITY < 100 AND ID > 0)"},
		},
		{
			name:     "PostgreSQL dialect quoted reference",
			srcNames: map[string]string{"c1": "ID", "c2": "QTY"},
			spNames:  map[string]string{"c1": "ID", "c2": "QTY"},
			checks:   []string{`("QTY" > 0)`},
			colId:    "c2",
			newName:  "QUANTITY",
			expected: []string{`("QUANTITY" > 0)`},
		},
		{
			name:     "Second rename matches the current Spanner name",
			srcNames: map[string]string{"c1": "ID", "c2": "qty"},
			spNames:  map[string]string{"c1": "ID", "c2": "quantity"},
			checks:   []string{"(quantity > 0)"},
			colId:    "c2",
			newName:  "item_qty",
			expected: []string{"(item_qty > 0)"},
		},
		{
			name:     "Similar column names are not touched",
			srcNames: map[string]string{"c1": "qty", "c2": "qty_max"},
			spNames:  map[string]string{"c1": "qty", "c2": "qty_max"},
			checks:   []string{"(qty < qty_max)"},
			colId:    "c1",
			newName:  "quantity",
			expected: []string{"(quantity < qty_max)"},
		},
		{
			// Columns differing only by case (e.g. PostgreSQL "Code" and "CODE"): match case-sensitively.
			name:     "Case-sensitive when another column differs only by case",
			srcNames: map[string]string{"c1": "Code", "c2": "CODE"},
			spNames:  map[string]string{"c1": "Code", "c2": "CODE_2"},
			checks:   []string{`("Code" <> '' AND "CODE" <> '')`},
			colId:    "c1",
			newName:  "code_a",
			expected: []string{`("code_a" <> '' AND "CODE" <> '')`},
		},
		{
			// A literal that matches the column name in a different case is a value, not a reference.
			name:     "String literals are not rewritten",
			srcNames: map[string]string{"c1": "STATUS", "c2": "ACTIVE"},
			spNames:  map[string]string{"c1": "STATUS", "c2": "ACTIVE"},
			checks:   []string{"(STATUS IN ('active', 'inactive', 'it''s active') AND ACTIVE IN (0, 1))"},
			colId:    "c2",
			newName:  "IS_ACTIVE",
			expected: []string{"(STATUS IN ('active', 'inactive', 'it''s active') AND IS_ACTIVE IN (0, 1))"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			conv := renameTestConv(tc.srcNames, tc.spNames, tc.checks)
			renameColumnInCheckConstraints(conv, "t1", tc.colId, tc.newName)
			var got []string
			for _, cc := range conv.SpSchema["t1"].CheckConstraints {
				got = append(got, cc.Expr)
			}
			assert.Equal(t, tc.expected, got)
		})
	}
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

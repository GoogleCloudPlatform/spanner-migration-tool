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

package common

import (
	"fmt"
	"testing"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/schema"
	"github.com/stretchr/testify/assert"
)

// stdInfoSchemaStub is a minimal per-table source: one table with one column
// and no constraints, foreign keys or indexes.
type stdInfoSchemaStub struct {
	StandardInfoSchema // not called; ProcessTable only uses the methods below
}

func (stdInfoSchemaStub) GetConstraints(*internal.Conv, SchemaAndName) ([]string, []schema.CheckConstraint, map[string][]string, error) {
	return []string{"id"}, nil, nil, nil
}

func (stdInfoSchemaStub) GetForeignKeys(*internal.Conv, SchemaAndName) ([]schema.ForeignKey, error) {
	return nil, nil
}

func (stdInfoSchemaStub) GetColumns(*internal.Conv, SchemaAndName, map[string][]string, []string) (map[string]schema.Column, []string, error) {
	return map[string]schema.Column{"c1": {Name: "id", Id: "c1", Type: schema.Type{Name: "int"}}}, []string{"c1"}, nil
}

func (stdInfoSchemaStub) GetIndexes(*internal.Conv, SchemaAndName, map[string]string) ([]schema.Index, error) {
	return nil, nil
}

func (stdInfoSchemaStub) GetTableName(schemaName, tableName string) string {
	return tableName
}

// disabledObjectsStub also implements DisabledObjectsProvider.
type disabledObjectsStub struct {
	stdInfoSchemaStub
	err error
}

func (s disabledObjectsStub) GetDisabledObjects(*internal.Conv, SchemaAndName) ([]string, []string, []string, error) {
	if s.err != nil {
		return nil, nil, nil, s.err
	}
	return []string{"fk_off"}, []string{"ck_off"}, []string{"ix_off"}, nil
}

func TestProcessTable_DisabledObjectsProvider(t *testing.T) {
	is := &InfoSchemaImpl{}
	table := SchemaAndName{Schema: "dbo", Name: "t"}

	// A source with the optional interface: the names land on the table.
	tbl, err := is.ProcessTable(internal.MakeConv(), table, disabledObjectsStub{})
	assert.NoError(t, err)
	assert.Equal(t, []string{"fk_off"}, tbl.DisabledForeignKeys)
	assert.Equal(t, []string{"ck_off"}, tbl.DisabledCheckConstraints)
	assert.Equal(t, []string{"ix_off"}, tbl.DisabledIndexes)

	// A source without it behaves as before.
	tbl, err = is.ProcessTable(internal.MakeConv(), table, stdInfoSchemaStub{})
	assert.NoError(t, err)
	assert.Equal(t, "t", tbl.Name)
	assert.Nil(t, tbl.DisabledForeignKeys)
	assert.Nil(t, tbl.DisabledCheckConstraints)
	assert.Nil(t, tbl.DisabledIndexes)

	// An error from the optional interface fails the table, like the other getters.
	_, err = is.ProcessTable(internal.MakeConv(), table, disabledObjectsStub{err: fmt.Errorf("boom")})
	assert.ErrorContains(t, err, "couldn't get disabled objects for table dbo.t: boom")
}

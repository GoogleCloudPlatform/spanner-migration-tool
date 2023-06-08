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

package common

import (
	"reflect"
	"testing"

	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/schema"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
)

func Test_quoteIfNeeded(t *testing.T) {

	type arg struct {
		s string
	}

	ExpectedTableName := "tableName"

	tests := []struct {
		name              string
		args              arg
		expectedTableName string
	}{
		{
			name: "quoteIfNeeded",
			args: arg{
				s: "tableName",
			},
			expectedTableName: ExpectedTableName,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := quoteIfNeeded(tt.args.s)
			if !reflect.DeepEqual(result, tt.expectedTableName) {
				t.Errorf("CvtForeignKeysHelper() = %v and wants %v", result, tt.expectedTableName)
			}
		})
	}
}

func Test_cvtPrimaryKeys(t *testing.T) {
	conv := internal.Conv{
		SrcSchema: map[string]schema.Table{
			"t1": {
				Name:   "table1",
				Id:     "t1",
				ColIds: []string{"c1", "c2"},
				ColDefs: map[string]schema.Column{
					"c1": {Name: "a", Id: "c1", Type: schema.Type{Name: ddl.String, Mods: []int64{255}}},
					"c2": {Name: "b", Id: "c2", Type: schema.Type{Name: ddl.Numeric, Mods: []int64{6, 4}}},
				},
				ForeignKeys: []schema.ForeignKey{{Name: "fk1", Id: "f1", ColumnNames: []string{"a"}, ColIds: []string{"c1"}, ReferTableId: "t2", ReferColumnIds: []string{"c3"}, ReferTableName: "table2", ReferColumnNames: []string{"c"}}},
			},
			"t2": {
				Name:   "table2",
				Id:     "t2",
				ColIds: []string{"c3"},
				ColDefs: map[string]schema.Column{
					"c3": {Name: "c", Id: "c3", Type: schema.Type{Name: ddl.String, Mods: []int64{255}}},
				},
			},
		},
		UsedNames: map[string]bool{},
	}
	srcTableId := "t1"
	srcKeys := []schema.Key{
		{
			ColId: "c1",
			Desc:  true,
			Order: 1,
		},
	}
	resultIndexKey := []ddl.IndexKey{{
		ColId: "c1",
		Desc:  true,
		Order: 1,
	},
	}

	type arg struct {
		conv       *internal.Conv
		srcTableTd string
		srcKeys    []schema.Key
	}

	tests := []struct {
		name             string
		args             arg
		expectedIndexKey []ddl.IndexKey
	}{
		{
			name: "Creating PrimaryKeys for the tables",
			args: arg{
				conv:       &conv,
				srcTableTd: srcTableId,
				srcKeys:    srcKeys,
			},
			expectedIndexKey: resultIndexKey,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := cvtPrimaryKeys(tt.args.conv, tt.args.srcTableTd, tt.args.srcKeys)
			if !reflect.DeepEqual(result, tt.expectedIndexKey) {
				t.Errorf("CvtForeignKeysHelper() = %v and wants %v", result, tt.expectedIndexKey)
			}
		})
	}
}

func Test_cvtForeignKeys(t *testing.T) {
	conv := internal.Conv{
		SrcSchema: map[string]schema.Table{
			"t1": {
				Name:   "table1",
				Id:     "t1",
				ColIds: []string{"c1", "c2"},
				ColDefs: map[string]schema.Column{
					"c1": {Name: "a", Id: "c1", Type: schema.Type{Name: ddl.String, Mods: []int64{255}}},
					"c2": {Name: "b", Id: "c2", Type: schema.Type{Name: ddl.Numeric, Mods: []int64{6, 4}}},
				},
				ForeignKeys: []schema.ForeignKey{{Name: "fk1", Id: "f1", ColumnNames: []string{"a"}, ColIds: []string{"c1"}, ReferTableId: "t2", ReferColumnIds: []string{"c3"}, ReferTableName: "table2", ReferColumnNames: []string{"c"}}},
			},
			"t2": {
				Name:   "table2",
				Id:     "t2",
				ColIds: []string{"c3"},
				ColDefs: map[string]schema.Column{
					"c3": {Name: "c", Id: "c3", Type: schema.Type{Name: ddl.String, Mods: []int64{255}}},
				},
			},
		},
		UsedNames: map[string]bool{},
	}
	spTableName := "table1"
	srcTableId := "t1"
	srcKeys := []schema.ForeignKey{
		{
			Name:             "fk1",
			ColIds:           []string{"c1"},
			ColumnNames:      []string{"a"},
			ReferTableId:     "t2",
			ReferTableName:   "table2",
			ReferColumnIds:   []string{"c3"},
			ReferColumnNames: []string{"c"},
			Id:               "f1",
		},
	}

	resultForeignKey := []ddl.Foreignkey{{
		Name:           "fk1",
		ColIds:         []string{"c1"},
		ReferTableId:   "t2",
		ReferColumnIds: []string{"c3"},
		Id:             "f1",
	},
	}

	type arg struct {
		conv        *internal.Conv
		spTableName string
		srcTableTd  string
		srcKey      []schema.ForeignKey
		isRestore   bool
	}

	tc := []struct {
		name               string
		args               arg
		expectedForeignKey []ddl.Foreignkey
		Error              error
	}{
		{
			name: "relating ForeignKey relation between tables",
			args: arg{
				conv:        &conv,
				spTableName: spTableName,
				srcTableTd:  srcTableId,
				srcKey:      srcKeys,
				isRestore:   false,
			},
			expectedForeignKey: resultForeignKey,
			Error:              nil,
		},
	}
	for _, tt := range tc {
		t.Run(tt.name, func(t *testing.T) {
			result := cvtForeignKeys(tt.args.conv, tt.args.spTableName, tt.args.srcTableTd, tt.args.srcKey, tt.args.isRestore)
			if !reflect.DeepEqual(result, tt.expectedForeignKey) {
				t.Errorf("CvtForeignKeysHelper() = %v and wants %v,%v", result, tt.expectedForeignKey, tt.Error)
			}
		})
	}
}

func Test_cvtIndexes(t *testing.T) {
	conv := internal.MakeConv()
	tableId := "t1"
	spCols := []string{"c1", "c2", "c3"}

	spSchema := []ddl.CreateIndex{
		{
			Name:    "indexName",
			TableId: "t1",
			Unique:  true,
			Keys: []ddl.IndexKey{
				{"c1", true, 1},
				{"c2", true, 2},
				{"c3", true, 3},
			},
			Id:              "t1",
			StoredColumnIds: []string{"c1", "c2", "c3"},
		},
	}

	srcIndexs := []schema.Index{
		{
			Name:   "indexName",
			Unique: true,
			Keys: []schema.Key{
				{"c1", true, 1},
				{"c2", true, 2},
				{"c3", true, 3},
			},
			Id:              "t1",
			StoredColumnIds: []string{"c1", "c2", "c3"},
		},
	}

	type arg struct {
		conv       *internal.Conv
		tableId    string
		srcIndexes []schema.Index
		spCols     []string
	}

	tests := []struct {
		name                string
		args                arg
		ExpectedCreateIndex []ddl.CreateIndex
	}{
		{
			name: "Adding IndexKey to the table",
			args: arg{
				conv:       conv,
				tableId:    tableId,
				srcIndexes: srcIndexs,
				spCols:     spCols,
			},
			ExpectedCreateIndex: spSchema,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := cvtIndexes(tt.args.conv, tt.args.tableId, tt.args.srcIndexes, tt.args.spCols)
			if !reflect.DeepEqual(got, tt.ExpectedCreateIndex) {
				t.Errorf("cvtIndexes() = %v and wants %v", got, tt.ExpectedCreateIndex)
			}
		})
	}
}

func Test_cvtForeignKeysForAReferenceTable(t *testing.T) {
	conv := internal.Conv{
		SrcSchema: map[string]schema.Table{
			"t1": {
				Name:   "table1",
				Id:     "t1",
				ColIds: []string{"c1", "c2"},
				ColDefs: map[string]schema.Column{
					"c1": {Name: "a", Id: "c1", Type: schema.Type{Name: ddl.String, Mods: []int64{255}}},
					"c2": {Name: "b", Id: "c2", Type: schema.Type{Name: ddl.Numeric, Mods: []int64{6, 4}}},
				},
				ForeignKeys: []schema.ForeignKey{{Name: "fk1", Id: "f1", ColumnNames: []string{"a"}, ColIds: []string{"c1"}, ReferTableId: "t2", ReferColumnIds: []string{"c3"}, ReferTableName: "table2", ReferColumnNames: []string{"c"}}},
			},
			"t2": {
				Name:   "table2",
				Id:     "t2",
				ColIds: []string{"c3"},
				ColDefs: map[string]schema.Column{
					"c3": {Name: "c", Id: "c3", Type: schema.Type{Name: ddl.String, Mods: []int64{255}}},
				},
			},
		},
		UsedNames: map[string]bool{},
		SpSchema: ddl.Schema{
			"t2": ddl.CreateTable{
				Name:   "table2",
				ColIds: []string{"c3"},
				ColDefs: map[string]ddl.ColumnDef{
					"c3": {Name: "c", Id: "c3", T: ddl.Type{Name: ddl.String, Len: 255}},
				},
				Id: "t2",
			},
		},
	}
	tableId := "t2"
	referTableId := "t2"
	srcKey := []schema.ForeignKey{
		{
			Name:             "fk1",
			ColIds:           []string{"c1"},
			ColumnNames:      []string{"a"},
			ReferTableId:     "t2",
			ReferTableName:   "table2",
			ReferColumnIds:   []string{"c3"},
			ReferColumnNames: []string{"c"},
			Id:               "f1",
		},
	}
	resultForeignKey := []ddl.Foreignkey{
		{
			Name:           "fk1",
			ColIds:         []string{"c1"},
			ReferTableId:   "t2",
			ReferColumnIds: []string{"c3"},
			Id:             "f1",
		},
		{
			Name:           "fk1",
			ColIds:         []string{"c1"},
			ReferTableId:   "t2",
			ReferColumnIds: []string{"c3"},
			Id:             "f1",
		},
	}
	spKey := []ddl.Foreignkey{{
		Name:           "fk1",
		ColIds:         []string{"c1"},
		ReferTableId:   "t2",
		ReferColumnIds: []string{"c3"},
		Id:             "f1",
	},
	}

	tc := []struct {
		conv               *internal.Conv
		TableId            string
		ReferTableId       string
		srcKeys            []schema.ForeignKey
		spKeys             []ddl.Foreignkey
		expectedForeignKey []ddl.Foreignkey
	}{
		{
			conv:               &conv,
			srcKeys:            srcKey,
			TableId:            tableId,
			ReferTableId:       referTableId,
			spKeys:             spKey,
			expectedForeignKey: resultForeignKey,
		},
	}

	for _, tt := range tc {
		t.Run("testCase for referTable", func(t *testing.T) {
			result := cvtForeignKeysForAReferenceTable(tt.conv, tt.TableId, tt.ReferTableId, tt.srcKeys, tt.spKeys)
			if !reflect.DeepEqual(result, tt.expectedForeignKey) {
				t.Errorf("CvtForeignKeysHelper() = %v and wants %v ", result, tt.expectedForeignKey)
			}
		})
	}
}

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
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/expressions_api"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/mocks"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/schema"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func Test_quoteIfNeeded(t *testing.T) {

	type arg struct {
		s string
	}

	tests := []struct {
		name              string
		args              arg
		expectedTableName string
	}{
		{
			name: "quoteIfNeeded",
			args: arg{
				s: "table Name",
			},
			expectedTableName: "\"table Name\"",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := quoteIfNeeded(tt.args.s)
			if !reflect.DeepEqual(result, tt.expectedTableName) {
				t.Errorf("quoteIfNeeded() returned incorrect output, got %v and want %v", result, tt.expectedTableName)
			}
		})
	}
}

func Test_cvtPrimaryKeys(t *testing.T) {
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
		srcKeys []schema.Key
	}

	tests := []struct {
		name             string
		args             arg
		expectedIndexKey []ddl.IndexKey
	}{
		{
			name: "Creating PrimaryKeys for the tables",
			args: arg{
				srcKeys: srcKeys,
			},
			expectedIndexKey: resultIndexKey,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := cvtPrimaryKeys(tt.args.srcKeys)
			if !reflect.DeepEqual(result, tt.expectedIndexKey) {
				t.Errorf("cvtPrimaryKeys() output doesn't match, got %v and want %v", result, tt.expectedIndexKey)
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
				ForeignKeys: []schema.ForeignKey{{Name: "fk1", Id: "f1", ColumnNames: []string{"a"}, ColIds: []string{"c1"}, ReferTableId: "t2", ReferColumnIds: []string{"c3"}, ReferTableName: "table2", ReferColumnNames: []string{"c"}, OnDelete: constants.FK_RESTRICT, OnUpdate: constants.FK_CASCADE}},
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
		UsedNames:    map[string]bool{},
		SchemaIssues: map[string]internal.TableIssues{},
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
			OnDelete:         constants.FK_RESTRICT,
			OnUpdate:         constants.FK_CASCADE,
		},
	}

	resultForeignKey := []ddl.Foreignkey{{
		Name:           "fk1",
		ColIds:         []string{"c1"},
		ReferTableId:   "t2",
		ReferColumnIds: []string{"c3"},
		Id:             "f1",
		OnDelete:       constants.FK_NO_ACTION,
		OnUpdate:       constants.FK_NO_ACTION,
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
			name: "creating foreign key in spanner schema",
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
				t.Errorf("cvtForeignKeys() output mismatch, got %v and want %v,%v", result, tt.expectedForeignKey, tt.Error)
			}
		})
	}
}

func Test_cvtIndexes(t *testing.T) {
	tableId := "t1"
	spColIds := []string{"c1", "c2", "c3"}

	spIndexes := []ddl.CreateIndex{
		{
			Name:    "indexName",
			TableId: "t1",
			Unique:  true,
			Keys: []ddl.IndexKey{
				{ColId: "c1", Desc: true, Order: 1},
				{ColId: "c2", Desc: true, Order: 2},
				{ColId: "c3", Desc: true, Order: 3},
			},
			Id:              "t1",
			StoredColumnIds: []string{"c1", "c2", "c3"},
		},
	}

	srcIndexes := []schema.Index{
		{
			Name:   "indexName",
			Unique: true,
			Keys: []schema.Key{
				{ColId: "c1", Desc: true, Order: 1},
				{ColId: "c2", Desc: true, Order: 2},
				{ColId: "c3", Desc: true, Order: 3},
			},
			Id:              "t1",
			StoredColumnIds: []string{"c1", "c2", "c3"},
		},
	}

	conv := internal.Conv{
		SrcSchema: map[string]schema.Table{
			"t1": {
				Name:   "table1",
				Id:     "t1",
				ColIds: []string{"c1", "c2", "c3"},
				ColDefs: map[string]schema.Column{
					"c1": {Name: "a", Id: "c1", Type: schema.Type{Name: ddl.String, Mods: []int64{255}}},
					"c2": {Name: "b", Id: "c2", Type: schema.Type{Name: ddl.String, Mods: []int64{255}}},
					"c3": {Name: "c", Id: "cc3", Type: schema.Type{Name: ddl.String, Mods: []int64{255}}},
				},
				Indexes: srcIndexes,
			},
		},
		UsedNames: map[string]bool{},
		SpSchema: ddl.Schema{
			"t1": ddl.CreateTable{
				Name:   "table1",
				ColIds: []string{"c1", "c2", "c3"},
				ColDefs: map[string]ddl.ColumnDef{
					"c1": {Name: "a", Id: "c1", T: ddl.Type{Name: ddl.String, Len: 255}},
					"c2": {Name: "b", Id: "c2", T: ddl.Type{Name: ddl.String, Len: 255}},
					"c3": {Name: "c", Id: "c3", T: ddl.Type{Name: ddl.String, Len: 255}},
				},
				Id: "t1",
			},
		},
	}

	type arg struct {
		conv       *internal.Conv
		tableId    string
		srcIndexes []schema.Index
		spColIds   []string
	}

	tests := []struct {
		name              string
		args              arg
		ExpectedSpIndexes []ddl.CreateIndex
	}{
		{
			name: "Adding Index to the table",
			args: arg{
				conv:       &conv,
				tableId:    tableId,
				srcIndexes: srcIndexes,
				spColIds:   spColIds,
			},
			ExpectedSpIndexes: spIndexes,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := cvtIndexes(tt.args.conv, tt.args.tableId, tt.args.srcIndexes, tt.args.spColIds, tt.args.conv.SpSchema[tt.args.tableId].ColDefs)
			if !reflect.DeepEqual(got, tt.ExpectedSpIndexes) {
				t.Errorf("cvtIndexes() = %v and wants %v", got, tt.ExpectedSpIndexes)
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
				ForeignKeys: []schema.ForeignKey{{Name: "fk1", Id: "f1", ColumnNames: []string{"a"}, ColIds: []string{"c1"}, ReferTableId: "t2", ReferColumnIds: []string{"c3"}, ReferTableName: "table2", ReferColumnNames: []string{"c"}, OnDelete: constants.FK_RESTRICT, OnUpdate: constants.FK_CASCADE}},
			},
			"t2": {
				Name:   "table2",
				Id:     "t2",
				ColIds: []string{"c3"},
				ColDefs: map[string]schema.Column{
					"c3": {Name: "c", Id: "c3", Type: schema.Type{Name: ddl.String, Mods: []int64{255}}},
				},
				ForeignKeys: []schema.ForeignKey{},
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
				Id:          "t2",
				ForeignKeys: []ddl.Foreignkey{},
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
			OnDelete:         constants.FK_RESTRICT,
			OnUpdate:         constants.FK_CASCADE,
		},
	}
	resultForeignKey := []ddl.Foreignkey{
		{
			Name:           "fk1",
			ColIds:         []string{"c1"},
			ReferTableId:   "t2",
			ReferColumnIds: []string{"c3"},
			Id:             "f1",
			OnDelete:       constants.FK_NO_ACTION,
			OnUpdate:       constants.FK_NO_ACTION,
		},
		{
			Name:           "fk1",
			ColIds:         []string{"c1"},
			ReferTableId:   "t2",
			ReferColumnIds: []string{"c3"},
			Id:             "f1",
			OnDelete:       constants.FK_NO_ACTION,
			OnUpdate:       constants.FK_NO_ACTION,
		},
	}
	spKey := []ddl.Foreignkey{{
		Name:           "fk1",
		ColIds:         []string{"c1"},
		ReferTableId:   "t2",
		ReferColumnIds: []string{"c3"},
		Id:             "f1",
		OnDelete:       constants.FK_NO_ACTION,
		OnUpdate:       constants.FK_NO_ACTION,
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
				t.Errorf("cvtForeignKeysForAReferenceTable() = %v and wants %v ", result, tt.expectedForeignKey)
			}
		})
	}
}

func Test_SchemaToSpannerSequenceHelper(t *testing.T) {
	expectedConv := internal.MakeConv()
	expectedConv.SpSequences["s1"] = ddl.Sequence{
		Name:             "Sequence1",
		Id:               "s1",
		SequenceKind:     "BIT REVERSED POSITIVE",
		SkipRangeMin:     "1",
		SkipRangeMax:     "2",
		StartWithCounter: "3",
	}
	tc := []struct {
		expectedConv *internal.Conv
		srcSequence  ddl.Sequence
	}{
		{
			expectedConv: expectedConv,
			srcSequence: ddl.Sequence{
				Name:             "Sequence1",
				Id:               "s1",
				SequenceKind:     constants.AUTO_INCREMENT,
				SkipRangeMin:     "1",
				SkipRangeMax:     "2",
				StartWithCounter: "3",
			},
		},
	}

	for _, tt := range tc {
		conv := internal.MakeConv()
		ss := SchemaToSpannerImpl{
			DdlV: &expressions_api.MockDDLVerifier{},
		}
		ss.SchemaToSpannerSequenceHelper(conv, tt.srcSequence)
		assert.Equal(t, expectedConv, conv)
	}
}

func Test_cvtCheckContraint(t *testing.T) {

	conv := internal.MakeConv()
	srcSchema := []schema.CheckConstraint{
		{
			Id:     "cc1",
			Name:   "check_1",
			Expr:   "age > 0",
			ExprId: "expr1",
		},
		{
			Id:     "cc2",
			Name:   "check_2",
			Expr:   "age < 99",
			ExprId: "expr2",
		},
		{
			Id:     "cc3",
			Name:   "@invalid_name", // incompatabile name
			Expr:   "age != 0",
			ExprId: "expr3",
		},
	}
	spSchema := []ddl.CheckConstraint{
		{
			Id:     "cc1",
			Name:   "check_1",
			Expr:   "age > 0",
			ExprId: "expr1",
		},
		{
			Id:     "cc2",
			Name:   "check_2",
			Expr:   "age < 99",
			ExprId: "expr2",
		},
		{
			Id:     "cc3",
			Name:   "Ainvalid_name",
			Expr:   "age != 0",
			ExprId: "expr3",
		},
	}
	result := cvtCheckConstraint(conv, srcSchema)
	assert.Equal(t, spSchema, result)
}

func TestSpannerSchemaApplyExpressions(t *testing.T) {
	makeConv := func() *internal.Conv {
		conv := internal.MakeConv()
		conv.SchemaIssues = make(map[string]internal.TableIssues)
		conv.SchemaIssues["table1"] = internal.TableIssues{
			ColumnLevelIssues: make(map[string][]internal.SchemaIssue),
		}
		conv.SpSchema = ddl.Schema{
			"table1": {
				ColDefs: map[string]ddl.ColumnDef{
					"col1": {},
				},
			},
		}
		return conv
	}

	makeResultConv := func(SpSchema ddl.Schema, SchemaIssues map[string]internal.TableIssues) *internal.Conv {
		conv := internal.MakeConv()
		conv.SpSchema = SpSchema
		conv.SchemaIssues = SchemaIssues
		return conv
	}

	testCases := []struct {
		name         string
		conv         *internal.Conv
		expressions  internal.VerifyExpressionsOutput
		expectedConv *internal.Conv
	}{
		{
			name: "successful default value application",
			conv: makeConv(),
			expressions: internal.VerifyExpressionsOutput{
				ExpressionVerificationOutputList: []internal.ExpressionVerificationOutput{
					{
						Result: true,
						ExpressionDetail: internal.ExpressionDetail{
							Type:         "DEFAULT",
							ExpressionId: "expr1",
							Expression:   "SELECT 1",
							Metadata:     map[string]string{"TableId": "table1", "ColId": "col1"},
						},
					},
				},
			},
			expectedConv: makeResultConv(
				ddl.Schema{
					"table1": {
						ColDefs: map[string]ddl.ColumnDef{
							"col1": {
								DefaultValue: ddl.DefaultValue{
									IsPresent: true,
									Value: ddl.Expression{
										ExpressionId: "expr1",
										Statement:    "SELECT 1",
									},
								},
							},
						},
					},
				}, map[string]internal.TableIssues{
					"table1": {
						ColumnLevelIssues: make(map[string][]internal.SchemaIssue),
					},
				}),
		},
		{
			name: "failed default value application",
			conv: makeConv(),
			expressions: internal.VerifyExpressionsOutput{
				ExpressionVerificationOutputList: []internal.ExpressionVerificationOutput{
					{
						Result: false,
						ExpressionDetail: internal.ExpressionDetail{
							Type:         "DEFAULT",
							ExpressionId: "expr1",
							Expression:   "SELECT 1",
							Metadata:     map[string]string{"TableId": "table1", "ColId": "col1"},
						},
					},
				},
			},
			expectedConv: makeResultConv(
				ddl.Schema{
					"table1": {
						ColDefs: map[string]ddl.ColumnDef{
							"col1": {},
						},
					},
				},
				map[string]internal.TableIssues{
					"table1": {
						ColumnLevelIssues: map[string][]internal.SchemaIssue{
							"col1": {internal.DefaultValue},
						},
					},
				}),
		},
		{
			name: "successful stored generated value application",
			conv: makeConv(),
			expressions: internal.VerifyExpressionsOutput{
				ExpressionVerificationOutputList: []internal.ExpressionVerificationOutput{
					{
						Result: true,
						ExpressionDetail: internal.ExpressionDetail{
							Type:         "STORED",
							ExpressionId: "expr2",
							Expression:   "col2 + 1",
							Metadata:     map[string]string{"TableId": "table1", "ColId": "col1"},
						},
					},
				},
			},
			expectedConv: makeResultConv(
				ddl.Schema{
					"table1": {
						ColDefs: map[string]ddl.ColumnDef{
							"col1": {
								GeneratedColumn: ddl.GeneratedColumn{
									IsPresent: true,
									Value: ddl.Expression{
										ExpressionId: "expr2",
										Statement:    "(col2 + 1)",
									},
									Type: ddl.GeneratedColStored,
								},
							},
						},
					},
				}, map[string]internal.TableIssues{
					"table1": {
						ColumnLevelIssues: make(map[string][]internal.SchemaIssue),
					},
				}),
		},
		{
			name: "failed stored generated value application",
			conv: makeConv(),
			expressions: internal.VerifyExpressionsOutput{
				ExpressionVerificationOutputList: []internal.ExpressionVerificationOutput{
					{
						Result: false,
						ExpressionDetail: internal.ExpressionDetail{
							Type:         "STORED",
							ExpressionId: "expr2",
							Expression:   "col2 + 1",
							Metadata:     map[string]string{"TableId": "table1", "ColId": "col1"},
						},
					},
				},
			},
			expectedConv: makeResultConv(
				ddl.Schema{
					"table1": {
						ColDefs: map[string]ddl.ColumnDef{
							"col1": {},
						},
					},
				},
				map[string]internal.TableIssues{
					"table1": {
						ColumnLevelIssues: map[string][]internal.SchemaIssue{
							"col1": {internal.GeneratedColumnValueError},
						},
					},
				}),
		},
		{
			name: "successful virtual generated value application",
			conv: makeConv(),
			expressions: internal.VerifyExpressionsOutput{
				ExpressionVerificationOutputList: []internal.ExpressionVerificationOutput{
					{
						Result: true,
						ExpressionDetail: internal.ExpressionDetail{
							Type:         "VIRTUAL",
							ExpressionId: "expr3",
							Expression:   "col2 + 2",
							Metadata:     map[string]string{"TableId": "table1", "ColId": "col1"},
						},
					},
				},
			},
			expectedConv: makeResultConv(
				ddl.Schema{
					"table1": {
						ColDefs: map[string]ddl.ColumnDef{
							"col1": {
								GeneratedColumn: ddl.GeneratedColumn{
									IsPresent: true,
									Value: ddl.Expression{
										ExpressionId: "expr3",
										Statement:    "(col2 + 2)",
									},
									Type: ddl.GeneratedColVirtual,
								},
							},
						},
					},
				}, map[string]internal.TableIssues{
					"table1": {
						ColumnLevelIssues: make(map[string][]internal.SchemaIssue),
					},
				}),
		},
		{
			name: "failed virtual generated value application",
			conv: makeConv(),
			expressions: internal.VerifyExpressionsOutput{
				ExpressionVerificationOutputList: []internal.ExpressionVerificationOutput{
					{
						Result: false,
						ExpressionDetail: internal.ExpressionDetail{
							Type:         "VIRTUAL",
							ExpressionId: "expr3",
							Expression:   "col2 + 2",
							Metadata:     map[string]string{"TableId": "table1", "ColId": "col1"},
						},
					},
				},
			},
			expectedConv: makeResultConv(
				ddl.Schema{
					"table1": {
						ColDefs: map[string]ddl.ColumnDef{
							"col1": {},
						},
					},
				},
				map[string]internal.TableIssues{
					"table1": {
						ColumnLevelIssues: map[string][]internal.SchemaIssue{
							"col1": {internal.GeneratedColumnValueError},
						},
					},
				}),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			spannerSchemaApplyExpressions(tc.conv, tc.expressions)
			assert.Equal(t, tc.expectedConv, tc.conv)
		})
	}
}

func TestVerifyCheckConstraintExpressions(t *testing.T) {
	tests := []struct {
		name                    string
		expressions             []ddl.CheckConstraint
		expectedResults         []internal.ExpressionVerificationOutput
		expectedCheckConstraint []ddl.CheckConstraint
		expectedResponse        bool
	}{
		{
			name: "AllValidExpressions",
			expressions: []ddl.CheckConstraint{
				{Expr: "(col1 > 0)", ExprId: "expr1", Name: "check1"},
			},
			expectedResults: []internal.ExpressionVerificationOutput{
				{Result: true, Err: nil, ExpressionDetail: internal.ExpressionDetail{Expression: "(col1 > 0)", Type: "CHECK", Metadata: map[string]string{"tableId": "t1"}, ExpressionId: "expr1"}},
			},
			expectedCheckConstraint: []ddl.CheckConstraint{
				{Expr: "(col1 > 0)", ExprId: "expr1", Name: "check1"},
			},
			expectedResponse: false,
		},
		{
			name: "InvalidSyntaxError",
			expressions: []ddl.CheckConstraint{
				{Expr: "(col1 > 0)", ExprId: "expr1", Name: "check1"},
				{Expr: "(col1 > 18", ExprId: "expr2", Name: "check2"},
			},
			expectedResults: []internal.ExpressionVerificationOutput{
				{Result: true, Err: nil, ExpressionDetail: internal.ExpressionDetail{Expression: "(col1 > 0)", Type: "CHECK", Metadata: map[string]string{"tableId": "t1"}, ExpressionId: "expr1"}},
				{Result: false, Err: errors.New("Syntax error ..."), ExpressionDetail: internal.ExpressionDetail{Expression: "(col1 > 18", Type: "CHECK", Metadata: map[string]string{"tableId": "t1"}, ExpressionId: "expr2"}},
			},
			expectedCheckConstraint: []ddl.CheckConstraint{
				{Expr: "(col1 > 0)", ExprId: "expr1", Name: "check1"},
			},
			expectedResponse: true,
		},
		{
			name: "NameError",
			expressions: []ddl.CheckConstraint{
				{Expr: "(col1 > 0)", ExprId: "expr1", Name: "check1"},
				{Expr: "(col1 > 18)", ExprId: "expr2", Name: "check2"},
			},
			expectedResults: []internal.ExpressionVerificationOutput{
				{Result: true, Err: nil, ExpressionDetail: internal.ExpressionDetail{Expression: "(col1 > 0)", Type: "CHECK", Metadata: map[string]string{"tableId": "t1"}, ExpressionId: "expr1"}},
				{Result: false, Err: errors.New("Unrecognized name ..."), ExpressionDetail: internal.ExpressionDetail{Expression: "(col1 > 18)", Type: "CHECK", Metadata: map[string]string{"tableId": "t1"}, ExpressionId: "expr2"}},
			},
			expectedCheckConstraint: []ddl.CheckConstraint{
				{Expr: "(col1 > 0)", ExprId: "expr1", Name: "check1"},
			},
			expectedResponse: true,
		},
		{
			name: "TypeError",
			expressions: []ddl.CheckConstraint{
				{Expr: "(col1 > 0)", ExprId: "expr1", Name: "check1"},
				{Expr: "(col1 > 18)", ExprId: "expr2", Name: "check2"},
			},
			expectedResults: []internal.ExpressionVerificationOutput{
				{Result: true, Err: nil, ExpressionDetail: internal.ExpressionDetail{Expression: "(col1 > 0)", Type: "CHECK", Metadata: map[string]string{"tableId": "t1"}, ExpressionId: "expr1"}},
				{Result: false, Err: errors.New("No matching signature for operator"), ExpressionDetail: internal.ExpressionDetail{Expression: "(col1 > 18)", Type: "CHECK", Metadata: map[string]string{"tableId": "t1"}, ExpressionId: "expr2"}},
			},
			expectedCheckConstraint: []ddl.CheckConstraint{
				{Expr: "(col1 > 0)", ExprId: "expr1", Name: "check1"},
			},
			expectedResponse: true,
		},
		{
			name: "FunctionError",
			expressions: []ddl.CheckConstraint{
				{Expr: "(col1 > 0)", ExprId: "expr1", Name: "check1"},
				{Expr: "(col1 > 18)", ExprId: "expr2", Name: "check2"},
			},
			expectedResults: []internal.ExpressionVerificationOutput{
				{Result: true, Err: nil, ExpressionDetail: internal.ExpressionDetail{Expression: "(col1 > 0)", Type: "CHECK", Metadata: map[string]string{"tableId": "t1"}, ExpressionId: "expr1"}},
				{Result: false, Err: errors.New("Function not found"), ExpressionDetail: internal.ExpressionDetail{Expression: "(col1 > 18)", Type: "CHECK", Metadata: map[string]string{"tableId": "t1"}, ExpressionId: "expr2"}},
			},
			expectedCheckConstraint: []ddl.CheckConstraint{
				{Expr: "(col1 > 0)", ExprId: "expr1", Name: "check1"},
			},
			expectedResponse: true,
		},
		{
			name: "GenericError",
			expressions: []ddl.CheckConstraint{
				{Expr: "(col1 > 0)", ExprId: "expr1", Name: "check1"},
				{Expr: "(col1 > 18)", ExprId: "expr2", Name: "check2"},
			},
			expectedResults: []internal.ExpressionVerificationOutput{
				{Result: true, Err: nil, ExpressionDetail: internal.ExpressionDetail{Expression: "(col1 > 0)", Type: "CHECK", Metadata: map[string]string{"tableId": "t1"}, ExpressionId: "expr1"}},
				{Result: false, Err: errors.New("Unhandle error"), ExpressionDetail: internal.ExpressionDetail{Expression: "(col1 > 18)", Type: "CHECK", Metadata: map[string]string{"tableId": "t1"}, ExpressionId: "expr2"}},
			},
			expectedCheckConstraint: []ddl.CheckConstraint{
				{Expr: "(col1 > 0)", ExprId: "expr1", Name: "check1"},
			},
			expectedResponse: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockAccessor := new(mocks.MockExpressionVerificationAccessor)
			handler := &SchemaToSpannerImpl{ExpressionVerificationAccessor: mockAccessor}

			conv := internal.MakeConv()

			ctx := context.Background()

			conv.SpSchema = map[string]ddl.CreateTable{
				"t1": {
					Name:        "table1",
					Id:          "t1",
					PrimaryKeys: []ddl.IndexKey{{ColId: "c1"}},
					ColIds:      []string{"c1"},
					ColDefs: map[string]ddl.ColumnDef{
						"c1": {Name: "col1", Id: "c1", T: ddl.Type{Name: ddl.Int64}},
					},
					CheckConstraints: tc.expressions,
				},
			}

			mockAccessor.On("VerifyExpressions", ctx, mock.Anything).Return(internal.VerifyExpressionsOutput{
				ExpressionVerificationOutputList: tc.expectedResults,
			})
			mockAccessor.On("RefreshSpannerClient", ctx, mock.Anything, mock.Anything).Return(nil)
			handler.VerifyExpressions(conv)
			assert.Equal(t, conv.SpSchema["t1"].CheckConstraints, tc.expectedCheckConstraint)

		})
	}
}

func TestSchemaToSpannerDDLHelper_CassandraOpts(t *testing.T) {
	conv := internal.MakeConv()
	conv.Source = constants.CASSANDRA

	srcTable := schema.Table{
		Name:   "users",
		Id:     "t1",
		ColIds: []string{"c1"},
		ColDefs: map[string]schema.Column{
			"c1": {Name: "user_id", Id: "c1", Type: schema.Type{Name: "uuid"}},
		},
	}

	mockToddl := new(MockOptionProvider)

	expectedSpannerType := ddl.Type{Name: ddl.String, Len: 36}
	mockToddl.On("ToSpannerType", mock.Anything, "", mock.Anything, mock.Anything).Return(expectedSpannerType, []internal.SchemaIssue(nil))

	expectedAnnotation := "uuid"
	mockToddl.On("GetTypeOption", "uuid", expectedSpannerType).Return(expectedAnnotation)

	ss := SchemaToSpannerImpl{} 
	err := ss.SchemaToSpannerDDLHelper(conv, mockToddl, srcTable, false)

	assert.Nil(t, err)
	assert.Contains(t, conv.SpSchema, "t1", "Spanner schema for table t1 should be created")
	spTable := conv.SpSchema["t1"]
	assert.Contains(t, spTable.ColDefs, "c1", "Spanner column for c1 should be created")
	spCol := spTable.ColDefs["c1"]
	assert.NotNil(t, spCol.Opts)
	assert.Contains(t, spCol.Opts, "cassandra_type")
	assert.Equal(t, expectedAnnotation, spCol.Opts["cassandra_type"])
	mockToddl.AssertCalled(t, "ToSpannerType", mock.Anything, "", mock.AnythingOfType("schema.Type"), mock.AnythingOfType("bool"))
	mockToddl.AssertCalled(t, "GetTypeOption", "uuid", expectedSpannerType)
}

// Child partitions must be left out of the Spanner schema by default, since the
// parent table already yields all of their rows.
func TestSchemaToSpannerDDLHelper_PartitionedTables(t *testing.T) {
	makeSrcTable := func(partitionParent string) schema.Table {
		return schema.Table{
			Name:   "orders_2024",
			Id:     "t1",
			ColIds: []string{"c1"},
			ColDefs: map[string]schema.Column{
				"c1": {Name: "order_id", Id: "c1", Type: schema.Type{Name: "bigint"}},
			},
			PartitionParent: partitionParent,
		}
	}

	testCases := []struct {
		name            string
		partitionParent string
		isRestore       bool
		wantConverted   bool
		wantIssue       bool
	}{
		{
			name:            "child partition is skipped and flagged by default",
			partitionParent: "orders",
			isRestore:       false,
			wantConverted:   false,
			wantIssue:       true,
		},
		{
			name:            "child partition is converted when explicitly restored",
			partitionParent: "orders",
			isRestore:       true,
			wantConverted:   true,
			wantIssue:       false,
		},
		{
			name:            "regular table is unaffected",
			partitionParent: "",
			isRestore:       false,
			wantConverted:   true,
			wantIssue:       false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			conv := internal.MakeConv()
			srcTable := makeSrcTable(tc.partitionParent)
			conv.SrcSchema[srcTable.Id] = srcTable

			mockToddl := new(MockToDdl)
			mockToddl.On("ToSpannerType", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
				Return(ddl.Type{Name: ddl.Int64}, []internal.SchemaIssue(nil)).Maybe()

			ss := SchemaToSpannerImpl{}
			err := ss.SchemaToSpannerDDLHelper(conv, mockToddl, srcTable, tc.isRestore)
			assert.Nil(t, err)

			_, converted := conv.SpSchema[srcTable.Id]
			assert.Equal(t, tc.wantConverted, converted)

			hasIssue := IsSchemaIssuePresent(
				conv.SchemaIssues[srcTable.Id].TableLevelIssues, internal.PartitionedTable)
			assert.Equal(t, tc.wantIssue, hasIssue)

			// A skipped partition stays in SrcSchema so the UI can grey it out and
			// offer Restore.
			assert.Contains(t, conv.SrcSchema, srcTable.Id)
		})
	}
}

func TestBuildSchemaTable_PropagatesPartitionParent(t *testing.T) {
	for _, parent := range []string{"orders", ""} {
		table := BuildSchemaTable(
			SchemaAndName{Schema: "public", Name: "orders_2024"},
			"orders_2024",
			map[string]schema.Column{"c1": {Name: "order_id", Id: "c1"}},
			[]string{"c1"},
			nil, nil, nil, nil,
			parent,
		)
		assert.Equal(t, parent, table.PartitionParent)
	}
}

func TestCreatePrimaryKeyExpressionVerifyInput(t *testing.T) {
	expressions := internal.VerifyExpressionsOutput{
		ExpressionVerificationOutputList: []internal.ExpressionVerificationOutput{
			{
				Result: true,
				ExpressionDetail: internal.ExpressionDetail{
					Metadata: map[string]string{"TableId": "t1", "IsPrimaryKey": "true"},
				},
			},
			{
				Result: true,
				ExpressionDetail: internal.ExpressionDetail{
					Metadata: map[string]string{"TableId": "t2", "IsPrimaryKey": "false"},
				},
			},
			{
				Result: false,
				ExpressionDetail: internal.ExpressionDetail{
					Metadata: map[string]string{"TableId": "t3", "IsPrimaryKey": "true"},
				},
			},
			{
				Result: true,
				ExpressionDetail: internal.ExpressionDetail{
					Metadata: map[string]string{"TableId": "t1", "IsPrimaryKey": "true"},
				},
			},
		},
	}
	res := createPrimaryKeyExpressionVerifyInput(expressions)
	assert.Equal(t, 1, len(res))
	assert.Equal(t, "t1", res[0].Metadata["TableId"])
}

func TestRemoveGeneratedColumnExpressionFromPrimaryKey(t *testing.T) {
	conv := internal.MakeConv()
	conv.SchemaIssues = make(map[string]internal.TableIssues)
	conv.SchemaIssues["t1"] = internal.TableIssues{ColumnLevelIssues: make(map[string][]internal.SchemaIssue)}
	conv.SpSchema = ddl.Schema{
		"t1": ddl.CreateTable{
			PrimaryKeys: []ddl.IndexKey{{ColId: "c1"}},
			ColDefs: map[string]ddl.ColumnDef{
				"c1": {
					GeneratedColumn: ddl.GeneratedColumn{
						IsPresent: true,
					},
				},
			},
		},
	}
	removeGeneratedColumnExpressionFromPrimaryKey(conv, "t1")
	assert.False(t, conv.SpSchema["t1"].ColDefs["c1"].GeneratedColumn.IsPresent)
	assert.Contains(t, conv.SchemaIssues["t1"].ColumnLevelIssues["c1"], internal.GeneratedColumnValueError)
}

func TestApplyExpressionGeneratedColumnPKErrors(t *testing.T) {
	conv := internal.MakeConv()
	conv.SchemaIssues = make(map[string]internal.TableIssues)
	conv.SchemaIssues["t1"] = internal.TableIssues{ColumnLevelIssues: make(map[string][]internal.SchemaIssue)}
	conv.SpSchema = ddl.Schema{
		"t1": ddl.CreateTable{
			PrimaryKeys: []ddl.IndexKey{{ColId: "c1"}},
			ColDefs: map[string]ddl.ColumnDef{
				"c1": {
					GeneratedColumn: ddl.GeneratedColumn{
						IsPresent: true,
					},
				},
			},
		},
	}
	expressions := internal.VerifyExpressionsOutput{
		ExpressionVerificationOutputList: []internal.ExpressionVerificationOutput{
			{
				Result: false,
				ExpressionDetail: internal.ExpressionDetail{
					Metadata: map[string]string{"TableId": "t1", "IsPrimaryKey": "true"},
				},
			},
		},
	}
	applyExpressionGeneratedColumnPKErrors(conv, expressions)
	assert.False(t, conv.SpSchema["t1"].ColDefs["c1"].GeneratedColumn.IsPresent)
}

// Spanner identity columns must be INT64. An identity column that the source maps to
// another type keeps its type and loses the identity: if it is not part of the primary
// key, it gets a warning; if it is, a new INT64 identity column replaces it in the
// primary key and a unique index keeps the source primary key columns unique.
func TestSchemaToSpannerDDLHelper_IdentityColumnType(t *testing.T) {
	identity := &ddl.AutoGenCol{Name: constants.IDENTITY, GenerationType: constants.IDENTITY}
	srcAutoGen := ddl.AutoGenCol{Name: constants.AUTO_INCREMENT, GenerationType: constants.AUTO_INCREMENT}
	tests := []struct {
		name           string
		primaryKeys    []schema.Key
		mappedType     ddl.Type
		mappedIssues   []internal.SchemaIssue
		autoGen        *ddl.AutoGenCol
		expectedAuto   ddl.AutoGenCol
		expectedIssues []internal.SchemaIssue
		expectNewPk    bool
	}{
		{
			name:           "INT64 identity primary key is unchanged",
			primaryKeys:    []schema.Key{{ColId: "c1"}},
			mappedType:     ddl.Type{Name: ddl.Int64},
			autoGen:        identity,
			expectedAuto:   *identity,
			expectedIssues: []internal.SchemaIssue{internal.IdentitySkipRange},
		},
		{
			name:           "INT64 identity non key column is unchanged",
			mappedType:     ddl.Type{Name: ddl.Int64},
			autoGen:        identity,
			expectedAuto:   *identity,
			expectedIssues: []internal.SchemaIssue{internal.IdentitySkipRange},
		},
		{
			name:           "NUMERIC identity non key column loses the identity with a warning",
			primaryKeys:    []schema.Key{{ColId: "c2"}},
			mappedType:     ddl.Type{Name: ddl.Numeric},
			mappedIssues:   []internal.SchemaIssue{internal.Numeric},
			autoGen:        identity,
			expectedAuto:   ddl.AutoGenCol{},
			expectedIssues: []internal.SchemaIssue{internal.Numeric, internal.IdentityTypeNotSupported},
		},
		{
			name:           "NUMERIC identity primary key is replaced by a new INT64 identity column",
			primaryKeys:    []schema.Key{{ColId: "c1"}},
			mappedType:     ddl.Type{Name: ddl.Numeric},
			mappedIssues:   []internal.SchemaIssue{internal.Numeric},
			autoGen:        identity,
			expectedAuto:   ddl.AutoGenCol{},
			expectedIssues: []internal.SchemaIssue{internal.Numeric},
			expectNewPk:    true,
		},
		{
			name:           "NUMERIC identity in a composite primary key is replaced in place",
			primaryKeys:    []schema.Key{{ColId: "c2", Order: 1}, {ColId: "c1", Desc: true, Order: 2}},
			mappedType:     ddl.Type{Name: ddl.Numeric},
			autoGen:        identity,
			expectedAuto:   ddl.AutoGenCol{},
			expectedIssues: nil,
			expectNewPk:    true,
		},
		{
			name:           "no auto generation returned does not panic",
			primaryKeys:    []schema.Key{{ColId: "c1"}},
			mappedType:     ddl.Type{Name: ddl.Numeric},
			autoGen:        nil,
			expectedAuto:   ddl.AutoGenCol{},
			expectedIssues: nil,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			conv := internal.MakeConv()
			srcTable := schema.Table{
				Name:        "T",
				Id:          "t1",
				ColIds:      []string{"c1", "c2"},
				PrimaryKeys: tc.primaryKeys,
				ColDefs: map[string]schema.Column{
					"c1": {Name: "ID", Id: "c1", NotNull: true, Type: schema.Type{Name: "NUMBER"}, AutoGen: srcAutoGen},
					"c2": {Name: "synth_id", Id: "c2", NotNull: true, Type: schema.Type{Name: "VARCHAR2"}},
				},
			}
			conv.SrcSchema[srcTable.Id] = srcTable
			mockToddl := new(MockToDdl)
			mockToddl.On("ToSpannerType", mock.Anything, "", schema.Type{Name: "NUMBER"}, mock.Anything).Return(tc.mappedType, tc.mappedIssues)
			mockToddl.On("ToSpannerType", mock.Anything, "", schema.Type{Name: "VARCHAR2"}, mock.Anything).Return(ddl.Type{Name: ddl.String, Len: 10}, []internal.SchemaIssue(nil))
			if tc.autoGen == nil {
				mockToddl.On("GetColumnAutoGen", mock.Anything, srcAutoGen, "c1", "t1").Return(nil, nil)
			} else {
				mockToddl.On("GetColumnAutoGen", mock.Anything, srcAutoGen, "c1", "t1").Return(tc.autoGen, nil)
			}

			ss := SchemaToSpannerImpl{}
			err := ss.SchemaToSpannerDDLHelper(conv, mockToddl, srcTable, false)
			assert.Nil(t, err)

			spTable := conv.SpSchema["t1"]
			col := spTable.ColDefs["c1"]
			// The source column always keeps its type and NOT NULL.
			assert.Equal(t, tc.mappedType, col.T)
			assert.True(t, col.NotNull)
			assert.Equal(t, tc.expectedAuto, col.AutoGen)
			assert.Equal(t, tc.expectedIssues, conv.SchemaIssues["t1"].ColumnLevelIssues["c1"])
			mockToddl.AssertNotCalled(t, "ToSpannerType", mock.Anything, ddl.Int64, mock.Anything, mock.Anything)

			if !tc.expectNewPk {
				assert.Equal(t, []string{"c1", "c2"}, spTable.ColIds)
				assert.Empty(t, spTable.Indexes)
				return
			}
			// A new INT64 identity column replaces the source column in the primary key.
			assert.Equal(t, 3, len(spTable.ColIds))
			newColId := spTable.ColIds[2]
			newCol := spTable.ColDefs[newColId]
			assert.Equal(t, "synth_id0", newCol.Name) // "synth_id" is already a column name.
			assert.Equal(t, ddl.Type{Name: ddl.Int64}, newCol.T)
			assert.True(t, newCol.NotNull)
			assert.Equal(t, constants.IDENTITY, newCol.AutoGen.GenerationType)
			assert.Equal(t, []internal.SchemaIssue{internal.IdentityPrimaryKeyAdded, internal.IdentitySkipRange}, conv.SchemaIssues["t1"].ColumnLevelIssues[newColId])

			var expectedPk, expectedUniqueKeys []ddl.IndexKey
			for _, k := range tc.primaryKeys {
				expectedUniqueKeys = append(expectedUniqueKeys, ddl.IndexKey{ColId: k.ColId, Desc: k.Desc, Order: k.Order})
				colId := k.ColId
				if colId == "c1" {
					colId = newColId
				}
				expectedPk = append(expectedPk, ddl.IndexKey{ColId: colId, Desc: k.Desc, Order: k.Order})
			}
			assert.Equal(t, expectedPk, spTable.PrimaryKeys)
			// A unique index keeps the source primary key columns unique.
			assert.Equal(t, 1, len(spTable.Indexes))
			assert.True(t, spTable.Indexes[0].Unique)
			assert.Equal(t, "T_original_pk", spTable.Indexes[0].Name)
			assert.Equal(t, "t1", spTable.Indexes[0].TableId)
			assert.Equal(t, expectedUniqueKeys, spTable.Indexes[0].Keys)
		})
	}
}

func TestUniqueColumnName(t *testing.T) {
	colDefs := map[string]ddl.ColumnDef{"c1": {Name: "ID"}}
	assert.Equal(t, "synth_id", uniqueColumnName(colDefs, "synth_id"))
	colDefs["c2"] = ddl.ColumnDef{Name: "SYNTH_ID"}
	colDefs["c3"] = ddl.ColumnDef{Name: "synth_id0"}
	assert.Equal(t, "synth_id1", uniqueColumnName(colDefs, "synth_id"))
}

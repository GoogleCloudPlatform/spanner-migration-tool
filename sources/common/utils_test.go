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

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/logger"
	"github.com/cloudspannerecosystem/harbourbridge/schema"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
)

func init() {
	logger.Log = zap.NewNop()
}

func TestToNotNull(t *testing.T) {
	conv := internal.MakeConv()
	assert.Equal(t, false, ToNotNull(conv, "YES"))
	assert.Equal(t, true, ToNotNull(conv, "NO"))
	ToNotNull(conv, "Something else")
	assert.Equal(t, int64(1), conv.Unexpecteds())
}

func TestGetColsAndSchemas(t *testing.T) {
	tableName := "testtable"
	tableId := "t1"
	cols := []string{"a", "b", "c"}
	colIds := []string{"c1", "c2", "c3"}
	spSchema := ddl.CreateTable{
		Name:   tableName,
		Id:     tableId,
		ColIds: colIds,
		ColDefs: map[string]ddl.ColumnDef{
			"c1": {Name: "a", Id: "c1", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
			"c2": {Name: "b", Id: "c2", T: ddl.Type{Name: ddl.Numeric}},
			"c3": {Name: "c", Id: "c3", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
		},
		PrimaryKeys: []ddl.IndexKey{{ColId: "c1"}},
	}
	srcSchema := schema.Table{
		Name:   tableName,
		Id:     tableId,
		ColIds: colIds,
		ColDefs: map[string]schema.Column{
			"c1": {Name: "a", Id: "c1", Type: schema.Type{Name: "String"}},
			"c2": {Name: "b", Id: "c2", Type: schema.Type{Name: "Number"}},
			"c3": {Name: "c", Id: "c3", Type: schema.Type{Name: "NumberString"}},
		},
		PrimaryKeys: []schema.Key{{ColId: "c1"}},
	}

	conv := internal.MakeConv()
	conv.SpSchema[spSchema.Id] = spSchema
	conv.SrcSchema[srcSchema.Id] = srcSchema

	type args struct {
		conv    *internal.Conv
		tableId string
	}
	tests := []struct {
		name    string
		args    args
		want    schema.Table
		want1   string
		want2   []string
		want3   ddl.CreateTable
		wantErr bool
	}{
		{
			name: "test for checking correctness of output",
			args: args{
				conv:    conv,
				tableId: tableId,
			},
			want:    srcSchema,
			want1:   tableName,
			want2:   cols,
			want3:   spSchema,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, got2, got3, err := GetColsAndSchemas(tt.args.conv, tt.args.tableId)
			if (err != nil) != tt.wantErr {
				t.Errorf("getColsAndSchemas() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getColsAndSchemas() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("getColsAndSchemas() got1 = %v, want %v", got1, tt.want1)
			}
			if !reflect.DeepEqual(got2, tt.want2) {
				t.Errorf("getColsAndSchemas() got2 = %v, want %v", got2, tt.want2)
			}
			if !reflect.DeepEqual(got3, tt.want3) {
				t.Errorf("getColsAndSchemas() got3 = %v, want %v", got3, tt.want3)
			}
		})
	}
}

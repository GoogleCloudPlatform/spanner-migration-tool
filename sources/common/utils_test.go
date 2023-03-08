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
	"fmt"
	"math/rand"
	"reflect"
	"sync"
	"testing"
	"time"

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
	cols := []string{"a", "b", "c"}
	spSchema := ddl.CreateTable{
		Name:     tableName,
		ColNames: cols,
		ColDefs: map[string]ddl.ColumnDef{
			"a": {Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
			"b": {Name: "b", T: ddl.Type{Name: ddl.Numeric}},
			"c": {Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
		},
		Pks: []ddl.IndexKey{{Col: "a"}},
	}
	srcSchema := schema.Table{
		Name:     tableName,
		ColNames: cols,
		ColDefs: map[string]schema.Column{
			"a": {Name: "a", Type: schema.Type{Name: "String"}},
			"b": {Name: "b", Type: schema.Type{Name: "Number"}},
			"c": {Name: "c", Type: schema.Type{Name: "NumberString"}},
		},
		PrimaryKeys: []schema.Key{{Column: "a"}},
	}

	conv := internal.MakeConv()
	conv.SpSchema[spSchema.Name] = spSchema
	conv.SrcSchema[srcSchema.Name] = srcSchema
	conv.ToSource[spSchema.Name] = internal.NameAndCols{Name: srcSchema.Name, Cols: make(map[string]string)}
	conv.ToSpanner[srcSchema.Name] = internal.NameAndCols{Name: spSchema.Name, Cols: make(map[string]string)}
	for i := range spSchema.ColNames {
		conv.ToSource[spSchema.Name].Cols[spSchema.ColNames[i]] = srcSchema.ColNames[i]
		conv.ToSpanner[srcSchema.Name].Cols[srcSchema.ColNames[i]] = spSchema.ColNames[i]
	}

	type args struct {
		conv     *internal.Conv
		srcTable string
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
				conv:     conv,
				srcTable: tableName,
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
			got, got1, got2, got3, err := GetColsAndSchemas(tt.args.conv, tt.args.srcTable)
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

func TestWorkerPool(t *testing.T) {
	input := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

	f := func(i int, mutex *sync.Mutex) TaskResult[int] {
		sleepTime := time.Duration(rand.Intn(1000 * 1000))
		time.Sleep(sleepTime)
		res := TaskResult[int]{Result: i, Err: nil}
		return res
	}

	out, _ := RunParallelTasks(input, 5, f, false)
	assert.Equal(t, len(input), len(out), fmt.Sprintln("jobs not processed"))
}

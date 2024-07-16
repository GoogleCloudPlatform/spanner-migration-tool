// Copyright 2022 Google LLC
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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/proto/migration"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/schema"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/session"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func init() {
	logger.Log = zap.NewNop()
}

func TestUpdateTableSchema(t *testing.T) {

	tc := []struct {
		name         string
		table        string
		payload      string
		statusCode   int64
		conv         *internal.Conv
		expectedConv *internal.Conv
	}{
		{
			name:  "Test remove success",
			table: "t1",
			payload: `
		{
		  "UpdateCols":{
			"c3": { "Removed": true }
		}
		}`,
			statusCode: http.StatusOK,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:   "t1",
						ColIds: []string{"c1", "c2", "c3"},
						ColDefs: map[string]ddl.ColumnDef{
							"c1": {Name: "a", Id: "c1", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
							"c2": {Name: "b", Id: "c2", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
							"c3": {Name: "c", Id: "c3", T: ddl.Type{Name: ddl.Int64}},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c1"}},
					}},
				SchemaIssues: map[string]internal.TableIssues{
					"t1": {
						ColumnLevelIssues: map[string][]internal.SchemaIssue{
							"c3": {internal.Widened},
						},
					},
				},
				Audit: internal.Audit{MigrationType: migration.MigrationData_SCHEMA_AND_DATA.Enum()},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:   "t1",
						ColIds: []string{"c1", "c2"},
						ColDefs: map[string]ddl.ColumnDef{
							"c1": {Name: "a", Id: "c1", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
							"c2": {Name: "b", Id: "c2", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c1"}},
					}},
				SchemaIssues: map[string]internal.TableIssues{
					"t1": {
						ColumnLevelIssues: map[string][]internal.SchemaIssue{},
					},
				},
			},
		},
		{
			name:  "Test Add success",
			table: "t1",
			payload: `
			{
			  "UpdateCols":{
				"c3": { "Add": true, "ToType": "STRING"}
			}
			}`,
			statusCode: http.StatusOK,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:   "t1",
						Id:     "t1",
						ColIds: []string{"c1", "c2"},
						ColDefs: map[string]ddl.ColumnDef{
							"c1": {Id: "c1", Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
							"c2": {Id: "c2", Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c1"}},
					}},
				SrcSchema: map[string]schema.Table{
					"t1": {
						Id:     "t1",
						Name:   "t1",
						ColIds: []string{"c1", "c2"},
						ColDefs: map[string]schema.Column{
							"c1": {Id: "c1", Name: "a", Type: schema.Type{Name: "varchar", Mods: []int64{6}}},
							"c2": {Id: "c2", Name: "b", Type: schema.Type{Name: "varchar", Mods: []int64{6}}},
							"c3": {Id: "c3", Name: "c", Type: schema.Type{Name: "varchar", Mods: []int64{}}},
						},
						PrimaryKeys: []schema.Key{{ColId: "c1"}},
					}},

				Audit:        internal.Audit{MigrationType: migration.MigrationData_SCHEMA_AND_DATA.Enum()},
				SchemaIssues: make(map[string]internal.TableIssues),
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Id:     "t1",
						Name:   "t1",
						ColIds: []string{"c1", "c2", "c3"},
						ColDefs: map[string]ddl.ColumnDef{
							"c1": {Id: "c1", Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
							"c2": {Id: "c2", Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
							"c3": {Id: "c3", Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c1"}},
					}},
				SrcSchema: map[string]schema.Table{
					"t1": {
						Id:     "t1",
						Name:   "t1",
						ColIds: []string{"c1", "c2"},
						ColDefs: map[string]schema.Column{
							"c1": {Id: "c1", Name: "a", Type: schema.Type{Name: "varchar", Mods: []int64{6}}},
							"c2": {Id: "c2", Name: "b", Type: schema.Type{Name: "varchar", Mods: []int64{6}}},
							"c3": {Id: "c3", Name: "c", Type: schema.Type{Name: "varchar", Mods: []int64{}}},
						},
						PrimaryKeys: []schema.Key{{ColId: "c1"}},
					}},
				SchemaIssues: map[string]internal.TableIssues{
					"t1": {},
				},
			},
		},
		{
			name:  "Test rename success",
			table: "t1",
			payload: `
				{
				  "UpdateCols":{
					"c1": { "Rename": "aa" }
				}
				}`,
			statusCode: http.StatusOK,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:   "t1",
						ColIds: []string{"c1", "c2", "c3"},
						ColDefs: map[string]ddl.ColumnDef{
							"c1": {Name: "a", Id: "c1", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
							"c2": {Name: "b", Id: "c2", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
							"c3": {Name: "c", Id: "c3", T: ddl.Type{Name: ddl.Int64}},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c1"}},
					}},
				SrcSchema: map[string]schema.Table{
					"t1": {
						Name:   "t1",
						ColIds: []string{"c1", "c2", "c3"},
						ColDefs: map[string]schema.Column{
							"c1": {Name: "a", Id: "c1", Type: schema.Type{Name: "varchar", Mods: []int64{6}}},
							"c2": {Name: "b", Id: "c2", Type: schema.Type{Name: "varchar", Mods: []int64{6}}},
							"c3": {Name: "c", Id: "c3", Type: schema.Type{Name: "bigint", Mods: []int64{}}},
						},
						PrimaryKeys: []schema.Key{{ColId: "c1"}},
					}},
				SchemaIssues: make(map[string]internal.TableIssues),
				Audit:        internal.Audit{MigrationType: migration.MigrationData_SCHEMA_AND_DATA.Enum()},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:   "t1",
						ColIds: []string{"c1", "c2", "c3"},
						ColDefs: map[string]ddl.ColumnDef{
							"c1": {Name: "aa", Id: "c1", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
							"c2": {Name: "b", Id: "c2", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
							"c3": {Name: "c", Id: "c3", T: ddl.Type{Name: ddl.Int64}},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c1"}},
					}},
				SrcSchema: map[string]schema.Table{
					"t1": {
						Name:   "t1",
						ColIds: []string{"c1", "c2", "c3"},
						ColDefs: map[string]schema.Column{
							"c1": {Name: "a", Id: "c1", Type: schema.Type{Name: "varchar", Mods: []int64{6}}},
							"c2": {Name: "b", Id: "c2", Type: schema.Type{Name: "varchar", Mods: []int64{6}}},
							"c3": {Name: "c", Id: "c3", Type: schema.Type{Name: "bigint", Mods: []int64{}}},
						},
						PrimaryKeys: []schema.Key{{ColId: "c1"}},
					}},
				SchemaIssues: map[string]internal.TableIssues{
					"t1": {},
				},
			},
		},
		{
			name:  "Test change column length success",
			table: "t1",
			payload: `
				{
				  "UpdateCols":{
					"c1": { "MaxColLength": "20" }
				}
				}`,
			statusCode: http.StatusOK,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:   "t1",
						ColIds: []string{"c1", "c2", "c3"},
						ColDefs: map[string]ddl.ColumnDef{
							"c1": {Name: "a", Id: "c1", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
							"c2": {Name: "b", Id: "c2", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
							"c3": {Name: "c", Id: "c3", T: ddl.Type{Name: ddl.Int64}},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c1"}},
					}},
				SrcSchema: map[string]schema.Table{
					"t1": {
						Name:   "t1",
						ColIds: []string{"c1", "c2", "c3"},
						ColDefs: map[string]schema.Column{
							"c1": {Name: "a", Id: "c1", Type: schema.Type{Name: "varchar", Mods: []int64{6}}},
							"c2": {Name: "b", Id: "c2", Type: schema.Type{Name: "varchar", Mods: []int64{6}}},
							"c3": {Name: "c", Id: "c3", Type: schema.Type{Name: "bigint", Mods: []int64{}}},
						},
						PrimaryKeys: []schema.Key{{ColId: "c1"}},
					}},
				SchemaIssues: make(map[string]internal.TableIssues),
				Audit:        internal.Audit{MigrationType: migration.MigrationData_SCHEMA_AND_DATA.Enum()},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:   "t1",
						ColIds: []string{"c1", "c2", "c3"},
						ColDefs: map[string]ddl.ColumnDef{
							"c1": {Name: "a", Id: "c1", T: ddl.Type{Name: ddl.String, Len: 20}},
							"c2": {Name: "b", Id: "c2", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
							"c3": {Name: "c", Id: "c3", T: ddl.Type{Name: ddl.Int64}},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c1"}},
					}},
				SrcSchema: map[string]schema.Table{
					"t1": {
						Name:   "t1",
						ColIds: []string{"c1", "c2", "c3"},
						ColDefs: map[string]schema.Column{
							"c1": {Name: "a", Id: "c1", Type: schema.Type{Name: "varchar", Mods: []int64{6}}},
							"c2": {Name: "b", Id: "c2", Type: schema.Type{Name: "varchar", Mods: []int64{6}}},
							"c3": {Name: "c", Id: "c3", Type: schema.Type{Name: "bigint", Mods: []int64{}}},
						},
						PrimaryKeys: []schema.Key{{ColId: "c1"}},
					}},
				SchemaIssues: map[string]internal.TableIssues{
					"t1": {},
				},
			},
		},
		{
			name:  "Test change type success",
			table: "t1",
			payload: `
			{
			  "UpdateCols":{
				"c1": { "ToType": "STRING" },
				"c2": { "ToType": "BYTES" }
			}
			}`,
			statusCode: http.StatusOK,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:   "t1",
						ColIds: []string{"c1", "c2"},
						ColDefs: map[string]ddl.ColumnDef{
							"c1": {Name: "a", Id: "c1", T: ddl.Type{Name: ddl.Int64}},
							"c2": {Name: "b", Id: "c2", T: ddl.Type{Name: ddl.String, Len: 6}},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c1"}},
					}},
				SrcSchema: map[string]schema.Table{
					"t1": {
						Name:   "t1",
						ColIds: []string{"c1", "c2"},
						ColDefs: map[string]schema.Column{
							"c1": {Name: "a", Id: "c1", Type: schema.Type{Name: "bigint", Mods: []int64{}}},
							"c2": {Name: "b", Id: "c2", Type: schema.Type{Name: "varchar", Mods: []int64{6}}},
						},
						PrimaryKeys: []schema.Key{{ColId: "c1"}},
					}},
				SchemaIssues: map[string]internal.TableIssues{
					"t1": {
						ColumnLevelIssues: make(map[string][]internal.SchemaIssue),
					},
				},
				Audit: internal.Audit{MigrationType: migration.MigrationData_SCHEMA_AND_DATA.Enum()},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:   "t1",
						ColIds: []string{"c1", "c2"},
						ColDefs: map[string]ddl.ColumnDef{
							"c1": {Name: "a", Id: "c1", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
							"c2": {Name: "b", Id: "c2", T: ddl.Type{Name: ddl.Bytes, Len: 6}},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c1"}},
					}},
				SrcSchema: map[string]schema.Table{
					"t1": {
						Name:   "t1",
						ColIds: []string{"c1", "c2"},
						ColDefs: map[string]schema.Column{
							"c1": {Name: "a", Id: "c1", Type: schema.Type{Name: "bigint", Mods: []int64{}}},
							"c2": {Name: "b", Id: "c2", Type: schema.Type{Name: "varchar", Mods: []int64{6}}},
						},
						PrimaryKeys: []schema.Key{{ColId: "c1"}},
					}},
				SchemaIssues: map[string]internal.TableIssues{
					"t1": {
						ColumnLevelIssues: map[string][]internal.SchemaIssue{
							"c1": {internal.Widened},
						},
					},
				},
			},
		},
		{
			name:  "Test change type success with foreign keys 1",
			table: "t1",
			payload: `
        {
          "UpdateCols":{
            "c1": { "ToType": "STRING" },
            "c2": { "ToType": "BYTES" }
        }
        }`,
			statusCode: http.StatusOK,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:   "table1",
						Id:     "t1",
						ColIds: []string{"c1", "c2"},
						ColDefs: map[string]ddl.ColumnDef{
							"c1": {Name: "a", Id: "c1", T: ddl.Type{Name: ddl.Int64}},
							"c2": {Name: "b", Id: "c2", T: ddl.Type{Name: ddl.String, Len: 6}},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c1"}},
						ForeignKeys: []ddl.Foreignkey{{Name: "fk1", Id: "f1", ColIds: []string{"c1"}, ReferTableId: "t2", ReferColumnIds: []string{"c3"}}},
					},
					"t2": {
						Name:   "table2",
						Id:     "t2",
						ColIds: []string{"c3"},
						ColDefs: map[string]ddl.ColumnDef{
							"c3": {Name: "c", Id: "c3", T: ddl.Type{Name: ddl.Int64}},
						},
					}},
				SrcSchema: map[string]schema.Table{
					"t1": {
						Name:   "table1",
						Id:     "t1",
						ColIds: []string{"c1", "c2"},
						ColDefs: map[string]schema.Column{
							"c1": {Name: "a", Id: "c1", Type: schema.Type{Name: "bigint", Mods: []int64{}}},
							"c2": {Name: "b", Id: "c2", Type: schema.Type{Name: "varchar", Mods: []int64{6}}},
						},
						PrimaryKeys: []schema.Key{{ColId: "c1"}},
						ForeignKeys: []schema.ForeignKey{{Name: "fk1", Id: "f1", ColIds: []string{"c1"}, ReferTableId: "t2", ReferColumnIds: []string{"c3"}}},
					},
					"t2": {
						Name:   "table2",
						Id:     "t2",
						ColIds: []string{"c3"},
						ColDefs: map[string]schema.Column{
							"c3": {Name: "c", Id: "c3", Type: schema.Type{Name: "bigint", Mods: []int64{}}},
						},
					},
				},
				SchemaIssues: map[string]internal.TableIssues{
					"t1": {
						ColumnLevelIssues: make(map[string][]internal.SchemaIssue),
					},
					"t2": {
						ColumnLevelIssues: make(map[string][]internal.SchemaIssue),
					},
				},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_MIGRATION_TYPE_UNSPECIFIED.Enum(),
				},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:   "table1",
						Id:     "t1",
						ColIds: []string{"c1", "c2"},
						ColDefs: map[string]ddl.ColumnDef{
							"c1": {Name: "a", Id: "c1", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
							"c2": {Name: "b", Id: "c2", T: ddl.Type{Name: ddl.Bytes, Len: 6}},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c1"}},
						ForeignKeys: []ddl.Foreignkey{{Name: "fk1", Id: "f1", ColIds: []string{"c1"}, ReferTableId: "t2", ReferColumnIds: []string{"c3"}}},
					},
					"t2": {
						Name:   "table2",
						Id:     "t2",
						ColIds: []string{"c3"},
						ColDefs: map[string]ddl.ColumnDef{
							"c3": {Name: "c", Id: "c3", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
						},
					},
				},
				SrcSchema: map[string]schema.Table{
					"t1": {
						Name:   "table1",
						Id:     "t1",
						ColIds: []string{"c1", "c2"},
						ColDefs: map[string]schema.Column{
							"c1": {Name: "a", Id: "c1", Type: schema.Type{Name: "bigint", Mods: []int64{}}},
							"c2": {Name: "b", Id: "c2", Type: schema.Type{Name: "varchar", Mods: []int64{6}}},
						},
						PrimaryKeys: []schema.Key{{ColId: "c1"}},
						ForeignKeys: []schema.ForeignKey{{Name: "fk1", Id: "f1", ColIds: []string{"c1"}, ReferTableId: "t2", ReferColumnIds: []string{"c3"}}},
					},
					"t2": {
						Name:   "table2",
						Id:     "t2",
						ColIds: []string{"c3"},
						ColDefs: map[string]schema.Column{
							"c3": {Name: "c", Id: "c3", Type: schema.Type{Name: "bigint", Mods: []int64{}}},
						},
					},
				},
				SchemaIssues: map[string]internal.TableIssues{
					"t1": {
						ColumnLevelIssues: map[string][]internal.SchemaIssue{
							"c1": {internal.Widened},
						},
					},
					"t2": {
						ColumnLevelIssues: map[string][]internal.SchemaIssue{
							"c3": {internal.Widened},
						},
					},
				},
			},
		},
		{
			name:  "Test change type success with foreign keys 2",
			table: "t2",
			payload: `
		{
		  "UpdateCols":{
		    "c3": { "ToType": "STRING" }
		}
		}`,
			statusCode: http.StatusOK,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:   "table1",
						Id:     "t1",
						ColIds: []string{"c1", "c2"},
						ColDefs: map[string]ddl.ColumnDef{
							"c1": {Name: "a", Id: "c1", T: ddl.Type{Name: ddl.Int64}},
							"c2": {Name: "b", Id: "c2", T: ddl.Type{Name: ddl.String, Len: 6}},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c1"}},
						ForeignKeys: []ddl.Foreignkey{{Name: "fk1", Id: "f1", ColIds: []string{"c1"}, ReferTableId: "t2", ReferColumnIds: []string{"c3"}}},
					},
					"t2": {
						Name:   "table2",
						Id:     "t2",
						ColIds: []string{"c3"},
						ColDefs: map[string]ddl.ColumnDef{
							"c3": {Name: "c", Id: "c3", T: ddl.Type{Name: ddl.Int64}},
						},
					}},
				SrcSchema: map[string]schema.Table{
					"t1": {
						Name:   "table1",
						Id:     "t1",
						ColIds: []string{"c1", "c2"},
						ColDefs: map[string]schema.Column{
							"c1": {Name: "a", Id: "c1", Type: schema.Type{Name: "bigint", Mods: []int64{}}},
							"c2": {Name: "b", Id: "c2", Type: schema.Type{Name: "varchar", Mods: []int64{6}}},
						},
						PrimaryKeys: []schema.Key{{ColId: "c1"}},
						ForeignKeys: []schema.ForeignKey{{Name: "fk1", Id: "f1", ColIds: []string{"c1"}, ReferTableId: "t2", ReferColumnIds: []string{"c3"}}},
					},
					"t2": {
						Name:   "table2",
						Id:     "t2",
						ColIds: []string{"c3"},
						ColDefs: map[string]schema.Column{
							"c3": {Name: "c", Id: "c3", Type: schema.Type{Name: "bigint", Mods: []int64{}}},
						},
					},
				},
				SchemaIssues: map[string]internal.TableIssues{
					"t1": {
						ColumnLevelIssues: make(map[string][]internal.SchemaIssue),
					},
					"t2": {
						ColumnLevelIssues: make(map[string][]internal.SchemaIssue),
					},
				},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_MIGRATION_TYPE_UNSPECIFIED.Enum(),
				},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:   "table1",
						Id:     "t1",
						ColIds: []string{"c1", "c2"},
						ColDefs: map[string]ddl.ColumnDef{
							"c1": {Name: "a", Id: "c1", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
							"c2": {Name: "b", Id: "c2", T: ddl.Type{Name: ddl.String, Len: 6}},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c1"}},
						ForeignKeys: []ddl.Foreignkey{{Name: "fk1", Id: "f1", ColIds: []string{"c1"}, ReferTableId: "t2", ReferColumnIds: []string{"c3"}}},
					},
					"t2": {
						Name:   "table2",
						Id:     "t2",
						ColIds: []string{"c3"},
						ColDefs: map[string]ddl.ColumnDef{
							"c3": {Name: "c", Id: "c3", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
						},
					}},
				SrcSchema: map[string]schema.Table{
					"t1": {
						Name:   "table1",
						Id:     "t1",
						ColIds: []string{"c1", "c2"},
						ColDefs: map[string]schema.Column{
							"c1": {Name: "a", Id: "c1", Type: schema.Type{Name: "bigint", Mods: []int64{}}},
							"c2": {Name: "b", Id: "c2", Type: schema.Type{Name: "varchar", Mods: []int64{6}}},
						},
						PrimaryKeys: []schema.Key{{ColId: "c1"}},
						ForeignKeys: []schema.ForeignKey{{Name: "fk1", Id: "f1", ColIds: []string{"c1"}, ReferTableId: "t2", ReferColumnIds: []string{"c3"}}},
					},
					"t2": {
						Name:   "table2",
						Id:     "t2",
						ColIds: []string{"c3"},
						ColDefs: map[string]schema.Column{
							"c3": {Name: "c", Id: "c3", Type: schema.Type{Name: "bigint", Mods: []int64{}}},
						},
					}},
				SchemaIssues: map[string]internal.TableIssues{
					"t1": {
						ColumnLevelIssues: map[string][]internal.SchemaIssue{
							"c1": {internal.Widened},
						},
					},
					"t2": {
						ColumnLevelIssues: map[string][]internal.SchemaIssue{
							"c3": {internal.Widened},
						},
					},
				},
			},
		},
		{
			name:  "Test change type success with interleave 1",
			table: "t2",
			payload: `
		{
		  "UpdateCols":{
		    "c3": { "ToType": "STRING" }
		}
		}`,
			statusCode: http.StatusOK,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:   "table1",
						Id:     "t1",
						ColIds: []string{"c1", "c2"},
						ColDefs: map[string]ddl.ColumnDef{
							"c1": {Name: "a", Id: "c1", T: ddl.Type{Name: ddl.Int64}},
							"c2": {Name: "b", Id: "c2", T: ddl.Type{Name: ddl.String, Len: 6}},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c1"}},
						ParentTable: ddl.InterleavedParent{Id: "t2", OnDelete: constants.FK_CASCADE},
					},
					"t2": {
						Name:   "table2",
						Id:     "t2",
						ColIds: []string{"c3"},
						ColDefs: map[string]ddl.ColumnDef{
							"c3": {Name: "a", Id: "c3", T: ddl.Type{Name: ddl.Int64}},
						},
					}},
				SrcSchema: map[string]schema.Table{
					"t1": {
						Name:   "table1",
						Id:     "t1",
						ColIds: []string{"c1", "c2"},
						ColDefs: map[string]schema.Column{
							"c1": {Name: "a", Id: "c1", Type: schema.Type{Name: "bigint", Mods: []int64{}}},
							"c2": {Name: "b", Id: "c2", Type: schema.Type{Name: "varchar", Mods: []int64{6}}},
						},
						PrimaryKeys: []schema.Key{{ColId: "c1"}},
						ForeignKeys: []schema.ForeignKey{{Name: "fk1", Id: "f1", ColIds: []string{"c1"}, ReferTableId: "t2", ReferColumnIds: []string{"c3"}}},
					},
					"t2": {
						Name:   "table2",
						Id:     "t2",
						ColIds: []string{"c3"},
						ColDefs: map[string]schema.Column{
							"c3": {Name: "c", Id: "c3", Type: schema.Type{Name: "bigint", Mods: []int64{}}},
						},
					},
				},
				SchemaIssues: map[string]internal.TableIssues{
					"t1": {
						ColumnLevelIssues: make(map[string][]internal.SchemaIssue),
					},
					"t2": {
						ColumnLevelIssues: make(map[string][]internal.SchemaIssue),
					},
				},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_MIGRATION_TYPE_UNSPECIFIED.Enum(),
				},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:   "table1",
						Id:     "t1",
						ColIds: []string{"c1", "c2"},
						ColDefs: map[string]ddl.ColumnDef{
							"c1": {Name: "a", Id: "c1", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
							"c2": {Name: "b", Id: "c2", T: ddl.Type{Name: ddl.String, Len: 6}},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c1"}},
						ParentTable: ddl.InterleavedParent{Id: "t2", OnDelete: constants.FK_CASCADE},
					},
					"t2": {
						Name:   "table2",
						Id:     "t2",
						ColIds: []string{"c3"},
						ColDefs: map[string]ddl.ColumnDef{
							"c3": {Name: "a", Id: "c3", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
						},
					}},
				SrcSchema: map[string]schema.Table{
					"t1": {
						Name:   "table1",
						Id:     "t1",
						ColIds: []string{"c1", "c2"},
						ColDefs: map[string]schema.Column{
							"c1": {Name: "a", Id: "c1", Type: schema.Type{Name: "bigint", Mods: []int64{}}},
							"c2": {Name: "b", Id: "c2", Type: schema.Type{Name: "varchar", Mods: []int64{6}}},
						},
						PrimaryKeys: []schema.Key{{ColId: "c1"}},
						ForeignKeys: []schema.ForeignKey{{Name: "fk1", Id: "f1", ColIds: []string{"c1"}, ReferTableId: "t2", ReferColumnIds: []string{"c3"}}},
					},
					"t2": {
						Name:   "table2",
						Id:     "t2",
						ColIds: []string{"c3"},
						ColDefs: map[string]schema.Column{
							"c3": {Name: "c", Id: "c3", Type: schema.Type{Name: "bigint", Mods: []int64{}}},
						},
					}},
				SchemaIssues: map[string]internal.TableIssues{
					"t1": {
						ColumnLevelIssues: map[string][]internal.SchemaIssue{
							"c1": {internal.Widened},
						},
					},
					"t2": {
						ColumnLevelIssues: map[string][]internal.SchemaIssue{
							"c3": {internal.Widened},
						},
					},
				},
			},
		},
		{
			name:  "Test change type success with interleave 2",
			table: "t1",
			payload: `
		{
		  "UpdateCols":{
		    "c1": { "ToType": "STRING" }
		}
		}`,
			statusCode: http.StatusOK,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:   "table1",
						Id:     "t1",
						ColIds: []string{"c1", "c2"},
						ColDefs: map[string]ddl.ColumnDef{
							"c1": {Name: "a", Id: "c1", T: ddl.Type{Name: ddl.Int64}},
							"c2": {Name: "b", Id: "c2", T: ddl.Type{Name: ddl.String, Len: 6}},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c1"}},
						ParentTable: ddl.InterleavedParent{Id: "t2", OnDelete: constants.FK_NO_ACTION},
					},
					"t2": {
						Name:   "table2",
						Id:     "t2",
						ColIds: []string{"c3"},
						ColDefs: map[string]ddl.ColumnDef{
							"c3": {Name: "a", Id: "c3", T: ddl.Type{Name: ddl.Int64}},
						},
					}},
				SrcSchema: map[string]schema.Table{
					"t1": {
						Name:   "table1",
						Id:     "t1",
						ColIds: []string{"c1", "c2"},
						ColDefs: map[string]schema.Column{
							"c1": {Name: "a", Id: "c1", Type: schema.Type{Name: "bigint", Mods: []int64{}}},
							"c2": {Name: "b", Id: "c2", Type: schema.Type{Name: "varchar", Mods: []int64{6}}},
						},
						PrimaryKeys: []schema.Key{{ColId: "c1"}},
						ForeignKeys: []schema.ForeignKey{{Name: "fk1", Id: "f1", ColIds: []string{"c1"}, ReferTableId: "t2", ReferColumnIds: []string{"c3"}}},
					},
					"t2": {
						Name:   "table2",
						Id:     "t2",
						ColIds: []string{"c3"},
						ColDefs: map[string]schema.Column{
							"c3": {Name: "c", Id: "c3", Type: schema.Type{Name: "bigint", Mods: []int64{}}},
						},
					},
				},
				SchemaIssues: map[string]internal.TableIssues{
					"t1": {
						ColumnLevelIssues: make(map[string][]internal.SchemaIssue),
					},
					"t2": {
						ColumnLevelIssues: make(map[string][]internal.SchemaIssue),
					},
				},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_MIGRATION_TYPE_UNSPECIFIED.Enum(),
				},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:   "table1",
						Id:     "t1",
						ColIds: []string{"c1", "c2"},
						ColDefs: map[string]ddl.ColumnDef{
							"c1": {Name: "a", Id: "c1", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
							"c2": {Name: "b", Id: "c2", T: ddl.Type{Name: ddl.String, Len: 6}},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c1"}},
						ParentTable: ddl.InterleavedParent{Id: "t2", OnDelete: constants.FK_NO_ACTION},
					},
					"t2": {
						Name:   "table2",
						Id:     "t2",
						ColIds: []string{"c3"},
						ColDefs: map[string]ddl.ColumnDef{
							"c3": {Name: "a", Id: "c3", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
						},
					}},
				SrcSchema: map[string]schema.Table{
					"t1": {
						Name:   "table1",
						Id:     "t1",
						ColIds: []string{"c1", "c2"},
						ColDefs: map[string]schema.Column{
							"c1": {Name: "a", Id: "c1", Type: schema.Type{Name: "bigint", Mods: []int64{}}},
							"c2": {Name: "b", Id: "c2", Type: schema.Type{Name: "varchar", Mods: []int64{6}}},
						},
						PrimaryKeys: []schema.Key{{ColId: "c1"}},
						ForeignKeys: []schema.ForeignKey{{Name: "fk1", Id: "f1", ColIds: []string{"c1"}, ReferTableId: "t2", ReferColumnIds: []string{"c3"}}},
					},
					"t2": {
						Name:   "table2",
						Id:     "t2",
						ColIds: []string{"c3"},
						ColDefs: map[string]schema.Column{
							"c3": {Name: "c", Id: "c3", Type: schema.Type{Name: "bigint", Mods: []int64{}}},
						},
					}},
				SchemaIssues: map[string]internal.TableIssues{
					"t1": {
						ColumnLevelIssues: map[string][]internal.SchemaIssue{
							"c1": {internal.Widened},
						},
					},
					"t2": {
						ColumnLevelIssues: map[string][]internal.SchemaIssue{
							"c3": {internal.Widened},
						},
					},
				},
			},
		},
		{
			name:  "Test rename success for interleaved table",
			table: "t1",
			payload: `
		{
		  "UpdateCols":{
			"c1": { "Rename": "aa" }
		}
		}`,
			statusCode: http.StatusOK,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:   "t1",
						ColIds: []string{"c1", "c2", "c3"},
						ColDefs: map[string]ddl.ColumnDef{
							"c1": {Name: "a", Id: "c1", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c2": {Name: "b", Id: "c2", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c3": {Name: "c", Id: "c3", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c1", Desc: false}, {ColId: "c2", Desc: false}},
						ParentTable: ddl.InterleavedParent{Id: "t2", OnDelete: constants.FK_CASCADE},
					},
					"t2": {
						Name:   "t2",
						ColIds: []string{"c4", "c5", "c6"},
						ColDefs: map[string]ddl.ColumnDef{
							"c4": {Name: "a", Id: "c4", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c5": {Name: "b", Id: "c5", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c6": {Name: "c", Id: "c6", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c4", Desc: false}},
					},
				},
				SrcSchema: map[string]schema.Table{
					"t1": {
						Name:   "t1",
						ColIds: []string{"c1", "c2", "c3"},
						ColDefs: map[string]schema.Column{
							"c1": {Name: "a", Id: "c1", Type: schema.Type{Name: "bigint", Mods: []int64{}}, NotNull: true},
							"c2": {Name: "b", Id: "c2", Type: schema.Type{Name: "bigint", Mods: []int64{}}, NotNull: true},
							"c3": {Name: "c", Id: "c3", Type: schema.Type{Name: "varchar", Mods: []int64{6}}, NotNull: true},
						},
						PrimaryKeys: []schema.Key{{ColId: "c1", Desc: false}, {ColId: "c2", Desc: false}},
						ForeignKeys: []schema.ForeignKey{{Name: "fk1", ColIds: []string{"c1"}, ReferTableId: "t2", ReferColumnIds: []string{"c4"}}},
					},
					"t2": {
						Name:   "t2",
						ColIds: []string{"c4", "c5", "c6"},
						ColDefs: map[string]schema.Column{
							"c4": {Name: "a", Id: "c4", Type: schema.Type{Name: "bigint", Mods: []int64{}}, NotNull: true},
							"c5": {Name: "b", Id: "c5", Type: schema.Type{Name: "bigint", Mods: []int64{}}, NotNull: true},
							"c6": {Name: "c", Id: "c6", Type: schema.Type{Name: "varchar", Mods: []int64{6}}, NotNull: true},
						},
						PrimaryKeys: []schema.Key{{ColId: "c4", Desc: false}},
					},
				},
				Audit:        internal.Audit{MigrationType: migration.MigrationData_SCHEMA_AND_DATA.Enum()},
				SchemaIssues: make(map[string]internal.TableIssues),
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:   "t1",
						ColIds: []string{"c1", "c2", "c3"},
						ColDefs: map[string]ddl.ColumnDef{
							"c1": {Name: "aa", Id: "c1", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c2": {Name: "b", Id: "c2", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c3": {Name: "c", Id: "c3", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c1", Desc: false}, {ColId: "c2", Desc: false}},
						ParentTable: ddl.InterleavedParent{Id: "t2", OnDelete: constants.FK_CASCADE},
					},
					"t2": {
						Name:   "t2",
						ColIds: []string{"c4", "c5", "c6"},
						ColDefs: map[string]ddl.ColumnDef{
							"c4": {Name: "aa", Id: "c4", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c5": {Name: "b", Id: "c5", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c6": {Name: "c", Id: "c6", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c4", Desc: false}},
					},
				},
				SrcSchema: map[string]schema.Table{
					"t1": {
						Name:   "t1",
						ColIds: []string{"c1", "c2", "c3"},
						ColDefs: map[string]schema.Column{
							"c1": {Name: "a", Id: "c1", Type: schema.Type{Name: "bigint", Mods: []int64{}}, NotNull: true},
							"c2": {Name: "b", Id: "c2", Type: schema.Type{Name: "bigint", Mods: []int64{}}, NotNull: true},
							"c3": {Name: "c", Id: "c3", Type: schema.Type{Name: "varchar", Mods: []int64{6}}, NotNull: true},
						},
						PrimaryKeys: []schema.Key{{ColId: "c1", Desc: false}, {ColId: "c2", Desc: false}},
						ForeignKeys: []schema.ForeignKey{{Name: "fk1", ColIds: []string{"c1"}, ReferTableId: "t2", ReferColumnIds: []string{"c4"}}},
					},
					"t2": {
						Name:   "t2",
						ColIds: []string{"c4", "c5", "c6"},
						ColDefs: map[string]schema.Column{
							"c4": {Name: "a", Id: "c4", Type: schema.Type{Name: "bigint", Mods: []int64{}}, NotNull: true},
							"c5": {Name: "b", Id: "c5", Type: schema.Type{Name: "bigint", Mods: []int64{}}, NotNull: true},
							"c6": {Name: "c", Id: "c6", Type: schema.Type{Name: "varchar", Mods: []int64{6}}, NotNull: true},
						},
						PrimaryKeys: []schema.Key{{ColId: "c4", Desc: false}},
					},
				},
				SchemaIssues: map[string]internal.TableIssues{
					"t1": {},
				},
			},
		},
		{
			name:  "Test rename success for interleaved table 2",
			table: "t1",
			payload: `
		{
		  "UpdateCols":{
			"c1": { "Rename": "aa" }
		}
		}`,
			statusCode: http.StatusOK,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:   "table1",
						Id:     "t1",
						ColIds: []string{"c1", "c2", "c3"},
						ColDefs: map[string]ddl.ColumnDef{
							"c1": {Name: "a", Id: "c1", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c2": {Name: "b", Id: "c2", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c3": {Name: "c", Id: "c3", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c1", Desc: false}, {ColId: "c2", Desc: false}},
					},
					"t2": {
						Name:   "table2",
						Id:     "t2",
						ColIds: []string{"c4", "c5", "c6"},
						ColDefs: map[string]ddl.ColumnDef{
							"c4": {Name: "a", Id: "c4", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c5": {Name: "b", Id: "c5", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c6": {Name: "c", Id: "c6", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c4", Desc: false}},
						ParentTable: ddl.InterleavedParent{Id: "t1", OnDelete: constants.FK_NO_ACTION},
					},
				},
				SrcSchema: map[string]schema.Table{
					"t1": {
						Name:   "table1",
						Id:     "t1",
						ColIds: []string{"c1", "c2", "c3"},
						ColDefs: map[string]schema.Column{
							"c1": {Name: "a", Id: "c1", Type: schema.Type{Name: "bigint", Mods: []int64{}}, NotNull: true},
							"c2": {Name: "b", Id: "c2", Type: schema.Type{Name: "bigint", Mods: []int64{}}, NotNull: true},
							"c3": {Name: "c", Id: "c3", Type: schema.Type{Name: "varchar", Mods: []int64{6}}, NotNull: true},
						},
						PrimaryKeys: []schema.Key{{ColId: "c1", Desc: false}, {ColId: "c2", Desc: false}},
					},
					"t2": {
						Name:   "table2",
						Id:     "t2",
						ColIds: []string{"c4", "c5", "c6"},
						ColDefs: map[string]schema.Column{
							"c4": {Name: "a", Id: "c4", Type: schema.Type{Name: "bigint", Mods: []int64{}}, NotNull: true},
							"c5": {Name: "b", Id: "c5", Type: schema.Type{Name: "bigint", Mods: []int64{}}, NotNull: true},
							"c6": {Name: "c", Id: "c6", Type: schema.Type{Name: "varchar", Mods: []int64{6}}, NotNull: true},
						},
						PrimaryKeys: []schema.Key{{ColId: "c4", Desc: false}},
						ForeignKeys: []schema.ForeignKey{{Name: "fk1", ColIds: []string{"c4"}, ReferTableId: "t1", ReferColumnIds: []string{"c1"}}},
					},
				},
				SchemaIssues: make(map[string]internal.TableIssues),
				Audit:        internal.Audit{MigrationType: migration.MigrationData_SCHEMA_AND_DATA.Enum()},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:   "table1",
						Id:     "t1",
						ColIds: []string{"c1", "c2", "c3"},
						ColDefs: map[string]ddl.ColumnDef{
							"c1": {Name: "aa", Id: "c1", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c2": {Name: "b", Id: "c2", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c3": {Name: "c", Id: "c3", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c1", Desc: false}, {ColId: "c2", Desc: false}},
					},
					"t2": {
						Name:   "table2",
						Id:     "t2",
						ColIds: []string{"c4", "c5", "c6"},
						ColDefs: map[string]ddl.ColumnDef{
							"c4": {Name: "aa", Id: "c4", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c5": {Name: "b", Id: "c5", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c6": {Name: "c", Id: "c6", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c4", Desc: false}},
						ParentTable: ddl.InterleavedParent{Id: "t1", OnDelete: constants.FK_NO_ACTION},
					},
				},
				SrcSchema: map[string]schema.Table{
					"t1": {
						Name:   "table1",
						Id:     "t1",
						ColIds: []string{"c1", "c2", "c3"},
						ColDefs: map[string]schema.Column{
							"c1": {Name: "a", Id: "c1", Type: schema.Type{Name: "bigint", Mods: []int64{}}, NotNull: true},
							"c2": {Name: "b", Id: "c2", Type: schema.Type{Name: "bigint", Mods: []int64{}}, NotNull: true},
							"c3": {Name: "c", Id: "c3", Type: schema.Type{Name: "varchar", Mods: []int64{6}}, NotNull: true},
						},
						PrimaryKeys: []schema.Key{{ColId: "c1", Desc: false}, {ColId: "c2", Desc: false}},
					},
					"t2": {
						Name:   "table2",
						Id:     "t2",
						ColIds: []string{"c4", "c5", "c6"},
						ColDefs: map[string]schema.Column{
							"c4": {Name: "a", Id: "c4", Type: schema.Type{Name: "bigint", Mods: []int64{}}, NotNull: true},
							"c5": {Name: "b", Id: "c5", Type: schema.Type{Name: "bigint", Mods: []int64{}}, NotNull: true},
							"c6": {Name: "c", Id: "c6", Type: schema.Type{Name: "varchar", Mods: []int64{6}}, NotNull: true},
						},
						PrimaryKeys: []schema.Key{{ColId: "c4", Desc: false}},
						ForeignKeys: []schema.ForeignKey{{Name: "fk1", ColIds: []string{"c4"}, ReferTableId: "t1", ReferColumnIds: []string{"c1"}}},
					},
				},
				SchemaIssues: map[string]internal.TableIssues{
					"t1": {},
				},
			},
		},
		{
			name:  "Test change column length success for interleaved table",
			table: "t1",
			payload: `
		{
		  "UpdateCols":{
			"c1": { "MaxColLength": "20" }
		}
		}`,
			statusCode: http.StatusOK,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:   "t1",
						ColIds: []string{"c1", "c2", "c3"},
						ColDefs: map[string]ddl.ColumnDef{
							"c1": {Name: "a", Id: "c1", T: ddl.Type{Name: ddl.String, Len: 50}, NotNull: true},
							"c2": {Name: "b", Id: "c2", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c3": {Name: "c", Id: "c3", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c1", Desc: false}, {ColId: "c2", Desc: false}},
						ParentTable: ddl.InterleavedParent{Id: "t2", OnDelete: constants.FK_CASCADE},
					},
					"t2": {
						Name:   "t2",
						ColIds: []string{"c4", "c5", "c6"},
						ColDefs: map[string]ddl.ColumnDef{
							"c4": {Name: "a", Id: "c4", T: ddl.Type{Name: ddl.String, Len: 50}, NotNull: true},
							"c5": {Name: "b", Id: "c5", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c6": {Name: "c", Id: "c6", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c4", Desc: false}},
					},
				},
				SrcSchema: map[string]schema.Table{
					"t1": {
						Name:   "t1",
						ColIds: []string{"c1", "c2", "c3"},
						ColDefs: map[string]schema.Column{
							"c1": {Name: "a", Id: "c1", Type: schema.Type{Name: "varchar", Mods: []int64{50}}, NotNull: true},
							"c2": {Name: "b", Id: "c2", Type: schema.Type{Name: "bigint", Mods: []int64{}}, NotNull: true},
							"c3": {Name: "c", Id: "c3", Type: schema.Type{Name: "varchar", Mods: []int64{6}}, NotNull: true},
						},
						PrimaryKeys: []schema.Key{{ColId: "c1", Desc: false}, {ColId: "c2", Desc: false}},
						ForeignKeys: []schema.ForeignKey{{Name: "fk1", ColIds: []string{"c1"}, ReferTableId: "t2", ReferColumnIds: []string{"c4"}}},
					},
					"t2": {
						Name:   "t2",
						ColIds: []string{"c4", "c5", "c6"},
						ColDefs: map[string]schema.Column{
							"c4": {Name: "a", Id: "c4", Type: schema.Type{Name: "varchar", Mods: []int64{50}}, NotNull: true},
							"c5": {Name: "b", Id: "c5", Type: schema.Type{Name: "bigint", Mods: []int64{}}, NotNull: true},
							"c6": {Name: "c", Id: "c6", Type: schema.Type{Name: "varchar", Mods: []int64{6}}, NotNull: true},
						},
						PrimaryKeys: []schema.Key{{ColId: "c4", Desc: false}},
					},
				},
				Audit:        internal.Audit{MigrationType: migration.MigrationData_SCHEMA_AND_DATA.Enum()},
				SchemaIssues: make(map[string]internal.TableIssues),
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:   "t1",
						ColIds: []string{"c1", "c2", "c3"},
						ColDefs: map[string]ddl.ColumnDef{
							"c1": {Name: "a", Id: "c1", T: ddl.Type{Name: ddl.String, Len: 20}, NotNull: true},
							"c2": {Name: "b", Id: "c2", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c3": {Name: "c", Id: "c3", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c1", Desc: false}, {ColId: "c2", Desc: false}},
						ParentTable: ddl.InterleavedParent{Id: "t2", OnDelete: constants.FK_CASCADE},
					},
					"t2": {
						Name:   "t2",
						ColIds: []string{"c4", "c5", "c6"},
						ColDefs: map[string]ddl.ColumnDef{
							"c4": {Name: "a", Id: "c4", T: ddl.Type{Name: ddl.String, Len: 20}, NotNull: true},
							"c5": {Name: "b", Id: "c5", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c6": {Name: "c", Id: "c6", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c4", Desc: false}},
					},
				},
				SrcSchema: map[string]schema.Table{
					"t1": {
						Name:   "t1",
						ColIds: []string{"c1", "c2", "c3"},
						ColDefs: map[string]schema.Column{
							"c1": {Name: "a", Id: "c1", Type: schema.Type{Name: "varchar", Mods: []int64{50}}, NotNull: true},
							"c2": {Name: "b", Id: "c2", Type: schema.Type{Name: "bigint", Mods: []int64{}}, NotNull: true},
							"c3": {Name: "c", Id: "c3", Type: schema.Type{Name: "varchar", Mods: []int64{6}}, NotNull: true},
						},
						PrimaryKeys: []schema.Key{{ColId: "c1", Desc: false}, {ColId: "c2", Desc: false}},
						ForeignKeys: []schema.ForeignKey{{Name: "fk1", ColIds: []string{"c1"}, ReferTableId: "t2", ReferColumnIds: []string{"c4"}}},
					},
					"t2": {
						Name:   "t2",
						ColIds: []string{"c4", "c5", "c6"},
						ColDefs: map[string]schema.Column{
							"c4": {Name: "a", Id: "c4", Type: schema.Type{Name: "varchar", Mods: []int64{50}}, NotNull: true},
							"c5": {Name: "b", Id: "c5", Type: schema.Type{Name: "bigint", Mods: []int64{}}, NotNull: true},
							"c6": {Name: "c", Id: "c6", Type: schema.Type{Name: "varchar", Mods: []int64{6}}, NotNull: true},
						},
						PrimaryKeys: []schema.Key{{ColId: "c4", Desc: false}},
					},
				},
				SchemaIssues: map[string]internal.TableIssues{
					"t1": {},
				},
			},
		},
		{
			name:  "Test change type success for related foreign key columns",
			table: "t1",
			payload: `
			{
			  "UpdateCols":{
				"c1": { "ToType": "STRING" }
			}
			}`,
			statusCode: http.StatusOK,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:   "t1",
						ColIds: []string{"c1"},
						ColDefs: map[string]ddl.ColumnDef{
							"c1": {Name: "a", Id: "c1", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c1", Desc: false}},
						ForeignKeys: []ddl.Foreignkey{{Name: "fk1", ColIds: []string{"c1"}, ReferTableId: "t2", ReferColumnIds: []string{"c4"}}},
					},
					"t2": {
						Name:   "t2",
						ColIds: []string{"c4"},
						ColDefs: map[string]ddl.ColumnDef{
							"c4": {Name: "a", Id: "c4", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c4", Desc: false}},
					},
				},
				SrcSchema: map[string]schema.Table{
					"t1": {
						Name:   "t1",
						ColIds: []string{"c1"},
						ColDefs: map[string]schema.Column{
							"c1": {Name: "a", Id: "c1", Type: schema.Type{Name: "bigint", Mods: []int64{}}, NotNull: true},
						},
						PrimaryKeys: []schema.Key{{ColId: "c1", Desc: false}},
						ForeignKeys: []schema.ForeignKey{{Name: "fk1", ColIds: []string{"c1"}, ReferTableId: "t2", ReferColumnIds: []string{"c4"}}},
					},
					"t2": {
						Name:   "t2",
						ColIds: []string{"c4"},
						ColDefs: map[string]schema.Column{
							"c4": {Name: "a", Id: "c4", Type: schema.Type{Name: "bigint", Mods: []int64{}}, NotNull: true},
						},
						PrimaryKeys: []schema.Key{{ColId: "c4", Desc: false}},
					},
				},
				Audit: internal.Audit{MigrationType: migration.MigrationData_SCHEMA_AND_DATA.Enum()},
				SchemaIssues: map[string]internal.TableIssues{
					"t1": {
						ColumnLevelIssues: make(map[string][]internal.SchemaIssue),
					},
					"t2": {
						ColumnLevelIssues: make(map[string][]internal.SchemaIssue),
					},
				},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:   "t1",
						ColIds: []string{"c1"},
						ColDefs: map[string]ddl.ColumnDef{
							"c1": {Name: "a", Id: "c1", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c1", Desc: false}},
						ForeignKeys: []ddl.Foreignkey{{Name: "fk1", ColIds: []string{"c1"}, ReferTableId: "t2", ReferColumnIds: []string{"c4"}}},
					},
					"t2": {
						Name:   "t2",
						ColIds: []string{"c4"},
						ColDefs: map[string]ddl.ColumnDef{
							"c4": {Name: "a", Id: "c4", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c4", Desc: false}},
					},
				},
				SrcSchema: map[string]schema.Table{
					"t1": {
						Name:   "t1",
						ColIds: []string{"c1"},
						ColDefs: map[string]schema.Column{
							"c1": {Name: "a", Id: "c1", Type: schema.Type{Name: "bigint", Mods: []int64{}}, NotNull: true},
						},
						PrimaryKeys: []schema.Key{{ColId: "c1", Desc: false}},
						ForeignKeys: []schema.ForeignKey{{Name: "fk1", ColIds: []string{"c1"}, ReferTableId: "t2", ReferColumnIds: []string{"c4"}}},
					},
					"t2": {
						Name:   "t2",
						ColIds: []string{"c4"},
						ColDefs: map[string]schema.Column{
							"c4": {Name: "a", Id: "c4", Type: schema.Type{Name: "bigint", Mods: []int64{}}, NotNull: true},
						},
						PrimaryKeys: []schema.Key{{ColId: "c4", Desc: false}},
					},
				},
				SchemaIssues: map[string]internal.TableIssues{
					"t1": {
						ColumnLevelIssues: map[string][]internal.SchemaIssue{
							"c1": {internal.Widened},
						},
					},
					"t2": {
						ColumnLevelIssues: map[string][]internal.SchemaIssue{
							"c4": {internal.Widened},
						},
					},
				},
			},
		},
		{
			name:  "Test update auto-gen UUID",
			table: "t1",
			payload: `{
				"UpdateCols":{ "c2": {"AutoGen":{"Name":"UUID","GenerationType":"Pre-defined"}}}
				}`,
			statusCode: http.StatusOK,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:   "table1",
						Id:     "t1",
						ColIds: []string{"c1", "c2"},
						ColDefs: map[string]ddl.ColumnDef{
							"c1": {Name: "a", Id: "c1", T: ddl.Type{Name: ddl.Int64}},
							"c2": {Name: "b", Id: "c2", T: ddl.Type{Name: ddl.String, Len: 6}},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c1"}},
						ParentTable: ddl.InterleavedParent{Id: "t2", OnDelete: constants.FK_NO_ACTION},
					},
				},
				SrcSchema: map[string]schema.Table{
					"t1": {
						Name:   "table1",
						ColIds: []string{"c1", "c2"},
						ColDefs: map[string]schema.Column{
							"c1": {Name: "a", Id: "c1", Type: schema.Type{Name: "bigint", Mods: []int64{}}},
							"c2": {Name: "b", Id: "c2", Type: schema.Type{Name: "varchar", Mods: []int64{6}}},
						},
						PrimaryKeys: []schema.Key{{ColId: "c1"}},
					}},
				SchemaIssues: map[string]internal.TableIssues{
					"t1": {
						ColumnLevelIssues: map[string][]internal.SchemaIssue{
							"c1": {internal.Widened},
						},
					},
				},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_MIGRATION_TYPE_UNSPECIFIED.Enum(),
				},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:   "table1",
						Id:     "t1",
						ColIds: []string{"c1", "c2"},
						ColDefs: map[string]ddl.ColumnDef{
							"c1": {Name: "a", Id: "c1", T: ddl.Type{Name: ddl.Int64}},
							"c2": {Name: "b", Id: "c2", T: ddl.Type{Name: ddl.String, Len: 6}, AutoGen: ddl.AutoGenCol{Name: "UUID", GenerationType: "Pre-defined"}},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c1"}},
						ParentTable: ddl.InterleavedParent{Id: "t2", OnDelete: constants.FK_NO_ACTION},
					},
				},
				SrcSchema: map[string]schema.Table{
					"t1": {
						Name:   "table1",
						ColIds: []string{"c1", "c2"},
						ColDefs: map[string]schema.Column{
							"c1": {Name: "a", Id: "c1", Type: schema.Type{Name: "bigint", Mods: []int64{}}},
							"c2": {Name: "b", Id: "c2", Type: schema.Type{Name: "varchar", Mods: []int64{6}}},
						},
						PrimaryKeys: []schema.Key{{ColId: "c1"}},
					}},
				SchemaIssues: map[string]internal.TableIssues{
					"t1": {
						ColumnLevelIssues: map[string][]internal.SchemaIssue{
							"c1": {internal.Widened},
						},
					},
				},
			},
		},
		{
			name:  "Test update auto-gen Sequence",
			table: "t1",
			payload: `{
				"UpdateCols":{ "c1": {"AutoGen":{"Name":"seq1","GenerationType":"Sequence"}}}
				}`,
			statusCode: http.StatusOK,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:   "table1",
						Id:     "t1",
						ColIds: []string{"c1", "c2"},
						ColDefs: map[string]ddl.ColumnDef{
							"c1": {Name: "a", Id: "c1", T: ddl.Type{Name: ddl.Int64}},
							"c2": {Name: "b", Id: "c2", T: ddl.Type{Name: ddl.String, Len: 6}},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c1"}},
						ParentTable: ddl.InterleavedParent{Id: "t2", OnDelete: constants.FK_CASCADE},
					},
				},
				SrcSchema: map[string]schema.Table{
					"t1": {
						Name:   "table1",
						ColIds: []string{"c1", "c2"},
						ColDefs: map[string]schema.Column{
							"c1": {Name: "a", Id: "c1", Type: schema.Type{Name: "bigint", Mods: []int64{}}},
							"c2": {Name: "b", Id: "c2", Type: schema.Type{Name: "varchar", Mods: []int64{6}}},
						},
						PrimaryKeys: []schema.Key{{ColId: "c1"}},
					}},
				SchemaIssues: map[string]internal.TableIssues{
					"t1": {
						ColumnLevelIssues: map[string][]internal.SchemaIssue{
							"c1": {internal.Widened},
						},
					},
				},
				SpSequences: map[string]ddl.Sequence{
					"s1": {
						Id:              "s1",
						Name:            "seq1",
						SequenceKind:    "BIT REVERSED POSITIVE",
						ColumnsUsingSeq: make(map[string][]string),
					},
				},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_MIGRATION_TYPE_UNSPECIFIED.Enum(),
				},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:   "table1",
						Id:     "t1",
						ColIds: []string{"c1", "c2"},
						ColDefs: map[string]ddl.ColumnDef{
							"c1": {Name: "a", Id: "c1", T: ddl.Type{Name: ddl.Int64}, AutoGen: ddl.AutoGenCol{Name: "seq1", GenerationType: "Sequence"}},
							"c2": {Name: "b", Id: "c2", T: ddl.Type{Name: ddl.String, Len: 6}},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c1"}},
						ParentTable: ddl.InterleavedParent{Id: "t2", OnDelete: constants.FK_CASCADE},
					},
				},
				SrcSchema: map[string]schema.Table{
					"t1": {
						Name:   "table1",
						ColIds: []string{"c1", "c2"},
						ColDefs: map[string]schema.Column{
							"c1": {Name: "a", Id: "c1", Type: schema.Type{Name: "bigint", Mods: []int64{}}},
							"c2": {Name: "b", Id: "c2", Type: schema.Type{Name: "varchar", Mods: []int64{6}}},
						},
						PrimaryKeys: []schema.Key{{ColId: "c1"}},
					}},
				SchemaIssues: map[string]internal.TableIssues{
					"t1": {
						ColumnLevelIssues: map[string][]internal.SchemaIssue{
							"c1": {internal.Widened},
						},
					},
				},
				SpSequences: map[string]ddl.Sequence{
					"s1": {
						Id:           "s1",
						Name:         "seq1",
						SequenceKind: "BIT REVERSED POSITIVE",
						ColumnsUsingSeq: map[string][]string{
							"t1": {"c1"},
						},
					},
				},
			},
		},
	}

	for _, tc := range tc {

		sessionState := session.GetSessionState()
		sessionState.Conv = tc.conv
		sessionState.Driver = constants.MYSQL

		payload := tc.payload

		req, err := http.NewRequest("POST", "/typemap/table?table="+tc.table, strings.NewReader(payload))
		if err != nil {
			t.Fatal(err)
		}

		req.Header.Set("Content-Type", "application/json")

		rr := httptest.NewRecorder()

		handler := http.HandlerFunc(UpdateTableSchema)

		handler.ServeHTTP(rr, req)

		res := &internal.Conv{}

		json.Unmarshal(rr.Body.Bytes(), &res)

		if status := rr.Code; int64(status) != tc.statusCode {
			t.Errorf("handler returned wrong status code: got %v want %v",
				status, tc.statusCode)
		}

		if tc.statusCode == http.StatusOK {
			assert.Equal(t, tc.expectedConv, res, tc.name)
		}
	}
}

package internal

import (
	"testing"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/schema"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
	"github.com/stretchr/testify/assert"
)

func TestExtractOverridesFromConv(t *testing.T) {
	tests := []struct {
		name     string
		conv     *Conv
		expected *OverridesFile
	}{
		{
			name: "no renames",
			conv: &Conv{
				SrcSchema: map[string]schema.Table{
					"t1": {Name: "users", ColDefs: map[string]schema.Column{
						"c1": {Name: "id"},
						"c2": {Name: "name"},
					}},
				},
				SpSchema: map[string]ddl.CreateTable{
					"t1": {Name: "users", ColDefs: map[string]ddl.ColumnDef{
						"c1": {Name: "id"},
						"c2": {Name: "name"},
					}},
				},
				ToSpanner: map[string]NameAndCols{
					"users": {
						Name: "users",
						Cols: map[string]string{
							"id":   "id",
							"name": "name",
						},
					},
				},
			},
			expected: &OverridesFile{
				RenamedTables:  map[string]string{},
				RenamedColumns: map[string]map[string]string{},
			},
		},
		{
			name: "table rename only",
			conv: &Conv{
				SrcSchema: map[string]schema.Table{
					"t1": {Name: "users", ColDefs: map[string]schema.Column{
						"c1": {Name: "id"},
						"c2": {Name: "name"},
					}},
				},
				SpSchema: map[string]ddl.CreateTable{
					"t1": {Name: "Users", ColDefs: map[string]ddl.ColumnDef{
						"c1": {Name: "id"},
						"c2": {Name: "name"},
					}},
				},
				ToSpanner: map[string]NameAndCols{
					"users": {
						Name: "Users",
						Cols: map[string]string{
							"id":   "id",
							"name": "name",
						},
					},
				},
			},
			expected: &OverridesFile{
				RenamedTables: map[string]string{
					"users": "Users",
				},
				RenamedColumns: map[string]map[string]string{},
			},
		},
		{
			name: "column rename only",
			conv: &Conv{
				SrcSchema: map[string]schema.Table{
					"t1": {Name: "users", ColDefs: map[string]schema.Column{
						"c1": {Name: "id"},
						"c2": {Name: "user_name"},
					}},
				},
				SpSchema: map[string]ddl.CreateTable{
					"t1": {Name: "users", ColDefs: map[string]ddl.ColumnDef{
						"c1": {Name: "id"},
						"c2": {Name: "name"},
					}},
				},
				ToSpanner: map[string]NameAndCols{
					"users": {
						Name: "users",
						Cols: map[string]string{
							"id":        "id",
							"user_name": "name",
						},
					},
				},
			},
			expected: &OverridesFile{
				RenamedTables: map[string]string{},
				RenamedColumns: map[string]map[string]string{
					"users": {
						"user_name": "name",
					},
				},
			},
		},
		{
			name: "table and column renames",
			conv: &Conv{
				SrcSchema: map[string]schema.Table{
					"t1": {Name: "user_table", ColDefs: map[string]schema.Column{
						"c1": {Name: "user_id"},
						"c2": {Name: "user_name"},
					}},
				},
				SpSchema: map[string]ddl.CreateTable{
					"t1": {Name: "Users", ColDefs: map[string]ddl.ColumnDef{
						"c1": {Name: "id"},
						"c2": {Name: "name"},
					}},
				},
				ToSpanner: map[string]NameAndCols{
					"user_table": {
						Name: "Users",
						Cols: map[string]string{
							"user_id":   "id",
							"user_name": "name",
						},
					},
				},
			},
			expected: &OverridesFile{
				RenamedTables: map[string]string{
					"user_table": "Users",
				},
				RenamedColumns: map[string]map[string]string{
					"user_table": {
						"user_id":   "id",
						"user_name": "name",
					},
				},
			},
		},
		{
			name: "multiple tables with renames",
			conv: &Conv{
				SrcSchema: map[string]schema.Table{
					"t1": {Name: "user_table", ColDefs: map[string]schema.Column{
						"c1": {Name: "user_id"},
					}},
					"t2": {Name: "order_table", ColDefs: map[string]schema.Column{
						"c2": {Name: "order_id"},
					}},
				},
				SpSchema: map[string]ddl.CreateTable{
					"t1": {Name: "Users", ColDefs: map[string]ddl.ColumnDef{
						"c1": {Name: "id"},
					}},
					"t2": {Name: "Orders", ColDefs: map[string]ddl.ColumnDef{
						"c2": {Name: "id"},
					}},
				},
				ToSpanner: map[string]NameAndCols{
					"user_table": {
						Name: "Users",
						Cols: map[string]string{
							"user_id": "id",
						},
					},
					"order_table": {
						Name: "Orders",
						Cols: map[string]string{
							"order_id": "id",
						},
					},
				},
			},
			expected: &OverridesFile{
				RenamedTables: map[string]string{
					"user_table":  "Users",
					"order_table": "Orders",
				},
				RenamedColumns: map[string]map[string]string{
					"user_table": {
						"user_id": "id",
					},
					"order_table": {
						"order_id": "id",
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ExtractOverridesFromConv(tt.conv)
			assert.Equal(t, tt.expected, result, "Test case failed: %s", tt.name)
		})
	}
}

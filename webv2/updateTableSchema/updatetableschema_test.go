package updateTableSchema

/*
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
			name:  "Test remove fail column part of PK",
			table: "t1",
			payload: `
    {
      "UpdateCols":{
		"a": { "Removed": true }
	}
    }`,
			statusCode: http.StatusBadRequest,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:     "t1",
						ColNames: []string{"a", "b"},
						ColDefs: map[string]ddl.ColumnDef{
							"a": {Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
							"b": {Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
						},
						Pks: []ddl.IndexKey{{Col: "a"}},
					}},
				ToSource: map[string]internal.NameAndCols{
					"t1": {Name: "t1", Cols: map[string]string{"a": "a", "b": "b"}},
				},
			},
		},
		{
			name:  "Test remove fail column part of secondary index",
			table: "t1",
			payload: `
    {
      "UpdateCols":{
		"b": { "Removed": true }
	}
    }`,
			statusCode: http.StatusPreconditionFailed,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:     "t1",
						ColNames: []string{"a", "b"},
						ColDefs: map[string]ddl.ColumnDef{
							"a": {Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
							"b": {Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
						},
						Pks:     []ddl.IndexKey{{Col: "a"}},
						Indexes: []ddl.CreateIndex{{Name: "idx", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}}},
					}},
				ToSource: map[string]internal.NameAndCols{
					"t1": {Name: "t1", Cols: map[string]string{"a": "a", "b": "b"}},
				},
			},
		},
		{
			name:  "Test remove fail column part of FK",
			table: "t1",
			payload: `
    {
      "UpdateCols":{
		"b": { "Removed": true }
	}
    }`,
			statusCode: http.StatusPreconditionFailed,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:     "t1",
						ColNames: []string{"a", "b"},
						ColDefs: map[string]ddl.ColumnDef{
							"a": {Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
							"b": {Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
						},
						Pks: []ddl.IndexKey{{Col: "a"}},
						Fks: []ddl.Foreignkey{{Name: "fk1", Columns: []string{"b"}, ReferTable: "t2", ReferColumns: []string{"b"}}},
					}},
				ToSource: map[string]internal.NameAndCols{
					"t1": {Name: "t1", Cols: map[string]string{"a": "a", "b": "b"}},
				},
			},
		},
		{
			name:  "Test remove fail column referenced by FK",
			table: "t1",
			payload: `
    {
      "UpdateCols":{
		"b": { "Removed": true }
	}
    }`,
			statusCode: http.StatusPreconditionFailed,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:     "t1",
						ColNames: []string{"a", "b"},
						ColDefs: map[string]ddl.ColumnDef{
							"a": {Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
							"b": {Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
						},
						Pks: []ddl.IndexKey{{Col: "a"}},
					},
					"t2": {
						Name:     "t2",
						ColNames: []string{"aa", "bb"},
						ColDefs: map[string]ddl.ColumnDef{
							"aa": {Name: "aa", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
							"bb": {Name: "bb", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
						},
						Pks: []ddl.IndexKey{{Col: "aa"}},
						Fks: []ddl.Foreignkey{{Name: "fk1", Columns: []string{"bb"}, ReferTable: "t1", ReferColumns: []string{"b"}}},
					},
				},
				ToSource: map[string]internal.NameAndCols{
					"t1": {Name: "t1", Cols: map[string]string{"a": "a", "b": "b"}},
				},
			},
		},
		{
			name:  "Test remove success",
			table: "t1",
			payload: `
    {
      "UpdateCols":{
		"c": { "Removed": true }
	}
    }`,
			statusCode: http.StatusOK,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:     "t1",
						ColNames: []string{"a", "b", "c"},
						ColDefs: map[string]ddl.ColumnDef{
							"a": {Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
							"b": {Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
							"c": {Name: "c", T: ddl.Type{Name: ddl.Int64}},
						},
						Pks: []ddl.IndexKey{{Col: "a"}},
					}},
				Issues: map[string]map[string][]internal.SchemaIssue{
					"t1": {
						"c": {internal.Widened},
					},
				},
				ToSource: map[string]internal.NameAndCols{
					"t1": {Name: "t1", Cols: map[string]string{"a": "a", "b": "b", "c": "c"}},
				},
				ToSpanner: map[string]internal.NameAndCols{
					"t1": {Name: "t1", Cols: map[string]string{"a": "a", "b": "b", "c": "c"}},
				},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:     "t1",
						ColNames: []string{"a", "b"},
						ColDefs: map[string]ddl.ColumnDef{
							"a": {Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
							"b": {Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
						},
						Pks: []ddl.IndexKey{{Col: "a"}},
					}},
				Issues: map[string]map[string][]internal.SchemaIssue{
					"t1": {},
				},
				ToSource: map[string]internal.NameAndCols{
					"t1": {Name: "t1", Cols: map[string]string{"a": "a", "b": "b"}},
				},
				ToSpanner: map[string]internal.NameAndCols{
					"t1": {Name: "t1", Cols: map[string]string{"a": "a", "b": "b"}},
				},
			},
		},
		{
			name:  "Test rename fail column part of PK and child table",
			table: "t1",
			payload: `
    {
      "UpdateCols":{
		"b": { "Rename": "bb" }
	}
    }`,
			statusCode: http.StatusBadRequest,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:     "t1",
						ColNames: []string{"a", "b", "c"},
						ColDefs: map[string]ddl.ColumnDef{
							"a": {Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
							"b": {Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
							"c": {Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
						},
						Pks:    []ddl.IndexKey{{Col: "a"}, {Col: "b"}},
						Parent: "t2",
					}},
				ToSource: map[string]internal.NameAndCols{
					"t1": {Name: "t1", Cols: map[string]string{"a": "a", "b": "b", "c": "c"}},
				},
			},
		},
		{
			name:  "Test rename fail column part of PK and parent table",
			table: "t1",
			payload: `
		{
		  "UpdateCols":{
			"a": { "Rename": "aa" }
		}
		}`,
			statusCode: http.StatusBadRequest,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:     "t1",
						ColNames: []string{"a", "b"},
						ColDefs: map[string]ddl.ColumnDef{
							"a": {Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
							"b": {Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
						},
						Pks: []ddl.IndexKey{{Col: "a"}},
					},
					"t2": {
						Name:     "t2",
						ColNames: []string{"a", "b", "c"},
						ColDefs: map[string]ddl.ColumnDef{
							"a": {Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
							"b": {Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
							"c": {Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
						},
						Pks:    []ddl.IndexKey{{Col: "a"}, {Col: "b"}},
						Parent: "t1",
					},
				},
				ToSource: map[string]internal.NameAndCols{
					"t1": {Name: "t1", Cols: map[string]string{"a": "a", "b": "b"}},
					"t2": {Name: "t2", Cols: map[string]string{"a": "a", "b": "b", "c": "c"}},
				},
			},
		},
		{
			name:  "Test rename fail column part of secondary index",
			table: "t1",
			payload: `
		{
		  "UpdateCols":{
			"b": { "Rename": "bb" }
		}
		}`,
			statusCode: http.StatusPreconditionFailed,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:     "t1",
						ColNames: []string{"a", "b"},
						ColDefs: map[string]ddl.ColumnDef{
							"a": {Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
							"b": {Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
						},
						Pks:     []ddl.IndexKey{{Col: "a"}},
						Indexes: []ddl.CreateIndex{{Name: "idx", Table: "t1", Unique: false, Keys: []ddl.IndexKey{{Col: "b", Desc: false}}}},
					}},
				ToSource: map[string]internal.NameAndCols{
					"t1": {Name: "t1", Cols: map[string]string{"a": "a", "b": "b"}},
				},
			},
		},
		{
			name:  "Test rename fail column part of FK",
			table: "t1",
			payload: `
		{
		  "UpdateCols":{
			"b": { "Rename": "bb" }
		}
		}`,
			statusCode: http.StatusPreconditionFailed,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:     "t1",
						ColNames: []string{"a", "b"},
						ColDefs: map[string]ddl.ColumnDef{
							"a": {Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
							"b": {Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
						},
						Pks: []ddl.IndexKey{{Col: "a"}},
						Fks: []ddl.Foreignkey{{Name: "fk1", Columns: []string{"b"}, ReferTable: "t2", ReferColumns: []string{"b"}}},
					}},
				ToSource: map[string]internal.NameAndCols{
					"t1": {Name: "t1", Cols: map[string]string{"a": "a", "b": "b"}},
				},
			},
		},
		{
			name:  "Test rename fail column referenced by FK",
			table: "t1",
			payload: `
		{
		  "UpdateCols":{
			"b": { "Rename": "bb" }
		}
		}`,
			statusCode: http.StatusPreconditionFailed,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:     "t1",
						ColNames: []string{"a", "b"},
						ColDefs: map[string]ddl.ColumnDef{
							"a": {Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
							"b": {Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
						},
						Pks: []ddl.IndexKey{{Col: "a"}},
					},
					"t2": {
						Name:     "t2",
						ColNames: []string{"aa", "bb"},
						ColDefs: map[string]ddl.ColumnDef{
							"aa": {Name: "aa", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
							"bb": {Name: "bb", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
						},
						Pks: []ddl.IndexKey{{Col: "aa"}},
						Fks: []ddl.Foreignkey{{Name: "fk1", Columns: []string{"bb"}, ReferTable: "t1", ReferColumns: []string{"b"}}},
					},
				},
				ToSource: map[string]internal.NameAndCols{
					"t1": {Name: "t1", Cols: map[string]string{"a": "a", "b": "b"}},
				},
			},
		},
		{
			name:  "Test rename success",
			table: "t1",
			payload: `
    {
      "UpdateCols":{
		"a": { "Rename": "aa" }
	}
    }`,
			statusCode: http.StatusOK,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:     "t1",
						ColNames: []string{"a", "b", "c"},
						ColDefs: map[string]ddl.ColumnDef{
							"a": {Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
							"b": {Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
							"c": {Name: "c", T: ddl.Type{Name: ddl.Int64}},
						},
						Pks: []ddl.IndexKey{{Col: "a"}},
					}},
				ToSource: map[string]internal.NameAndCols{
					"t1": {Name: "t1", Cols: map[string]string{"a": "a", "b": "b", "c": "c"}},
				},
				ToSpanner: map[string]internal.NameAndCols{
					"t1": {Name: "t1", Cols: map[string]string{"a": "a", "b": "b", "c": "c"}},
				},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:     "t1",
						ColNames: []string{"aa", "b", "c"},
						ColDefs: map[string]ddl.ColumnDef{
							"aa": {Name: "aa", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
							"b":  {Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
							"c":  {Name: "c", T: ddl.Type{Name: ddl.Int64}},
						},
						Pks: []ddl.IndexKey{{Col: "aa"}},
					}},
				ToSource: map[string]internal.NameAndCols{
					"t1": {Name: "t1", Cols: map[string]string{"aa": "a", "b": "b", "c": "c"}},
				},
				ToSpanner: map[string]internal.NameAndCols{
					"t1": {Name: "t1", Cols: map[string]string{"a": "aa", "b": "b", "c": "c"}},
				},
			},
		},
		{
			name:  "Test change type success",
			table: "t1",
			payload: `
    {
      "UpdateCols":{
		"a": { "ToType": "STRING" },
		"b": { "ToType": "BYTES" }
	}
    }`,
			statusCode: http.StatusOK,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:     "t1",
						ColNames: []string{"a", "b"},
						ColDefs: map[string]ddl.ColumnDef{
							"a": {Name: "a", T: ddl.Type{Name: ddl.Int64}},
							"b": {Name: "b", T: ddl.Type{Name: ddl.String, Len: 6}},
						},
						Pks: []ddl.IndexKey{{Col: "a"}},
					}},
				SrcSchema: map[string]schema.Table{
					"t1": {
						Name:     "t1",
						ColNames: []string{"a", "b"},
						ColDefs: map[string]schema.Column{
							"a": {Name: "a", Type: schema.Type{Name: "bigint", Mods: []int64{}}},
							"b": {Name: "b", Type: schema.Type{Name: "varchar", Mods: []int64{6}}},
						},
						PrimaryKeys: []schema.Key{{Column: "a"}},
					}},
				ToSource: map[string]internal.NameAndCols{
					"t1": {Name: "t1", Cols: map[string]string{"a": "a", "b": "b"}},
				},
				ToSpanner: map[string]internal.NameAndCols{
					"t1": {Name: "t1", Cols: map[string]string{"a": "a", "b": "b"}},
				},
				Issues: map[string]map[string][]internal.SchemaIssue{
					"t1": {},
				},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:     "t1",
						ColNames: []string{"a", "b"},
						ColDefs: map[string]ddl.ColumnDef{
							"a": {Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
							"b": {Name: "b", T: ddl.Type{Name: ddl.Bytes, Len: 6}},
						},
						Pks: []ddl.IndexKey{{Col: "a"}},
					}},
				SrcSchema: map[string]schema.Table{
					"t1": {
						Name:     "t1",
						ColNames: []string{"a", "b"},
						ColDefs: map[string]schema.Column{
							"a": {Name: "a", Type: schema.Type{Name: "bigint", Mods: []int64{}}},
							"b": {Name: "b", Type: schema.Type{Name: "varchar", Mods: []int64{6}}},
						},
						PrimaryKeys: []schema.Key{{Column: "a"}},
					}},
				ToSource: map[string]internal.NameAndCols{
					"t1": {Name: "t1", Cols: map[string]string{"a": "a", "b": "b"}},
				},
				ToSpanner: map[string]internal.NameAndCols{
					"t1": {Name: "t1", Cols: map[string]string{"a": "a", "b": "b"}},
				},
				Issues: map[string]map[string][]internal.SchemaIssue{
					"t1": {
						"a": {internal.Widened},
					},
				},
			},
		},
		{
			name:  "Test add or remove not null",
			table: "t1",
			payload: `
    {
      "UpdateCols":{
		"b": { "NotNull": "ADDED" },
		"c": { "NotNull": "REMOVED" }
	}
    }`,
			statusCode: http.StatusOK,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:     "t1",
						ColNames: []string{"a", "b", "c"},
						ColDefs: map[string]ddl.ColumnDef{
							"a": {Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
							"b": {Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
							"c": {Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						Pks: []ddl.IndexKey{{Col: "a"}},
					}},
				ToSource: map[string]internal.NameAndCols{
					"t1": {Name: "t1", Cols: map[string]string{"a": "a", "b": "b", "c": "c"}},
				},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:     "t1",
						ColNames: []string{"a", "b", "c"},
						ColDefs: map[string]ddl.ColumnDef{
							"a": {Name: "a", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
							"b": {Name: "b", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
							"c": {Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
						},
						Pks: []ddl.IndexKey{{Col: "a"}},
					}},
				ToSource: map[string]internal.NameAndCols{
					"t1": {Name: "t1", Cols: map[string]string{"a": "a", "b": "b", "c": "c"}},
				},
			},
		},
	}
	for _, tc := range tc {
		sessionState := session.GetSessionState()
		sessionState.Driver = constants.MYSQL
		sessionState.Conv = tc.conv
		payload := tc.payload
		req, err := http.NewRequest("POST", "/typemap/table?table="+tc.table, strings.NewReader(payload))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		handler := http.HandlerFunc(UpdateTableSchema)
		handler.ServeHTTP(rr, req)
		var res *internal.Conv
		json.Unmarshal(rr.Body.Bytes(), &res)
		if status := rr.Code; int64(status) != tc.statusCode {
			t.Errorf("handler returned wrong status code: got %v want %v",
				status, tc.statusCode)
		}
		if tc.statusCode == http.StatusOK {
			assert.Equal(t, tc.expectedConv, res)
		}
	}
}

*/

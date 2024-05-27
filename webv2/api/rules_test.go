package api_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/proto/migration"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/schema"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/api"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/session"
	"github.com/stretchr/testify/assert"
)

func TestApplyRule(t *testing.T) {
	tcAddIndex := []struct {
		name         string
		input        internal.Rule
		statusCode   int64
		conv         *internal.Conv
		expectedConv *internal.Conv
	}{
		{
			name: "Add Index with unique name",
			input: internal.Rule{
				Name:              "rule-index1",
				ObjectType:        "Table",
				AssociatedObjects: "t1",
				Enabled:           true,
				Type:              constants.AddIndex,
				Data: ddl.CreateIndex{
					Name:    "idx3",
					TableId: "t1",
					Unique:  false,
					Keys:    []ddl.IndexKey{{ColId: "c2", Desc: false, Order: 1}},
				},
			},
			statusCode: http.StatusOK,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name: "table1",
						Id:   "t1",
						Indexes: []ddl.CreateIndex{
							{Name: "idx1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}},
							{Name: "idx2", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c3", Desc: false}, {ColId: "c4", Desc: false}}}},
					}},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"table1": true, "idx1": true, "idx2": true},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name: "table1",
						Id:   "t1",
						Indexes: []ddl.CreateIndex{
							{Name: "idx1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}},
							{Name: "idx2", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c3", Desc: false}, {ColId: "c4", Desc: false}}},
							{Id: "i1", Name: "idx3", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false, Order: 1}}},
						},
					}},
			},
		},
		{
			name: "New name conflicts with an existing table",
			input: internal.Rule{
				Name:              "rule-index1",
				ObjectType:        "Table",
				AssociatedObjects: "t1",
				Enabled:           true,
				Type:              constants.AddIndex,
				Data: map[string]interface{}{
					"Name":    "table1",
					"TableId": "t1",
					"Unique":  false,
					"Keys":    []interface{}{map[string]interface{}{"ColId": "c2", "Desc": false}},
				},
			},
			statusCode: http.StatusInternalServerError,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name: "table1",
						Id:   "t1",
						Indexes: []ddl.CreateIndex{{Name: "idx1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}},
							{Name: "idx2", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c3", Desc: false}, {ColId: "c4", Desc: false}}}},
					}},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"table1": true, "idx1": true, "idx2": true},
			},
		},
		{
			name: "New name conflicts with an existing index",
			input: internal.Rule{
				Name:              "rule-index1",
				ObjectType:        "Table",
				AssociatedObjects: "t1",
				Enabled:           true,
				Type:              constants.AddIndex,
				Data: map[string]interface{}{
					"Name":    "idx2",
					"TableId": "t1",
					"Unique":  false,
					"Keys":    []interface{}{map[string]interface{}{"ColId": "c2", "Desc": false}},
				},
			},
			statusCode: http.StatusInternalServerError,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name: "table1",
						Id:   "t1",
						Indexes: []ddl.CreateIndex{{Name: "idx1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}},
							{Name: "idx2", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c3", Desc: false}, {ColId: "c4", Desc: false}}}},
					}},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"table1": true, "idx1": true, "idx2": true},
			},
		},
		{
			name: "Invalid input",
			input: internal.Rule{
				Name:              "rule-index1",
				ObjectType:        "Table",
				AssociatedObjects: "t1",
				Enabled:           true,
				Type:              constants.AddIndex,
				Data:              []string{"test1"},
			},
			statusCode: http.StatusInternalServerError,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name: "table1",
						Id:   "t1",
						Indexes: []ddl.CreateIndex{{Name: "idx1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}},
							{Name: "idx2", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c3", Desc: false}, {ColId: "c4", Desc: false}}}},
					}},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"table1": true, "idx1": true, "idx2": true},
			},
		},
	}
	for _, tc := range tcAddIndex {
		sessionState := session.GetSessionState()

		sessionState.Driver = constants.MYSQL
		sessionState.Conv = tc.conv

		inputBytes, err := json.Marshal(tc.input)
		if err != nil {
			t.Fatal(err)
		}
		buffer := bytes.NewBuffer(inputBytes)

		req, err := http.NewRequest("POST", "/applyrule", buffer)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		handler := http.HandlerFunc(api.ApplyRule)
		handler.ServeHTTP(rr, req)
		var res *internal.Conv
		json.Unmarshal(rr.Body.Bytes(), &res)
		if status := rr.Code; int64(status) != tc.statusCode {
			t.Errorf("%s : handler returned wrong status code: got %v want %v",
				tc.name, status, tc.statusCode)
		}
		if tc.statusCode == http.StatusOK {
			tc.expectedConv.Rules = internal.MakeConv().Rules
			tc.expectedConv.Rules = append(tc.expectedConv.Rules, tc.input)

			// Marshall and unmarshall the data field of rule with its proper type i.e ddl.CreateIndex.
			// Else unmarshalling data field of rule as interface convert int to float64.
			// In this particular case, order of index-key would be unmarshall to float64 instead of int.
			dataBytes, err := json.Marshal(res.Rules[0].Data)
			assert.Equal(t, err, nil)
			var data ddl.CreateIndex
			json.Unmarshal(dataBytes, &data)

			// Removing random ids before comparison.
			addedRule := res.Rules[0]
			data.Id = ""
			addedRule.Data = data
			addedRule.Id = ""
			res.Rules[0] = addedRule

			assert.Equal(t, tc.expectedConv, res)
		}
	}

	tcSetGlobalDataTypePostgres := []struct {
		name           string
		payload        string
		statusCode     int64
		expectedSchema ddl.CreateTable
		expectedIssues internal.TableIssues
	}{
		{
			name: "Test type change",
			payload: `{
				"Name":              "rule1",
				"Type":              "global_datatype_change",
				"ObjectType":        "Column",
				"AssociatedObjects": "All Columns",
				"Enabled":           true,
				"Data":
	{
	  	"bool":"STRING",
		"int8":"STRING",
		"float4":"STRING",
		"varchar":"BYTES",
		"numeric":"STRING",
		"timestamptz":"STRING",
		"bigserial":"STRING",
		"bpchar":"BYTES",
		"bytea":"STRING",
		"date":"STRING",
		"float8":"STRING",
		"int4":"STRING",
		"serial":"STRING",
		"text":"BYTES",
		"timestamp":"STRING"
	}
		}`,
			statusCode: http.StatusOK,
			expectedSchema: ddl.CreateTable{
				Name:   "table1",
				Id:     "t1",
				ColIds: []string{"c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11", "c12", "c13", "c14", "c15", "c16", "c17"},
				ColDefs: map[string]ddl.ColumnDef{
					"c1":  {Name: "a", Id: "c1", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"c2":  {Name: "b", Id: "c2", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"c3":  {Name: "c", Id: "c3", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"c4":  {Name: "d", Id: "c4", T: ddl.Type{Name: ddl.Bytes, Len: 6}},
					"c5":  {Name: "e", Id: "c5", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"c6":  {Name: "f", Id: "c6", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"c7":  {Name: "g", Id: "c7", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"c8":  {Name: "h", Id: "c8", T: ddl.Type{Name: ddl.Bytes, Len: int64(1)}},
					"c9":  {Name: "i", Id: "c9", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"c10": {Name: "j", Id: "c10", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"c11": {Name: "k", Id: "c11", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"c12": {Name: "l", Id: "c12", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"c13": {Name: "m", Id: "c13", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"c14": {Name: "n", Id: "c14", T: ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}},
					"c15": {Name: "o", Id: "c15", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"c16": {Name: "p", Id: "c16", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"c17": {Name: "q", Id: "c17", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
				},
				PrimaryKeys: []ddl.IndexKey{{ColId: "c1"}},
			},
			expectedIssues: internal.TableIssues{
				ColumnLevelIssues: map[string][]internal.SchemaIssue{
					"c1":  {internal.Widened},
					"c2":  {internal.Widened},
					"c3":  {internal.Widened},
					"c5":  {internal.Widened},
					"c6":  {internal.Widened},
					"c7":  {internal.Widened, internal.Serial},
					"c10": {internal.Widened},
					"c11": {internal.Widened},
					"c12": {internal.Widened},
					"c13": {internal.Widened, internal.Serial},
					"c15": {internal.Widened},
					"c16": {internal.Widened},
					"c17": {internal.NoGoodType},
				},
			},
		},
		{
			name: "Test type change 2",
			payload: `{
				"Name":              "rule1",
				"Type":              "global_datatype_change",
				"ObjectType":        "Column",
				"AssociatedObjects": "All Columns",
				"Enabled":           true,
				"Data":
		{
		  	"bool":"INT64",
			"int8":"STRING",
			"float4":"STRING"
		}
			}`,
			statusCode: http.StatusOK,
			expectedSchema: ddl.CreateTable{
				Name:   "table1",
				Id:     "t1",
				ColIds: []string{"c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11", "c12", "c13", "c14", "c15", "c16", "c17"},
				ColDefs: map[string]ddl.ColumnDef{
					"c1":  {Name: "a", Id: "c1", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"c2":  {Name: "b", Id: "c2", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"c3":  {Name: "c", Id: "c3", T: ddl.Type{Name: ddl.Int64}},
					"c4":  {Name: "d", Id: "c4", T: ddl.Type{Name: ddl.String, Len: int64(6)}},
					"c5":  {Name: "e", Id: "c5", T: ddl.Type{Name: ddl.Numeric}},
					"c6":  {Name: "f", Id: "c6", T: ddl.Type{Name: ddl.Timestamp}},
					"c7":  {Name: "g", Id: "c7", T: ddl.Type{Name: ddl.Int64}},
					"c8":  {Name: "h", Id: "c8", T: ddl.Type{Name: ddl.String, Len: int64(1)}},
					"c9":  {Name: "i", Id: "c9", T: ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}},
					"c10": {Name: "j", Id: "c10", T: ddl.Type{Name: ddl.Date}},
					"c11": {Name: "k", Id: "c11", T: ddl.Type{Name: ddl.Float64}},
					"c12": {Name: "l", Id: "c12", T: ddl.Type{Name: ddl.Int64}},
					"c13": {Name: "m", Id: "c13", T: ddl.Type{Name: ddl.Int64}},
					"c14": {Name: "n", Id: "c14", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"c15": {Name: "o", Id: "c15", T: ddl.Type{Name: ddl.Timestamp}},
					"c16": {Name: "p", Id: "c16", T: ddl.Type{Name: ddl.Int64}},
					"c17": {Name: "q", Id: "c17", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
				},
				PrimaryKeys: []ddl.IndexKey{{ColId: "c1"}},
			},
			expectedIssues: internal.TableIssues{
				ColumnLevelIssues: map[string][]internal.SchemaIssue{
					"c1":  {internal.Widened},
					"c2":  {internal.Widened},
					"c3":  {internal.Widened},
					"c7":  {internal.Serial},
					"c12": {internal.Widened},
					"c13": {internal.Serial},
					"c15": {internal.Timestamp},
					"c16": {internal.Widened},
					"c17": {internal.NoGoodType},
				},
			},
		},
		{
			name: "Test bad payload data request",
			payload: `{
				"Name":              "rule1",
				"Type":              "global_datatype_change",
				"ObjectType":        "Column",
				"AssociatedObjects": "All Columns",
				"Enabled":           true,
				"Data":
		{
		  	"bool":"INT64",
			"int8":"STRING",
			"float4":"STRING",
		}
			}`,
			statusCode: http.StatusBadRequest,
		},
	}
	for _, tc := range tcSetGlobalDataTypePostgres {

		sessionState := session.GetSessionState()

		sessionState.Driver = constants.POSTGRES
		sessionState.Conv = internal.MakeConv()
		buildConvPostgres(sessionState.Conv)
		payload := tc.payload
		req, err := http.NewRequest("POST", "/applyrule", strings.NewReader(payload))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		handler := http.HandlerFunc(api.ApplyRule)
		handler.ServeHTTP(rr, req)
		var res *internal.Conv
		json.Unmarshal(rr.Body.Bytes(), &res)
		if status := rr.Code; int64(status) != tc.statusCode {
			t.Errorf("handler returned wrong status code: got %v want %v",
				status, tc.statusCode)
		}

		if tc.statusCode == http.StatusOK {
			assert.Equal(t, tc.expectedSchema, res.SpSchema["t1"])
			assert.Equal(t, tc.expectedIssues, res.SchemaIssues["t1"])
		}
	}

	tcSetGlobalDataTypeMysql := []struct {
		name           string
		payload        string
		statusCode     int64
		expectedSchema ddl.CreateTable
		expectedIssues internal.TableIssues
	}{
		{
			name: "Test type change",
			payload: `{
			"Name":              "rule1",
			"Type":              "global_datatype_change",
			"ObjectType":        "Column",
			"AssociatedObjects": "All Columns",
			"Enabled":           true,
			"Data":
	{
	  	"bool":"STRING",
		"smallint":"STRING",
		"float":"STRING",
		"varchar":"BYTES",
		"numeric":"STRING",
		"timestamp":"STRING",
		"decimal":"STRING",
		"json":"BYTES",
		"binary":"STRING",
		"blob":"STRING",
		"double":"STRING",
		"date":"STRING",
		"time":"STRING",
		"enum":"STRING",
		"text":"BYTES"
	}
		}`,
			statusCode: http.StatusOK,
			expectedSchema: ddl.CreateTable{
				Name:   "table1",
				Id:     "t1",
				ColIds: []string{"c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11", "c12", "c13", "c14", "c15", "c16"},
				ColDefs: map[string]ddl.ColumnDef{
					"c1":  {Name: "a", Id: "c1", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"c2":  {Name: "b", Id: "c2", T: ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}},
					"c3":  {Name: "c", Id: "c3", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"c4":  {Name: "d", Id: "c4", T: ddl.Type{Name: ddl.Bytes, Len: 6}},
					"c5":  {Name: "e", Id: "c5", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"c6":  {Name: "f", Id: "c6", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"c7":  {Name: "g", Id: "c7", T: ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}},
					"c8":  {Name: "h", Id: "c8", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"c9":  {Name: "i", Id: "c9", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"c10": {Name: "j", Id: "c10", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"c11": {Name: "k", Id: "c11", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"c12": {Name: "l", Id: "c12", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"c13": {Name: "m", Id: "c13", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"c14": {Name: "n", Id: "c14", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"c15": {Name: "o", Id: "c15", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"c16": {Name: "p", Id: "c16", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
				},
				PrimaryKeys: []ddl.IndexKey{{ColId: "c1"}},
			},
			expectedIssues: internal.TableIssues{
				ColumnLevelIssues: map[string][]internal.SchemaIssue{
					"c1":  {internal.Widened},
					"c3":  {internal.Widened},
					"c5":  {internal.Widened},
					"c10": {internal.Widened},
					"c11": {internal.Widened},
					"c12": {internal.Widened},
					"c13": {internal.Widened},
					"c14": {internal.Widened},
					"c15": {internal.Widened},
					"c16": {internal.Time},
				},
			},
		},
		{
			name: "Test type change 2",
			payload: `{
				"Name":              "rule1",
				"Type":              "global_datatype_change",
				"ObjectType":        "Column",
				"AssociatedObjects": "All Columns",
				"Enabled":           true,
				"Data":
		{
		  	"bool":"INT64",
			"varchar":"BYTES"
		}
			}`,
			statusCode: http.StatusOK,
			expectedSchema: ddl.CreateTable{
				Name:   "table1",
				Id:     "t1",
				ColIds: []string{"c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11", "c12", "c13", "c14", "c15", "c16"},
				ColDefs: map[string]ddl.ColumnDef{
					"c1":  {Name: "a", Id: "c1", T: ddl.Type{Name: ddl.Int64}},
					"c2":  {Name: "b", Id: "c2", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"c3":  {Name: "c", Id: "c3", T: ddl.Type{Name: ddl.Int64}},
					"c4":  {Name: "d", Id: "c4", T: ddl.Type{Name: ddl.Bytes, Len: 6}},
					"c5":  {Name: "e", Id: "c5", T: ddl.Type{Name: ddl.Numeric}},
					"c6":  {Name: "f", Id: "c6", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"c7":  {Name: "g", Id: "c7", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
					"c8":  {Name: "h", Id: "c8", T: ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}},
					"c9":  {Name: "i", Id: "c9", T: ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}},
					"c10": {Name: "j", Id: "c10", T: ddl.Type{Name: ddl.Int64}},
					"c11": {Name: "k", Id: "c11", T: ddl.Type{Name: ddl.Float64}},
					"c12": {Name: "l", Id: "c12", T: ddl.Type{Name: ddl.Float64}},
					"c13": {Name: "m", Id: "c13", T: ddl.Type{Name: ddl.Numeric}},
					"c14": {Name: "n", Id: "c14", T: ddl.Type{Name: ddl.Date}},
					"c15": {Name: "o", Id: "c15", T: ddl.Type{Name: ddl.Timestamp}},
					"c16": {Name: "p", Id: "c16", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
				},
				PrimaryKeys: []ddl.IndexKey{{ColId: "c1"}},
			},
			expectedIssues: internal.TableIssues{
				ColumnLevelIssues: map[string][]internal.SchemaIssue{
					"c1":  {internal.Widened},
					"c3":  {internal.Widened},
					"c10": {internal.Widened},
					"c12": {internal.Widened},
					"c15": {internal.Time},
				},
			},
		},
		{
			name: "Test bad request",
			payload: `{
				"Name":              "rule1",
				"Type":              "global_datatype_change",
				"ObjectType":        "Column",
				"AssociatedObjects": "All Columns",
				"Enabled":           true,
				"Data":
		{
		  	"bool":"INT64",
			"smallint":"STRING",
		}
			}`,
			statusCode: http.StatusBadRequest,
		},
	}
	for _, tc := range tcSetGlobalDataTypeMysql {
		sessionState := session.GetSessionState()

		sessionState.Driver = constants.MYSQL
		sessionState.Conv = internal.MakeConv()
		buildConvMySQL(sessionState.Conv)
		payload := tc.payload
		req, err := http.NewRequest("POST", "/applyrule", strings.NewReader(payload))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		handler := http.HandlerFunc(api.ApplyRule)
		handler.ServeHTTP(rr, req)
		var res *internal.Conv
		json.Unmarshal(rr.Body.Bytes(), &res)
		if status := rr.Code; int64(status) != tc.statusCode {
			t.Errorf("handler returned wrong status code: got %v want %v",
				status, tc.statusCode)
		}

		if tc.statusCode == http.StatusOK {
			assert.Equal(t, tc.expectedSchema, res.SpSchema["t1"])
			assert.Equal(t, tc.expectedIssues, res.SchemaIssues["t1"])
		}
	}
}

func TestDropRule(t *testing.T) {
	tc := []struct {
		name         string
		ruleId       string
		statusCode   int64
		conv         *internal.Conv
		expectedConv *internal.Conv
	}{
		{
			name:       "drop a valid add index rule",
			ruleId:     "r101",
			statusCode: http.StatusOK,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name: "table1",
						Id:   "t1",
						Indexes: []ddl.CreateIndex{
							{Name: "idx1", Id: "i1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c1", Desc: false}}},
							{Name: "idx2", Id: "i2", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c3", Desc: false}, {ColId: "c4", Desc: false}}},
							{Name: "idx3", Id: "i3", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false, Order: 1}}},
						},
					}},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"table1": true, "idx1": true, "idx2": true, "idx3": true},
				Rules: []internal.Rule{{
					Id:                "r101",
					Name:              "add_index",
					Type:              constants.AddIndex,
					ObjectType:        "table",
					AssociatedObjects: "t1",
					Enabled:           true,
					Data:              ddl.CreateIndex{Name: "idx3", Id: "i3", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false, Order: 1}}},
				}},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name: "table1",
						Id:   "t1",
						Indexes: []ddl.CreateIndex{
							{Name: "idx1", Id: "i1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c1", Desc: false}}},
							{Name: "idx2", Id: "i2", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c3", Desc: false}, {ColId: "c4", Desc: false}}},
						},
					}},
			},
		},
		{
			name:       "drop a vaild add global data type rule",
			ruleId:     "r101",
			statusCode: http.StatusOK,
			conv: &internal.Conv{
				SchemaIssues: map[string]internal.TableIssues{
					"t1": {},
				},
				SrcSchema: map[string]schema.Table{
					"t1": {
						Name:   "table1",
						ColIds: []string{"c1", "c2", "c3"},
						ColDefs: map[string]schema.Column{
							"c1": {Name: "a", Type: schema.Type{Name: "bigint"}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: "c1"},
							"c2": {Name: "b", Type: schema.Type{Name: "bigint"}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: "c2"},
							"c3": {Name: "c", Type: schema.Type{Name: "varchar"}, NotNull: false, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: "c3"},
						},
						PrimaryKeys: []schema.Key{{ColId: "c1", Desc: false, Order: 1}},
						Id:          "t1",
					},
				},
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:   "table1",
						ColIds: []string{"c1", "c2", "c3"},
						ColDefs: map[string]ddl.ColumnDef{
							"c1": {Name: "a", Id: "c1", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c2": {Name: "b", Id: "c2", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
							"c3": {Name: "c", Id: "c3", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c1", Desc: false}},
						Id:          "t1",
					},
				},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_MIGRATION_TYPE_UNSPECIFIED.Enum(),
				},
				Rules: []internal.Rule{
					{
						Id:                "r101",
						Name:              "bigint to BTYES",
						Type:              constants.GlobalDataTypeChange,
						ObjectType:        "Column",
						AssociatedObjects: "All Columns",
						Enabled:           true,
						Data: map[string]string{
							"bigint": ddl.String,
						},
					},
				},
			},
			expectedConv: &internal.Conv{
				SchemaIssues: map[string]internal.TableIssues{
					"t1": {},
				},
				SrcSchema: map[string]schema.Table{
					"t1": {
						Name:   "table1",
						ColIds: []string{"c1", "c2", "c3"},
						ColDefs: map[string]schema.Column{
							"c1": {Name: "a", Type: schema.Type{Name: "bigint"}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: "c1"},
							"c2": {Name: "b", Type: schema.Type{Name: "bigint"}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: "c2"},
							"c3": {Name: "c", Type: schema.Type{Name: "varchar"}, NotNull: false, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: "c3"},
						},
						PrimaryKeys: []schema.Key{{ColId: "c1", Desc: false, Order: 1}},
						Id:          "t1",
					},
				},
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:   "table1",
						ColIds: []string{"c1", "c2", "c3"},
						ColDefs: map[string]ddl.ColumnDef{
							"c1": {Name: "a", Id: "c1", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c2": {Name: "b", Id: "c2", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c3": {Name: "c", Id: "c3", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c1", Desc: false}},
						Id:          "t1",
					},
				},
			},
		},
		{
			name:       "drop rule with an invalid rule-id",
			ruleId:     "ABC",
			statusCode: http.StatusBadRequest,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name: "table1",
						Id:   "t1",
						Indexes: []ddl.CreateIndex{
							{Name: "idx1", Id: "i1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c1", Desc: false}}},
							{Name: "idx2", Id: "i2", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c3", Desc: false}, {ColId: "c4", Desc: false}}},
							{Name: "idx3", Id: "i3", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false, Order: 1}}},
						},
					}},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"table1": true, "idx1": true, "idx2": true, "idx3": true},
				Rules: []internal.Rule{{
					Id:                "r101",
					Name:              "add_index",
					Type:              constants.AddIndex,
					ObjectType:        "table",
					AssociatedObjects: "t1",
					Enabled:           true,
					Data:              ddl.CreateIndex{Name: "idx3", Id: "i3", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false, Order: 1}}},
				}},
			},
		},
		{
			name:       "drop a disabled valid add index rule",
			ruleId:     "r101",
			statusCode: http.StatusOK,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name: "table1",
						Id:   "t1",
						Indexes: []ddl.CreateIndex{
							{Name: "idx1", Id: "i1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c1", Desc: false}}},
							{Name: "idx2", Id: "i2", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c3", Desc: false}, {ColId: "c4", Desc: false}}},
						},
					}},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"table1": true, "idx1": true, "idx2": true},
				Rules: []internal.Rule{{
					Id:                "r101",
					Name:              "add_index",
					Type:              constants.AddIndex,
					ObjectType:        "table",
					AssociatedObjects: "t1",
					Enabled:           false,
					Data:              ddl.CreateIndex{Name: "idx3", Id: "i3", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false, Order: 1}}},
				}},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name: "table1",
						Id:   "t1",
						Indexes: []ddl.CreateIndex{
							{Name: "idx1", Id: "i1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c1", Desc: false}}},
							{Name: "idx2", Id: "i2", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c3", Desc: false}, {ColId: "c4", Desc: false}}},
						},
					}},
			},
		},
	}
	for _, tc := range tc {
		sessionState := session.GetSessionState()
		sessionState.Driver = constants.MYSQL
		sessionState.Conv = tc.conv
		payload := `{}`
		req, err := http.NewRequest("POST", "/dropRule?id="+tc.ruleId, strings.NewReader(payload))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		handler := http.HandlerFunc(api.DropRule)
		handler.ServeHTTP(rr, req)
		var res *internal.Conv
		json.Unmarshal(rr.Body.Bytes(), &res)
		if status := rr.Code; int64(status) != tc.statusCode {
			t.Errorf("%s : handler returned wrong status code: got %v want %v",
				tc.name, status, tc.statusCode)
		}
		if tc.statusCode == http.StatusOK {
			assert.Equal(t, tc.expectedConv, res)
		}
	}

}

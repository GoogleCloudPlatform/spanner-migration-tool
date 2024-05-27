package api_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal/reports"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/proto/migration"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/schema"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/api"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/session"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/types"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func init() {
	logger.Log = zap.NewNop()
}

func TestGetTypeMapNoDriver(t *testing.T) {
	sessionState := session.GetSessionState()
	sessionState.Driver = ""
	sessionState.Conv = nil
	req, err := http.NewRequest("GET", "/typemap", nil)
	if err != nil {
		t.Fatal(err)
	}
	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(api.GetTypeMap)
	handler.ServeHTTP(rr, req)

	status := rr.Code

	if status != http.StatusNotFound {
		t.Errorf("handler returned wrong status code : got %v want %v",
			status, http.StatusNotFound)
	}

}

func TestGetTypeMapPostgres(t *testing.T) {
	sessionState := session.GetSessionState()
	sessionState.Driver = constants.POSTGRES
	sessionState.Conv = internal.MakeConv()
	buildConvPostgres(sessionState.Conv)
	req, err := http.NewRequest("GET", "/typemap", nil)
	if err != nil {
		t.Fatal(err)
	}
	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(api.GetTypeMap)
	handler.ServeHTTP(rr, req)
	var typemap map[string][]types.TypeIssue
	json.Unmarshal(rr.Body.Bytes(), &typemap)
	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusOK)
	}
	expectedTypemap := map[string][]types.TypeIssue{
		"bool": {
			{T: ddl.Bool, DisplayT: ddl.Bool},
			{T: ddl.Int64, Brief: reports.IssueDB[internal.Widened].Brief, DisplayT: ddl.Int64},
			{T: ddl.String, Brief: reports.IssueDB[internal.Widened].Brief, DisplayT: ddl.String}},
		"bigserial": {
			{T: ddl.Int64, Brief: reports.IssueDB[internal.Serial].Brief, DisplayT: ddl.Int64},
			{T: ddl.String, Brief: reports.IssueDB[internal.Widened].Brief + ", " + reports.IssueDB[internal.Serial].Brief, DisplayT: ddl.String}},
		"bpchar": {
			{T: ddl.Bytes, DisplayT: ddl.Bytes},
			{T: ddl.String, DisplayT: ddl.String}},
		"bytea": {
			{T: ddl.Bytes, DisplayT: ddl.Bytes},
			{T: ddl.String, DisplayT: ddl.String}},
		"date": {
			{T: ddl.Date, DisplayT: ddl.Date},
			{T: ddl.String, Brief: reports.IssueDB[internal.Widened].Brief, DisplayT: ddl.String}},
		"float8": {
			{T: ddl.Float64, DisplayT: ddl.Float64},
			{T: ddl.String, Brief: reports.IssueDB[internal.Widened].Brief, DisplayT: ddl.String}},
		"float4": {
			{T: ddl.Float64, Brief: reports.IssueDB[internal.Widened].Brief, DisplayT: ddl.Float64},
			{T: ddl.String, Brief: reports.IssueDB[internal.Widened].Brief, DisplayT: ddl.String}},
		"int8": {
			{T: ddl.Int64, DisplayT: ddl.Int64},
			{T: ddl.String, Brief: reports.IssueDB[internal.Widened].Brief, DisplayT: ddl.String}},
		"int4": {
			{T: ddl.Int64, Brief: reports.IssueDB[internal.Widened].Brief, DisplayT: ddl.Int64},
			{T: ddl.String, Brief: reports.IssueDB[internal.Widened].Brief, DisplayT: ddl.String}},
		"numeric": {
			{T: ddl.String, Brief: reports.IssueDB[internal.Widened].Brief, DisplayT: ddl.String},
			{T: ddl.Numeric, DisplayT: ddl.Numeric}},
		"serial": {
			{T: ddl.Int64, Brief: reports.IssueDB[internal.Serial].Brief, DisplayT: ddl.Int64},
			{T: ddl.String, Brief: reports.IssueDB[internal.Widened].Brief + ", " + reports.IssueDB[internal.Serial].Brief, DisplayT: ddl.String}},
		"text": {
			{T: ddl.Bytes, DisplayT: ddl.Bytes},
			{T: ddl.String, DisplayT: ddl.String}},
		"timestamptz": {
			{T: ddl.String, Brief: reports.IssueDB[internal.Widened].Brief, DisplayT: ddl.String},
			{T: ddl.Timestamp, DisplayT: ddl.Timestamp}},
		"timestamp": {
			{T: ddl.String, Brief: reports.IssueDB[internal.Widened].Brief, DisplayT: ddl.String},
			{T: ddl.Timestamp, Brief: reports.IssueDB[internal.Timestamp].Brief, DisplayT: ddl.Timestamp}},
		"varchar": {
			{T: ddl.Bytes, DisplayT: ddl.Bytes},
			{T: ddl.String, DisplayT: ddl.String}},
		"path": {
			{T: ddl.String, Brief: reports.IssueDB[internal.NoGoodType].Brief, DisplayT: ddl.String}},
	}
	assert.Equal(t, expectedTypemap, typemap)

}

func TestGetConversionPostgres(t *testing.T) {
	sessionState := session.GetSessionState()

	sessionState.Driver = constants.POSTGRES
	sessionState.Conv = internal.MakeConv()
	buildConvPostgres(sessionState.Conv)
	req, err := http.NewRequest("GET", "/conversion", nil)
	if err != nil {
		t.Fatal(err)
	}
	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(api.GetConversionRate)
	handler.ServeHTTP(rr, req)
	var result map[string]string
	json.Unmarshal(rr.Body.Bytes(), &result)
	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusOK)
	}
	assert.Equal(t, 2, len(result))
	assert.Contains(t, result, "t1")
	assert.Contains(t, result, "t2")
}

func TestGetTypeMapMySQL(t *testing.T) {
	sessionState := session.GetSessionState()

	sessionState.Driver = constants.MYSQL
	sessionState.Conv = internal.MakeConv()
	buildConvMySQL(sessionState.Conv)
	req, err := http.NewRequest("GET", "/typemap", nil)
	if err != nil {
		t.Fatal(err)
	}
	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(api.GetTypeMap)
	handler.ServeHTTP(rr, req)
	var typemap map[string][]types.TypeIssue
	json.Unmarshal(rr.Body.Bytes(), &typemap)
	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusOK)
	}
	expectedTypemap := map[string][]types.TypeIssue{
		"bool": {
			{T: ddl.Bool, DisplayT: ddl.Bool},
			{T: ddl.Int64, Brief: reports.IssueDB[internal.Widened].Brief, DisplayT: ddl.Int64},
			{T: ddl.String, Brief: reports.IssueDB[internal.Widened].Brief, DisplayT: ddl.String}},
		"varchar": {
			{T: ddl.Bytes, DisplayT: ddl.Bytes},
			{T: ddl.String, DisplayT: ddl.String}},
		"text": {
			{T: ddl.Bytes, DisplayT: ddl.Bytes},
			{T: ddl.String, DisplayT: ddl.String}},
		"enum": {
			{T: ddl.String, DisplayT: ddl.String}},
		"json": {
			{T: ddl.Bytes, DisplayT: ddl.Bytes},
			{T: ddl.String, DisplayT: ddl.String},
			{T: ddl.JSON, DisplayT: ddl.JSON}},
		"binary": {
			{T: ddl.Bytes, DisplayT: ddl.Bytes},
			{T: ddl.String, DisplayT: ddl.String}},
		"blob": {
			{T: ddl.Bytes, DisplayT: ddl.Bytes},
			{T: ddl.String, DisplayT: ddl.String}},
		"integer": {
			{T: ddl.Int64, Brief: reports.IssueDB[internal.Widened].Brief, DisplayT: ddl.Int64},
			{T: ddl.String, Brief: reports.IssueDB[internal.Widened].Brief, DisplayT: ddl.String}},
		"smallint": {
			{T: ddl.Int64, Brief: reports.IssueDB[internal.Widened].Brief, DisplayT: ddl.Int64},
			{T: ddl.String, Brief: reports.IssueDB[internal.Widened].Brief, DisplayT: ddl.String}},
		"double": {
			{T: ddl.Float64, DisplayT: ddl.Float64},
			{T: ddl.String, Brief: reports.IssueDB[internal.Widened].Brief, DisplayT: ddl.String}},
		"float": {
			{T: ddl.Float64, Brief: reports.IssueDB[internal.Widened].Brief, DisplayT: ddl.Float64},
			{T: ddl.String, Brief: reports.IssueDB[internal.Widened].Brief, DisplayT: ddl.String}},
		"numeric": {
			{T: ddl.String, Brief: reports.IssueDB[internal.Widened].Brief, DisplayT: ddl.String},
			{T: ddl.Numeric, DisplayT: ddl.Numeric}},
		"decimal": {
			{T: ddl.String, Brief: reports.IssueDB[internal.Widened].Brief, DisplayT: ddl.String},
			{T: ddl.Numeric, DisplayT: ddl.Numeric}},
		"date": {
			{T: ddl.Date, DisplayT: ddl.Date},
			{T: ddl.String, Brief: reports.IssueDB[internal.Widened].Brief, DisplayT: ddl.String}},
		"timestamp": {
			{T: ddl.String, Brief: reports.IssueDB[internal.Widened].Brief, DisplayT: ddl.String},
			{T: ddl.Timestamp, DisplayT: ddl.Timestamp}},
		"time": {
			{T: ddl.String, Brief: reports.IssueDB[internal.Time].Brief, DisplayT: ddl.String}},
	}
	assert.Equal(t, expectedTypemap, typemap)

}

func TestGetConversionMySQL(t *testing.T) {
	sessionState := session.GetSessionState()

	sessionState.Driver = constants.MYSQL
	sessionState.Conv = internal.MakeConv()
	buildConvMySQL(sessionState.Conv)
	req, err := http.NewRequest("GET", "/conversion", nil)
	if err != nil {
		t.Fatal(err)
	}
	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(api.GetConversionRate)
	handler.ServeHTTP(rr, req)
	var result map[string]string
	json.Unmarshal(rr.Body.Bytes(), &result)
	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusOK)
	}
	assert.Equal(t, 2, len(result))
	assert.Contains(t, result, "t1")
	assert.Contains(t, result, "t2")
}

func TestGetDDL(t *testing.T) {
	tc := []struct {
		name        string
		conv        *internal.Conv
		expectedDDL map[string]string
		statusCode  int64
	}{
		{
			name: "Get valid ddl with index and foreign key",
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{"t1": {
					Name:   "table1",
					ColIds: []string{"c1", "c2", "c3"},
					ColDefs: map[string]ddl.ColumnDef{"c1": ddl.ColumnDef{Name: "a", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
						"c2": ddl.ColumnDef{Name: "b", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
						"c3": ddl.ColumnDef{Name: "c", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true}},
					PrimaryKeys: []ddl.IndexKey{ddl.IndexKey{ColId: "c1", Desc: false}},
					ForeignKeys: []ddl.Foreignkey{ddl.Foreignkey{Name: "fk1", ColIds: []string{"c1"}, ReferTableId: "t2", ReferColumnIds: []string{"c4"}}},
					Indexes:     []ddl.CreateIndex{{Name: "index1", TableId: "t1", Id: "i1", Keys: []ddl.IndexKey{{ColId: "c1", Desc: false, Order: 1}}}},
				},
					"t2": {Name: "table2",
						ColIds:  []string{"c4"},
						ColDefs: map[string]ddl.ColumnDef{"c4": {Name: "d", T: ddl.Type{Name: ddl.Int64}, NotNull: true}},
					},
				},
			},
			expectedDDL: map[string]string{"t1": "CREATE TABLE table1 (\n\ta INT64 NOT NULL ,\n\tb INT64 NOT NULL ,\n\tc STRING(MAX) NOT NULL ,\n) PRIMARY KEY (a);\n\nCREATE INDEX index1 ON table1 (a);\n\nALTER TABLE table1 ADD CONSTRAINT fk1 FOREIGN KEY (a) REFERENCES table2 (d);",
				"t2": "CREATE TABLE table2 (\n\td INT64 NOT NULL ,\n) ;"},
			statusCode: http.StatusOK,
		},
	}

	for _, tc := range tc {
		sessionState := session.GetSessionState()

		sessionState.Driver = constants.MYSQL
		sessionState.Conv = tc.conv

		req, err := http.NewRequest("GET", "/ddl", nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		handler := http.HandlerFunc(api.GetDDL)
		handler.ServeHTTP(rr, req)
		var res map[string]string
		json.Unmarshal(rr.Body.Bytes(), &res)
		if status := rr.Code; int64(status) != tc.statusCode {
			t.Errorf("%s : handler returned wrong status code: got %v want %v",
				tc.name, status, tc.statusCode)
		}
		if tc.statusCode == http.StatusOK {
			assert.Equal(t, tc.expectedDDL, res)
		}
	}
}

func TestDropForeignKey(t *testing.T) {
	tc := []struct {
		name         string
		table        string
		input        interface{}
		statusCode   int64
		conv         *internal.Conv
		expectedConv *internal.Conv
	}{
		{
			name:  "Test drop valid FK success",
			table: "t1",
			input: []ddl.Foreignkey{{Name: "fk1", ColIds: []string{"c2"}, ReferTableId: "reft1", ReferColumnIds: []string{"ref_c1"}, Id: "f1"},
				{Name: "", ColIds: []string{}, ReferTableId: "", ReferColumnIds: []string{}, Id: "f2"}},
			statusCode: http.StatusOK,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						ForeignKeys: []ddl.Foreignkey{{Name: "fk1", ColIds: []string{"c2"}, ReferTableId: "reft1", ReferColumnIds: []string{"ref_c1"}, Id: "f1"},
							{Name: "fk2", ColIds: []string{"c3", "c4"}, ReferTableId: "reft2", ReferColumnIds: []string{"ref_c2", "ref_c3"}, Id: "f2"}},
					}},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						ForeignKeys: []ddl.Foreignkey{{Name: "fk1", ColIds: []string{"c2"}, ReferTableId: "reft1", ReferColumnIds: []string{"ref_c1"}, Id: "f1"}},
					}},
			},
		},
	}
	for _, tc := range tc {
		sessionState := session.GetSessionState()

		sessionState.Driver = constants.MYSQL
		sessionState.Conv = tc.conv

		inputBytes, err := json.Marshal(tc.input)
		if err != nil {
			t.Fatal(err)
		}
		buffer := bytes.NewBuffer(inputBytes)

		req, err := http.NewRequest("POST", "/update/fks?table="+tc.table, buffer)

		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		handler := http.HandlerFunc(api.UpdateForeignKeys)
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

func TestUpdateIndexes(t *testing.T) {
	tc := []struct {
		name         string
		tableId      string
		input        []ddl.CreateIndex
		statusCode   int64
		conv         *internal.Conv
		expectedConv *internal.Conv
	}{
		{
			name:       "Add a valid index key",
			tableId:    "t1",
			input:      []ddl.CreateIndex{{Name: "idx", Id: "i1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false, Order: 1}, {ColId: "c3", Desc: true, Order: 2}}}},
			statusCode: http.StatusOK,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx", Id: "i1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false, Order: 1}}}},
					}},
				SrcSchema: map[string]schema.Table{
					"t1": {
						Indexes: []schema.Index{{Name: "idx", Id: "i1", Keys: []schema.Key{{ColId: "c2", Desc: false, Order: 1}}}},
					},
				},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"t1": true, "idx": true},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx", Id: "i1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false, Order: 1}, {ColId: "c3", Desc: true, Order: 2}}}},
					}},
				SrcSchema: map[string]schema.Table{
					"t1": {
						Indexes: []schema.Index{{Name: "idx", Id: "i1", Keys: []schema.Key{{ColId: "c2", Desc: false, Order: 1}}}},
					},
				},
			},
		},
		{
			name:       "Change the order of two index keys",
			tableId:    "t1",
			input:      []ddl.CreateIndex{{Name: "idx", Id: "i1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false, Order: 2}, {ColId: "c3", Desc: true, Order: 1}}}},
			statusCode: http.StatusOK,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx", Id: "i1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false, Order: 1}, {ColId: "c3", Desc: true, Order: 2}}}},
					}},
				SrcSchema: map[string]schema.Table{
					"t1": {
						Indexes: []schema.Index{{Name: "idx", Id: "i1", Keys: []schema.Key{{ColId: "c2", Desc: false, Order: 1}, {ColId: "c3", Desc: true, Order: 2}}}},
					},
				},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"t1": true, "idx": true},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx", Id: "i1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false, Order: 2}, {ColId: "c3", Desc: true, Order: 1}}}},
					}},
				SrcSchema: map[string]schema.Table{
					"t1": {
						Indexes: []schema.Index{{Name: "idx", Id: "i1", Keys: []schema.Key{{ColId: "c2", Desc: false, Order: 2}, {ColId: "c3", Desc: true, Order: 1}}}},
					},
				},
			},
		},
		{
			name:       "Delete an index key column",
			tableId:    "t1",
			input:      []ddl.CreateIndex{{Name: "idx", Id: "i1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false, Order: 1}}}},
			statusCode: http.StatusOK,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx", Id: "i1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false, Order: 1}, {ColId: "c3", Desc: true, Order: 2}}}},
					}},
				SrcSchema: map[string]schema.Table{
					"t1": {
						Indexes: []schema.Index{{Name: "idx", Id: "i1", Keys: []schema.Key{{ColId: "c2", Desc: false, Order: 1}, {ColId: "c3", Desc: true, Order: 2}}}},
					},
				},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"t1": true, "idx": true},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx", Id: "i1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false, Order: 1}}}},
					}},
				SrcSchema: map[string]schema.Table{
					"t1": {
						Indexes: []schema.Index{{Name: "idx", Id: "i1", Keys: []schema.Key{{ColId: "c2", Desc: false, Order: 1}, {ColId: "c3", Desc: true, Order: 2}}}},
					},
				},
			},
		},
		{
			name:       "Test rename indexes name",
			tableId:    "t1",
			input:      []ddl.CreateIndex{{Name: "idx_new", Id: "i1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false, Order: 1}}}},
			statusCode: http.StatusOK,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx", Id: "i1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false, Order: 1}}}},
					}},
				SrcSchema: map[string]schema.Table{
					"t1": {
						Indexes: []schema.Index{{Name: "idx", Id: "i1", Keys: []schema.Key{{ColId: "c2", Desc: false, Order: 1}}}},
					},
				},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"t1": true, "idx": true},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx_new", Id: "i1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false, Order: 1}}}},
					}},
				SrcSchema: map[string]schema.Table{
					"t1": {
						Indexes: []schema.Index{{Name: "idx", Id: "i1", Keys: []schema.Key{{ColId: "c2", Desc: false, Order: 1}}}},
					},
				},
			},
		},
		{
			name:       "Two Index key columns can not have same order",
			tableId:    "t1",
			input:      []ddl.CreateIndex{{Name: "idx_new", Id: "i1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false, Order: 1}, {ColId: "c3", Desc: true, Order: 1}}}},
			statusCode: http.StatusBadRequest,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx", Id: "i1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false, Order: 1}, {ColId: "c3", Desc: true, Order: 2}}}},
					}},
				SrcSchema: map[string]schema.Table{
					"t1": {
						Indexes: []schema.Index{{Name: "idx", Id: "i1", Keys: []schema.Key{{ColId: "c2", Desc: false, Order: 1}, {ColId: "c3", Desc: true, Order: 2}}}},
					},
				},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"t1": true, "idx": true},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx_new", Id: "i1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false, Order: 1}, {ColId: "c3", Desc: true, Order: 2}}}},
					}},
				SrcSchema: map[string]schema.Table{
					"t1": {
						Indexes: []schema.Index{{Name: "idx", Id: "i1", Keys: []schema.Key{{ColId: "c2", Desc: false, Order: 1}, {ColId: "c3", Desc: true, Order: 2}}}},
					},
				},
			},
		},
	}

	for _, tc := range tc {
		sessionState := session.GetSessionState()

		sessionState.Driver = constants.MYSQL
		sessionState.Conv = tc.conv

		inputBytes, err := json.Marshal(tc.input)
		if err != nil {
			t.Fatal(err)
		}
		buffer := bytes.NewBuffer(inputBytes)

		req, err := http.NewRequest("POST", "/update/indexes?table="+tc.tableId, buffer)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		handler := http.HandlerFunc(api.UpdateIndexes)
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

func TestRenameIndexes(t *testing.T) {
	tc := []struct {
		name         string
		table        string
		input        interface{}
		statusCode   int64
		conv         *internal.Conv
		expectedConv *internal.Conv
	}{
		{
			name:  "Test rename indexes",
			table: "t1",
			input: map[string]string{
				"i1": "idx_new",
			},
			statusCode: http.StatusOK,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx", Id: "i1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}}},
					}},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"t1": true, "idx": true},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx_new", Id: "i1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}}},
					}},
			},
		},
		{
			name:  "Test rename multiple indexes",
			table: "t1",
			input: map[string]string{
				"i1": "idx_new_1",
				"i2": "idx_new_2",
			},
			statusCode: http.StatusOK,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx_1", Id: "i1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}},
							{Name: "idx_2", Id: "i2", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}}},
					}},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"t1": true, "idx_1": true, "idx_2": true},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx_new_1", Id: "i1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}},
							{Name: "idx_new_2", Id: "i2", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}}},
					}},
			},
		},
		{
			name:  "New name conflicts with an existing table",
			table: "t1",
			input: map[string]string{
				"i1": "t1",
			},
			statusCode: http.StatusBadRequest,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx_1", Id: "i1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}},
							{Name: "idx_2", Id: "i2", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}}},
					}},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"t1": true, "idx_1": true, "idx_2": true},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx_1", Id: "i1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}},
							{Name: "idx_2", Id: "i2", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}}},
					}},
			},
		},
		{
			name:  "New name conflicts with an existing index",
			table: "t1",
			input: map[string]string{
				"i1": "idx_2",
			},
			statusCode: http.StatusBadRequest,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx_1", Id: "i1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}},
							{Name: "idx_2", Id: "i2", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}}},
					}},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"t1": true, "idx_1": true, "idx_2": true},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx_new_1", Id: "i1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}},
							{Name: "idx_new_2", Id: "i2", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}}},
					}},
			},
		},
		{
			name:  "New name conflicts with an existing foreign key",
			table: "t1",
			input: map[string]string{
				"i1": "fk1",
			},
			statusCode: http.StatusBadRequest,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx_1", Id: "i1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}},
							{Name: "idx_2", Id: "i2", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}}},
						ForeignKeys: []ddl.Foreignkey{{Name: "fk1", Id: "fkId1", ColIds: []string{"c2"}, ReferTableId: "reft1", ReferColumnIds: []string{"ref_b"}},
							{Name: "fk2", Id: "fkId2", ColIds: []string{"c3", "d"}, ReferTableId: "reft2", ReferColumnIds: []string{"ref_c", "ref_d"}}},
					}},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"t1": true, "idx_1": true, "idx_2": true, "fk1": true, "fk2": true},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx_new_1", Id: "i1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}},
							{Name: "idx_new_2", Id: "i2", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}}},
					}},
			},
		},
		{
			name:  "Given Index not available",
			table: "t1",
			input: map[string]string{
				"i1": "idx",
			},
			statusCode: http.StatusBadRequest,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx", Id: "i1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}}},
					}},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"t1": true, "idx": true},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx", Id: "i1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}}},
					}},
			},
		},
		{
			name:  "Conflicts within new name array",
			table: "t1",
			input: map[string]string{
				"i1": "idx_100",
				"i2": "idx_100",
			},
			statusCode: http.StatusBadRequest,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx1", Id: "i1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}},
							{Name: "idx2", Id: "i2", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}}},
					}},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"t1": true, "idx1": true, "idx2": true},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx1", Id: "i1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}},
							{Name: "idx2", Id: "i2", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}}},
					}},
			},
		},
		{
			name:       "Input Empty Map ",
			table:      "t1",
			input:      map[string]string{},
			statusCode: http.StatusOK,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx", Id: "i1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}}},
					}},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"t1": true, "idx": true},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx", Id: "i1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}}},
					}},
			},
		},
		{
			name:       "Invalid input",
			table:      "t1",
			input:      []string{"test1", "test2"},
			statusCode: http.StatusBadRequest,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx", Id: "i1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}}},
					}},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"t1": true, "idx": true},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx", Id: "i2", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}}},
					}},
			},
		},
	}

	for _, tc := range tc {
		sessionState := session.GetSessionState()

		sessionState.Driver = constants.MYSQL
		sessionState.Conv = tc.conv

		inputBytes, err := json.Marshal(tc.input)
		if err != nil {
			t.Fatal(err)
		}
		buffer := bytes.NewBuffer(inputBytes)

		req, err := http.NewRequest("POST", "/rename/indexes?table="+tc.table, buffer)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		handler := http.HandlerFunc(api.RenameIndexes)
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

func TestRenameForeignKeys(t *testing.T) {
	tc := []struct {
		name         string
		table        string
		input        interface{}
		statusCode   int64
		conv         *internal.Conv
		expectedConv *internal.Conv
	}{
		{
			name:  "Test rename foreignkey",
			table: "t1",
			input: []ddl.Foreignkey{{Name: "foreignkey1", ColIds: []string{"c2"}, ReferTableId: "reft1", ReferColumnIds: []string{"ref_b"}, Id: "f1"},
				{Name: "fk2", ColIds: []string{"c", "d"}, ReferTableId: "reft2", ReferColumnIds: []string{"ref_c", "ref_d"}, Id: "f2"}},
			statusCode: http.StatusOK,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						ForeignKeys: []ddl.Foreignkey{{Name: "fk1", Id: "f1", ColIds: []string{"c2"}, ReferTableId: "reft1", ReferColumnIds: []string{"ref_b"}},
							{Name: "fk2", Id: "f2", ColIds: []string{"c", "d"}, ReferTableId: "reft2", ReferColumnIds: []string{"ref_c", "ref_d"}}},
					}},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"t1": true, "fk1": true, "fk2": true},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						ForeignKeys: []ddl.Foreignkey{{Name: "foreignkey1", ColIds: []string{"c2"}, ReferTableId: "reft1", ReferColumnIds: []string{"ref_b"}, Id: "f1"},
							{Name: "fk2", ColIds: []string{"c", "d"}, ReferTableId: "reft2", ReferColumnIds: []string{"ref_c", "ref_d"}, Id: "f2"}},
					}},
			},
		},
		{
			name:  "Test rename multiple foreignkeys",
			table: "t1",
			input: []ddl.Foreignkey{{Name: "foreignkey1", ColIds: []string{"b"}, ReferTableId: "reft1", ReferColumnIds: []string{"ref_b"}, Id: "f1"},
				{Name: "foreignkey2", ColIds: []string{"c", "d"}, ReferTableId: "reft2", ReferColumnIds: []string{"ref_c", "ref_d"}, Id: "f2"}},
			statusCode: http.StatusOK,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						ForeignKeys: []ddl.Foreignkey{{Name: "fk1", ColIds: []string{"b"}, ReferTableId: "reft1", ReferColumnIds: []string{"ref_b"}, Id: "f1"},
							{Name: "fk2", ColIds: []string{"c", "d"}, ReferTableId: "reft2", ReferColumnIds: []string{"ref_c", "ref_d"}, Id: "f2"}},
					}},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"t1": true, "fk1": true, "fk2": true, "reft1": true, "reft2": true},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						ForeignKeys: []ddl.Foreignkey{{Name: "foreignkey1", ColIds: []string{"b"}, ReferTableId: "reft1", ReferColumnIds: []string{"ref_b"}, Id: "f1"},
							{Name: "foreignkey2", ColIds: []string{"c", "d"}, ReferTableId: "reft2", ReferColumnIds: []string{"ref_c", "ref_d"}, Id: "f2"}},
					}},
			},
		},
		{
			name:  "New name conflicts with an existing table",
			table: "t1",
			input: []ddl.Foreignkey{{Name: "t1", ColIds: []string{"b"}, ReferTableId: "reft1", ReferColumnIds: []string{"ref_b"}, Id: "f1"},
				{Name: "fk2", ColIds: []string{"c", "d"}, ReferTableId: "reft2", ReferColumnIds: []string{"ref_c", "ref_d"}, Id: "f2"}},
			statusCode: http.StatusBadRequest,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						ForeignKeys: []ddl.Foreignkey{{Name: "fk1", ColIds: []string{"b"}, ReferTableId: "reft1", ReferColumnIds: []string{"ref_b"}, Id: "f1"},
							{Name: "fk2", ColIds: []string{"c", "d"}, ReferTableId: "reft2", ReferColumnIds: []string{"ref_c", "ref_d"}, Id: "f2"}},
					}},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"t1": true, "fk1": true, "fk2": true, "reft1": true, "reft2": true},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						ForeignKeys: []ddl.Foreignkey{{Name: "foreignkey1", ColIds: []string{"b"}, ReferTableId: "reft1", ReferColumnIds: []string{"ref_b"}, Id: "f1"},
							{Name: "fk2", ColIds: []string{"c", "d"}, ReferTableId: "reft2", ReferColumnIds: []string{"ref_c", "ref_d"}, Id: "f2"}},
					}},
			},
		},
		{
			name:  "New name conflicts with an existing foreignkey",
			table: "t1",
			input: []ddl.Foreignkey{{Name: "fk2", ColIds: []string{"b"}, ReferTableId: "reft1", ReferColumnIds: []string{"ref_b"}, Id: "f1"},
				{Name: "fk2", ColIds: []string{"c", "d"}, ReferTableId: "reft2", ReferColumnIds: []string{"ref_c", "ref_d"}, Id: "f2"}},
			statusCode: http.StatusBadRequest,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						ForeignKeys: []ddl.Foreignkey{{Name: "fk1", ColIds: []string{"b"}, ReferTableId: "reft1", ReferColumnIds: []string{"ref_b"}, Id: "f1"},
							{Name: "fk2", ColIds: []string{"c", "d"}, ReferTableId: "reft2", ReferColumnIds: []string{"ref_c", "ref_d"}, Id: "f2"}},
					}},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"t1": true, "fk1": true, "fk2": true, "reft1": true, "reft2": true},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						ForeignKeys: []ddl.Foreignkey{{Name: "foreignkey1", ColIds: []string{"b"}, ReferTableId: "reft1", ReferColumnIds: []string{"ref_b"}, Id: "f1"},
							{Name: "fk2", ColIds: []string{"c", "d"}, ReferTableId: "reft2", ReferColumnIds: []string{"ref_c", "ref_d"}, Id: "f2"}},
					}},
			},
		},
		{
			name:  "New name conflicts with an existing indexes",
			table: "t1",
			input: []ddl.Foreignkey{{Name: "idx_1", ColIds: []string{"b"}, ReferTableId: "reft1", ReferColumnIds: []string{"ref_b"}, Id: "f1"},
				{Name: "fk2", ColIds: []string{"c", "d"}, ReferTableId: "reft2", ReferColumnIds: []string{"ref_c", "ref_d"}, Id: "f2"}},
			statusCode: http.StatusBadRequest,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx_1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "b", Desc: false}}},
							{Name: "idx_2", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "b", Desc: false}}}},
						ForeignKeys: []ddl.Foreignkey{{Name: "fk1", ColIds: []string{"b"}, ReferTableId: "reft1", ReferColumnIds: []string{"ref_b"}, Id: "f1"},
							{Name: "fk2", ColIds: []string{"c", "d"}, ReferTableId: "reft2", ReferColumnIds: []string{"ref_c", "ref_d"}, Id: "f2"}},
					}},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"t1": true, "idx_1": true, "idx_2": true, "fk1": true, "fk2": true, "reft1": true, "reft2": true},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx_new_1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}},
							{Name: "idx_new_2", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}}},
						ForeignKeys: []ddl.Foreignkey{{Name: "fk1", Id: "fkId1", ColIds: []string{"c2"}, ReferTableId: "reft1", ReferColumnIds: []string{"ref_b"}},
							{Name: "fk2", Id: "fkId2", ColIds: []string{"c3", "d"}, ReferTableId: "reft2", ReferColumnIds: []string{"ref_c", "ref_d"}}},
					}},
			},
		},
		{
			name:  "Conflicts within new name array",
			table: "t1",
			input: map[string]string{
				"fkId1": "fk_100",
				"fkId2": "fk_100",
			},
			statusCode: http.StatusBadRequest,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}},
							{Name: "idx2", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}}},
						ForeignKeys: []ddl.Foreignkey{{Name: "fk1", Id: "fkId1", ColIds: []string{"c2"}, ReferTableId: "reft1", ReferColumnIds: []string{"ref_b"}},
							{Name: "fk2", Id: "fkId2", ColIds: []string{"c3", "d"}, ReferTableId: "reft2", ReferColumnIds: []string{"ref_c", "ref_d"}}},
					}},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"t1": true, "idx1": true, "idx2": true},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}},
							{Name: "idx2", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}}},
						ForeignKeys: []ddl.Foreignkey{{Name: "fk1", Id: "fkId1", ColIds: []string{"c2"}, ReferTableId: "reft1", ReferColumnIds: []string{"ref_b"}},
							{Name: "fk2", Id: "fkId2", ColIds: []string{"c3", "d"}, ReferTableId: "reft2", ReferColumnIds: []string{"ref_c", "ref_d"}}},
					}},
			},
		},
		{
			name:       "Invalid input",
			table:      "t1",
			input:      []string{"test1", "test2"},
			statusCode: http.StatusBadRequest,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						ForeignKeys: []ddl.Foreignkey{{Name: "fk1", Id: "fkId1", ColIds: []string{"c2"}, ReferTableId: "reft1", ReferColumnIds: []string{"ref_b"}},
							{Name: "fk2", Id: "fkId2", ColIds: []string{"c3", "d"}, ReferTableId: "reft2", ReferColumnIds: []string{"ref_c", "ref_d"}}},
					}},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"t1": true, "fk1": true, "fk2": true, "reft1": true, "reft2": true},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						ForeignKeys: []ddl.Foreignkey{{Name: "fk1", Id: "fkId1", ColIds: []string{"c2"}, ReferTableId: "reft1", ReferColumnIds: []string{"ref_b"}},
							{Name: "fk2", Id: "fkId2", ColIds: []string{"c3", "d"}, ReferTableId: "reft2", ReferColumnIds: []string{"ref_c", "ref_d"}}},
					}},
			},
		},
		{
			name:  "Check non usage in another table",
			table: "t1",
			input: []ddl.Foreignkey{{Name: "t2_fk2", ColIds: []string{"b"}, ReferTableId: "reft1", ReferColumnIds: []string{"ref_b"}, Id: "f1"},
				{Name: "fk2", ColIds: []string{"c", "d"}, ReferTableId: "reft2", ReferColumnIds: []string{"ref_c", "ref_d"}, Id: "f2"}},
			statusCode: http.StatusBadRequest,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						ForeignKeys: []ddl.Foreignkey{{Name: "fk1", ColIds: []string{"b"}, ReferTableId: "reft1", ReferColumnIds: []string{"ref_b"}, Id: "f1"},
							{Name: "fk2", ColIds: []string{"c", "d"}, ReferTableId: "reft2", ReferColumnIds: []string{"ref_c", "ref_d"}, Id: "f2"}},
					},
					"t2": {
						ForeignKeys: []ddl.Foreignkey{{Name: "t2_fk1", ColIds: []string{"b"}, ReferTableId: "reft1", ReferColumnIds: []string{"ref_b"}, Id: "f3"},
							{Name: "t2_fk2", ColIds: []string{"c", "d"}, ReferTableId: "reft2", ReferColumnIds: []string{"ref_c", "ref_d"}, Id: "f4"}},
					}},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"t1": true, "t2": true, "fk1": true, "fk2": true, "reft1": true, "reft2": true, "t2_fk1": true, "t2_fk2": true},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						ForeignKeys: []ddl.Foreignkey{{Name: "fk1", ColIds: []string{"b"}, ReferTableId: "reft1", ReferColumnIds: []string{"ref_b"}, Id: "f1"},
							{Name: "fk2", ColIds: []string{"c", "d"}, ReferTableId: "reft2", ReferColumnIds: []string{"ref_c", "ref_d"}, Id: "f2"}},
					},
					"t2": {
						ForeignKeys: []ddl.Foreignkey{{Name: "t2_fk1", ColIds: []string{"b"}, ReferTableId: "reft1", ReferColumnIds: []string{"ref_b"}, Id: "f3"},
							{Name: "t2_fk2", ColIds: []string{"c", "d"}, ReferTableId: "reft2", ReferColumnIds: []string{"ref_c", "ref_d"}, Id: "f4"}},
					}},
			},
		},
	}
	for _, tc := range tc {
		sessionState := session.GetSessionState()

		sessionState.Driver = constants.MYSQL
		sessionState.Conv = tc.conv

		inputBytes, err := json.Marshal(tc.input)
		if err != nil {
			t.Fatal(err)
		}
		buffer := bytes.NewBuffer(inputBytes)

		req, err := http.NewRequest("POST", "/update/fks?table="+tc.table, buffer)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		handler := http.HandlerFunc(api.UpdateForeignKeys)
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

func TestDropSecondaryIndex(t *testing.T) {
	tc := []struct {
		name         string
		table        string
		payload      string
		statusCode   int64
		conv         *internal.Conv
		expectedConv *internal.Conv
	}{
		{
			name:       "Test drop valid secondary index success",
			table:      "t1",
			payload:    `{"Id":"i2"}`,
			statusCode: http.StatusOK,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx1", Id: "i1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}},
							{Name: "idx2", Id: "i2", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c3", Desc: false}, {ColId: "d", Desc: false}}}},
					}},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx1", Id: "i1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}}},
					}},
			},
		},
		{
			name:       "Test drop valid secondary index success added through rule addition",
			table:      "t1",
			payload:    `{"Id":"i2"}`,
			statusCode: http.StatusOK,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx1", Id: "i1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false, Order: 1}}},
							{Name: "idx2", Id: "i2", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c3", Desc: false, Order: 1}}}},
					}},
				Rules: []internal.Rule{{
					Id:                "r101",
					Name:              "add_index",
					Type:              constants.AddIndex,
					ObjectType:        "table",
					AssociatedObjects: "t1",
					Enabled:           true,
					Data:              ddl.CreateIndex{Name: "idx2", Id: "i2", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c3", Desc: false, Order: 1}}},
				}},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx1", Id: "i1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false, Order: 1}}}},
					}},
				Rules: []internal.Rule{{
					Id:                "r101",
					Name:              "add_index",
					Type:              constants.AddIndex,
					ObjectType:        "table",
					AssociatedObjects: "t1",
					Enabled:           false,
					Data:              map[string]interface{}{"Name": "idx2", "Id": "i2", "TableId": "t1", "Unique": false, "StoredColumnIds": nil, "Keys": []interface{}{map[string]interface{}{"ColId": "c3", "Desc": false, "Order": float64(1)}}},
				}},
			},
		},
		{
			name:       "Test drop secondary index invalid Id",
			table:      "t1",
			payload:    `{"Id":""}`,
			statusCode: http.StatusBadRequest,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx1", Id: "i1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}}},
					}},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
			},
		},
		{
			name:       "Test drop secondary index invalid Id 2",
			table:      "t1",
			payload:    `{"Id":"AB"}`,
			statusCode: http.StatusBadRequest,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Indexes: []ddl.CreateIndex{{Name: "idx1", Id: "i1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false}}}},
					}},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
			},
		},
	}
	for _, tc := range tc {
		sessionState := session.GetSessionState()

		sessionState.Driver = constants.MYSQL
		sessionState.Conv = tc.conv
		payload := tc.payload
		req, err := http.NewRequest("POST", "/drop/secondaryindex?table="+tc.table, strings.NewReader(payload))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		handler := http.HandlerFunc(api.DropSecondaryIndex)
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

func TestRestoreSecondaryIndex(t *testing.T) {
	tc := []struct {
		name         string
		tableId      string
		indexId      string
		statusCode   int64
		conv         *internal.Conv
		expectedConv *internal.Conv
	}{
		{
			name:       "Test restore valid secondary index success",
			tableId:    "t1",
			indexId:    "i1",
			statusCode: http.StatusOK,
			conv: &internal.Conv{
				SrcSchema: map[string]schema.Table{
					"t1": {
						Name:   "table1",
						ColIds: []string{"c1", "c2", "c3"},
						Indexes: []schema.Index{
							{Name: "idx1", Unique: false, Keys: []schema.Key{{ColId: "c2", Desc: false, Order: 1}}, Id: "i1"},
							{Name: "idx2", Unique: false, Keys: []schema.Key{{ColId: "c3", Desc: false, Order: 1}}, Id: "i2"},
						},
						Id: "t1",
					},
				},
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:   "table1",
						ColIds: []string{"c1", "c2", "c3"},
						Indexes: []ddl.CreateIndex{
							{Name: "idx2", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c3", Desc: false, Order: 1}}, Id: "i2"},
						},
						Id: "t1",
					},
				},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				UsedNames: map[string]bool{"t1": true, "idx2": true},
			},
			expectedConv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:   "table1",
						ColIds: []string{"c1", "c2", "c3"},
						Indexes: []ddl.CreateIndex{
							{Name: "idx2", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c3", Desc: false, Order: 1}}, Id: "i2"},
							{Name: "idx1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false, Order: 1}}, Id: "i1"},
						},
						Id: "t1",
					},
				},
			},
		},

		{
			name:       "Test restore secondary index invalid index id",
			tableId:    "t1",
			indexId:    "A",
			statusCode: http.StatusBadRequest,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:    "table1",
						Id:      "t1",
						ColIds:  []string{"c1", "c2", "c3"},
						Indexes: []ddl.CreateIndex{{Name: "idx1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false, Order: 1}}, Id: "i1"}},
					}},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
			},
			expectedConv: &internal.Conv{},
		},
		{
			name:       "Test drop secondary index invalid table id",
			tableId:    "X",
			indexId:    "i1",
			statusCode: http.StatusBadRequest,
			conv: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:    "table1",
						Id:      "t1",
						ColIds:  []string{"c1", "c2", "c3"},
						Indexes: []ddl.CreateIndex{{Name: "idx1", TableId: "t1", Unique: false, Keys: []ddl.IndexKey{{ColId: "c2", Desc: false, Order: 1}}, Id: "i1"}},
					}},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
			},
			expectedConv: &internal.Conv{},
		},
	}
	for _, tc := range tc {
		sessionState := session.GetSessionState()

		sessionState.Driver = constants.MYSQL
		sessionState.Conv = tc.conv
		payload := `{}`
		req, err := http.NewRequest("POST", "/restore/secondaryIndex?tableId="+tc.tableId+"&indexId="+tc.indexId, strings.NewReader(payload))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		handler := http.HandlerFunc(api.RestoreSecondaryIndex)
		handler.ServeHTTP(rr, req)
		var res *internal.Conv
		json.Unmarshal(rr.Body.Bytes(), &res)
		if status := rr.Code; int64(status) != tc.statusCode {
			t.Errorf("handler returned wrong status code: got %v want %v",
				status, tc.statusCode)
		}
		if tc.statusCode == http.StatusOK {
			assert.Equal(t, tc.expectedConv.SpSchema, res.SpSchema)
		}
	}
}

func TestDropTable(t *testing.T) {
	sessionState := session.GetSessionState()
	sessionState.Driver = constants.MYSQL

	c3 := &internal.Conv{
		SchemaIssues: map[string]internal.TableIssues{
			"t1": {},
			"t2": {},
		},
		SrcSchema: map[string]schema.Table{
			"t1": {
				Name:   "tn1",
				ColIds: []string{"c1", "c2", "c3"},
				ColDefs: map[string]schema.Column{
					"c1": {Name: "cn1", Type: schema.Type{Name: "bigint"}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: "c1"},
					"c2": {Name: "cn2", Type: schema.Type{Name: "bigint"}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: "c2"},
					"c3": {Name: "cn3", Type: schema.Type{Name: "varchar"}, NotNull: false, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: "c3"},
				},
				PrimaryKeys: []schema.Key{{ColId: "c1", Desc: false, Order: 1}},
				Id:          "t1",
			},

			"t2": {
				Name:   "tn2",
				ColIds: []string{"c4", "c5", "c6"},
				ColDefs: map[string]schema.Column{
					"c4": {Name: "cn4", Type: schema.Type{Name: "bigint"}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: "c4"},
					"c5": {Name: "cn5", Type: schema.Type{Name: "varchar"}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: "c5"},
					"c6": {Name: "cn6", Type: schema.Type{Name: "bigint"}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: "c6"},
				},
				Id: "t2",
			},
		},
		SpSchema: map[string]ddl.CreateTable{
			"t1": {
				Name:   "tn1",
				ColIds: []string{"c1", "c2", "c3"},
				ColDefs: map[string]ddl.ColumnDef{"c1": {Name: "cn1", T: ddl.Type{Name: ddl.Int64}, NotNull: true, Id: "c1"},
					"c2": {Name: "cn2", T: ddl.Type{Name: ddl.Int64}, NotNull: true, Id: "c2"},
					"c3": {Name: "cn3", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true, Id: "c3"},
				},
				PrimaryKeys: []ddl.IndexKey{{ColId: "c1", Desc: false}},
				Id:          "t1",
			},
			"t2": {
				Name:   "tn2",
				ColIds: []string{"c4", "c5", "c6", "c7"},
				ColDefs: map[string]ddl.ColumnDef{"c4": {Name: "cn4", T: ddl.Type{Name: ddl.Int64}, NotNull: true, Id: "c4"},
					"c5": {Name: "cn5", T: ddl.Type{Name: ddl.Int64}, NotNull: true, Id: "c5"},
					"c6": {Name: "cn6", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true, Id: "c6"},
					"c7": {Name: "synth_id", T: ddl.Type{Name: ddl.Int64}, NotNull: true, Id: "c7"},
				},
				PrimaryKeys: []ddl.IndexKey{{ColId: "c7", Desc: false}},
				Id:          "t2",
			}},
		Audit: internal.Audit{
			MigrationType: migration.MigrationData_MIGRATION_TYPE_UNSPECIFIED.Enum(),
		},
	}

	sessionState.Conv = c3

	payload := `{}`

	req, err := http.NewRequest("POST", "/drop/table?table=t1", strings.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}

	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	handler := http.HandlerFunc(api.DropTable)
	handler.ServeHTTP(rr, req)

	res := &internal.Conv{}

	json.Unmarshal(rr.Body.Bytes(), &res)

	expectedConv := &internal.Conv{

		SpSchema: map[string]ddl.CreateTable{
			"t2": {
				Name:   "tn2",
				ColIds: []string{"c4", "c5", "c6", "c7"},
				ColDefs: map[string]ddl.ColumnDef{"c4": {Name: "cn4", T: ddl.Type{Name: ddl.Int64}, NotNull: true, Id: "c4"},
					"c5": {Name: "cn5", T: ddl.Type{Name: ddl.Int64}, NotNull: true, Id: "c5"},
					"c6": {Name: "cn6", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true, Id: "c6"},
					"c7": {Name: "synth_id", T: ddl.Type{Name: ddl.Int64}, NotNull: true, Id: "c7"},
				},
				PrimaryKeys: []ddl.IndexKey{{ColId: "c7", Desc: false, Order: 0}},
				ForeignKeys: []ddl.Foreignkey{},
				Indexes:     []ddl.CreateIndex(nil),
				Id:          "t2",
			}},
	}

	assert.Equal(t, expectedConv.SpSchema, res.SpSchema)
}

func TestRestoreTable(t *testing.T) {
	sessionState := session.GetSessionState()

	sessionState.Driver = constants.MYSQL

	conv := &internal.Conv{

		SpSchema: map[string]ddl.CreateTable{
			"t1": {
				Name:   "tn1",
				ColIds: []string{"c1", "c2"},
				ColDefs: map[string]ddl.ColumnDef{"c1": {Name: "cn1", T: ddl.Type{Name: "STRING", IsArray: false}, NotNull: true, Comment: "", Id: "c1", AutoGen: ddl.AutoGenCol{Name: "", GenerationType: ""}},
					"c2": {Name: "cn2", T: ddl.Type{Name: "STRING", IsArray: false}, NotNull: true, Comment: "", Id: "c2", AutoGen: ddl.AutoGenCol{Name: "", GenerationType: ""}},
				},
				PrimaryKeys: []ddl.IndexKey{{ColId: "c1", Desc: false, Order: 1}},
				Id:          "t1",
			}},
		SrcSchema: map[string]schema.Table{
			"t2": {
				Name:   "tn2",
				ColIds: []string{"c3", "c4", "c5"},
				ColDefs: map[string]schema.Column{
					"c3": {Name: "cn3", Type: schema.Type{Name: "varchar"}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: "c3"},
					"c4": {Name: "cn4", Type: schema.Type{Name: "varchar"}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: "c4"},
					"c5": {Name: "cn5", Type: schema.Type{Name: "bigint"}, NotNull: false, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: "c5"},
				},
				PrimaryKeys: []schema.Key{{ColId: "c3", Desc: false, Order: 1}},
				Id:          "t2",
			},

			"t1": {
				Name:   "tn1",
				ColIds: []string{"c1", "c2"},
				ColDefs: map[string]schema.Column{
					"c1": {Name: "cn1", Type: schema.Type{Name: "varchar"}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: "c1"},
					"c2": {Name: "cn2", Type: schema.Type{Name: "char"}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: "c2"},
				},
				PrimaryKeys: []schema.Key{{ColId: "c1", Desc: false, Order: 1}},
				Id:          "t1",
			},
		},

		UsedNames: map[string]bool{
			"t1": true,
		},

		Audit: internal.Audit{
			MigrationType: migration.MigrationData_MIGRATION_TYPE_UNSPECIFIED.Enum(),
		},

		SchemaIssues: map[string]internal.TableIssues{},
	}

	sessionState.Conv = conv

	payload := `{}`

	req, err := http.NewRequest("POST", "/restore/table?table=t2", strings.NewReader(payload))

	if err != nil {
		t.Fatal(err)
	}

	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	handler := http.HandlerFunc(api.RestoreTable)
	handler.ServeHTTP(rr, req)

	res := &internal.Conv{}

	json.Unmarshal(rr.Body.Bytes(), &res)

	expectedConv := &internal.Conv{

		SpSchema: map[string]ddl.CreateTable{
			"t1": {
				Name:   "tn1",
				ColIds: []string{"c1", "c2"},
				ColDefs: map[string]ddl.ColumnDef{"c1": {Name: "cn1", T: ddl.Type{Name: "STRING", Len: 0, IsArray: false}, NotNull: true, Comment: "", Id: "c1", AutoGen: ddl.AutoGenCol{Name: "", GenerationType: ""}},
					"c2": {Name: "cn2", T: ddl.Type{Name: "STRING", Len: 0, IsArray: false}, NotNull: true, Comment: "", Id: "c2", AutoGen: ddl.AutoGenCol{Name: "", GenerationType: ""}},
				},
				PrimaryKeys: []ddl.IndexKey{{ColId: "c1", Desc: false, Order: 1}},
				Id:          "t1",
			},

			"t2": {
				Name:   "tn2",
				ColIds: []string{"c3", "c4", "c5"},
				ColDefs: map[string]ddl.ColumnDef{
					"c3": {Name: "cn3", T: ddl.Type{Name: "STRING", Len: 9223372036854775807, IsArray: false}, NotNull: true, Comment: "From: cn3 varchar", Id: "c3", AutoGen: ddl.AutoGenCol{Name: "", GenerationType: ""}},
					"c4": {Name: "cn4", T: ddl.Type{Name: "STRING", Len: 9223372036854775807, IsArray: false}, NotNull: true, Comment: "From: cn4 varchar", Id: "c4", AutoGen: ddl.AutoGenCol{Name: "", GenerationType: ""}},
					"c5": {Name: "cn5", T: ddl.Type{Name: "INT64", Len: 0, IsArray: false}, NotNull: false, Comment: "From: cn5 bigint", Id: "c5", AutoGen: ddl.AutoGenCol{Name: "", GenerationType: ""}},
				},
				PrimaryKeys: []ddl.IndexKey{{ColId: "c3", Desc: false, Order: 1}},
				Id:          "t2",
				Comment:     "Spanner schema for source table tn2",
			},
		},
	}
	assert.Equal(t, expectedConv.SpSchema, res.SpSchema)

}

// todo update SetParentTable with case III suggest interleve table column.
func TestSetParentTable(t *testing.T) {
	tests := []struct {
		name             string
		ct               *internal.Conv
		table            string
		statusCode       int64
		expectedResponse *types.TableInterleaveStatus
		expectedFKs      []ddl.Foreignkey
		parentTable      string
	}{
		{
			name:       "no conv provided",
			statusCode: http.StatusNotFound,
		},
		{
			name:       "no table name provided",
			statusCode: http.StatusBadRequest,
			ct: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{"t1": {
					Name:   "t1",
					ColIds: []string{"c1", "c2", "c3"},
					ColDefs: map[string]ddl.ColumnDef{"c1": ddl.ColumnDef{Name: "c1", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
						"c2": ddl.ColumnDef{Name: "c2", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
						"c3": ddl.ColumnDef{Name: "c3", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true}},
					PrimaryKeys: []ddl.IndexKey{ddl.IndexKey{ColId: "c1", Desc: false}},
					ForeignKeys: []ddl.Foreignkey{ddl.Foreignkey{Name: "fk1", ColIds: []string{"c1"}, ReferTableId: "t1", ReferColumnIds: []string{"c1"}},
						ddl.Foreignkey{Name: "fk2", ColIds: []string{"c3"}, ReferTableId: "t2", ReferColumnIds: []string{"c2"}}},
				}},
				SchemaIssues: map[string]internal.TableIssues{
					"t1": {
						ColumnLevelIssues: make(map[string][]internal.SchemaIssue),
					},
				},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
			},
		},
		{
			name: "table with synthetic PK",
			ct: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{"t1": {
					Name:   "t1",
					ColIds: []string{"c1", "c2", "c3"},
					ColDefs: map[string]ddl.ColumnDef{"c1": {Name: "c1", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
						"c2":       {Name: "c2", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
						"c3":       {Name: "c3", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						"synth_id": {Name: "synth_id", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
					},
					PrimaryKeys: []ddl.IndexKey{{ColId: "synth_id", Desc: false}},
					ForeignKeys: []ddl.Foreignkey{{Name: "fk1", ColIds: []string{"c1"}, ReferTableId: "t1", ReferColumnIds: []string{"c1"}},
						{Name: "fk2", ColIds: []string{"c3"}, ReferTableId: "t2", ReferColumnIds: []string{"c2"}}},
				}},
				SyntheticPKeys: map[string]internal.SyntheticPKey{"t1": internal.SyntheticPKey{ColId: "synth_id"}},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
				SchemaIssues: map[string]internal.TableIssues{
					"t1": {
						ColumnLevelIssues: make(map[string][]internal.SchemaIssue),
					},
				},
			},
			table:            "t1",
			statusCode:       http.StatusOK,
			expectedResponse: &types.TableInterleaveStatus{Possible: false, Comment: "Has synthetic pk"},
		},
		{
			name: "no valid prefix 1",
			ct: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:   "t1",
						ColIds: []string{"c1", "c2", "c3"},
						ColDefs: map[string]ddl.ColumnDef{"c1": {Name: "c1", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c2": {Name: "c2", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c3": {Name: "c3", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c1", Desc: false}},
						ForeignKeys: []ddl.Foreignkey{{Name: "fk1", ColIds: []string{"c1"}, ReferTableId: "t2", ReferColumnIds: []string{"c1"}}},
					},
					"t2": {
						Name:   "t2",
						ColIds: []string{"c1", "c2", "c3"},
						ColDefs: map[string]ddl.ColumnDef{"c1": {Name: "c1", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c2":       {Name: "c2", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c3":       {Name: "c3", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
							"synth_id": {Name: "synth_id", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "synth_id", Desc: false}},
					},
				},
				SyntheticPKeys: map[string]internal.SyntheticPKey{"t2": internal.SyntheticPKey{ColId: "synth_id"}},
				SchemaIssues: map[string]internal.TableIssues{
					"t1": {
						ColumnLevelIssues: make(map[string][]internal.SchemaIssue),
					},
				},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
			},
			table:            "t1",
			statusCode:       http.StatusOK,
			expectedResponse: &types.TableInterleaveStatus{Possible: false, Comment: "No valid prefix"},
			expectedFKs:      []ddl.Foreignkey{{Name: "fk1", ColIds: []string{"c1"}, ReferTableId: "t2", ReferColumnIds: []string{"c1"}}},
		},
		{
			name: "no valid prefix 2",
			ct: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:   "t1",
						ColIds: []string{"c1", "c2", "c3"},
						ColDefs: map[string]ddl.ColumnDef{"c1": {Name: "c1", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c2": {Name: "c2", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c3": {Name: "c3", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c1", Desc: false, Order: 1}},
						ForeignKeys: []ddl.Foreignkey{{Name: "fk1", ColIds: []string{"c1"}, ReferTableId: "t2", ReferColumnIds: []string{"c1"}}},
					},
					"t2": {
						Name:   "t2",
						ColIds: []string{"c1", "c2", "c3"},
						ColDefs: map[string]ddl.ColumnDef{"c1": {Name: "c1", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c2": {Name: "c2", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c3": {Name: "c3", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c1", Desc: false, Order: 1}, {ColId: "c2", Desc: false, Order: 2}},
					},
				},
				SchemaIssues: map[string]internal.TableIssues{
					"t1": {
						ColumnLevelIssues: make(map[string][]internal.SchemaIssue),
					},
				},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
			},
			table:            "t1",
			statusCode:       http.StatusOK,
			expectedResponse: &types.TableInterleaveStatus{Possible: false, Parent: "", Comment: "No valid prefix"},
			expectedFKs:      []ddl.Foreignkey{{}},
		},
		{
			name: "no valid prefix 3",
			ct: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:   "t1",
						ColIds: []string{"c1", "c2", "c3"},
						ColDefs: map[string]ddl.ColumnDef{"c1": {Name: "c1", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c2": {Name: "c2", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c3": {Name: "c3", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c1", Desc: false}, {ColId: "c2", Desc: false}},
						ForeignKeys: []ddl.Foreignkey{{Name: "fk1", ColIds: []string{"c3"}, ReferTableId: "t2", ReferColumnIds: []string{"c3"}}},
					},
					"t2": {
						Name:   "t2",
						ColIds: []string{"c1", "c2", "c3"},
						ColDefs: map[string]ddl.ColumnDef{"c1": {Name: "c1", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c2": {Name: "c2", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c3": {Name: "c3", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c1", Desc: false}},
					},
				},
				SchemaIssues: map[string]internal.TableIssues{
					"t1": {
						ColumnLevelIssues: make(map[string][]internal.SchemaIssue),
					},
				},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
			},
			table:            "t1",
			statusCode:       http.StatusOK,
			expectedResponse: &types.TableInterleaveStatus{Possible: false, Comment: "No valid prefix"},
			expectedFKs:      []ddl.Foreignkey{{Name: "fk1", ColIds: []string{"c3"}, ReferTableId: "t2", ReferColumnIds: []string{"c3"}}},
		},
		{
			name: "interleave possible on changing primary key order",
			ct: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:   "t1",
						ColIds: []string{"c1", "c2", "c3"},
						ColDefs: map[string]ddl.ColumnDef{"c1": {Name: "c1", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c2": {Name: "c2", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c3": {Name: "c3", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c1", Desc: false, Order: 2}, {ColId: "c2", Desc: false, Order: 2}},
						ForeignKeys: []ddl.Foreignkey{{Name: "fk1", ColIds: []string{"c1"}, ReferTableId: "t2", ReferColumnIds: []string{"c4"}}},
					},
					"t2": {
						Name:   "t2",
						ColIds: []string{"cc4", "c5", "c6"},
						ColDefs: map[string]ddl.ColumnDef{"c4": {Name: "d", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c5": {Name: "e", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c6": {Name: "f", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c1", Desc: false, Order: 1}, {ColId: "c2", Desc: false, Order: 2}},
					},
				},
				SchemaIssues: map[string]internal.TableIssues{
					"t1": {
						ColumnLevelIssues: map[string][]internal.SchemaIssue{
							"c1": {internal.InterleavedNotInOrder},
						},
					},
				},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
			},
			table:            "t1",
			statusCode:       http.StatusOK,
			expectedResponse: &types.TableInterleaveStatus{Possible: false, Parent: "", Comment: "No valid prefix"},
			expectedFKs:      []ddl.Foreignkey{{Name: "fk1", ColIds: []string{"c1"}, ReferTableId: "t2", ReferColumnIds: []string{"c4"}}},
			parentTable:      "",
		},
		{
			name: "successful interleave",
			ct: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:   "t1",
						ColIds: []string{"c1", "c2", "c3"},
						ColDefs: map[string]ddl.ColumnDef{"c1": {Name: "c1", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c2": {Name: "c2", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c3": {Name: "c3", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c1", Desc: false, Order: 1}, {ColId: "c2", Desc: false, Order: 2}},
						ForeignKeys: []ddl.Foreignkey{{Name: "fk1", ColIds: []string{"c1"}, ReferTableId: "t2", ReferColumnIds: []string{"c1"}}},
					},
					"t2": {
						Name:   "t2",
						ColIds: []string{"c1", "c2", "c3"},
						ColDefs: map[string]ddl.ColumnDef{"c1": {Name: "c1", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c2": {Name: "c2", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c3": {Name: "c3", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c1", Desc: false, Order: 1}},
					},
				},
				SchemaIssues: map[string]internal.TableIssues{
					"t1": {
						ColumnLevelIssues: make(map[string][]internal.SchemaIssue),
					},
				},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
			},
			table:            "t1",
			statusCode:       http.StatusOK,
			expectedResponse: &types.TableInterleaveStatus{Possible: true, Parent: "t2"},
			expectedFKs:      []ddl.Foreignkey{},
			parentTable:      "t2",
		},
		{
			name: "successful interleave with same primary key",
			ct: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:   "t1",
						ColIds: []string{"c1", "c2", "c3"},
						ColDefs: map[string]ddl.ColumnDef{"c1": {Name: "c1", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c2": {Name: "c2", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c3": {Name: "c3", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c1", Desc: false, Order: 1}, {ColId: "c2", Desc: false, Order: 2}},
						ForeignKeys: []ddl.Foreignkey{{Name: "fk1", ColIds: []string{"c1", "c2"}, ReferTableId: "t2", ReferColumnIds: []string{"c1", "c2"}}},
					},
					"t2": {
						Name:   "t2",
						ColIds: []string{"c1", "c2", "c3"},
						ColDefs: map[string]ddl.ColumnDef{"c1": {Name: "c1", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c2": {Name: "c2", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c3": {Name: "c3", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c1", Desc: false, Order: 1}, {ColId: "c2", Desc: false, Order: 2}},
					},
				},
				SchemaIssues: map[string]internal.TableIssues{
					"t1": {
						ColumnLevelIssues: make(map[string][]internal.SchemaIssue),
					},
				},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
			},
			table:            "t1",
			statusCode:       http.StatusOK,
			expectedResponse: &types.TableInterleaveStatus{Possible: true, Parent: "t2"},
			expectedFKs:      []ddl.Foreignkey{},
			parentTable:      "t2",
		},
		{
			name: "successful interleave with multiple fks refering multiple tables",
			ct: &internal.Conv{
				SpSchema: map[string]ddl.CreateTable{
					"t1": {
						Name:   "t1",
						ColIds: []string{"c1", "c2", "c3"},
						ColDefs: map[string]ddl.ColumnDef{"c1": {Name: "c1", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c2": {Name: "c2", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c3": {Name: "c3", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c1", Desc: false, Order: 1}, {ColId: "c2", Desc: false, Order: 2}},
						ForeignKeys: []ddl.Foreignkey{
							{Name: "fk1", ColIds: []string{"c3"}, ReferTableId: "t3", ReferColumnIds: []string{"c3"}},
							{Name: "fk1", ColIds: []string{"c1", "c2"}, ReferTableId: "t2", ReferColumnIds: []string{"c1", "c2"}}},
					},
					"t2": {
						Name:   "t2",
						ColIds: []string{"c1", "c2", "c3"},
						ColDefs: map[string]ddl.ColumnDef{"c1": {Name: "c1", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c2": {Name: "c2", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c3": {Name: "c3", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c1", Desc: false, Order: 1}, {ColId: "c2", Desc: false, Order: 2}},
					},
					"t3": {
						Name:   "t3",
						ColIds: []string{"c1", "c2", "c3"},
						ColDefs: map[string]ddl.ColumnDef{"c1": {Name: "c1", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c2": {Name: "c2", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c3": {Name: "c3", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c3", Desc: false, Order: 1}},
					},
				},
				SchemaIssues: map[string]internal.TableIssues{
					"t1": {
						ColumnLevelIssues: make(map[string][]internal.SchemaIssue),
					},
				},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
				},
			},
			table:            "t1",
			statusCode:       http.StatusOK,
			expectedResponse: &types.TableInterleaveStatus{Possible: true, Parent: "t2"},
			expectedFKs:      []ddl.Foreignkey{ddl.Foreignkey{Name: "fk1", ColIds: []string{"c1", "c2"}, ReferTableId: "t2", ReferColumnIds: []string{"c1", "c2"}, Id: ""}},
			parentTable:      "t2",
		},
	}
	for _, tc := range tests {
		sessionState := session.GetSessionState()

		sessionState.Driver = constants.MYSQL
		sessionState.Conv = tc.ct
		update := true
		req, err := http.NewRequest("GET", fmt.Sprintf("/setparent?table=%s&update=%v", tc.table, update), nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		handler := http.HandlerFunc(api.SetParentTable)
		handler.ServeHTTP(rr, req)

		type ParentTableSetResponse struct {
			TableInterleaveStatus *types.TableInterleaveStatus `json:"tableInterleaveStatus"`
			SessionState          *internal.Conv               `json:"sessionState"`
		}

		var res *types.TableInterleaveStatus

		if update {
			parentTableResponse := &ParentTableSetResponse{}
			json.Unmarshal(rr.Body.Bytes(), parentTableResponse)
			res = parentTableResponse.TableInterleaveStatus
		} else {
			res = &types.TableInterleaveStatus{}
			json.Unmarshal(rr.Body.Bytes(), res)
		}

		if status := rr.Code; int64(status) != tc.statusCode {
			t.Errorf("%s\nhandler returned wrong status code: got %v want %v",
				tc.name, status, tc.statusCode)
		}
		if tc.statusCode == http.StatusOK {
			assert.Equal(t, tc.expectedResponse, res, tc.name)
		}
		if tc.parentTable != "" {
			assert.Equal(t, tc.parentTable, sessionState.Conv.SpSchema[tc.table].ParentId, tc.name)
			assert.Equal(t, tc.expectedFKs, sessionState.Conv.SpSchema[tc.table].ForeignKeys, tc.name)
		}
	}
}

func TestRemoveParentTable(t *testing.T) {
	tc := []struct {
		name             string
		tableId          string
		statusCode       int64
		conv             *internal.Conv
		expectedSpSchema ddl.Schema
	}{
		{
			name:       "Remove interleaving with valid table id",
			tableId:    "t1",
			statusCode: http.StatusOK,
			conv: &internal.Conv{
				SchemaIssues: map[string]internal.TableIssues{
					"t1": {},
					"t2": {},
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
						PrimaryKeys: []schema.Key{{ColId: "c1", Desc: false, Order: 1}, {ColId: "c2", Desc: false, Order: 2}},
						ForeignKeys: []schema.ForeignKey{{Name: "fk1", ColIds: []string{"c1"}, ReferTableId: "t2", ReferColumnIds: []string{"c4"}, Id: "f1"}},
						Id:          "t1",
					},

					"t2": {
						Name:   "table2",
						ColIds: []string{"c4", "c5"},
						ColDefs: map[string]schema.Column{
							"c4": {Name: "a", Type: schema.Type{Name: "bigint"}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: true, AutoIncrement: false}, Id: "c4"},
							"c5": {Name: "d", Type: schema.Type{Name: "varchar"}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: "c5"},
						},
						Id:          "t2",
						PrimaryKeys: []schema.Key{{ColId: "c4", Desc: false, Order: 1}},
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
						PrimaryKeys: []ddl.IndexKey{{ColId: "c1", Desc: false, Order: 1}, {ColId: "c2", Desc: false, Order: 2}},
						Id:          "t1",
						ParentId:    "t2",
					},
					"t2": {
						Name:   "table2",
						ColIds: []string{"c4", "c5"},
						ColDefs: map[string]ddl.ColumnDef{
							"c4": {Name: "a", Id: "c4", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c5": {Name: "d", Id: "c5", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c4", Desc: false, Order: 1}},
						Id:          "t2",
					}},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_MIGRATION_TYPE_UNSPECIFIED.Enum(),
				},
				UsedNames: map[string]bool{"table1": true, "table2": true},
			},
			expectedSpSchema: ddl.Schema{
				"t1": {
					Name:   "table1",
					ColIds: []string{"c1", "c2", "c3"},
					ColDefs: map[string]ddl.ColumnDef{
						"c1": {Name: "a", Id: "c1", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
						"c2": {Name: "b", Id: "c2", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
						"c3": {Name: "c", Id: "c3", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
					},
					PrimaryKeys: []ddl.IndexKey{{ColId: "c1", Desc: false, Order: 1}, {ColId: "c2", Desc: false, Order: 2}},
					ForeignKeys: []ddl.Foreignkey{{Name: "fk1", ColIds: []string{"c1"}, ReferTableId: "t2", ReferColumnIds: []string{"c4"}, Id: "f1"}},
					Id:          "t1",
					ParentId:    "",
				},
				"t2": {
					Name:   "table2",
					ColIds: []string{"c4", "c5"},
					ColDefs: map[string]ddl.ColumnDef{
						"c4": {Name: "a", Id: "c4", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
						"c5": {Name: "d", Id: "c5", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
					},
					PrimaryKeys: []ddl.IndexKey{{ColId: "c4", Desc: false, Order: 1}},
					Id:          "t2",
				},
			},
		},

		{name: "Remove interleaving with invalid table id",
			tableId:    "A",
			statusCode: http.StatusBadRequest,
			conv: &internal.Conv{
				SchemaIssues: map[string]internal.TableIssues{
					"t1": {},
					"t2": {},
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
						PrimaryKeys: []schema.Key{{ColId: "c1", Desc: false, Order: 1}, {ColId: "c2", Desc: false, Order: 2}},
						ForeignKeys: []schema.ForeignKey{{Name: "fk1", ColIds: []string{"c1"}, ReferTableId: "t2", ReferColumnIds: []string{"c4"}, Id: "f1"}},
						Id:          "t1",
					},

					"t2": {
						Name:   "table2",
						ColIds: []string{"c4", "c5"},
						ColDefs: map[string]schema.Column{
							"c4": {Name: "a", Type: schema.Type{Name: "bigint"}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: true, AutoIncrement: false}, Id: "c4"},
							"c5": {Name: "d", Type: schema.Type{Name: "varchar"}, NotNull: true, Ignored: schema.Ignored{Check: false, Identity: false, Default: false, Exclusion: false, ForeignKey: false, AutoIncrement: false}, Id: "c5"},
						},
						Id:          "t2",
						PrimaryKeys: []schema.Key{{ColId: "c4", Desc: false, Order: 1}},
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
						PrimaryKeys: []ddl.IndexKey{{ColId: "c1", Desc: false, Order: 1}, {ColId: "c2", Desc: false, Order: 2}},
						Id:          "t1",
						ParentId:    "t2",
					},
					"t2": {
						Name:   "table2",
						ColIds: []string{"c4", "c5"},
						ColDefs: map[string]ddl.ColumnDef{
							"c4": {Name: "a", Id: "c4", T: ddl.Type{Name: ddl.Int64}, NotNull: true},
							"c5": {Name: "d", Id: "c5", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, NotNull: true},
						},
						PrimaryKeys: []ddl.IndexKey{{ColId: "c4", Desc: false, Order: 1}},
						Id:          "t2",
					}},
				Audit: internal.Audit{
					MigrationType: migration.MigrationData_MIGRATION_TYPE_UNSPECIFIED.Enum(),
				},
				UsedNames: map[string]bool{"table1": true, "table2": true},
			},
			expectedSpSchema: ddl.Schema{},
		},
	}

	for _, tc := range tc {
		sessionState := session.GetSessionState()
		sessionState.Driver = constants.MYSQL

		sessionState.Conv = tc.conv
		payload := `{}`
		req, err := http.NewRequest("POST", "/drop/removeParent?tableId="+tc.tableId, strings.NewReader(payload))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		handler := http.HandlerFunc(api.RemoveParentTable)
		handler.ServeHTTP(rr, req)
		var res *internal.Conv
		json.Unmarshal(rr.Body.Bytes(), &res)
		if status := rr.Code; int64(status) != tc.statusCode {
			t.Errorf("handler returned wrong status code: got %v want %v",
				status, tc.statusCode)
		}
		if tc.statusCode == http.StatusOK {
			assert.Equal(t, tc.expectedSpSchema, res.SpSchema)
		}
	}
}

func buildConvMySQL(conv *internal.Conv) {
	conv.SrcSchema = map[string]schema.Table{
		"t1": {
			Name:   "table1",
			Id:     "t1",
			ColIds: []string{"c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11", "c12", "c13", "c14", "c15", "c16"},
			ColDefs: map[string]schema.Column{
				"c1":  {Name: "a", Id: "c1", Type: schema.Type{Name: "bool"}},
				"c2":  {Name: "b", Id: "c2", Type: schema.Type{Name: "text"}},
				"c3":  {Name: "c", Id: "c3", Type: schema.Type{Name: "bool"}},
				"c4":  {Name: "d", Id: "c4", Type: schema.Type{Name: "varchar", Mods: []int64{6}}},
				"c5":  {Name: "e", Id: "c5", Type: schema.Type{Name: "numeric"}},
				"c6":  {Name: "f", Id: "c6", Type: schema.Type{Name: "enum"}},
				"c7":  {Name: "g", Id: "c7", Type: schema.Type{Name: "json"}},
				"c8":  {Name: "h", Id: "c8", Type: schema.Type{Name: "binary"}},
				"c9":  {Name: "i", Id: "c9", Type: schema.Type{Name: "blob"}},
				"c10": {Name: "j", Id: "c10", Type: schema.Type{Name: "smallint"}},
				"c11": {Name: "k", Id: "c11", Type: schema.Type{Name: "double"}},
				"c12": {Name: "l", Id: "c12", Type: schema.Type{Name: "float"}},
				"c13": {Name: "m", Id: "c13", Type: schema.Type{Name: "decimal"}},
				"c14": {Name: "n", Id: "c14", Type: schema.Type{Name: "date"}},
				"c15": {Name: "o", Id: "c15", Type: schema.Type{Name: "timestamp"}},
				"c16": {Name: "p", Id: "c16", Type: schema.Type{Name: "time"}},
			},
			PrimaryKeys: []schema.Key{{ColId: "c1"}}},
		"t2": {
			Name:   "table2",
			Id:     "t2",
			ColIds: []string{"c17", "c18", "c19"},
			ColDefs: map[string]schema.Column{
				"c17": {Name: "a", Id: "c17", Type: schema.Type{Name: "integer"}},
				"c18": {Name: "b", Id: "c18", Type: schema.Type{Name: "double"}},
				"c19": {Name: "c", Id: "c19", Type: schema.Type{Name: "bool"}},
			}},
	}
	conv.SpSchema = map[string]ddl.CreateTable{
		"t1": {
			Name:   "table1",
			Id:     "t1",
			ColIds: []string{"c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11", "c12", "c13", "c14", "c15", "c16"},
			ColDefs: map[string]ddl.ColumnDef{
				"c1":  {Name: "a", Id: "c1", T: ddl.Type{Name: ddl.Bool}},
				"c2":  {Name: "b", Id: "c2", T: ddl.Type{Name: ddl.String, Len: ddl.MaxLength}},
				"c3":  {Name: "c", Id: "c3", T: ddl.Type{Name: ddl.Bool}},
				"c4":  {Name: "d", Id: "c4", T: ddl.Type{Name: ddl.String, Len: int64(6)}},
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
		"t2": {
			Name:   "t2",
			ColIds: []string{"c17", "c18", "c19", "c20"},
			ColDefs: map[string]ddl.ColumnDef{
				"c17": {Name: "a", Id: "c17", T: ddl.Type{Name: ddl.Int64}},
				"c18": {Name: "b", Id: "c18", T: ddl.Type{Name: ddl.Float64}},
				"c19": {Name: "c", Id: "c19", T: ddl.Type{Name: ddl.Bool}},
				"c20": {Name: "synth_id", Id: "c20", T: ddl.Type{Name: ddl.Int64}},
			},
			PrimaryKeys: []ddl.IndexKey{{ColId: "c20"}},
		},
	}

	conv.SchemaIssues = map[string]internal.TableIssues{
		"t1": {
			ColumnLevelIssues: map[string][]internal.SchemaIssue{
				"c10": {internal.Widened},
				"c12": {internal.Widened},
				"c15": {internal.Time},
			},
		},
		"t2": {
			ColumnLevelIssues: map[string][]internal.SchemaIssue{
				"c17": {internal.Widened},
			},
		},
	}
	conv.SyntheticPKeys["t2"] = internal.SyntheticPKey{"c20", 0}
	conv.Audit.MigrationType = migration.MigrationData_SCHEMA_AND_DATA.Enum()
}

func buildConvPostgres(conv *internal.Conv) {
	conv.SrcSchema = map[string]schema.Table{
		"t1": {
			Name:   "table1",
			Id:     "t1",
			ColIds: []string{"c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11", "c12", "c13", "c14", "c15", "c16", "c17"},
			ColDefs: map[string]schema.Column{
				"c1":  {Name: "a", Id: "c1", Type: schema.Type{Name: "int8"}},
				"c2":  {Name: "b", Id: "c2", Type: schema.Type{Name: "float4"}},
				"c3":  {Name: "c", Id: "c3", Type: schema.Type{Name: "bool"}},
				"c4":  {Name: "d", Id: "c4", Type: schema.Type{Name: "varchar", Mods: []int64{6}}},
				"c5":  {Name: "e", Id: "c5", Type: schema.Type{Name: "numeric"}},
				"c6":  {Name: "f", Id: "c6", Type: schema.Type{Name: "timestamptz"}},
				"c7":  {Name: "g", Id: "c7", Type: schema.Type{Name: "bigserial"}},
				"c8":  {Name: "h", Id: "c8", Type: schema.Type{Name: "bpchar"}},
				"c9":  {Name: "i", Id: "c9", Type: schema.Type{Name: "bytea"}},
				"c10": {Name: "j", Id: "c10", Type: schema.Type{Name: "date"}},
				"c11": {Name: "k", Id: "c11", Type: schema.Type{Name: "float8"}},
				"c12": {Name: "l", Id: "c12", Type: schema.Type{Name: "int4"}},
				"c13": {Name: "m", Id: "c13", Type: schema.Type{Name: "serial"}},
				"c14": {Name: "n", Id: "c14", Type: schema.Type{Name: "text"}},
				"c15": {Name: "o", Id: "c15", Type: schema.Type{Name: "timestamp"}},
				"c16": {Name: "p", Id: "c16", Type: schema.Type{Name: "bool"}},
				"c17": {Name: "q", Id: "c17", Type: schema.Type{Name: "path"}},
			},
			PrimaryKeys: []schema.Key{{ColId: "c1"}}},
		"t2": {
			Name:   "t2",
			Id:     "t2",
			ColIds: []string{"c17", "c18", "c19"},
			ColDefs: map[string]schema.Column{
				"c17": {Name: "a", Id: "c17", Type: schema.Type{Name: "int8"}},
				"c18": {Name: "b", Id: "c18", Type: schema.Type{Name: "float4"}},
				"c19": {Name: "c", Id: "c19", Type: schema.Type{Name: "bool"}},
			}},
	}
	conv.SpSchema = map[string]ddl.CreateTable{
		"t1": {
			Name:   "table1",
			Id:     "t1",
			ColIds: []string{"c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11", "c12", "c13", "c14", "c15", "c16", "c17"},
			ColDefs: map[string]ddl.ColumnDef{
				"c1":  {Name: "a", Id: "c1", T: ddl.Type{Name: ddl.Int64}},
				"c2":  {Name: "b", Id: "c2", T: ddl.Type{Name: ddl.Float64}},
				"c3":  {Name: "c", Id: "c3", T: ddl.Type{Name: ddl.Bool}},
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
		"t2": {
			Name:   "table2",
			Id:     "t2",
			ColIds: []string{"c17", "c18", "c19", "c20"},
			ColDefs: map[string]ddl.ColumnDef{
				"c17": {Name: "a", Id: "c17", T: ddl.Type{Name: ddl.Int64}},
				"c18": {Name: "b", Id: "c18", T: ddl.Type{Name: ddl.Float64}},
				"c19": {Name: "c", Id: "c19", T: ddl.Type{Name: ddl.Bool}},
				"c20": {Name: "synth_id", Id: "c20", T: ddl.Type{Name: ddl.Int64}},
			},
			PrimaryKeys: []ddl.IndexKey{{ColId: "c20"}},
		},
	}

	conv.SchemaIssues = map[string]internal.TableIssues{
		"t1": {
			ColumnLevelIssues: map[string][]internal.SchemaIssue{
				"c2":  {internal.Widened},    //b
				"c7":  {internal.Serial},     //g
				"c12": {internal.Widened},    //l
				"c13": {internal.Serial},     //m
				"c15": {internal.Timestamp},  //o
				"c17": {internal.NoGoodType}, //q
			},
		},
		"t2": {
			ColumnLevelIssues: map[string][]internal.SchemaIssue{
				"c18": {internal.Widened},
			},
		},
	}
	conv.SyntheticPKeys["t2"] = internal.SyntheticPKey{"c20", 0}
	conv.Audit.MigrationType = migration.MigrationData_SCHEMA_AND_DATA.Enum()
}

func TestGetAutoGenMapMySQL(t *testing.T) {
	sessionState := session.GetSessionState()
	sessionState.Driver = constants.MYSQL
	sessionState.Conv = internal.MakeConv()
	buildConvMySQL(sessionState.Conv)

	sequences := make(map[string]ddl.Sequence)
	sequences["s1"] = ddl.Sequence{
		Name:         "Sequence1",
		Id:           "s1",
		SequenceKind: "BIT REVERSED POSITIVE",
	}
	sessionState.Conv.SpSequences = sequences

	expectedAutoGenMapPostgres := map[string][]types.AutoGen{
		"BOOL":        {types.AutoGen{Name: "", GenerationType: ""}},
		"BYTEA":       {types.AutoGen{Name: "", GenerationType: ""}},
		"DATE":        {types.AutoGen{Name: "", GenerationType: ""}},
		"FLOAT64":     {types.AutoGen{Name: "", GenerationType: ""}, types.AutoGen{Name: "Sequence1", GenerationType: "Sequence"}},
		"FLOAT8":      {types.AutoGen{Name: "", GenerationType: ""}, types.AutoGen{Name: "Sequence1", GenerationType: "Sequence"}},
		"INT64":       {types.AutoGen{Name: "", GenerationType: ""}, types.AutoGen{Name: "Sequence1", GenerationType: "Sequence"}},
		"INT8":        {types.AutoGen{Name: "", GenerationType: ""}, types.AutoGen{Name: "Sequence1", GenerationType: "Sequence"}},
		"JSONB":       {types.AutoGen{Name: "", GenerationType: ""}},
		"NUMERIC":     {types.AutoGen{Name: "", GenerationType: ""}},
		"TIMESTAMPTZ": {types.AutoGen{Name: "", GenerationType: ""}},
		"VARCHAR":     {types.AutoGen{Name: "", GenerationType: ""}, types.AutoGen{Name: "UUID", GenerationType: "Pre-defined"}}}

	expectedAutoGenMapMySql := map[string][]types.AutoGen{
		"BOOL":      {types.AutoGen{Name: "", GenerationType: ""}},
		"BYTES":     {types.AutoGen{Name: "", GenerationType: ""}},
		"DATE":      {types.AutoGen{Name: "", GenerationType: ""}},
		"FLOAT64":   {types.AutoGen{Name: "", GenerationType: ""}, types.AutoGen{Name: "Sequence1", GenerationType: "Sequence"}},
		"INT64":     {types.AutoGen{Name: "", GenerationType: ""}, types.AutoGen{Name: "Sequence1", GenerationType: "Sequence"}},
		"JSON":      {types.AutoGen{Name: "", GenerationType: ""}},
		"NUMERIC":   {types.AutoGen{Name: "", GenerationType: ""}},
		"STRING":    {types.AutoGen{Name: "", GenerationType: ""}, types.AutoGen{Name: "UUID", GenerationType: "Pre-defined"}},
		"TIMESTAMP": {types.AutoGen{Name: "", GenerationType: ""}}}
	tests := []struct {
		dialect            string
		expectedAutoGenMap map[string][]types.AutoGen
	}{
		{
			dialect:            constants.DIALECT_POSTGRESQL,
			expectedAutoGenMap: expectedAutoGenMapPostgres,
		},
		{
			dialect:            constants.DIALECT_GOOGLESQL,
			expectedAutoGenMap: expectedAutoGenMapMySql,
		},
	}
	for _, tc := range tests {
		var autoGenMap map[string][]types.AutoGen
		sessionState.Conv.SpDialect = tc.dialect
		req, err := http.NewRequest("GET", "/autoGenMap", nil)
		if err != nil {
			t.Fatal(err)
		}
		rr := httptest.NewRecorder()
		handler := http.HandlerFunc(api.GetAutoGenMap)
		handler.ServeHTTP(rr, req)
		json.Unmarshal(rr.Body.Bytes(), &autoGenMap)
		if status := rr.Code; status != http.StatusOK {
			t.Errorf("handler returned wrong status code: got %v want %v",
				status, http.StatusOK)
		}
		assert.Equal(t, tc.expectedAutoGenMap, autoGenMap, tc.dialect)
	}

}

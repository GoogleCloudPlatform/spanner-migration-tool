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
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/api"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/session"
	"github.com/stretchr/testify/assert"
)

func TestAddNewSequence(t *testing.T) {
	sessionState := session.GetSessionState()

	sessionState.Driver = constants.MYSQL
	sessionState.Conv = &internal.Conv{
		UsedNames: make(map[string]bool),
		SpSequences: make(map[string]ddl.Sequence),
	}

	seqInput := ddl.Sequence{
		Name:             "seq",
		SequenceKind:     "BIT REVERSED POSITIVE",
		SkipRangeMin:     "1",
		SkipRangeMax:     "2",
		StartWithCounter: "3",
	}
	inputBytes, err := json.Marshal(seqInput)
	if err != nil {
		t.Fatal(err)
	}
	buffer := bytes.NewBuffer(inputBytes)

	req, err := http.NewRequest("POST", "/AddSequence", buffer)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(api.AddNewSequence)
	handler.ServeHTTP(rr, req)
	var res *internal.Conv
	json.Unmarshal(rr.Body.Bytes(), &res)
	if status := rr.Code; int64(status) != http.StatusOK {
		t.Errorf("test create sequence : handler returned wrong status code: got %v want %v",
			status, http.StatusOK)
	}
	if rr.Code == http.StatusOK {
		assert.Equal(t, 1, len(res.SpSequences))
		for _, seq := range res.SpSequences {
			assert.Equal(t, seq.Name, seqInput.Name)
			assert.Equal(t, seq.SequenceKind, seqInput.SequenceKind)
			assert.Equal(t, seq.SkipRangeMax, seqInput.SkipRangeMax)
			assert.Equal(t, seq.SkipRangeMin, seqInput.SkipRangeMin)
			assert.Equal(t, seq.StartWithCounter, seqInput.StartWithCounter)
		}
	}
}

func TestUpdateSequence(t *testing.T) {
	columnsUsingSeq := map[string][]string{
		"t1": {"col1"},
	}
	sessionState := session.GetSessionState()
	sessionState.Driver = constants.MYSQL
	sessionState.Conv = &internal.Conv{
		SpSequences: map[string]ddl.Sequence{
			"s1": {
				Id:               "s1",
				Name:             "seq",
				SequenceKind:     "BIT REVERSED POSITIVE",
				SkipRangeMin:     "1",
				SkipRangeMax:     "2",
				StartWithCounter: "3",
				ColumnsUsingSeq:  columnsUsingSeq,
			},
		},
	}

	seqInput := ddl.Sequence{
		Id:               "s1",
		Name:             "seq",
		SequenceKind:     "BIT REVERSED POSITIVE",
		SkipRangeMin:     "5",
		SkipRangeMax:     "8",
		StartWithCounter: "9",
	}
	inputBytes, err := json.Marshal(seqInput)
	if err != nil {
		t.Fatal(err)
	}
	buffer := bytes.NewBuffer(inputBytes)

	req, err := http.NewRequest("POST", "/UpdateSequence", buffer)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(api.UpdateSequence)
	handler.ServeHTTP(rr, req)
	var res *internal.Conv
	json.Unmarshal(rr.Body.Bytes(), &res)
	if status := rr.Code; int64(status) != http.StatusOK {
		t.Errorf("test update sequence : handler returned wrong status code: got %v want %v",
			status, http.StatusOK)
	}
	if rr.Code == http.StatusOK {
		assert.Equal(t, 1, len(res.SpSequences))
		finalSeq := res.SpSequences["s1"]
		assert.Equal(t, finalSeq.Name, seqInput.Name)
		assert.Equal(t, finalSeq.SequenceKind, seqInput.SequenceKind)
		assert.Equal(t, finalSeq.SkipRangeMax, seqInput.SkipRangeMax)
		assert.Equal(t, finalSeq.SkipRangeMin, seqInput.SkipRangeMin)
		assert.Equal(t, finalSeq.StartWithCounter, seqInput.StartWithCounter)
		assert.Equal(t, finalSeq.ColumnsUsingSeq, columnsUsingSeq)
	}
}

func TestDropSequence(t *testing.T) {
	columnsUsingSeq := map[string][]string{
		"t1": {"c2"},
	}
	sessionState := session.GetSessionState()
	sessionState.Driver = constants.MYSQL
	sessionState.Conv = &internal.Conv{
		SpSchema: map[string]ddl.CreateTable{
			"t1": {Name: "table1",
				ColIds:  []string{"c1"},
				ColDefs: map[string]ddl.ColumnDef{"c2": {Name: "d", T: ddl.Type{Name: ddl.Int64}, NotNull: true, AutoGen: ddl.AutoGenCol{Name: "seq", GenerationType: constants.SEQUENCE}}},
			},
		},
		SpSequences: map[string]ddl.Sequence{
			"s1": {
				Id:               "s1",
				Name:             "seq",
				SequenceKind:     "BIT REVERSED POSITIVE",
				SkipRangeMin:     "1",
				SkipRangeMax:     "2",
				StartWithCounter: "3",
				ColumnsUsingSeq:  columnsUsingSeq,
			},
		},
	}

	payload := `{}`

	req, err := http.NewRequest("POST", "drop/sequence?sequence=s1", strings.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}

	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	handler := http.HandlerFunc(api.DropSequence)
	handler.ServeHTTP(rr, req)

	res := &internal.Conv{}

	json.Unmarshal(rr.Body.Bytes(), &res)
	if status := rr.Code; int64(status) != http.StatusOK {
		t.Errorf("test drop sequence : handler returned wrong status code: got %v want %v",
			status, http.StatusOK)
	}

	expectedConv := &internal.Conv{
		SpSchema: map[string]ddl.CreateTable{
			"t1": {Name: "table1",
				ColIds:  []string{"c1"},
				ColDefs: map[string]ddl.ColumnDef{"c2": {Name: "d", T: ddl.Type{Name: ddl.Int64}, NotNull: true, AutoGen: ddl.AutoGenCol{Name: "", GenerationType: ""}}},
			},
		},
		SpSequences: map[string]ddl.Sequence{},
	}

	if rr.Code == http.StatusOK {
		assert.Equal(t, 0, len(res.SpSequences))
		assert.Equal(t, res.SpSchema, expectedConv.SpSchema)
	}
}

func TestGetSequenceDDL(t *testing.T) {
	sessionState := session.GetSessionState()

	sessionState.Driver = constants.MYSQL
	sessionState.Conv = &internal.Conv{
		SpSequences: map[string]ddl.Sequence{
			"s1": {
				Id:               "s1",
				Name:             "seq",
				SequenceKind:     "BIT REVERSED POSITIVE",
				SkipRangeMin:     "1",
				SkipRangeMax:     "2",
				StartWithCounter: "3",
			},
		},
	}

	req, err := http.NewRequest("GET", "/seqDdl", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(api.GetSequenceDDL)
	handler.ServeHTTP(rr, req)
	var res map[string]string
	json.Unmarshal(rr.Body.Bytes(), &res)

	expectedSeqDDL := map[string]string{"s1": "CREATE SEQUENCE seq OPTIONS (  sequence_kind='bit_reversed_positive'  ,  skip_range_min = 1  ,  skip_range_max = 2  ,  start_with_counter = 3  ) "}

	if status := rr.Code; int64(status) != http.StatusOK {
		t.Errorf("Get Sequence DDL : handler returned wrong status code: got %v want %v",
			status, http.StatusOK)
	}
	if rr.Code == http.StatusOK {
		assert.Equal(t, expectedSeqDDL, res)
	}
}

func TestGetSequenceKind(t *testing.T) {
	sessionState := session.GetSessionState()

	sessionState.Driver = constants.MYSQL
	sessionState.Conv = &internal.Conv{
		SpSequences: map[string]ddl.Sequence{
			"s1": {
				Id:               "s1",
				Name:             "seq",
				SequenceKind:     "BIT REVERSED POSITIVE",
				SkipRangeMin:     "1",
				SkipRangeMax:     "2",
				StartWithCounter: "3",
			},
		},
	}

	req, err := http.NewRequest("GET", "/seqDdl", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(api.GetSequenceDDL)
	handler.ServeHTTP(rr, req)
	var res map[string]string
	json.Unmarshal(rr.Body.Bytes(), &res)

	expectedSeqDDL := map[string]string{"s1": "CREATE SEQUENCE seq OPTIONS (  sequence_kind='bit_reversed_positive'  ,  skip_range_min = 1  ,  skip_range_max = 2  ,  start_with_counter = 3  ) "}

	if status := rr.Code; int64(status) != http.StatusOK {
		t.Errorf("Get Sequence DDL : handler returned wrong status code: got %v want %v",
			status, http.StatusOK)
	}
	if rr.Code == http.StatusOK {
		assert.Equal(t, expectedSeqDDL, res)
	}
}

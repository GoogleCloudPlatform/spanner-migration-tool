package session_test

import (
	"encoding/json"
	"log"
	"os"
	"testing"
	"time"

	"github.com/cloudspannerecosystem/harbourbridge/common/constants"
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/webv2/session"
)

func getTestData() []session.SchemaConversionSession {

	conv := internal.Conv{
		SpDialect: constants.DIALECT_GOOGLESQL,
	}

	convStr, _ := json.Marshal(conv)

	return []session.SchemaConversionSession{
		{
			VersionId:              "v1",
			CreateTimestamp:        time.Now(),
			SchemaConversionObject: string(convStr),
			SessionMetadata: session.SessionMetadata{
				SessionName:  "session-1",
				DatabaseName: "BikeStore",
				DatabaseType: "mssql",
				EditorName:   "Go Test",
				Notes:        []string{"Initial"},
			},
		},
		{
			VersionId:              "v2",
			CreateTimestamp:        time.Now(),
			SchemaConversionObject: "",
			SessionMetadata: session.SessionMetadata{
				SessionName:  "session-2",
				DatabaseName: "BikeStore",
				DatabaseType: "mssql",
				EditorName:   "Go Test",
				Notes:        []string{"Column name updated", "Index added"},
			},
		},
		{
			VersionId:              "v3",
			CreateTimestamp:        time.Now(),
			SchemaConversionObject: "",
			SessionMetadata: session.SessionMetadata{
				SessionName:  "session-3",
				DatabaseName: "BikeStore",
				DatabaseType: "mssql",
				EditorName:   "Go Routine",
				Notes:        []string{"Data type updated"},
			},
		},
	}
}

func TestMain(m *testing.M) {
	log.Println("Initialize local store")
	store := session.NewLocalSessionStore()
	for _, d := range getTestData() {
		store.SaveSession(nil, d)
	}
	exitVal := m.Run()
	os.Exit(exitVal)
}

func TestNewLocalStore(t *testing.T) {
	st1 := session.NewLocalSessionStore()
	st2 := session.NewLocalSessionStore()

	r1, _ := st1.GetSessionsMetadata(nil)
	r2, _ := st2.GetSessionsMetadata(nil)

	expect := 3 // 3 items already present during init
	got := len(r1)
	if got != expect {
		t.Errorf("Expected: %d, got: %d", expect, got)
	}

	got = len(r2)
	if got != expect {
		t.Errorf("Expected: %d, got: %d", expect, got)
	}
}

func TestGetSessionsMetadata(t *testing.T) {
	st := session.NewLocalSessionStore()
	r, _ := st.GetSessionsMetadata(nil)

	expect := 3 // 3 items already present during init
	got := len(r)
	if got != expect {
		t.Errorf("Expected: %d, got: %d", expect, got)
	}
}

func TestGetConvWithMetadata(t *testing.T) {
	st := session.NewLocalSessionStore()

	expected := getTestData()[0]
	got, err := st.GetConvWithMetadata(nil, expected.VersionId)

	if err != nil {
		t.Errorf("Expected: No Errors, got: %s", err)
	}

	if expected.SessionName != got.SessionName {
		t.Errorf("Expected: %s, got: %s", expected.SessionName, got.SessionName)
	}

	if expected.DatabaseName != got.DatabaseName {
		t.Errorf("Expected: %s, got: %s", expected.DatabaseName, got.DatabaseName)
	}

	if expected.EditorName != got.EditorName {
		t.Errorf("Expected: %s, got: %s", expected.EditorName, got.EditorName)
	}

	conv, _ := json.Marshal(got.Conv)
	convStr := string(conv)
	if expected.SchemaConversionObject != convStr {
		t.Errorf("Expected: %s, got: %s", expected.SchemaConversionObject, convStr)
	}
}

func TestSaveSession(t *testing.T) {
	s1 := session.SchemaConversionSession{
		VersionId:              "v1",
		CreateTimestamp:        time.Now(),
		SchemaConversionObject: "",
		SessionMetadata: session.SessionMetadata{
			SessionName:  "session-1",
			DatabaseName: "BikeStore",
			DatabaseType: "mssql",
			EditorName:   "Go Test",
			Notes:        []string{"Initial"},
		},
	}

	s2 := session.SchemaConversionSession{
		VersionId:              "v1",
		CreateTimestamp:        time.Now(),
		SchemaConversionObject: "",
		SessionMetadata: session.SessionMetadata{
			SessionName:  "session-1",
			DatabaseName: "BikeStore",
			DatabaseType: "mssql",
			EditorName:   "Go Test",
			Notes:        []string{"Initial"},
		},
	}

	st := session.NewLocalSessionStore()
	st.SaveSession(nil, s1)
	st.SaveSession(nil, s2)

	r, _ := st.GetSessionsMetadata(nil)

	got := len(r)
	expect := 5 // 3 items already present during init

	if got != expect {
		t.Errorf("Expected: %d, got: %d", expect, got)
	}
}

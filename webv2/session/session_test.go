package session_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/session"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestIsSessionNameUnique(t *testing.T) {
	store := session.NewLocalSessionStore()

	sessions := []session.SchemaConversionSession{
		{
			VersionId:              "1234",
			SchemaConversionObject: "test_object",
			SessionMetadata: session.SessionMetadata{
				SessionName:  "Test Session 1",
				EditorName:   "Test Editor",
				DatabaseType: "MySQL",
				DatabaseName: "test_db",
				Dialect:      "mysql",
			},
		},
		{
			VersionId:              "5678",
			SchemaConversionObject: "test_object",
			SessionMetadata: session.SessionMetadata{
				SessionName:  "Test Session 1",
				EditorName:   "Test Editor",
				DatabaseType: "MySQL",
				DatabaseName: "test_db",
				Dialect:      "mysql",
			},
		},
		{
			VersionId:              "9101",
			SchemaConversionObject: "test_object",
			SessionMetadata: session.SessionMetadata{
				SessionName:  "Test Session 2",
				EditorName:   "Test Editor",
				DatabaseType: "MySQL",
				DatabaseName: "test_db",
				Dialect:      "mysql",
			},
		},
	}

	for i := range sessions {
		err := store.SaveSession(context.Background(), sessions[i])
		assert.Nil(t, err)
	}

	testCases := []struct {
		Name        string
		Session     session.SchemaConversionSession
		Expect      bool
		ExpectError error
	}{
		{
			Name: "NotUnique",
			Session: session.SchemaConversionSession{
				VersionId:              "9999",
				SchemaConversionObject: "test_object",
				SessionMetadata: session.SessionMetadata{
					SessionName:  "Test Session 1",
					EditorName:   "Test Editor",
					DatabaseType: "MySQL",
					DatabaseName: "test_db",
					Dialect:      "mysql",
				},
			},
			Expect:      false,
			ExpectError: nil,
		},
		{
			Name: "Unique",
			Session: session.SchemaConversionSession{
				VersionId:              "8888",
				SchemaConversionObject: "test_object",
				SessionMetadata: session.SessionMetadata{
					SessionName:  "Test Session 3",
					EditorName:   "Test Editor",
					DatabaseType: "MySQL",
					DatabaseName: "test_db",
					Dialect:      "mysql",
				},
			},
			Expect:      true,
			ExpectError: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			actual, err := store.IsSessionNameUnique(context.Background(), tc.Session)
			assert.Equal(t, tc.Expect, actual)
			assert.Equal(t, tc.ExpectError, err)
		})
	}
}

func TestSessionNameError(t *testing.T) {
	err := &session.SessionNameError{DbName: "my_db", DbType: "PostgreSQL"}
	expected := "session name already exists for database 'my_db' and database type 'PostgreSQL'."
	if err.Error() != expected {
		t.Errorf("Expected error message '%s', but got '%s'", expected, err.Error())
	}
}

func TestGetSessionState(t *testing.T) {
	// Call GetSessionState twice and ensure that it returns the same non-nil SessionState instance
	state1 := session.GetSessionState()
	state2 := session.GetSessionState()
	if state1 == nil || state2 == nil {
		t.Errorf("Expected GetSessionState to return a non-nil SessionState instance, but got nil")
	}
	if state1 != state2 {
		t.Errorf("Expected GetSessionState to return the same SessionState instance, but got different instances")
	}
}
func TestReadSessionFileForSessionMetadata(t *testing.T) {
	expectedMetadata := &session.SessionMetadata{
		DatabaseName: "mydb",
		DatabaseType: "postgres",
	}

	// Create a temporary file and write the JSON-encoded metadata to it
	file, err := ioutil.TempFile("", "metadata_*.json")
	require.NoError(t, err)

	err = json.NewEncoder(file).Encode(expectedMetadata)
	require.NoError(t, err)

	// Read the metadata from the temporary file
	actualMetadata := &session.SessionMetadata{}
	err = session.ReadSessionFileForSessionMetadata(actualMetadata, file.Name())
	require.NoError(t, err)

	assert.Equal(t, expectedMetadata, actualMetadata)
}

func TestIsOfflineSession(t *testing.T) {

	session.GetSessionState().IsOffline = true

	// Create a new request to the endpoint
	req, err := http.NewRequest("GET", "/isoffline", nil)
	if err != nil {
		t.Fatal(err)
	}

	// Create a response recorder to record the response from the handler
	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(session.IsOfflineSession)

	// Call the handler and record the response
	handler.ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusOK)
	}

	// Check that the response body is what we expect
	expected := true
	if rr.Body.String() != fmt.Sprintf("%t\n", expected) {
		t.Errorf("handler returned unexpected body: got %v want %v",
			rr.Body.String(), expected)
	}
}

func TestSetSessionStorageConnectionState(t *testing.T) {

	dbCreated, configValid := session.SetSessionStorageConnectionState("", "", "")
	if dbCreated != false {
		t.Errorf("Expected dbCreated to be false, but got %v", dbCreated)
	}
	if configValid != false {
		t.Errorf("Expected configValid to be false, but got %v", configValid)
	}
	if session.GetSessionState().IsOffline != true {
		t.Error("Expected IsOffline to be true, but got false")
	}

	dbCreated, configValid = session.SetSessionStorageConnectionState("my-project-id", "", "")
	if dbCreated != false {
		t.Errorf("Expected dbCreated to be false, but got %v", dbCreated)
	}
	if configValid != false {
		t.Errorf("Expected configValid to be false, but got %v", configValid)
	}
	if session.GetSessionState().IsOffline != true {
		t.Error("Expected IsOffline to be true, but got false")
	}

	dbCreated, configValid = session.SetSessionStorageConnectionState("", "", "my-instance-id")
	if dbCreated != false {
		t.Errorf("Expected dbCreated to be false, but got %v", dbCreated)
	}
	if configValid != false {
		t.Errorf("Expected configValid to be false, but got %v", configValid)
	}
	if session.GetSessionState().IsOffline != true {
		t.Error("Expected IsOffline to be true, but got false")
	}
}

func TestSessionSave(t *testing.T) {

	ctx := context.Background()
	store := session.NewLocalSessionStore()
	ss := session.NewSessionService(ctx, store)

	scs1 := session.SchemaConversionSession{
		SessionMetadata: session.SessionMetadata{
			DatabaseName: "my-db-1",
			DatabaseType: "mysql",
		},
	}
	err := ss.SaveSession(scs1)
	if err != nil {
		t.Errorf("Expected no error, but got %v", err)
	}

}

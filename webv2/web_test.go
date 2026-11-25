package webv2

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	ca "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/cassandra"
	cc "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/clients/cassandra"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	helpers "github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/helpers"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/session"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)


func TestValidateCassandraConnection(t *testing.T) {
	originalGetOrCreateClient := ca.GetOrCreateClient
	defer func() {
		ca.GetOrCreateClient = originalGetOrCreateClient
	}()

	config := types.DriverConfig{
		Host: "127.0.0.1", Port: "9042", Database: "test_keyspace", DataCenter: "dc1", User: "user", Password: "pass",
	}

	t.Run("Success", func(t *testing.T) {
		mockMetadata := new(cc.MockKeyspaceMetadata)
		mockClient := new(cc.MockCassandraCluster)
		mockClient.On("KeyspaceMetadata", "test_keyspace").Return(mockMetadata, nil).Once()
		mockClient.On("Close").Return().Once()

		ca.GetOrCreateClient = func(contactPoints []string, port int, keyspace, datacenter, user, password string) (cc.CassandraClusterInterface, error) {
			return mockClient, nil
		}

		metadata, err := validateCassandraConnection(config)
		assert.NoError(t, err)
		assert.Equal(t, mockMetadata, metadata)
		mockClient.AssertExpectations(t)
	})

	t.Run("Failure", func(t *testing.T) {
		expectedErr := errors.New("client creation failed")
		ca.GetOrCreateClient = func(contactPoints []string, port int, keyspace, datacenter, user, password string) (cc.CassandraClusterInterface, error) {
			return nil, expectedErr
		}

		metadata, err := validateCassandraConnection(config)
		assert.Nil(t, metadata)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Cassandra connection error")
		assert.ErrorIs(t, err, expectedErr)
	})
}

func TestDatabaseConnectionCassandra(t *testing.T) {
	originalGetOrCreateClient := ca.GetOrCreateClient
	defer func() {
		ca.GetOrCreateClient = originalGetOrCreateClient
	}()

	config := types.DriverConfig{
		Driver:     constants.CASSANDRA,
		Host:       "127.0.0.1",
		Port:       "9042",
		Database:   "test_keyspace",
		DataCenter: "dc1",
		User:       "user",
		Password:   "pass",
	}
	configJSON, _ := json.Marshal(config)

	t.Run("Success", func(t *testing.T) {
		mockMetadata := new(cc.MockKeyspaceMetadata)
		mockClient := new(cc.MockCassandraCluster)
		mockClient.On("KeyspaceMetadata", "test_keyspace").Return(mockMetadata, nil).Once()
		mockClient.On("Close").Return().Once()

		ca.GetOrCreateClient = func(contactPoints []string, port int, keyspace, datacenter, user, password string) (cc.CassandraClusterInterface, error) {
			return mockClient, nil
		}

		req, _ := http.NewRequest("POST", "/connect", bytes.NewBuffer(configJSON))
		rr := httptest.NewRecorder()
		handler := http.HandlerFunc(databaseConnection)
		handler.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusOK, rr.Code)

		sessionState := session.GetSessionState()
		assert.Equal(t, constants.CASSANDRA, sessionState.Driver)
		assert.Equal(t, "test_keyspace", sessionState.DbName)
		assert.Equal(t, mockMetadata, sessionState.KeyspaceMetadata)
		mockClient.AssertExpectations(t)
	})

	t.Run("Failure", func(t *testing.T) {
		expectedErr := errors.New("client creation failed")
		ca.GetOrCreateClient = func(contactPoints []string, port int, keyspace, datacenter, user, password string) (cc.CassandraClusterInterface, error) {
			return nil, expectedErr
		}

		req, _ := http.NewRequest("POST", "/connect", bytes.NewBuffer(configJSON))
		rr := httptest.NewRecorder()
		handler := http.HandlerFunc(databaseConnection)
		handler.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusInternalServerError, rr.Code)
		assert.Contains(t, rr.Body.String(), "Cassandra connection error")
	})
}

func TestSetSourceDBDetailsForDirectConnectCassandra(t *testing.T) {
	originalGetOrCreateClient := ca.GetOrCreateClient
	defer func() {
		ca.GetOrCreateClient = originalGetOrCreateClient
	}()

	config := types.DriverConfig{
		Driver:     constants.CASSANDRA,
		Host:       "127.0.0.1",
		Port:       "9042",
		Database:   "test_keyspace",
		DataCenter: "dc1",
		User:       "user",
		Password:   "pass",
	}
	configJSON, _ := json.Marshal(config)

	t.Run("Success", func(t *testing.T) {
		mockMetadata := new(cc.MockKeyspaceMetadata)
		mockClient := new(cc.MockCassandraCluster)
		mockClient.On("KeyspaceMetadata", "test_keyspace").Return(mockMetadata, nil).Once()
		mockClient.On("Close").Return().Once()

		ca.GetOrCreateClient = func(contactPoints []string, port int, keyspace, datacenter, user, password string) (cc.CassandraClusterInterface, error) {
			return mockClient, nil
		}

		req, _ := http.NewRequest("POST", "/SetSourceDBDetailsForDirectConnect", bytes.NewBuffer(configJSON))
		rr := httptest.NewRecorder()
		handler := http.HandlerFunc(setSourceDBDetailsForDirectConnect)
		handler.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusOK, rr.Code)

		sessionState := session.GetSessionState()
		assert.Equal(t, "test_keyspace", sessionState.DbName)
		assert.Equal(t, mockMetadata, sessionState.KeyspaceMetadata)
		assert.Equal(t, "127.0.0.1", sessionState.SourceDBConnDetails.Host)
		mockClient.AssertExpectations(t)
	})

	t.Run("Failure", func(t *testing.T) {
		expectedErr := errors.New("client creation failed")
		ca.GetOrCreateClient = func(contactPoints []string, port int, keyspace, datacenter, user, password string) (cc.CassandraClusterInterface, error) {
			return nil, expectedErr
		}

		req, _ := http.NewRequest("POST", "/SetSourceDBDetailsForDirectConnect", bytes.NewBuffer(configJSON))
		rr := httptest.NewRecorder()
		handler := http.HandlerFunc(setSourceDBDetailsForDirectConnect)
		handler.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusInternalServerError, rr.Code)
		assert.Contains(t, rr.Body.String(), "Cassandra connection error")
	})
}

func TestGetSourceAndTargetProfiles_Cassandra(t *testing.T) {
	sessionState := session.GetSessionState()
	sessionState.Conv = internal.MakeConv()
	sessionState.Driver = constants.CASSANDRA
	sessionState.DbName = "test_keyspace"
	sessionState.Dialect = constants.DIALECT_GOOGLESQL
	sessionState.SourceDBConnDetails = session.SourceDBConnDetails{
		Host:           "127.0.0.1",
		Port:           "9042",
		User:           "user",
		Password:       "pass",
		DataCenter:     "dc1",
		ConnectionType: helpers.DIRECT_CONNECT_MODE,
	}
	sessionState.SpannerProjectId = "test-project"
	sessionState.SpannerInstanceID = "test-instance"

	details := types.MigrationDetails{
		TargetDetails: types.TargetDetails{
			TargetDB: "test-spanner-db",
		},
		MigrationMode: helpers.SCHEMA_ONLY,
	}

	sourceProfile, _, _, _, err := getSourceAndTargetProfiles(context.Background(), sessionState, details)

	assert.NoError(t, err)
	assert.Equal(t, constants.CASSANDRA, sourceProfile.Driver)
	assert.Equal(t, "127.0.0.1", sourceProfile.Conn.Cassandra.Host)
	assert.Equal(t, "9042", sourceProfile.Conn.Cassandra.Port)
	assert.Equal(t, "user", sourceProfile.Conn.Cassandra.User)
	assert.Equal(t, "pass", sourceProfile.Conn.Cassandra.Pwd)
	assert.Equal(t, "test_keyspace", sourceProfile.Conn.Cassandra.Keyspace)
	assert.Equal(t, "dc1", sourceProfile.Conn.Cassandra.DataCenter)
}

func TestCreateDatabaseConnectionString(t *testing.T) {
	testCases := []struct {
		name           string
		config         types.DriverConfig
		expectedString string
		expectError    bool
	}{
		{
			name: "Postgres driver",
			config: types.DriverConfig{
				Driver:   constants.POSTGRES,
				Host:     "localhost",
				Port:     "5432",
				User:     "user",
				Password: "passw\\`~ord",
				Database: "testdb",
			},
			expectedString: "postgres://user:passw%5C%60~ord@localhost:5432/testdb?sslmode=disable",
			expectError:    false,
		},
		{
			name: "MySQL driver",
			config: types.DriverConfig{
				Driver:   constants.MYSQL,
				Host:     "localhost",
				Port:     "3306",
				User:     "user",
				Password: "passw\\`~ord",
				Database: "testdb",
			},
			expectedString: "user:passw\\`~ord@tcp(localhost:3306)/testdb",
			expectError:    false,
		},
		{
			name: "SQL Server driver",
			config: types.DriverConfig{
				Driver:   constants.SQLSERVER,
				Host:     "localhost",
				Port:     "1433",
				User:     "user",
				Password: "passw\\`~ord",
				Database: "testdb",
			},
			expectedString: "sqlserver://user:passw%5C%60~ord@localhost:1433?database=testdb",
			expectError:    false,
		},
		{
			name: "Oracle driver",
			config: types.DriverConfig{
				Driver:   constants.ORACLE,
				Host:     "localhost",
				Port:     "1521",
				User:     "user",
				Password: "passw\\`~ord",
				Database: "testdb",
			},
			expectedString: "oracle://user:passw%5C%60~ord@localhost:1521/testdb",
			expectError:    false,
		},
		{
			name: "Unsupported driver",
			config: types.DriverConfig{
				Driver:   "unsupported",
				Host:     "localhost",
				Port:     "1234",
				User:     "user",
				Password: "password",
				Database: "testdb",
			},
			expectedString: "",
			expectError:    true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actualString, err := createDatabaseConnectionString(tc.config)
			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedString, actualString)
			}
		})
	}
}

func TestSetSourceDBDetailsForDump_PathTraversal(t *testing.T) {
	origState := *session.GetSessionState()
	t.Cleanup(func() {
		*session.GetSessionState() = origState
	})

	tmpDir := t.TempDir()
	t.Chdir(tmpDir)

	uploadDir := filepath.Join(tmpDir, constants.UPLOAD_FILE_DIR)
	require.NoError(t, os.MkdirAll(uploadDir, os.ModePerm))
	outsideFile := filepath.Join(tmpDir, "secret.txt")
	require.NoError(t, os.WriteFile(outsideFile, []byte("SECRET_TOKEN"), 0600))
	require.NoError(t, os.WriteFile(filepath.Join(uploadDir, "dump.sql"), []byte("CREATE TABLE t (id INT);"), 0600))
	require.NoError(t, os.Symlink(outsideFile, filepath.Join(uploadDir, "evil_link.sql")))

	traversalPaths := []string{
		"../secret.txt",
		"evil_link.sql",
	}
	for _, p := range traversalPaths {
		t.Run("rejects_"+p, func(t *testing.T) {
			sessionState := session.GetSessionState()
			sessionState.SourceDBConnDetails = session.SourceDBConnDetails{}

			body, err := json.Marshal(types.DumpConfig{Driver: constants.MYSQLDUMP, FilePath: p})
			require.NoError(t, err)

			req := httptest.NewRequest(http.MethodPost, "/SetSourceDBDetailsForDump", bytes.NewReader(body))
			rr := httptest.NewRecorder()
			setSourceDBDetailsForDump(rr, req)

			assert.Equal(t, http.StatusBadRequest, rr.Code)
			assert.Empty(t, sessionState.SourceDBConnDetails.Path)
		})
	}

	t.Run("missing_file_returns_404", func(t *testing.T) {
		body, err := json.Marshal(types.DumpConfig{Driver: constants.MYSQLDUMP, FilePath: "does-not-exist.sql"})
		require.NoError(t, err)

		req := httptest.NewRequest(http.MethodPost, "/SetSourceDBDetailsForDump", bytes.NewReader(body))
		rr := httptest.NewRecorder()
		setSourceDBDetailsForDump(rr, req)

		assert.Equal(t, http.StatusNotFound, rr.Code)
	})

	t.Run("valid_file_returns_200", func(t *testing.T) {
		sessionState := session.GetSessionState()
		sessionState.SourceDBConnDetails = session.SourceDBConnDetails{}

		body, err := json.Marshal(types.DumpConfig{Driver: constants.MYSQLDUMP, FilePath: "dump.sql"})
		require.NoError(t, err)

		req := httptest.NewRequest(http.MethodPost, "/SetSourceDBDetailsForDump", bytes.NewReader(body))
		rr := httptest.NewRecorder()
		setSourceDBDetailsForDump(rr, req)

		assert.Equal(t, http.StatusOK, rr.Code)
		assert.Equal(t, filepath.Join(constants.UPLOAD_FILE_DIR, "dump.sql"), sessionState.SourceDBConnDetails.Path)
	})
}

func TestLoadSession_PathTraversal(t *testing.T) {
	origState := *session.GetSessionState()
	t.Cleanup(func() {
		*session.GetSessionState() = origState
	})

	tmpDir := t.TempDir()
	t.Chdir(tmpDir)

	uploadDir := filepath.Join(tmpDir, constants.UPLOAD_FILE_DIR)
	require.NoError(t, os.MkdirAll(uploadDir, os.ModePerm))
	outsideFile := filepath.Join(tmpDir, "secret_token.txt")
	require.NoError(t, os.WriteFile(outsideFile, []byte("A_SECRET_KEY"), 0600))
	require.NoError(t, os.Symlink(outsideFile, filepath.Join(uploadDir, "evil_session.json")))

	validSessionJSON := `{"DatabaseType":"mysql","DatabaseName":"testdb","SpDialect":"google_standard_sql"}`
	require.NoError(t, os.WriteFile(filepath.Join(uploadDir, "valid_session.json"), []byte(validSessionJSON), 0600))
	require.NoError(t, os.WriteFile(filepath.Join(uploadDir, "malformed.json"), []byte("not-json"), 0600))

	traversalPaths := []string{
		"../secret_token.txt",
		"evil_session.json",
	}
	for _, p := range traversalPaths {
		t.Run("rejects_"+p, func(t *testing.T) {
			body, err := json.Marshal(session.SessionParams{Driver: constants.MYSQL, FilePath: p})
			require.NoError(t, err)

			req := httptest.NewRequest(http.MethodPost, "/convert/session", bytes.NewReader(body))
			rr := httptest.NewRecorder()
			loadSession(rr, req)

			assert.Equal(t, http.StatusBadRequest, rr.Code)
			assert.Contains(t, rr.Body.String(), "Invalid session file path")
			assert.NotContains(t, rr.Body.String(), "invalid character")
		})
	}

	t.Run("missing_file_returns_404", func(t *testing.T) {
		body, err := json.Marshal(session.SessionParams{Driver: constants.MYSQL, FilePath: "nope.json"})
		require.NoError(t, err)

		req := httptest.NewRequest(http.MethodPost, "/convert/session", bytes.NewReader(body))
		rr := httptest.NewRecorder()
		loadSession(rr, req)

		assert.Equal(t, http.StatusNotFound, rr.Code)
	})

	t.Run("malformed_json_inside_upload_dir_returns_400", func(t *testing.T) {
		body, err := json.Marshal(session.SessionParams{Driver: constants.MYSQL, FilePath: "malformed.json"})
		require.NoError(t, err)

		req := httptest.NewRequest(http.MethodPost, "/convert/session", bytes.NewReader(body))
		rr := httptest.NewRecorder()
		loadSession(rr, req)

		assert.Equal(t, http.StatusBadRequest, rr.Code)
		assert.Contains(t, rr.Body.String(), "Failed to parse session file")
	})

	t.Run("valid_session_returns_200", func(t *testing.T) {
		body, err := json.Marshal(session.SessionParams{Driver: constants.MYSQL, FilePath: "valid_session.json"})
		require.NoError(t, err)

		req := httptest.NewRequest(http.MethodPost, "/convert/session", bytes.NewReader(body))
		rr := httptest.NewRecorder()
		loadSession(rr, req)

		assert.Equal(t, http.StatusOK, rr.Code)
		sessionState := session.GetSessionState()
		expectedPath := filepath.Join(constants.UPLOAD_FILE_DIR, "valid_session.json")
		assert.Equal(t, expectedPath, sessionState.SessionFile)
		assert.Equal(t, expectedPath, sessionState.SourceDBConnDetails.Path)
	})
}

func TestUploadFile_ConfinedToUploadDir(t *testing.T) {
	makeUploadRequest := func(t *testing.T, filename, content string) *http.Request {
		t.Helper()
		var buf bytes.Buffer
		writer := multipart.NewWriter(&buf)
		part, err := writer.CreateFormFile("myFile", filename)
		require.NoError(t, err)
		_, err = part.Write([]byte(content))
		require.NoError(t, err)
		require.NoError(t, writer.Close())

		req := httptest.NewRequest(http.MethodPost, "/uploadFile", &buf)
		req.Header.Set("Content-Type", writer.FormDataContentType())
		return req
	}

	t.Run("valid_upload_overwrites_stale_symlink_or_dir", func(t *testing.T) {
		tmpDir := t.TempDir()
		t.Chdir(tmpDir)

		uploadDir := filepath.Join(tmpDir, constants.UPLOAD_FILE_DIR)
		require.NoError(t, os.MkdirAll(filepath.Join(uploadDir, "sample.sql"), os.ModePerm))

		rr := httptest.NewRecorder()
		uploadFile(rr, makeUploadRequest(t, "sample.sql", "CREATE TABLE t (id INT);"))

		assert.Equal(t, http.StatusOK, rr.Code)
		content, err := os.ReadFile(filepath.Join(uploadDir, "sample.sql"))
		require.NoError(t, err)
		assert.Equal(t, "CREATE TABLE t (id INT);", string(content))
	})

	invalidFilenames := []string{"..", ".", `..\evil.sql`}
	for _, badName := range invalidFilenames {
		t.Run("rejects_"+badName+"_without_wiping_upload_dir", func(t *testing.T) {
			tmpDir := t.TempDir()
			t.Chdir(tmpDir)

			uploadDir := filepath.Join(tmpDir, constants.UPLOAD_FILE_DIR)
			require.NoError(t, os.MkdirAll(uploadDir, os.ModePerm))
			existingFile := filepath.Join(uploadDir, "existing.sql")
			require.NoError(t, os.WriteFile(existingFile, []byte("KEEP_ME"), 0600))

			rr := httptest.NewRecorder()
			uploadFile(rr, makeUploadRequest(t, badName, "MALICIOUS"))

			assert.Equal(t, http.StatusBadRequest, rr.Code)
			assert.Contains(t, rr.Body.String(), "invalid file name")
			content, err := os.ReadFile(existingFile)
			require.NoError(t, err)
			assert.Equal(t, "KEEP_ME", string(content))
		})
	}
}

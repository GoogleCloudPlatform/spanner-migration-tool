// Copyright 2026 Google LLC
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

package api_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/api"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/session"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConvertSchemaDump_PathTraversal(t *testing.T) {
	sessionState := session.GetSessionState()
	origState := *sessionState
	t.Cleanup(func() {
		*sessionState = origState
	})

	tmpDir := t.TempDir()
	t.Chdir(tmpDir)

	uploadDir := filepath.Join(tmpDir, constants.UPLOAD_FILE_DIR)
	require.NoError(t, os.MkdirAll(uploadDir, os.ModePerm))

	outsideFile := filepath.Join(tmpDir, "secret.sql")
	require.NoError(t, os.WriteFile(outsideFile, []byte("CREATE TABLE leaked (id INT PRIMARY KEY);"), 0600))
	require.NoError(t, os.Symlink(outsideFile, filepath.Join(uploadDir, "evil_link.sql")))
	require.NoError(t, os.WriteFile(
		filepath.Join(uploadDir, "valid.sql"),
		[]byte("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100));"),
		0600,
	))

	handler := &api.ExpressionsVerificationHandler{}

	traversalPaths := []string{
		"../secret.sql",
		"evil_link.sql",
	}
	for _, p := range traversalPaths {
		t.Run("rejects_"+p, func(t *testing.T) {
			sessionState := session.GetSessionState()
			sessionState.SourceDBConnDetails = session.SourceDBConnDetails{}

			reqBody := types.ConvertFromDumpRequest{
				Config: types.DumpConfig{
					Driver:   constants.MYSQLDUMP,
					FilePath: p,
				},
				SpannerDetails: types.SpannerDetails{
					Dialect: constants.DIALECT_GOOGLESQL,
				},
			}
			body, err := json.Marshal(reqBody)
			require.NoError(t, err)

			req := httptest.NewRequest(http.MethodPost, "/convert/dump", bytes.NewReader(body))
			rr := httptest.NewRecorder()
			handler.ConvertSchemaDump(rr, req)

			assert.Equal(t, http.StatusBadRequest, rr.Code)
			assert.Contains(t, rr.Body.String(), "Invalid dump file path")
			assert.Empty(t, sessionState.SourceDBConnDetails.Path)
		})
	}

	t.Run("missing_file_returns_404", func(t *testing.T) {
		reqBody := types.ConvertFromDumpRequest{
			Config: types.DumpConfig{
				Driver:   constants.MYSQLDUMP,
				FilePath: "missing.sql",
			},
			SpannerDetails: types.SpannerDetails{
				Dialect: constants.DIALECT_GOOGLESQL,
			},
		}
		body, err := json.Marshal(reqBody)
		require.NoError(t, err)

		req := httptest.NewRequest(http.MethodPost, "/convert/dump", bytes.NewReader(body))
		rr := httptest.NewRecorder()
		handler.ConvertSchemaDump(rr, req)

		assert.Equal(t, http.StatusNotFound, rr.Code)
	})

	t.Run("valid_dump_returns_200", func(t *testing.T) {
		sessionState := session.GetSessionState()
		sessionState.SourceDBConnDetails = session.SourceDBConnDetails{}

		reqBody := types.ConvertFromDumpRequest{
			Config: types.DumpConfig{
				Driver:   constants.MYSQLDUMP,
				FilePath: "valid.sql",
			},
			SpannerDetails: types.SpannerDetails{
				Dialect: constants.DIALECT_GOOGLESQL,
			},
		}
		body, err := json.Marshal(reqBody)
		require.NoError(t, err)

		req := httptest.NewRequest(http.MethodPost, "/convert/dump", bytes.NewReader(body))
		rr := httptest.NewRecorder()
		handler.ConvertSchemaDump(rr, req)

		assert.Equal(t, http.StatusOK, rr.Code)
		assert.Equal(t, filepath.Join(constants.UPLOAD_FILE_DIR, "valid.sql"), sessionState.SourceDBConnDetails.Path)
	})
}

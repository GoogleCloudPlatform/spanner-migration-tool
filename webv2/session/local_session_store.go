// Copyright 2022 Google LLC

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//      http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package session

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/cloudspannerecosystem/harbourbridge/common/utils"
	"github.com/cloudspannerecosystem/harbourbridge/conversion"
)

const hbOutputDirPath string = "harbour_bridge_output"

type localStore struct {
	sessions []SchemaConversionSession
}

var _ SessionStore = (*localStore)(nil)

var store *localStore

func NewLocalSessionStore() *localStore {
	if store == nil {
		store = &localStore{}
	}
	return store
}

func (st *localStore) GetSessionsMetadata(ctx context.Context) ([]SchemaConversionSession, error) {
	return st.sessions, nil
}

func (st *localStore) GetConvWithMetadata(ctx context.Context, versionId string) (ConvWithMetadata, error) {
	var convm ConvWithMetadata
	var match *SchemaConversionSession
	for _, s := range st.sessions {
		if s.VersionId == versionId {
			match = &s
			break
		}
	}

	if match == nil {
		return convm, fmt.Errorf("No session found in local")
	}

	convm.SessionMetadata = SessionMetadata{
		SessionName:  match.SessionName,
		EditorName:   match.EditorName,
		DatabaseType: match.DatabaseType,
		DatabaseName: match.DatabaseName,
	}

	err := json.Unmarshal([]byte(match.SchemaConversionObject), &convm.Conv)

	if err != nil {
		return convm, fmt.Errorf("Error during JSON unmarshalling : %v", err)
	}

	return convm, nil
}

func (st *localStore) SaveSession(ctx context.Context, scs SchemaConversionSession) error {
	st.sessions = append(st.sessions, scs)
	return nil
}

func (st *localStore) IsSessionNameUnique(ctx context.Context, scs SchemaConversionSession) (bool, error) {
	for _, s := range st.sessions {
		if s.SessionName == scs.SessionName && s.DatabaseType == scs.DatabaseType && s.DatabaseName == scs.DatabaseName {
			return false, nil
		}
	}
	return true, nil
}

func getSessionFilePath(dbName string) string {
	return fmt.Sprintf("%s/%s/%s.session.json", hbOutputDirPath, dbName, dbName)
}

// UpdateSessionFile updates the content of session file with
// latest sessionState.Conv while also dumping schemas and report.
func updateSessionFile() error {
	sessionState := GetSessionState()

	ioHelper := &utils.IOStreams{In: os.Stdin, Out: os.Stdout}
	_, err := conversion.WriteConvGeneratedFiles(sessionState.Conv, sessionState.DbName, sessionState.Driver, ioHelper.BytesRead, ioHelper.Out)
	if err != nil {
		return fmt.Errorf("encountered error %w. Cannot write files", err)
	}
	return nil
}

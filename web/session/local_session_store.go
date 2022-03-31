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
	"fmt"

	"github.com/cloudspannerecosystem/harbourbridge/conversion"
)

type localStore struct {
	sessions []SchemaConversionSession
}

var _ SessionStore = (*localStore)(nil)

var store *localStore

func NewLocalSessionStore() *localStore {
	if store == nil {
		// once.Do(
		// 	func() {
		// 		store = &localStore{}
		// 	})
		store = &localStore{}
	}
	return store
}

func (svc *localStore) GetSessionsMetadata(ctx context.Context) ([]SchemaConversionSession, error) {
	return svc.sessions, nil
}

func (svc *localStore) GetConvWithMetadata(ctx context.Context, versionId string) (ConvWithMetadata, error) {
	var convm ConvWithMetadata
	var match *SchemaConversionSession
	for _, s := range svc.sessions {
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

	err := conversion.ReadSessionFile(&convm.Conv, getSessionFilePath(match.DatabaseName))
	if err != nil {
		return convm, fmt.Errorf("Failed to open the session file : %v", err)
	}

	return convm, nil
}

func (svc *localStore) SaveSession(ctx context.Context, scs SchemaConversionSession) error {
	svc.sessions = append(svc.sessions, scs)
	return nil
}

func getSessionFilePath(dbName string) string {
	dirPath := "harbour_bridge_output"
	return fmt.Sprintf("%s/%s/%s.session.json", dirPath, dbName, dbName)
}

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
	for _, s := range svc.sessions {
		if s.VersionId == versionId {
			convm.SessionMetadata = SessionMetadata{
				SessionName:  s.SessionName,
				EditorName:   s.EditorName,
				DatabaseType: s.DatabaseType,
				DatabaseName: s.DatabaseName,
			}
			break
		}
	}
	return convm, nil
}

func (svc *localStore) SaveSession(ctx context.Context, scs SchemaConversionSession) error {
	svc.sessions = append(svc.sessions, scs)
	return nil
}

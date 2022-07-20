// Copyright 2022 Google LLC
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

package session

import (
	"context"
	"encoding/json"
	"fmt"

	"cloud.google.com/go/spanner"
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/proto/migration"
	"google.golang.org/api/iterator"
)

type spannerStore struct {
	spannerClient *spanner.Client
}

var _ SessionStore = (*spannerStore)(nil)

func NewRemoteSessionStore(spannerClient *spanner.Client) SessionStore {
	return &spannerStore{spannerClient: spannerClient}
}

func (st *spannerStore) GetSessionsMetadata(ctx context.Context) ([]SchemaConversionSession, error) {
	txn := st.spannerClient.ReadOnlyTransaction()
	defer txn.Close()

	query := spanner.Statement{
		SQL: `SELECT 
				VersionId,
				SessionName,
				EditorName,
				DatabaseType,
				DatabaseName,
				Notes,
				Tags,
				SchemaChanges,
				CreateTimestamp
			FROM SchemaConversionSession`,
	}
	iter := txn.Query(ctx, query)
	result := []SchemaConversionSession{}

	var err error
	for {
		row, e := iter.Next()
		if e == iterator.Done {
			break
		}
		if e != nil {
			err = e
			break
		}
		var scs SchemaConversionSession
		row.ToStruct(&scs)
		result = append(result, scs)
	}
	return result, err
}

func (st *spannerStore) GetConvWithMetadata(ctx context.Context, versionId string) (ConvWithMetadata, error) {
	txn := st.spannerClient.ReadOnlyTransaction()
	defer txn.Close()

	query := spanner.Statement{
		SQL: fmt.Sprintf(`SELECT 
								SessionName,
								EditorName,
								DatabaseType,
								DatabaseName,
								Notes,
								Tags,
								VersionId,
								PreviousVersionId,
								SchemaChanges,
								TO_JSON_STRING(SchemaConversionObject) AS SchemaConversionObject,
								CreateTimestamp
							FROM SchemaConversionSession 
							WHERE VersionId = '%s'`, versionId),
	}

	iter := txn.Query(ctx, query)
	var convm ConvWithMetadata
	var scs SchemaConversionSession
	err := iter.Do(func(row *spanner.Row) error {
		if err := row.ToStruct(&scs); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return convm, err
	}

	var conv internal.Conv
	if err := json.Unmarshal([]byte(scs.SchemaConversionObject), &conv); err != nil {
		return convm, err
	}

	convm.Conv = conv
	convm.Conv.Audit = internal.Audit{
		MigrationType: migration.MigrationData_SCHEMA_ONLY.Enum(),
	}
	convm.SessionMetadata = SessionMetadata{
		SessionName:  scs.SessionName,
		EditorName:   scs.EditorName,
		DatabaseType: scs.DatabaseType,
		DatabaseName: scs.DatabaseName,
		Notes:        scs.Notes,
		Tags:         scs.Tags,
	}

	return convm, nil
}

func (st *spannerStore) SaveSession(ctx context.Context, scs SchemaConversionSession) error {
	_, err := st.spannerClient.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		mutation, err := spanner.InsertStruct("SchemaConversionSession", scs)
		if err != nil {
			return err
		}
		err = txn.BufferWrite([]*spanner.Mutation{mutation})
		if err != nil {
			return err
		}
		return nil
	})
	return err
}

func (st *spannerStore) IsSessionNameUnique(ctx context.Context, scs SchemaConversionSession) (bool, error) {
	txn := st.spannerClient.ReadOnlyTransaction()
	defer txn.Close()

	query := spanner.Statement{
		SQL: fmt.Sprintf(`SELECT 
								SessionName,
								DatabaseType,
								DatabaseName,
							FROM SchemaConversionSession 
							WHERE 
								SessionName = '%s'
								AND DatabaseType = '%s'
								AND DatabaseName = '%s'`,
			scs.SessionName, scs.DatabaseType, scs.DatabaseName),
	}

	iter := txn.Query(ctx, query)
	_, err := iter.Next()
	if err == iterator.Done {
		return true, nil
	}
	return false, err
}

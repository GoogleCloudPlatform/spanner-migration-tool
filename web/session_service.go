package web

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"google.golang.org/api/iterator"
)

// TODO: Move the types to a common location
type SchemaConversionSession struct {
	SessionMetadata
	VersionId              string
	PreviousVersionId      []string
	SchemaChanges          string
	SchemaConversionObject string
	CreatedOn              time.Time
}

type SessionMetadata struct {
	SessionName  string
	EditorName   string
	DatabaseType string
	DatabaseName string
	Notes        []string
	Tags         []string
}

type ConvWithMetadata struct {
	SessionMetadata
	internal.Conv
}

type SessionService interface {
	GetSessionsMetadata(ctx context.Context) ([]SchemaConversionSession, error)
	GetConvWithMetadata(ctx context.Context, versionId string) (ConvWithMetadata, error)
	SaveSession(ctx context.Context, scs SchemaConversionSession) error
}

type service struct {
	spannerClient *spanner.Client
}

var _ SessionService = (*service)(nil)

func NewSessionService(spannerClient *spanner.Client) SessionService {
	return &service{spannerClient: spannerClient}
}

func (svc *service) GetSessionsMetadata(ctx context.Context) ([]SchemaConversionSession, error) {
	txn := svc.spannerClient.ReadOnlyTransaction()
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
				CreatedOn
			FROM SchemaConversionSession`,
	}
	iter := txn.Query(ctx, query)
	result := []SchemaConversionSession{}

	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			//handle
			break
		}
		var scs SchemaConversionSession
		row.ToStruct(&scs)
		result = append(result, scs)
	}
	return result, nil
}

func (svc *service) GetConvWithMetadata(ctx context.Context, versionId string) (ConvWithMetadata, error) {
	txn := svc.spannerClient.ReadOnlyTransaction()
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
								CreatedOn
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

func (svc *service) SaveSession(ctx context.Context, scs SchemaConversionSession) error {
	_, err := svc.spannerClient.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
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

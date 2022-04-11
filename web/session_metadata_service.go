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

var _ Service = (*service)(nil)

type SchemaConversionSession struct {
	VersionId              string
	PreviousVersionId      []string
	SessionName            string
	EditorName             string
	DatabaseType           string
	DatabaseName           string
	Notes                  []string
	Tags                   []string
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

type Service interface {
	GetSessions(ctx context.Context) ([]SchemaConversionSession, error)
	GetSession(ctx context.Context, versionId string) (internal.Conv, error)
	SaveSession(ctx context.Context, scs SchemaConversionSession) error
}

type service struct {
	spannerClient *spanner.Client
}

func NewService(spannerClient *spanner.Client) Service {
	return &service{spannerClient: spannerClient}
}

func (svc *service) GetSessions(ctx context.Context) ([]SchemaConversionSession, error) {
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

func (svc *service) GetSession(ctx context.Context, versionId string) (internal.Conv, error) {
	txn := svc.spannerClient.ReadOnlyTransaction()
	defer txn.Close()

	query := spanner.Statement{
		SQL: fmt.Sprintf(`SELECT TO_JSON_STRING(SchemaConversionObject) FROM SchemaConversionSession WHERE VersionId = '%s'`, versionId),
	}

	iter := txn.Query(ctx, query)
	var conv internal.Conv
	err := iter.Do(func(row *spanner.Row) error {
		var d string
		if e := row.Columns(&d); e != nil {
			return e
		}
		if e := json.Unmarshal([]byte(d), &conv); e != nil {
			return e
		}
		return nil
	})
	if err != nil {
		return conv, err
	}
	return conv, nil
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

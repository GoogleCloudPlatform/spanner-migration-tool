package session

import "context"

type SessionStore interface {
	GetSessionsMetadata(ctx context.Context) ([]SchemaConversionSession, error)
	GetConvWithMetadata(ctx context.Context, versionId string) (ConvWithMetadata, error)
	CreateSession(ctx context.Context, scs SchemaConversionSession) error
}

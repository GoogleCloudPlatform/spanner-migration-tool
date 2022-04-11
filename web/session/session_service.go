package session

import "context"

type SessionService interface {
	GetSessionsMetadata(ctx context.Context) ([]SchemaConversionSession, error)
	GetConvWithMetadata(ctx context.Context, versionId string) (ConvWithMetadata, error)
	SaveSession(ctx context.Context, scs SchemaConversionSession) error
}

package session

import (
	"context"

	"github.com/cloudspannerecosystem/harbourbridge/webv2/common"
)

type SessionService struct {
	store   SessionStore
	context context.Context
}

type SessionNameError struct{}

func (e *SessionNameError) Error() string {
	return "session name already exists"
}

func NewSessionService(ctx context.Context, store SessionStore) *SessionService {
	ss := new(SessionService)
	ss.store = store
	ss.context = ctx
	return ss
}

func (ss *SessionService) SaveSession(scs SchemaConversionSession) error {
	unique, err := ss.store.IsSessionNameUnique(ss.context, scs)
	if err != nil {
		return err
	}

	if !unique {
		return &SessionNameError{}
	}

	return ss.store.SaveSession(ss.context, scs)
}

func (ss *SessionService) GetSessionsMetadata() ([]SchemaConversionSession, error) {
	return ss.store.GetSessionsMetadata(ss.context)
}

func (ss *SessionService) GetConvWithMetadata(versionId string) (ConvWithMetadata, error) {
	return ss.store.GetConvWithMetadata(ss.context, versionId)
}

func SetSessionStorageConnectionState(projectId string, spInstanceId string) bool {
	sessionState := GetSessionState()
	sessionState.GCPProjectID = projectId
	sessionState.SpannerInstanceID = spInstanceId
	if projectId == "" || spInstanceId == "" {
		sessionState.IsOffline = true
		return false
	} else {
		if isExist, isDbCreated := common.CheckOrCreateMetadataDb(projectId, spInstanceId); isExist {
			sessionState.IsOffline = false
			return isDbCreated
		} else {
			sessionState.IsOffline = true
			return false
		}
	}
}

package sessionstate

import (
	"database/sql"
	"sync"

	"github.com/cloudspannerecosystem/harbourbridge/internal"
)

var once sync.Once

// SessionState stores information for the current migration session.
type SessionState struct {
	SourceDB    *sql.DB        // Connection to source database in case of direct connection
	DbName      string         // Name of source database
	Driver      string         // Name of HarbourBridge driver in use
	Conv        *internal.Conv // Current conversion state
	SessionFile string         // Path to session file
}

// sessionState maintains the current state of the session, and is used to
// track state from one request to the next. Session state is global:
// all requests see the same session state.
var sessionState *SessionState

func GetSessionState() *SessionState {
	if sessionState == nil {
		once.Do(
			func() {
				sessionState = &SessionState{}
			})
	}
	return sessionState
}

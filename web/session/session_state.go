package session

import (
	"sync"
)

var once sync.Once

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

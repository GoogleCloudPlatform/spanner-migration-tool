package summary

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/cloudspannerecosystem/harbourbridge/internal"
	sessionstate "github.com/cloudspannerecosystem/harbourbridge/web/session-state"
)

// getSummary returns table wise summary of conversion.
func GetSummary(w http.ResponseWriter, r *http.Request) {
	sessionState := sessionstate.GetSessionState()
	reports := internal.AnalyzeTables(sessionState.Conv, nil)

	summary := make(map[string]string)
	for _, t := range reports {
		var body strings.Builder
		for _, x := range t.Body {
			body.WriteString(x.Heading + "\n")
			for i, l := range x.Lines {
				body.WriteString(fmt.Sprintf("%d) %s.\n\n", i+1, l))
			}
		}
		summary[t.SrcTable] = body.String()
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(summary)
}

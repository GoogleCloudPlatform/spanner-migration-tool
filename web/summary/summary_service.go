package summary

import (
	"fmt"
	"strings"

	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/web/session"
)

// getSummary returns table wise summary of conversion.
func getSummary() map[string]string {
	sessionState := session.GetSessionState()
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
	return summary
}

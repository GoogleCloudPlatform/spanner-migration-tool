package api

import (
	"fmt"
	"strings"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/index"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/session"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/utilities"
)

func dropSecondaryIndexHelper(tableId, idxId string) error {
	if tableId == "" || idxId == "" {
		return fmt.Errorf("Table id or index id is empty")
	}
	sessionState := session.GetSessionState()
	sp := sessionState.Conv.SpSchema[tableId]
	position := -1
	for i, index := range sp.Indexes {
		if idxId == index.Id {
			position = i
			break
		}
	}
	if position < 0 || position >= len(sp.Indexes) {
		return fmt.Errorf("No secondary index found at position %d", position)
	}

	usedNames := sessionState.Conv.UsedNames
	delete(usedNames, strings.ToLower(sp.Indexes[position].Name))
	index.RemoveIndexIssues(tableId, sp.Indexes[position])

	sp.Indexes = utilities.RemoveSecondaryIndex(sp.Indexes, position)
	sessionState.Conv.SpSchema[tableId] = sp
	session.UpdateSessionFile()
	return nil
}
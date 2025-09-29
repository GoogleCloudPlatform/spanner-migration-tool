package api

import (
	"testing"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/session"
	"github.com/stretchr/testify/assert"
)

func TestDfs(t *testing.T) {
	tests := []struct {
		name          string
		conv          *internal.Conv
		startTableId  string
		expectedCycle bool
	}{
		{
			name: "No cycle in a simple hierarchy",
			conv: &internal.Conv{
				SpSchema: ddl.Schema{
					"t1": {Id: "t1", Name: "t1", ParentTable: ddl.InterleavedParent{Id: "t2"}},
					"t2": {Id: "t2", Name: "t2"},
				},
			},
			startTableId:  "t1",
			expectedCycle: false,
		},
		{
			name: "Direct cycle",
			conv: &internal.Conv{
				SpSchema: ddl.Schema{
					"t1": {Id: "t1", Name: "t1", ParentTable: ddl.InterleavedParent{Id: "t2"}},
					"t2": {Id: "t2", Name: "t2", ParentTable: ddl.InterleavedParent{Id: "t1"}},
				},
			},
			startTableId:  "t1",
			expectedCycle: true,
		},
		{
			name: "Longer cycle",
			conv: &internal.Conv{
				SpSchema: ddl.Schema{
					"t1": {Id: "t1", Name: "t1", ParentTable: ddl.InterleavedParent{Id: "t2"}},
					"t2": {Id: "t2", Name: "t2", ParentTable: ddl.InterleavedParent{Id: "t3"}},
					"t3": {Id: "t3", Name: "t3", ParentTable: ddl.InterleavedParent{Id: "t1"}},
				},
			},
			startTableId:  "t1",
			expectedCycle: true,
		},
		{
			name: "No parent table",
			conv: &internal.Conv{
				SpSchema: ddl.Schema{
					"t1": {Id: "t1", Name: "t1"},
				},
			},
			startTableId:  "t1",
			expectedCycle: false,
		},
		{
			name: "Multiple branches with no cycle",
			conv: &internal.Conv{
				SpSchema: ddl.Schema{
					"t1": {Id: "t1", Name: "t1", ParentTable: ddl.InterleavedParent{Id: "t3"}},
					"t2": {Id: "t2", Name: "t2", ParentTable: ddl.InterleavedParent{Id: "t3"}},
					"t3": {Id: "t3", Name: "t3"},
				},
			},
			startTableId:  "t1",
			expectedCycle: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sessionState := session.GetSessionState()
			sessionState.Conv = tt.conv

			visited := make(map[string]bool)
			hasCycle := dfs(tt.startTableId, sessionState, visited)

			assert.Equal(t, tt.expectedCycle, hasCycle)
		})
	}
}

package api

import (
	"testing"
)

func TestHasCycleCheckDfs(t *testing.T) {
	tests := []struct {
		name     string
		graph    map[string][]string
		start    string
		parent   string
		visited  map[string]bool
		expected bool
	}{
		{
			name: "No cycle in simple graph",
			graph: map[string][]string{
				"A": {"B"},
				"B": {"A", "C"},
				"C": {"B"},
			},
			start:    "A",
			parent:   "",
			visited:  make(map[string]bool),
			expected: false,
		},
		{
			name: "Cycle in simple graph",
			graph: map[string][]string{
				"A": {"B"},
				"B": {"A", "C"},
				"C": {"B", "A"},
			},
			start:    "A",
			parent:   "",
			visited:  make(map[string]bool),
			expected: true,
		},
		{
			name: "No cycle in disconnected graph",
			graph: map[string][]string{
				"A": {"B"},
				"B": {"A"},
				"C": {"D"},
				"D": {"C"},
			},
			start:    "A",
			parent:   "",
			visited:  make(map[string]bool),
			expected: false,
		},
		{
			name: "Cycle in disconnected graph",
			graph: map[string][]string{
				"A": {"B"},
				"B": {"A"},
				"C": {"D", "E"},
				"E": {"D"},
			},
			start:    "C",
			parent:   "",
			visited:  make(map[string]bool),
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := hasCycleCheckDfs(tt.start, tt.parent, tt.graph, tt.visited)
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}
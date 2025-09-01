// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetSessionFileName(t *testing.T) {
	testCases := []struct {
		name            string
		sessionFileName string
		filePrefix      string
		expected        string
	}{
		{
			name:            "Empty session file name",
			sessionFileName: "",
			filePrefix:      "my-prefix",
			expected:        "my-prefix.session.json",
		},
		{
			name:            "Session file name with .json suffix",
			sessionFileName: "my-session.json",
			filePrefix:      "my-prefix",
			expected:        "my-session.json",
		},
		{
			name:            "Session file name with a different extension",
			sessionFileName: "my-session.txt",
			filePrefix:      "my-prefix",
			expected:        "my-session.json",
		},
		{
			name:            "Session file name with no extension",
			sessionFileName: "my-session",
			filePrefix:      "my-prefix",
			expected:        "my-session.json",
		},
		{
			name:            "Session file name with multiple dots",
			sessionFileName: "my.special.session.json",
			filePrefix:      "my-prefix",
			expected:        "my.special.session.json",
		},
		{
			name:            "Session file name with multiple dots and different extension",
			sessionFileName: "my.special.session.dat",
			filePrefix:      "my-prefix",
			expected:        "my.special.session.json",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := GetSessionFileName(tc.sessionFileName, tc.filePrefix)
			assert.Equal(t, tc.expected, actual)
		})
	}
}

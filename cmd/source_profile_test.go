package cmd

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewSourceProfileFile(t *testing.T) {
	testCases := []struct {
		name         string
		params       map[string]string
		pipedToStdin bool
		want         SourceProfileFile
	}{
		{
			name:         "no params, file piped",
			params:       map[string]string{},
			pipedToStdin: true,
			want:         SourceProfileFile{format: "dump"},
		},
		{
			name:         "format param, file piped",
			params:       map[string]string{"format": "dump"},
			pipedToStdin: true,
			want:         SourceProfileFile{format: "dump"},
		},
		{
			name:         "format and path param, file piped -- piped file takes precedence",
			params:       map[string]string{"format": "dump", "file": "file1.mysqldump"},
			pipedToStdin: true,
			want:         SourceProfileFile{format: "dump"},
		},
		{
			name:         "format and path param, no file piped",
			params:       map[string]string{"format": "dump", "file": "file1.mysqldump"},
			pipedToStdin: false,
			want:         SourceProfileFile{format: "dump", path: "file1.mysqldump"},
		},
		{
			name:         "only path param, no file piped -- defauly dump format",
			params:       map[string]string{"file": "file1.mysqldump"},
			pipedToStdin: false,
			want:         SourceProfileFile{format: "dump", path: "file1.mysqldump"},
		},
	}

	for _, tc := range testCases {
		// Override filePipedToStdin with the test value.
		filePipedToStdin = func() bool { return tc.pipedToStdin }

		profile := NewSourceProfileFile(tc.params)
		assert.Equal(t, profile, tc.want, tc.name)
	}
}

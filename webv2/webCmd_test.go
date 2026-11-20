// Copyright 2024 Google LLC
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

package webv2

import (
	"context"
	"flag"
	"testing"

	"github.com/google/subcommands"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zapcore"
)

func TestWebCmdSetFlags(t *testing.T) {
	testName := "Default Values"
	expectedValues := WebCmd{
		logLevel:         "DEBUG",
		open:             false,
		port:             8080,
		validate:         false,

	}

	webCmd := WebCmd{}
	fs := flag.NewFlagSet("testSetFlags", flag.ContinueOnError)
	webCmd.SetFlags(fs)
	assert.Equal(t, expectedValues, webCmd, testName)
}

// An invalid log level makes the webapp fail to start, which must be reported
// as a failure rather than exiting 0. See issue #1314.
func TestWebCmdExecuteInvalidLogLevel(t *testing.T) {
	// Guard the premise: if "verbose" ever became a valid level, Execute would
	// start a server and block forever.
	var level zapcore.Level
	if err := level.Set("verbose"); err == nil {
		t.Skip("\"verbose\" is no longer an invalid log level")
	}

	webCmd := WebCmd{logLevel: "verbose", port: 8080}
	f := flag.NewFlagSet("web", flag.ContinueOnError)
	assert.Equal(t, subcommands.ExitFailure, webCmd.Execute(context.Background(), f))
}

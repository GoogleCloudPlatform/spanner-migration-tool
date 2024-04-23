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
	"flag"
	"testing"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/stretchr/testify/assert"
)

func TestWebCmdSetFlags(t *testing.T) {
	testName := "Default Values"
	expectedValues := WebCmd{
		logLevel:         "DEBUG",
		open:             false,
		port:             8080,
		validate:         false,
		dataflowTemplate: constants.DEFAULT_TEMPLATE_PATH,
	}

	webCmd := WebCmd{}
	fs := flag.NewFlagSet("testSetFlags", flag.ContinueOnError)
	webCmd.SetFlags(fs)
	assert.Equal(t, expectedValues, webCmd, testName)
}

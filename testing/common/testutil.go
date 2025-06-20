// Copyright 2020 Google LLC
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

package common

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strings"
)

func RunCommand(args string, projectID string) error {
	// Be aware that when testing with the command, the time `now` might be
	// different between file prefixes and the contents in the files. This
	// is because file prefixes use `now` from here (the test function) and
	// the generated time in the files uses a `now` inside the command, which
	// can be different.
	executionPath := fetchExecutionPath(args)
	// A better regex and replacement:
	// Find a hyphen that is preceded by a space.
	// The ` ` (space) ensures we are at the start of an argument.
	re := regexp.MustCompile(` -`)

	// Replace every occurrence of " -" with " --".
	// This is safe because values with hyphens (like in source-uri) won't have a space before the internal hyphen.
	newString := re.ReplaceAllString(" "+args, " --")

	// The previous operation added a leading space, so we trim it.
	newString = strings.TrimSpace(newString)

	// Let's refine the regex for a single, powerful replacement.
	// This regex uses a "positive lookahead" `(?=...)` to find a hyphen
	// that is followed by a letter, ensuring it's a flag. It replaces
	// only the hyphen without needing to handle surrounding characters.
	// Note: Go's standard regexp engine has some limitations with lookaheads.
	// A simpler, very effective approach is to match the space before the hyphen.

	finalRe := regexp.MustCompile(`(\s)-([a-zA-Z])`)
	finalString := finalRe.ReplaceAllString(args, "$1--$2")

	fmt.Printf("Original String: %s\n", args)
	fmt.Printf("Modified String: %s", finalString)

	fmt.Printf("Execution path: %s\n", executionPath)
	cmd := exec.Command("bash", "-c", fmt.Sprintf("%s %v", executionPath, finalString))
	var out, stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("GCLOUD_PROJECT=%s", projectID),
	)
	if err := cmd.Run(); err != nil {
		fmt.Printf("stdout: %q\n", out.String())
		fmt.Printf("stderr: %q\n", stderr.String())
		return err
	}
	return nil
}

// Depending on the environment variables set, returns the path / command to be executed
// for the migration tool.
// If `SPANNER_MIGRATION_TOOL_TESTS_USE_GCLOUD` is set to true, GCLOUD command is constructed.
// If `SPANNER_MIGRATION_TOOL_TESTS_BINARY_PATH` is set, that path is returned.
// Else "go run github.com/GoogleCloudPlatform/spanner-migration-tool" is returned.
func fetchExecutionPath(args string) string {
	if useGcloud := os.Getenv("SPANNER_MIGRATION_TOOL_TESTS_USE_GCLOUD"); useGcloud == "true" {
		return fetchGcloudCommand(args)

	}
	if binaryPath := os.Getenv("SPANNER_MIGRATION_TOOL_TESTS_BINARY_PATH"); binaryPath != "" {
		return binaryPath
	}
	return "go run github.com/GoogleCloudPlatform/spanner-migration-tool"
}

// Depending on the arguments passed, it returns the respective gcloud command.
// import command: gcloud alpha spanner databases
// else: gcloud alpha spanner migrate
func fetchGcloudCommand(args string) string {
	if strings.HasPrefix(args, "import") {
		return "gcloud alpha spanner databases"
	}
	return "gcloud alpha spanner migrate"
}

// Clears the env variables specified in the input list and stashes the values
// in a map.
func ClearEnvVariables(vars []string) map[string]string {
	envVars := make(map[string]string)
	for _, v := range vars {
		envVars[v] = os.Getenv(v)
		os.Setenv(v, "")
	}
	return envVars
}

func RestoreEnvVariables(params map[string]string) {
	for k, v := range params {
		os.Setenv(k, v)
	}
}

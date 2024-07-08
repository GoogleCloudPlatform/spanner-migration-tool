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

package common

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

type GoldenTestCase struct {
	InputSchema        string
	ExpectedGSQLSchema string
	ExpectedPSQLSchema string
}

type GoldenParseStatus int

const (
	InputSchema GoldenParseStatus = iota
	GSQLSchema
	PSQLSchema
)

const (
	GoldenTestPartSeparator = "--"
	GoldenTestCaseSeparator = "=="
)

func GoldenTestCasesFrom(path string) ([]GoldenTestCase, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var testCases []GoldenTestCase
	var inputSchema, expectedGSQLSchema, expectedPSQLSchema strings.Builder
	parsingStatus := InputSchema
	lineNum := 0
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lineNum++
		line := scanner.Text()

		// End of a test case
		if line == GoldenTestCaseSeparator {
			if inputSchema.Len() == 0 {
				return nil, fmt.Errorf("bad format: Invalid test case at line %d, missing source schema", lineNum)
			}
			if expectedGSQLSchema.Len() == 0 {
				return nil, fmt.Errorf("bad format: Invalid test case at line %d, missing expected GoogleSQL schema", lineNum)
			}
			if expectedPSQLSchema.Len() == 0 {
				return nil, fmt.Errorf("bad format: Invalid test case at line %d, missing expected PostgreSQL schema", lineNum)
			}
			sanitizedGSQLSchema := strings.TrimRight(expectedGSQLSchema.String(), "\n")
			sanitizedPSQLSchema := strings.TrimRight(expectedPSQLSchema.String(), "\n")
			testCases = append(testCases, GoldenTestCase{
				InputSchema:        inputSchema.String(),
				ExpectedGSQLSchema: sanitizedGSQLSchema,
				ExpectedPSQLSchema: sanitizedPSQLSchema})
			parsingStatus = InputSchema
			continue
		}

		// End of part of a test case
		if line == GoldenTestPartSeparator {
			switch parsingStatus {
			case InputSchema:
				parsingStatus = GSQLSchema
			case GSQLSchema:
				parsingStatus = PSQLSchema
			default:
				return nil, fmt.Errorf("bad format: expected end of test case at line %d", lineNum)
			}
			continue
		}

		switch parsingStatus {
		case InputSchema:
			inputSchema.WriteString(line + "\n")
		case GSQLSchema:
			expectedGSQLSchema.WriteString(line + "\n")
		case PSQLSchema:
			expectedPSQLSchema.WriteString(line + "\n")
		}
	}

	// Test case is invalid
	if parsingStatus != InputSchema {
		return nil, fmt.Errorf("bad format: Invalid test case at line %d", lineNum)
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return testCases, nil
}

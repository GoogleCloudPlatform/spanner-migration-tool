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
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type GoldenTestCase struct {
	Name     string
	Input    string
	GSQLWant string
	PSQLWant string
}

type parseStatus int

const (
	parsingInput parseStatus = iota
	parsingGSQL
	parsingPSQL
)

const (
	goldenTestCaseNamePrefix    = "--"
	goldenGoogleSQLExpectation  = "-- GoogleSQL"
	goldenPostgreSQLExpectation = "-- PostgreSQL"
	goldenTestCaseEndOfTest     = "=="
)

func GoldenTestCasesFrom(t testing.TB, dir string) []GoldenTestCase {
	t.Helper()
	var tests []GoldenTestCase
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("error when reading golden tests from dir %s: %s", dir, err)
		return nil
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			path := filepath.Join(dir, entry.Name())
			ts := goldenTestCasesFromFile(t, path)
			if err != nil {
				return nil
			}
			tests = append(tests, ts...)
		}
	}
	return tests
}

func goldenTestCasesFromFile(t testing.TB, filePath string) []GoldenTestCase {
	t.Helper()
	file, err := os.Open(filePath)
	dirName := filepath.Base(filepath.Dir(filePath))
	fileName := filepath.Base(filePath)
	if err != nil {
		t.Fatalf("error when reading golden tests from path %s: %s", filePath, err)
		return nil
	}
	defer file.Close()

	var testCases []GoldenTestCase
	var testName, inputSchema, GSQLWant, PSQLWant strings.Builder
	parsingStatus := parsingInput
	lineNum := 0
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lineNum++
		line := scanner.Text()

		// First line should be the test case name
		if testName.Len() == 0 {
			testName.WriteString(dirName)
			testName.WriteString("/")
			testName.WriteString(fileName)
			testName.WriteString("/")
			testName.WriteString(strings.Trim(strings.ReplaceAll(line, goldenTestCaseNamePrefix, ""), ""))
			continue
		}
		// Beginning of GoogleSQL expectation
		if line == goldenGoogleSQLExpectation {
			if GSQLWant.Len() != 0 {
				t.Fatal("bad format: Duplicated GoogleSQL definition in test case")
				return nil
			}
			parsingStatus = parsingGSQL
			continue
		}
		// Beginning of PostgreSQL expectation
		if line == goldenPostgreSQLExpectation {
			if PSQLWant.Len() != 0 {
				t.Fatal("bad format: Duplicated PostgreSQL definition in test case")
				return nil
			}
			parsingStatus = parsingPSQL
			continue
		}

		// End of a test case
		if line == goldenTestCaseEndOfTest {
			if inputSchema.Len() == 0 {
				t.Fatalf("bad format: Invalid test case at line %d, missing source schema", lineNum)
				return nil
			}
			if GSQLWant.Len() == 0 {
				t.Fatalf("bad format: Invalid test case at line %d, missing expected GoogleSQL schema", lineNum)
				return nil
			}
			if PSQLWant.Len() == 0 {
				t.Fatalf("bad format: Invalid test case at line %d, missing expected PostgreSQL schema", lineNum)
				return nil
			}
			testCases = append(testCases, GoldenTestCase{
				Name:     testName.String(),
				Input:    inputSchema.String(),
				GSQLWant: strings.TrimRight(GSQLWant.String(), "\n"),
				PSQLWant: strings.TrimRight(PSQLWant.String(), "\n")})
			parsingStatus = parsingInput
			testName.Reset()
			inputSchema.Reset()
			GSQLWant.Reset()
			PSQLWant.Reset()
			continue
		}

		// Test body
		switch parsingStatus {
		case parsingInput:
			inputSchema.WriteString(line)
			inputSchema.WriteString("\n")
		case parsingGSQL:
			GSQLWant.WriteString(line)
			GSQLWant.WriteString("\n")
		case parsingPSQL:
			PSQLWant.WriteString(line)
			PSQLWant.WriteString("\n")
		}
	}

	// Test case not finished
	if parsingStatus != parsingInput {
		t.Fatalf("bad format: Invalid test case at line %d", lineNum)
		return nil
	}

	if err := scanner.Err(); err != nil {
		t.Fatalf("bad format: Error when scanning golden test file %s: %s", filePath, err)
		return nil
	}

	return testCases
}

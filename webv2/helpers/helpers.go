// Copyright 2022 Google LLC
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

// Package web defines web APIs to be used with harbourbridge frontend.
// Apart from schema conversion, this package involves API to update
// converted schema.
package utils

import (
	"fmt"
	"os"

	"github.com/cloudspannerecosystem/harbourbridge/common/utils"
	"github.com/cloudspannerecosystem/harbourbridge/conversion"
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/webv2/session"
)

// UpdateSessionFile updates the content of session file with
// latest sessionState.Conv while also dumping schemas and report.
func UpdateSessionFile() error {
	sessionState := session.GetSessionState()

	ioHelper := &utils.IOStreams{In: os.Stdin, Out: os.Stdout}
	_, err := conversion.WriteConvGeneratedFiles(sessionState.Conv, sessionState.DbName, sessionState.Driver, ioHelper.BytesRead, ioHelper.Out)
	if err != nil {
		return fmt.Errorf("Error encountered while updating session session file %w", err)
	}
	return nil
}

// ContainString check string is present in given list.
func ContainString(fc []string, col string) string {

	for _, s := range fc {
		if s == col {
			return col
		}
	}
	return ""
}

// RemoveSchemaIssue removes issue from the given list.
func RemoveSchemaIssue(schemaissue []internal.SchemaIssue, issue internal.SchemaIssue) []internal.SchemaIssue {

	for i := 0; i < len(schemaissue); i++ {
		if schemaissue[i] == issue {
			schemaissue = append(schemaissue[:i], schemaissue[i+1:]...)
		}
	}
	return schemaissue
}

// IsSchemaIssuePresent checks if issue is present in the given schemaissue list.
func IsSchemaIssuePresent(schemaissue []internal.SchemaIssue, issue internal.SchemaIssue) bool {

	for _, s := range schemaissue {
		if s == issue {
			return true
		}
	}
	return false
}

// DuplicateInArray checks if there is any duplicate element present in the list.
func DuplicateInArray(element []int) int {
	visited := make(map[int]bool, 0)
	for i := 0; i < len(element); i++ {
		if visited[element[i]] == true {
			return element[i]
		} else {
			visited[element[i]] = true
		}
	}
	return -1
}

// Difference gives list of element that are only present in first list.
func Difference(listone, listtwo []int) []int {

	hashmap := make(map[int]int, len(listtwo))

	for _, val := range listtwo {
		hashmap[val]++
	}

	var diff []int

	for _, val := range listone {

		_, found := hashmap[val]
		if !found {
			diff = append(diff, val)
		}
	}
	return diff
}

// RemoveSchemaIssues remove all  Hotspot and Interleaved from given list.
// RemoveSchemaIssues is used when we are adding or removing primary key column from primary key.
func RemoveSchemaIssues(schemaissue []internal.SchemaIssue) []internal.SchemaIssue {

	switch {

	case IsSchemaIssuePresent(schemaissue, internal.HotspotAutoIncrement):
		schemaissue = RemoveSchemaIssue(schemaissue, internal.HotspotAutoIncrement)
		fallthrough

	case IsSchemaIssuePresent(schemaissue, internal.HotspotTimestamp):
		schemaissue = RemoveSchemaIssue(schemaissue, internal.HotspotTimestamp)
		fallthrough

	case IsSchemaIssuePresent(schemaissue, internal.InterleavedOrder):
		schemaissue = RemoveSchemaIssue(schemaissue, internal.InterleavedOrder)

	case IsSchemaIssuePresent(schemaissue, internal.InterleavedNotInOrder):
		schemaissue = RemoveSchemaIssue(schemaissue, internal.InterleavedNotInOrder)
		fallthrough

	case IsSchemaIssuePresent(schemaissue, internal.InterleavedAddColumn):
		schemaissue = RemoveSchemaIssue(schemaissue, internal.InterleavedAddColumn)
	}

	return schemaissue
}

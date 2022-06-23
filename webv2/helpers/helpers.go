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
package helpers

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

// RemoveSchemaIssues remove all  hotspot and interleaved from given list.
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

// IsColumnPresent check col is present in given columns list.
func IsColumnPresent(columns []string, col string) string {

	for _, c := range columns {
		if c == col {
			return col
		}
	}
	return ""
}

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

package conversion

import (
	"bytes"
	"crypto/sha256"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/utils"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/schema"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
	"github.com/stretchr/testify/assert"
)

func TestReadSessionFile(t *testing.T) {
	createdExpectedConv := func() *internal.Conv {
		expectedConv := internal.MakeConv()
		expectedConv.SpSchema = map[string]ddl.CreateTable{
			"t1": {
				Name:          "numbers",
				ColIds:        []string{"c1", "c2"},
				ShardIdColumn: "c1",
				ColDefs: map[string]ddl.ColumnDef{
					"c1": {
						Name:    "id",
						NotNull: true,
						Comment: "From: id int(10)",
						Id:      "c2",
					},
					"c2": {
						Name:    "value",
						NotNull: false,
						Id:      "c2",
					},
				},
				PrimaryKeys: []ddl.IndexKey{
					{
						ColId: "c1",
						Order: 1,
					},
				},
				Comment: "Spanner schema for source table numbers",
				Id:      "t1",
			},
		}
		expectedConv.SrcSchema = map[string]schema.Table{
			"t1": {
				Name:   "numbers",
				Schema: "default",
				ColIds: []string{"c1", "c2"},
				ColDefs: map[string]schema.Column{
					"c1": {
						Name: "id",
						Type: schema.Type{
							Name: "int",
							Mods: []int64{10},
						},
						NotNull: true,
						Id:      "c1",
					},
					"c2": {
						Name: "value",
						Type: schema.Type{
							Name: "int",
							Mods: []int64{10},
						},
						NotNull: true,
						Id:      "c2",
					},
				},
				PrimaryKeys: []schema.Key{
					{
						ColId: "c1",
						Desc:  false,
						Order: 1,
					},
				},
				Id: "t1",
			},
		}
		expectedConv.SchemaIssues = map[string]internal.TableIssues{
			"t1": {
				ColumnLevelIssues: map[string][]internal.SchemaIssue{
					"c1": {14},
				},
			},
		}
		return expectedConv
	}
	expectedConvWithSequences := createdExpectedConv()
	expectedConvWithSequences.SpSequences = map[string]ddl.Sequence{
		"s1": {
			Name:         "Seq",
			Id:           "s1",
			SequenceKind: "BIT REVERSED POSITIVE",
		},
	}
	expectedConvWithSequences.SrcSequences = map[string]ddl.Sequence{
		"s1": {
			Name:         "Seq",
			Id:           "s1",
			SequenceKind: "BIT REVERSED POSITIVE",
		},
	}
	testCases := []struct {
		name         string
		filePath     string
		expectedConv *internal.Conv
		expectError  bool
	}{
		{
			name:         "test basic session file",
			filePath:     filepath.Join("..", "test_data", "basic_session_file_test.json"),
			expectedConv: expectedConvWithSequences,
			expectError:  false,
		},
		{
			name:         "test session file without sequences",
			filePath:     filepath.Join("..", "test_data", "basic_sessions_file_wo_sequences_test.json"),
			expectedConv: createdExpectedConv(),
			expectError:  false,
		},
	}
	for _, tc := range testCases {
		conv := internal.MakeConv()
		err := ReadSessionFile(conv, tc.filePath)
		assert.Equal(t, tc.expectError, err != nil, tc.name)
		assert.Equal(t, &tc.expectedConv, &conv, tc.name)
	}
}

func TestWriteSessionFile(t *testing.T) {
	createConv := func() *internal.Conv {
		conv := internal.MakeConv()
		conv.SpSchema = map[string]ddl.CreateTable{
			"t1": {
				Name:          "numbers",
				ColIds:        []string{"c1", "c2"},
				ShardIdColumn: "c1",
				ColDefs: map[string]ddl.ColumnDef{
					"c1": {
						Name:    "id",
						NotNull: true,
						Comment: "From: id int(10)",
						Id:      "c2",
					},
					"c2": {
						Name:    "value",
						NotNull: false,
						Id:      "c2",
					},
				},
				PrimaryKeys: []ddl.IndexKey{
					{
						ColId: "c1",
						Order: 1,
					},
				},
				Comment: "Spanner schema for source table numbers",
				Id:      "t1",
			},
		}
		conv.SrcSchema = map[string]schema.Table{
			"t1": {
				Name:   "numbers",
				Schema: "default",
				ColIds: []string{"c1", "c2"},
				ColDefs: map[string]schema.Column{
					"c1": {
						Name: "id",
						Type: schema.Type{
							Name: "int",
							Mods: []int64{10},
						},
						NotNull: true,
						Id:      "c1",
					},
					"c2": {
						Name: "value",
						Type: schema.Type{
							Name: "int",
							Mods: []int64{10},
						},
						NotNull: true,
						Id:      "c2",
					},
				},
				PrimaryKeys: []schema.Key{
					{
						ColId: "c1",
						Desc:  false,
						Order: 1,
					},
				},
				Id: "t1",
			},
		}
		conv.SchemaIssues = map[string]internal.TableIssues{
			"t1": {
				ColumnLevelIssues: map[string][]internal.SchemaIssue{
					"c1": {14},
				},
			},
		}
		conv.SpSequences = map[string]ddl.Sequence{
			"s1": {
				Name:         "Seq",
				Id:           "s1",
				SequenceKind: "BIT REVERSED POSITIVE",
			},
		}
		conv.SrcSequences = map[string]ddl.Sequence{
			"s1": {
				Name:         "Seq",
				Id:           "s1",
				SequenceKind: "BIT REVERSED POSITIVE",
			},
		}
		return conv
	}
	conv := createConv()
	testCases := []struct {
		name             string
		expectedFilePath string
		conv             *internal.Conv
		expectError      bool
	}{
		{
			name:             "test basic session file",
			expectedFilePath: filepath.Join("..", "test_data", "basic_session_file_test.json"),
			conv:             conv,
			expectError:      false,
		},
	}
	for _, tc := range testCases {
		ioHelper := &utils.IOStreams{Out: os.Stdout}
		WriteSessionFile(tc.conv, "session_file", ioHelper.Out)

		equal, err := compareFiles("session_file", tc.expectedFilePath)
		assert.NoError(t, err, "error comparing files")
		assert.True(t, equal, "session_file does not match expected file")

		err = os.Remove("session_file")
		assert.NoError(t, err, "error deleting session_file")
	}
}

// compareFiles function (using hashing)
func compareFiles(file1, file2 string) (bool, error) {
	hash1, err := hashFile(file1)
	if err != nil {
		return false, err
	}
	hash2, err := hashFile(file2)
	if err != nil {
		return false, err
	}
	return bytes.Equal(hash1, hash2), nil
}

func hashFile(filePath string) ([]byte, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return nil, err
	}
	return h.Sum(nil),
		nil
}

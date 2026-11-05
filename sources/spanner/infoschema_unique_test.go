// Copyright 2026 Google LLC
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

package spanner

import (
	"context"
	"testing"

	"cloud.google.com/go/spanner"
	spannerclient "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/clients/spanner/client"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/common"
	"github.com/stretchr/testify/assert"
	"google.golang.org/api/iterator"
)

// TestGetIndexes_GoogleSQLUniquePreserved is a regression test: on the default
// GoogleSQL dialect a genuinely UNIQUE index must be recorded as Unique. The
// bug unconditionally ran `isUnique = isPgUnique == "YES"` after the dialect
// branches, clobbering the boolean scanned on the GoogleSQL path to false.
func TestGetIndexes_GoogleSQLUniquePreserved(t *testing.T) {
	// Columns are scanned positionally: name, column, sequence, ordering, isUnique.
	row, err := spanner.NewRow(
		[]string{"index_name", "column_name", "ordinal_position", "column_ordering", "is_unique"},
		[]interface{}{"idx_unique", "col_a", int64(1), "ASC", true},
	)
	assert.Nil(t, err)

	calls := 0
	iter := &spannerclient.RowIteratorMock{
		NextMock: func() (*spanner.Row, error) {
			calls++
			if calls == 1 {
				return row, nil
			}
			return nil, iterator.Done
		},
		StopMock: func() {},
	}
	client := spannerclient.SpannerClientMock{
		SingleMock: func() spannerclient.ReadOnlyTransaction {
			return &spannerclient.ReadOnlyTransactionMock{
				QueryMock: func(ctx context.Context, stmt spanner.Statement) spannerclient.RowIterator {
					return iter
				},
			}
		},
	}

	isi := InfoSchemaImpl{
		SpannerClient: client,
		Ctx:           context.Background(),
		SpDialect:     constants.DIALECT_GOOGLESQL,
	}
	conv := internal.MakeConv()
	got, err := isi.GetIndexes(conv, common.SchemaAndName{Name: "t"}, map[string]string{"col_a": "c1"})
	assert.Nil(t, err)
	assert.Equal(t, 1, len(got))
	assert.True(t, got[0].Unique, "GoogleSQL UNIQUE index must be recorded as Unique")
}

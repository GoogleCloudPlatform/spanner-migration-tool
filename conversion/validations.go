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
	"context"

	sp "cloud.google.com/go/spanner"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/spanner"
)

// ValidateTables validates that all the tables in the database are empty.
// It returns the name of the first non-empty table if found, and an empty string otherwise.
func ValidateTables(ctx context.Context, client *sp.Client, spDialect string) (string, error) {
	infoSchema := spanner.InfoSchemaImpl{Client: client, Ctx: ctx, SpDialect: spDialect}
	tables, err := infoSchema.GetTables()
	if err != nil {
		return "", err
	}
	for _, table := range tables {
		count, err := infoSchema.GetRowCount(table)
		if err != nil {
			return "", err
		}
		if count != 0 {
			return table.Name, nil
		}
	}
	return "", nil
}

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
	"fmt"

	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/schema"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
)

// ToNotNull returns true if a column is not nullable and false if it is.
func ToNotNull(conv *internal.Conv, isNullable string) bool {
	switch isNullable {
	case "YES":
		return false
	case "NO":
		return true
	}
	conv.Unexpected(fmt.Sprintf("isNullable column has unknown value: %s", isNullable))
	return false
}

// GetColsAndSchemas provides information about columns and schema for a table.
func GetColsAndSchemas(conv *internal.Conv, srcTable string) (schema.Table, string, []string, ddl.CreateTable, error) {
	srcSchema := conv.SrcSchema[srcTable]
	spTable, err1 := internal.GetSpannerTable(conv, srcTable)
	spCols, err2 := internal.GetSpannerCols(conv, srcTable, srcSchema.ColIds)
	spSchema, ok := conv.SpSchema[spTable]
	var err error
	if err1 != nil || err2 != nil || !ok {
		err = fmt.Errorf(fmt.Sprintf("err1=%s, err2=%s, ok=%t", err1, err2, ok))
	}
	return srcSchema, spTable, spCols, spSchema, err
}

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

package utilities

import (
	"fmt"

	"github.com/cloudspannerecosystem/harbourbridge/common/constants"
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/sources/oracle"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
	"github.com/cloudspannerecosystem/harbourbridge/webv2/session"
	"github.com/cloudspannerecosystem/harbourbridge/webv2/typemap"
)

func GetType(conv *internal.Conv, newType, tableId, colId string) (ddl.CreateTable, ddl.Type, error) {
	sessionState := session.GetSessionState()

	sp := conv.SpSchema[tableId]
	srcCol := conv.SrcSchema[tableId].ColDefs[colId]
	var ty ddl.Type
	var issues []internal.SchemaIssue
	switch sessionState.Driver {
	case constants.MYSQL, constants.MYSQLDUMP:
		ty, issues = typemap.ToSpannerTypeMySQL(srcCol.Type.Name, newType, srcCol.Type.Mods)
	case constants.PGDUMP, constants.POSTGRES:
		ty, issues = typemap.ToSpannerTypePostgres(srcCol.Type.Name, newType, srcCol.Type.Mods)
	case constants.SQLSERVER:
		ty, issues = typemap.ToSpannerTypeSQLserver(srcCol.Type.Name, newType, srcCol.Type.Mods)
	case constants.ORACLE:
		ty, issues = oracle.ToSpannerTypeWeb(conv, newType, srcCol.Type.Name, srcCol.Type.Mods)
	default:
		return sp, ty, fmt.Errorf("driver : '%s' is not supported", sessionState.Driver)
	}
	if len(srcCol.Type.ArrayBounds) > 1 {
		ty = ddl.Type{Name: ddl.String, Len: ddl.MaxLength}
		issues = append(issues, internal.MultiDimensionalArray)
	}
	if srcCol.Ignored.Default {
		issues = append(issues, internal.DefaultValue)
	}
	if srcCol.Ignored.AutoIncrement {
		issues = append(issues, internal.AutoIncrement)
	}
	if conv.SchemaIssues != nil && len(issues) > 0 {
		conv.SchemaIssues[tableId][colId] = issues
	}
	ty.IsArray = len(srcCol.Type.ArrayBounds) == 1
	return sp, ty, nil
}

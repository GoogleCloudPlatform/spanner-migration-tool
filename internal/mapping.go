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

package internal

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
)

// GetSpannerTable maps a source DB table name into a legal Spanner table
// name. Note that source DB column names can be essentially any string, but
// Spanner column names must use a limited character set. This means that
// getSpannerTable may have to change a name to make it legal, we must ensure
// that:
// a) the new table name is legal
// b) the new table name doesn't clash with other Spanner table names
// c) we consistently return the same name for this table.
//
// conv.UsedNames tracks Spanner names that have been used for table names, foreign key constraints
// and indexes. We use this to ensure we generate unique names when
// we map from source dbs to Spanner since Spanner requires all these names to be
// distinct and should not differ only in case.
func GetSpannerTable(conv *Conv, tableId string) (string, error) {
	if tableId == "" {
		return "", fmt.Errorf("bad parameter: table-id string is empty")
	}

	if sp, found := conv.SpSchema[tableId]; found {
		return sp.Name, nil
	}
	srcTableName := conv.SrcSchema[tableId].Name
	spTableName := getSpannerValidName(conv, srcTableName)
	if spTableName != srcTableName {
		VerbosePrintf("Mapping source DB table %s to Spanner table %s\n", srcTableName, spTableName)
		logger.Log.Debug(fmt.Sprintf("Mapping source DB table %s to Spanner table %s\n", srcTableName, spTableName))
	}
	return spTableName, nil
}

// GetSpannerCol maps a source DB table/column into a legal Spanner column
// name. If mustExist is true, we return error if the column is new.
// Note that source DB column names can be essentially any string, but
// Spanner column names must use a limited character set. This means that
// getSpannerCol may have to change a name to make it legal, we must ensure
// that:
// a) the new col name is legal
// b) the new col name doesn't clash with other col names in the same table
// c) we consistently return the same name for the same col.
func GetSpannerCol(conv *Conv, tableId, colId string, spColDef map[string]ddl.ColumnDef) (string, error) {
	if tableId == "" {
		return "", fmt.Errorf("bad parameter: table id string is empty")
	}
	if colId == "" {
		return "", fmt.Errorf("bad parameter: column id string is empty")
	}
	if spCol, found := spColDef[colId]; found {
		return spCol.Name, nil
	}
	srcTable := conv.SrcSchema[tableId]
	srcColName := srcTable.ColDefs[colId].Name

	spColName, _ := FixName(srcColName)
	usedColNames := map[string]bool{}
	for _, spCol := range spColDef {
		usedColNames[spCol.Name] = true
	}
	if _, found := usedColNames[spColName]; found {
		// spColName has been used before i.e. FixName caused a collision.
		// Add unique postfix: use number of cols in this table so far.
		// However, there is a chance this has already been used,
		// so need to iterate
		id := len(spColDef)
		for {
			c := spColName + "_" + strconv.Itoa(id)
			if _, found := usedColNames[c]; !found {
				spColName = c
				break
			}
			id++
		}
	}
	if spColName != srcColName {
		VerbosePrintf("Mapping source DB col %s (table %s) to Spanner col %s\n", srcColName, srcTable.Name, spColName)
		logger.Log.Debug(fmt.Sprintf("Mapping source DB col %s (table %s) to Spanner col %s\n", srcColName, srcTable.Name, spColName))
	}
	return spColName, nil
}

// GetSpannerCols maps a slice of source columns into their corresponding
// Spanner columns using GetSpannerCol.
func GetSpannerCols(conv *Conv, tableId string, srcCols []string) ([]string, error) {
	var spCols []string
	for _, srcColName := range srcCols {
		colId, err := GetColIdFromSrcName(conv.SrcSchema[tableId].ColDefs, srcColName)
		if err != nil {
			return nil, err
		}
		spCol, err := GetSpannerCol(conv, tableId, colId, conv.SpSchema[tableId].ColDefs)
		if err != nil {
			return nil, err
		}
		spCols = append(spCols, spCol)
	}
	return spCols, nil
}

// ToSpannerForeignKey maps source foreign key name to
// legal Spanner foreign key name.
// If the srcKeyName is empty string we can just return
// empty string without error.
// If the srcKeyName is not empty we need to make sure
// of the following things:
// a) the new foreign key name is legal
// b) the new foreign key name doesn't clash with other Spanner
//
//	foreign key names
//
// Note that foreign key constraint names in Spanner have to be globally unique
// (across the database). But in some source databases, such as PostgreSQL,
// they only have to be unique for a table. Hence we must map each source
// constraint name to a unique spanner constraint name.
func ToSpannerForeignKey(conv *Conv, srcFkName string) string {
	if srcFkName == "" {
		return ""
	}
	return getSpannerValidName(conv, srcFkName)
}

// ToSpannerOnDelete maps the source ON DELETE action
// to the corresponding Spanner compatible action.
// The following mapping is followed:
// a) CASCADE/NO ACTION -> mapped to the same as source action
// b) all others -> NO ACTION (default)
//
// Since only MySQL and PostgreSQL have this functionality
// as of yet, for other sources OnDelete fields are
// kept empty i.e. "" is mapped to ""
//
// For all source actions converted to a different action,
// an issue is appended to it's TableLevelIssues to
// generate a warning message for the user
func ToSpannerOnDelete(conv *Conv, srcTableId string, srcDeleteRule string) string {
	srcDeleteRule = strings.ToUpper(srcDeleteRule)
	if srcDeleteRule == constants.NO_ACTION || srcDeleteRule == constants.CASCADE || srcDeleteRule == "" {
		return srcDeleteRule
	}

	if conv.SchemaIssues == nil {
		conv.SchemaIssues = make(map[string]TableIssues)
	}
	conv.SchemaIssues[srcTableId] = TableIssues{
		TableLevelIssues:  append(conv.SchemaIssues[srcTableId].TableLevelIssues, ForeignKeyOnDelete),
		ColumnLevelIssues: conv.SchemaIssues[srcTableId].ColumnLevelIssues}

	return constants.NO_ACTION
}

// ToSpannerOnUpdate maps the source ON UPDATE action
// to the corresponding Spanner compatible action.
// The following mapping is followed:
// all actions -> NO ACTION (default)
// (Spanner only supports ON UPDATE NO ACTION)
//
// Since only MySQL and PostgreSQL have this functionality
// as of yet, for other sources OnDelete fields are
// kept empty i.e. "" is mapped to ""
//
// For all source actions converted to a different action,
// an issue is appended to it's TableLevelIssues to
// generate a warning message for the user
func ToSpannerOnUpdate(conv *Conv, srcTableId string, srcUpdateRule string) string {
	srcUpdateRule = strings.ToUpper(srcUpdateRule)
	if srcUpdateRule == constants.NO_ACTION || srcUpdateRule == "" {
		return srcUpdateRule
	}

	if conv.SchemaIssues == nil {
		conv.SchemaIssues = make(map[string]TableIssues)
	}
	conv.SchemaIssues[srcTableId] = TableIssues{
		TableLevelIssues:  append(conv.SchemaIssues[srcTableId].TableLevelIssues, ForeignKeyOnUpdate),
		ColumnLevelIssues: conv.SchemaIssues[srcTableId].ColumnLevelIssues}

	return constants.NO_ACTION
}

// ToSpannerIndexName maps source index name to legal Spanner index name.
// We need to make sure of the following things:
// a) the new index name is legal
// b) the new index name doesn't clash with other Spanner
//
//	index names
//
// Note that index key constraint names in Spanner have to be globally unique
// (across the database). But in some source databases, such as MySQL,
// they only have to be unique for a table. Hence we must map each source
// constraint name to a unique spanner constraint name.
func ToSpannerIndexName(conv *Conv, srcIndexName string) string {
	return getSpannerValidName(conv, srcIndexName)
}

// conv.UsedNames tracks Spanner names that have been used for table names, foreign key constraints
// and indexes. We use this to ensure we generate unique names when
// we map from source dbs to Spanner since Spanner requires all these names to be
// distinct and should not differ only in case.
func getSpannerValidName(conv *Conv, srcName string) string {
	spKeyName, _ := FixName(srcName)
	if _, found := conv.UsedNames[strings.ToLower(spKeyName)]; found {
		// spKeyName has been used before.
		// Add unique postfix: use number of keys so far.
		// However, there is a chance this has already been used,
		// so need to iterate.
		id := len(conv.UsedNames)
		for {
			c := spKeyName + "_" + strconv.Itoa(id)
			if _, found := conv.UsedNames[strings.ToLower(c)]; !found {
				spKeyName = c
				break
			}
			id++
		}
	}
	conv.UsedNames[strings.ToLower(spKeyName)] = true
	return spKeyName
}

// ResolveRefs resolves all table and column references in foreign key constraints
// in the Spanner Schema. Note: Spanner requires that DDL references match
// the case of the referenced object, but this is not so for many source databases.
//
// TODO: Expand ResolveRefs to primary keys and indexes.
func ResolveRefs(conv *Conv) {
	for table, spTable := range conv.SpSchema {
		spTable.ForeignKeys = resolveFks(conv, table, spTable.ForeignKeys)
		conv.SpSchema[table] = spTable
	}
}

// resolveFks returns resolved version of fks.
// Foreign key constraints that can't be resolved are dropped.
func resolveFks(conv *Conv, table string, fks []ddl.Foreignkey) []ddl.Foreignkey {
	var resolved []ddl.Foreignkey
	for _, fk := range fks {
		var err error
		if fk.ColIds, err = resolveColRefs(conv, table, fk.ColIds); err != nil {
			conv.Unexpected(fmt.Sprintf("Can't resolve Columns in foreign key constraint: %s", err))
			delete(conv.UsedNames, strings.ToLower(fk.Name))
			continue
		}
		if fk.ReferTableId, err = resolveTableRef(conv, fk.ReferTableId); err != nil {
			conv.Unexpected(fmt.Sprintf("Can't resolve ReferTable in foreign key constraint: %s", err))
			delete(conv.UsedNames, strings.ToLower(fk.Name))
			continue
		}
		if fk.ReferColumnIds, err = resolveColRefs(conv, fk.ReferTableId, fk.ReferColumnIds); err != nil {
			conv.Unexpected(fmt.Sprintf("Can't resolve ReferColumnIds in foreign key constraint: %s", err))
			delete(conv.UsedNames, strings.ToLower(fk.Name))
			continue
		}
		resolved = append(resolved, fk)
	}
	return resolved
}

func resolveTableRef(conv *Conv, tableRef string) (string, error) {
	if _, ok := conv.SpSchema[tableRef]; ok {
		return tableRef, nil
	}
	// Do case-insensitive search for tableRef.
	tr := strings.ToLower(tableRef)
	for t := range conv.SpSchema {
		if strings.ToLower(t) == tr {
			return t, nil
		}
	}
	return "", fmt.Errorf("can't resolve table %v", tableRef)
}

func resolveColRefs(conv *Conv, tableRef string, colRefs []string) ([]string, error) {
	table, err := resolveTableRef(conv, tableRef)
	if err != nil {
		return nil, err
	}
	resolveColRef := func(colRef string) (string, error) {
		if _, ok := conv.SpSchema[table].ColDefs[colRef]; ok {
			return colRef, nil
		}
		// Do case-insensitive search for colRef.
		cr := strings.ToLower(colRef)
		for _, c := range conv.SpSchema[table].ColIds {
			if strings.ToLower(c) == cr {
				return c, nil
			}
		}
		return "", fmt.Errorf("can't resolve column: table=%v, column=%v", tableRef, colRef)
	}
	var cols []string
	for _, colRef := range colRefs {
		c, err := resolveColRef(colRef)
		if err != nil {
			return nil, err
		}
		cols = append(cols, c)
	}
	return cols, nil
}

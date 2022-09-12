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

/*
Package common creates an outline for common functionality across the multiple
source databases we support.
While adding new methods or code here
1.  Ensure that the changes do not adversely impact any source that uses the
	common code
2.	Test cases might not sufficiently cover all cases, so integration and
	manual testing should be done ensure no functionality is breaking. Most of
	the test cases that cover the code in this package will lie in the
	implementing source databases, so it might not be required to have unit
	tests for each method here.
3.	Any functions added here should be used by two or more databases
4.	If it looks like the code is getting more complex due to refactoring,
	it is probably better off leaving the functionality out of common
*/
package common

import (
	"fmt"
	"strconv"
	"unicode"

	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/schema"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
)

// ToDdl interface is meant to be implemented by all sources. When support for a
// new target database is added, please add a new method here with the output
// type expected. In case a particular source to target transoformation is not
// supported, an error is to be returned by the corresponding method.
type ToDdl interface {
	ToSpannerType(conv *internal.Conv, columnType schema.Type) (ddl.Type, []internal.SchemaIssue)
}

// SchemaToSpannerDDL performs schema conversion from the source DB schema to
// Spanner. It uses the source schema in conv.SrcSchema, and writes
// the Spanner schema to conv.SpSchema.
func SchemaToSpannerDDL(conv *internal.Conv, toddl ToDdl) error {
	for _, srcTable := range conv.SrcSchema {
		SchemaToSpannerDDLHelper(conv, toddl, srcTable, false)
	}
	internal.ResolveRefs(conv)
	return nil
}

func SchemaToSpannerDDLHelper(conv *internal.Conv, toddl ToDdl, srcTable schema.Table, isRestore bool) error {
	spTableName, err := internal.GetSpannerTable(conv, srcTable.Name)
	if err != nil {
		conv.Unexpected(fmt.Sprintf("Couldn't map source table %s to Spanner: %s", srcTable.Name, err))
		return err
	}
	var spColNames []string
	spColDef := make(map[string]ddl.ColumnDef)
	conv.Issues[srcTable.Name] = make(map[string][]internal.SchemaIssue)
	// Iterate over columns using ColNames order.
	for _, srcColName := range srcTable.ColNames {
		srcCol := srcTable.ColDefs[srcColName]
		colName, err := internal.GetSpannerCol(conv, srcTable.Name, srcCol.Name, false)
		if err != nil {
			conv.Unexpected(fmt.Sprintf("Couldn't map source column %s of table %s to Spanner: %s", srcTable.Name, srcCol.Name, err))
			continue
		}
		spColNames = append(spColNames, colName)
		ty, issues := toddl.ToSpannerType(conv, srcCol.Type)
		// TODO(hengfeng): add issues for all elements of srcCol.Ignored.
		if srcCol.Ignored.ForeignKey {
			issues = append(issues, internal.ForeignKey)
		}
		if srcCol.Name != colName {
			issues = append(issues, internal.IllegalName)
		}
		if srcCol.Ignored.Default {
			issues = append(issues, internal.DefaultValue)
		}
		if srcCol.Ignored.AutoIncrement { //TODO(adibh) - check why this is not there in postgres
			issues = append(issues, internal.AutoIncrement)
		}
		if len(issues) > 0 {
			conv.Issues[srcTable.Name][srcCol.Name] = issues
		}
		spColDef[colName] = ddl.ColumnDef{
			Name:    colName,
			T:       ty,
			NotNull: srcCol.NotNull,
			Comment: "From: " + quoteIfNeeded(srcCol.Name) + " " + srcCol.Type.Print(),
		}
	}
	comment := "Spanner schema for source table " + quoteIfNeeded(srcTable.Name)
	conv.SpSchema[spTableName] = ddl.CreateTable{
		Name:     spTableName,
		ColNames: spColNames,
		ColDefs:  spColDef,
		Pks:      cvtPrimaryKeys(conv, srcTable.Name, srcTable.PrimaryKeys),
		Fks:      cvtForeignKeys(conv, spTableName, srcTable.Name, srcTable.ForeignKeys, isRestore),
		Indexes:  cvtIndexes(conv, spTableName, srcTable.Name, srcTable.Indexes),
		Comment:  comment}
	return nil
}

func quoteIfNeeded(s string) string {
	for _, r := range s {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || unicode.IsPunct(r) {
			continue
		}
		return strconv.Quote(s)
	}
	return s
}

func cvtPrimaryKeys(conv *internal.Conv, srcTable string, srcKeys []schema.Key) []ddl.IndexKey {
	var spKeys []ddl.IndexKey
	for _, k := range srcKeys {
		spCol, err := internal.GetSpannerCol(conv, srcTable, k.Column, true)
		if err != nil {
			conv.Unexpected(fmt.Sprintf("Can't map key for table %s", srcTable))
			continue
		}
		spKeys = append(spKeys, ddl.IndexKey{Col: spCol, Desc: k.Desc})
	}
	return spKeys
}

func cvtForeignKeys(conv *internal.Conv, spTableName string, srcTable string, srcKeys []schema.ForeignKey, isRestore bool) []ddl.Foreignkey {
	var spKeys []ddl.Foreignkey
	for _, key := range srcKeys {
		spKey, err := cvtForeignKeysHelper(conv, spTableName, srcTable, key, isRestore)
		if err != nil {
			continue
		}
		spKeys = append(spKeys, spKey)
	}
	return spKeys
}

func cvtForeignKeysHelper(conv *internal.Conv, spTableName string, srcTable string, srcKey schema.ForeignKey, isRestore bool) (ddl.Foreignkey, error) {
	if len(srcKey.Columns) != len(srcKey.ReferColumns) {
		conv.Unexpected(fmt.Sprintf("ConvertForeignKeys: columns and referColumns don't have the same lengths: len(columns)=%d, len(referColumns)=%d for source table: %s, referenced table: %s", len(srcKey.Columns), len(srcKey.ReferColumns), srcTable, srcKey.ReferTable))
		return ddl.Foreignkey{}, fmt.Errorf("ConvertForeignKeys: columns and referColumns don't have the same lengths")
	}
	_, isPresent := conv.UsedNames[srcKey.ReferTable]
	if !isPresent && isRestore {
		return ddl.Foreignkey{}, nil
	}
	spReferTable, err := internal.GetSpannerTable(conv, srcKey.ReferTable)

	if err != nil {
		conv.Unexpected(fmt.Sprintf("Can't map foreign key for source table: %s, referenced table: %s", srcTable, srcKey.ReferTable))
		return ddl.Foreignkey{}, err
	}
	var spCols, spReferCols []string
	for i, col := range srcKey.Columns {
		spCol, err1 := internal.GetSpannerCol(conv, srcTable, col, false)
		spReferCol, err2 := internal.GetSpannerCol(conv, srcKey.ReferTable, srcKey.ReferColumns[i], false)
		if err1 != nil || err2 != nil {
			conv.Unexpected(fmt.Sprintf("Can't map foreign key for table: %s, referenced table: %s, column: %s", srcTable, srcKey.ReferTable, col))
			continue
		}
		spCols = append(spCols, spCol)
		spReferCols = append(spReferCols, spReferCol)
	}
	spKeyName := internal.ToSpannerForeignKey(conv, srcKey.Name)

	spKey := ddl.Foreignkey{
		Name:         spKeyName,
		Columns:      spCols,
		ReferTable:   spReferTable,
		ReferColumns: spReferCols}
	conv.Audit.ToSpannerFkIdx[srcTable].ForeignKey[srcKey.Name] = spKeyName
	conv.Audit.ToSourceFkIdx[spTableName].ForeignKey[spKeyName] = srcKey.Name

	return spKey, nil
}

func cvtIndexes(conv *internal.Conv, spTableName string, srcTable string, srcIndexes []schema.Index) []ddl.CreateIndex {
	var spIndexes []ddl.CreateIndex
	for _, srcIndex := range srcIndexes {
		var spKeys []ddl.IndexKey
		var spStoredColumns []string

		for _, k := range srcIndex.Keys {
			spCol, err := internal.GetSpannerCol(conv, srcTable, k.Column, true)
			if err != nil {
				conv.Unexpected(fmt.Sprintf("Can't map index key column name for table %s column %s", srcTable, k.Column))
				continue
			}
			spKeys = append(spKeys, ddl.IndexKey{Col: spCol, Desc: k.Desc})
		}
		for _, k := range srcIndex.StoredColumns {
			spCol, err := internal.GetSpannerCol(conv, srcTable, k, true)
			if err != nil {
				conv.Unexpected(fmt.Sprintf("Can't map index column name for table %s column %s", srcTable, k))
				continue
			}
			spStoredColumns = append(spStoredColumns, spCol)
		}
		if srcIndex.Name == "" {
			// Generate a name if index name is empty in MySQL.
			// Collision of index name will be handled by ToSpannerIndexName.
			srcIndex.Name = fmt.Sprintf("Index_%s", srcTable)
		}
		spIndexName := internal.ToSpannerIndexName(conv, srcIndex.Name)
		spIndex := ddl.CreateIndex{
			Name:          spIndexName,
			Table:         spTableName,
			Unique:        srcIndex.Unique,
			Keys:          spKeys,
			StoredColumns: spStoredColumns,
		}
		spIndexes = append(spIndexes, spIndex)
		conv.Audit.ToSpannerFkIdx[srcTable].Index[srcIndex.Name] = spIndexName
		conv.Audit.ToSourceFkIdx[spTableName].Index[spIndexName] = srcIndex.Name
	}
	return spIndexes
}

func SrcTableToSpannerDDL(conv *internal.Conv, toddl ToDdl, srcTable schema.Table) error {
	err := SchemaToSpannerDDLHelper(conv, toddl, srcTable, true)
	if err != nil {
		return err
	}
	for srcTableName, sourceTable := range conv.SrcSchema {
		if _, isPresent := conv.ToSpanner[srcTableName]; !isPresent {
			continue
		}
		spTableName := conv.ToSpanner[srcTableName].Name
		if _, isPresent := conv.SpSchema[spTableName]; !isPresent {
			continue
		}
		spTable := conv.SpSchema[spTableName]
		if sourceTable.Name != srcTable.Name {
			spTable.Fks = cvtForeignKeysForAReferenceTable(conv, spTableName, srcTableName, srcTable.Name, sourceTable.ForeignKeys, spTable.Fks)
			conv.SpSchema[spTableName] = spTable
		}
	}
	internal.ResolveRefs(conv)
	return nil
}

func cvtForeignKeysForAReferenceTable(conv *internal.Conv, spTableName string, srcTable string, referTable string, srcKeys []schema.ForeignKey, spKeys []ddl.Foreignkey) []ddl.Foreignkey {
	for _, key := range srcKeys {
		if key.ReferTable == referTable {
			spKey, err := cvtForeignKeysHelper(conv, spTableName, srcTable, key, true)
			if err != nil {
				continue
			}
			spKeys = append(spKeys, spKey)
		}
	}
	return spKeys
}

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

package postgres

import (
	"fmt"
	"strconv"
	"unicode"

	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/schema"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
)

// schemaToDDL performs schema conversion from the source DB schema to
// Spanner. It uses the source schema in conv.SrcSchema, and writes
// the Spanner schema to conv.SpSchema.
func schemaToDDL(conv *internal.Conv) error {
	schemaForeignKeys := make(map[string]bool)
	schemaIndexKeys := make(map[string]bool)

	for _, srcTable := range conv.SrcSchema {
		spTableName, err := internal.GetSpannerTable(conv, srcTable.Name)
		if err != nil {
			conv.Unexpected(fmt.Sprintf("Couldn't map source table %s to Spanner: %s", srcTable.Name, err))
			continue
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
			ty, issues := toSpannerType(conv, srcCol.Type.Name, srcCol.Type.Mods)
			if len(srcCol.Type.ArrayBounds) > 1 {
				ty = ddl.Type{Name: ddl.String, Len: ddl.MaxLength}
				issues = append(issues, internal.MultiDimensionalArray)
			}
			// TODO: add issues for all elements of srcCol.Ignored.
			if srcCol.Ignored.ForeignKey {
				issues = append(issues, internal.ForeignKey)
			}
			if srcCol.Ignored.Default {
				issues = append(issues, internal.DefaultValue)
			}
			if len(issues) > 0 {
				conv.Issues[srcTable.Name][srcCol.Name] = issues
			}
			ty.IsArray = len(srcCol.Type.ArrayBounds) == 1
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
			Fks:      cvtForeignKeys(conv, srcTable.Name, srcTable.ForeignKeys, schemaForeignKeys),
			Indexes:  cvtIndexes(conv, spTableName, srcTable.Name, srcTable.Indexes, schemaIndexKeys),
			Comment:  comment}
	}
	internal.ResolveRefs(conv)
	return nil
}

// toSpannerType maps a scalar source schema type (defined by id and
// mods) into a Spanner type. This is the core source-to-Spanner type
// mapping.  toSpannerType returns the Spanner type and a list of type
// conversion issues encountered.
func toSpannerType(conv *internal.Conv, id string, mods []int64) (ddl.Type, []internal.SchemaIssue) {
	// TODO: Remove use of maxExpectedMods. It was added as a sanity check for the
	// initial version of HarbourBridge. It has now out-lived its usefulness: it isn't
	// uncovering issues (and it's unlikely to) and it's cluttering code.
	maxExpectedMods := func(n int) {
		if len(mods) > n {
			conv.Unexpected(fmt.Sprintf("Found %d mods while processing type id=%s", len(mods), id))
		}
	}
	switch id {
	case "bool", "boolean":
		maxExpectedMods(0)
		return ddl.Type{Name: ddl.Bool}, nil
	case "bigserial":
		maxExpectedMods(0)
		return ddl.Type{Name: ddl.Int64}, []internal.SchemaIssue{internal.Serial}
	case "bpchar", "character": // Note: Postgres internal name for char is bpchar (aka blank padded char).
		maxExpectedMods(1)
		if len(mods) > 0 {
			return ddl.Type{Name: ddl.String, Len: mods[0]}, nil
		}
		// Note: bpchar without length specifier is equivalent to bpchar(1)
		return ddl.Type{Name: ddl.String, Len: 1}, nil
	case "bytea":
		maxExpectedMods(0)
		return ddl.Type{Name: ddl.Bytes, Len: ddl.MaxLength}, nil
	case "date":
		maxExpectedMods(0)
		return ddl.Type{Name: ddl.Date}, nil
	case "float8", "double precision":
		maxExpectedMods(0)
		return ddl.Type{Name: ddl.Float64}, nil
	case "float4", "real":
		maxExpectedMods(0)
		return ddl.Type{Name: ddl.Float64}, []internal.SchemaIssue{internal.Widened}
	case "int8", "bigint":
		maxExpectedMods(0)
		return ddl.Type{Name: ddl.Int64}, nil
	case "int4", "integer":
		maxExpectedMods(0)
		return ddl.Type{Name: ddl.Int64}, []internal.SchemaIssue{internal.Widened}
	case "int2", "smallint":
		maxExpectedMods(0)
		return ddl.Type{Name: ddl.Int64}, []internal.SchemaIssue{internal.Widened}
	case "numeric":
		maxExpectedMods(2)
		// PostgreSQL's NUMERIC type can have a specified precision of up to 1000
		// digits (and scale can be anything from 0 up to the value of 'precision').
		// If precision and scale are not specified, then values of any precision
		// or scale can be stored, up to the implementation's limits (can be up to
		// 131072 digits before the decimal point and up to 16383 digits after
		// the decimal point).
		// Spanner's NUMERIC type can store up to 29 digits before the
		// decimal point and up to 9 after the decimal point -- it is
		// equivalent to PostgreSQL's NUMERIC(38,9) type.
		//
		// TODO: Generate appropriate SchemaIssue to warn of different precision
		// capabilities between PostgreSQL and Spanner NUMERIC.
		return ddl.Type{Name: ddl.Numeric}, nil
	case "serial":
		maxExpectedMods(0)
		return ddl.Type{Name: ddl.Int64}, []internal.SchemaIssue{internal.Serial}
	case "text":
		maxExpectedMods(0)
		return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
	case "timestamptz", "timestamp with time zone":
		maxExpectedMods(1)
		return ddl.Type{Name: ddl.Timestamp}, nil
	case "timestamp", "timestamp without time zone":
		maxExpectedMods(1)
		// Map timestamp without timezone to Spanner timestamp.
		return ddl.Type{Name: ddl.Timestamp}, []internal.SchemaIssue{internal.Timestamp}
	case "varchar", "character varying":
		maxExpectedMods(1)
		if len(mods) > 0 {
			return ddl.Type{Name: ddl.String, Len: mods[0]}, nil
		}
		return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, nil
	}
	return ddl.Type{Name: ddl.String, Len: ddl.MaxLength}, []internal.SchemaIssue{internal.NoGoodType}
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

func cvtForeignKeys(conv *internal.Conv, srcTable string, srcKeys []schema.ForeignKey, schemaForeignKeys map[string]bool) []ddl.Foreignkey {
	var spKeys []ddl.Foreignkey
	for _, key := range srcKeys {
		if len(key.Columns) != len(key.ReferColumns) {
			conv.Unexpected(fmt.Sprintf("ConvertForeignKeys: columns and referColumns don't have the same lengths: len(columns)=%d, len(referColumns)=%d for source table: %s, referenced table: %s", len(key.Columns), len(key.ReferColumns), srcTable, key.ReferTable))
			continue
		}
		spReferTable, err := internal.GetSpannerTable(conv, key.ReferTable)
		if err != nil {
			conv.Unexpected(fmt.Sprintf("Can't map foreign key for source table: %s, referenced table: %s", srcTable, key.ReferTable))
			continue
		}
		var spCols, spReferCols []string
		for i, col := range key.Columns {
			spCol, err1 := internal.GetSpannerCol(conv, srcTable, col, false)
			spReferCol, err2 := internal.GetSpannerCol(conv, key.ReferTable, key.ReferColumns[i], false)
			if err1 != nil || err2 != nil {
				conv.Unexpected(fmt.Sprintf("Can't map foreign key for table: %s, referenced table: %s, column: %s", srcTable, key.ReferTable, col))
				continue
			}
			spCols = append(spCols, spCol)
			spReferCols = append(spReferCols, spReferCol)
		}
		spKeyName := internal.ToSpannerForeignKey(key.Name, schemaForeignKeys)
		spKey := ddl.Foreignkey{
			Name:         spKeyName,
			Columns:      spCols,
			ReferTable:   spReferTable,
			ReferColumns: spReferCols}
		spKeys = append(spKeys, spKey)
	}
	return spKeys
}

func cvtIndexes(conv *internal.Conv, spTableName string, srcTable string, srcIndexes []schema.Index, schemaIndexKeys map[string]bool) []ddl.CreateIndex {
	var spIndexes []ddl.CreateIndex
	for _, srcIndex := range srcIndexes {
		var spKeys []ddl.IndexKey
		for _, k := range srcIndex.Keys {
			spCol, err := internal.GetSpannerCol(conv, srcTable, k.Column, true)
			if err != nil {
				conv.Unexpected(fmt.Sprintf("Can't map index key column name for table %s", srcTable))
				continue
			}
			spKeys = append(spKeys, ddl.IndexKey{Col: spCol, Desc: k.Desc})
		}
		if srcIndex.Name == "" {
			// Generate a name if index name is empty in Postgres.
			// Collision of index name will be handled by ToSpannerIndexKey.
			srcIndex.Name = fmt.Sprintf("Index_%s", srcTable)
		}
		spKeyName := internal.ToSpannerIndexKey(srcIndex.Name, schemaIndexKeys)
		spIndex := ddl.CreateIndex{
			Name:   spKeyName,
			Table:  spTableName,
			Unique: srcIndex.Unique,
			Keys:   spKeys,
		}
		spIndexes = append(spIndexes, spIndex)
	}
	return spIndexes
}

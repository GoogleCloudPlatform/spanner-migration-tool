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
	"unicode"

	"github.com/cloudspannerecosystem/harbourbridge/schema"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
)

// schemaToDDL performs schema conversion from the source DB schema to
// Spanner. It uses the source schema in conv.SrcSchema, and writes
// the Spanner schema to conv.SpSchema.
func schemaToDDL(conv *Conv) error {
	for _, srcTable := range conv.SrcSchema {
		spTableName, err := GetSpannerTable(conv, srcTable.Name)
		if err != nil {
			conv.Unexpected(fmt.Sprintf("Couldn't map source table %s to Spanner: %s", srcTable.Name, err))
			continue
		}
		var spColNames []string
		spColDef := make(map[string]ddl.ColumnDef)
		conv.Issues[srcTable.Name] = make(map[string][]SchemaIssue)
		// Iterate over columns using ColNames order.
		for _, srcColName := range srcTable.ColNames {
			srcCol := srcTable.ColDefs[srcColName]
			colName, err := GetSpannerCol(conv, srcTable.Name, srcCol.Name, false)
			if err != nil {
				conv.Unexpected(fmt.Sprintf("Couldn't map source column %s of table %s to Spanner: %s", srcTable.Name, srcCol.Name, err))
				continue
			}
			spColNames = append(spColNames, colName)
			ty, issues := toSpannerType(conv, srcCol.Type.Name, srcCol.Type.Mods)
			if len(srcCol.Type.ArrayBounds) > 1 {
				ty = ddl.String{Len: ddl.MaxLength{}}
				issues = append(issues, MultiDimensionalArray)
			}
			// TODO: add issues for all elements of srcCol.Ignored.
			if srcCol.Ignored.ForeignKey {
				issues = append(issues, ForeignKey)
			}
			if srcCol.Ignored.Default {
				issues = append(issues, DefaultValue)
			}
			if len(issues) > 0 {
				conv.Issues[srcTable.Name][srcCol.Name] = issues
			}
			spColDef[colName] = ddl.ColumnDef{
				Name:    colName,
				T:       ty,
				IsArray: len(srcCol.Type.ArrayBounds) == 1,
				NotNull: srcCol.NotNull,
				Comment: "From: " + quoteIfNeeded(srcCol.Name) + " " + printSourceType(srcCol.Type),
			}
		}
		comment := "Spanner schema for source table " + quoteIfNeeded(srcTable.Name)
		conv.SpSchema[spTableName] = ddl.CreateTable{
			Name:     spTableName,
			ColNames: spColNames,
			ColDefs:  spColDef,
			Pks:      cvtPrimaryKeys(conv, srcTable.Name, srcTable.PrimaryKeys),
			Comment:  comment}
	}
	return nil
}

// toSpannerType maps a scalar source schema type (defined by id and
// mods) into a Spanner type. This is the core source-to-Spanner type
// mapping.  toSpannerType returns the Spanner type and a list of type
// conversion issues encountered.
func toSpannerType(conv *Conv, id string, mods []int64) (ddl.ScalarType, []SchemaIssue) {
	maxExpectedMods := func(n int) {
		if len(mods) > n {
			conv.Unexpected(fmt.Sprintf("Found %d mods while processing type id=%s", len(mods), id))
		}
	}
	switch id {
	case "bool", "boolean":
		maxExpectedMods(0)
		return ddl.Bool{}, nil
	case "bigserial":
		maxExpectedMods(0)
		return ddl.Int64{}, []SchemaIssue{Serial}
	case "bpchar", "character": // Note: Postgres internal name for char is bpchar (aka blank padded char).
		maxExpectedMods(1)
		if len(mods) > 0 {
			return ddl.String{Len: ddl.Int64Length{Value: mods[0]}}, nil
		}
		// Note: bpchar without length specifier is equivalent to bpchar(1)
		return ddl.String{Len: ddl.Int64Length{Value: 1}}, nil
	case "bytea":
		maxExpectedMods(0)
		return ddl.Bytes{Len: ddl.MaxLength{}}, nil
	case "date":
		maxExpectedMods(0)
		return ddl.Date{}, nil
	case "float8", "double precision":
		maxExpectedMods(0)
		return ddl.Float64{}, nil
	case "float4", "real":
		maxExpectedMods(0)
		return ddl.Float64{}, []SchemaIssue{Widened}
	case "int8", "bigint":
		maxExpectedMods(0)
		return ddl.Int64{}, nil
	case "int4", "integer":
		maxExpectedMods(0)
		return ddl.Int64{}, []SchemaIssue{Widened}
	case "int2", "smallint":
		maxExpectedMods(0)
		return ddl.Int64{}, []SchemaIssue{Widened}
	case "numeric": // Map all numeric types to float64.
		maxExpectedMods(2)
		if len(mods) > 0 && mods[0] <= 15 {
			// float64 can represent this numeric type faithfully.
			// Note: int64 has 53 bits for mantissa, which is ~15.96
			// decimal digits.
			return ddl.Float64{}, []SchemaIssue{NumericThatFits}
		}
		return ddl.Float64{}, []SchemaIssue{Numeric}
	case "serial":
		maxExpectedMods(0)
		return ddl.Int64{}, []SchemaIssue{Serial}
	case "text":
		maxExpectedMods(0)
		return ddl.String{Len: ddl.MaxLength{}}, nil
	case "timestamptz", "timestamp with time zone":
		maxExpectedMods(1)
		return ddl.Timestamp{}, nil
	case "timestamp", "timestamp without time zone":
		maxExpectedMods(1)
		// Map timestamp without timezone to Spanner timestamp.
		return ddl.Timestamp{}, []SchemaIssue{Timestamp}
	case "varchar", "character varying":
		maxExpectedMods(1)
		if len(mods) > 0 {
			return ddl.String{Len: ddl.Int64Length{Value: mods[0]}}, nil
		}
		return ddl.String{Len: ddl.MaxLength{}}, nil
	}
	return ddl.String{Len: ddl.MaxLength{}}, []SchemaIssue{NoGoodType}
}

func printSourceType(ty schema.Type) string {
	s := ty.Name
	if len(ty.Mods) > 0 {
		var l []string
		for _, x := range ty.Mods {
			l = append(l, strconv.FormatInt(x, 10))
		}
		s = fmt.Sprintf("%s(%s)", s, strings.Join(l, ","))
	}
	if len(ty.ArrayBounds) > 0 {
		l := []string{s}
		for _, x := range ty.ArrayBounds {
			if x == -1 {
				l = append(l, "[]")
			} else {
				l = append(l, fmt.Sprintf("[%d]", x))
			}
		}
		s = strings.Join(l, "")
	}
	return s
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

func cvtPrimaryKeys(conv *Conv, srcTable string, srcKeys []schema.Key) []ddl.IndexKey {
	var spKeys []ddl.IndexKey
	for _, k := range srcKeys {
		spCol, err := GetSpannerCol(conv, srcTable, k.Column, true)
		if err != nil {
			conv.Unexpected(fmt.Sprintf("Can't map key for table %s", srcTable))
			continue
		}
		spKeys = append(spKeys, ddl.IndexKey{Col: spCol, Desc: k.Desc})
	}
	return spKeys
}

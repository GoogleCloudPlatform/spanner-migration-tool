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

package table

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/common"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/session"
	utilities "github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/utilities"
)

const (
	NotNullAdded   string = "ADDED"
	NotNullRemoved string = "REMOVED"
)

var SpannerToCassandra = map[string]string{
	ddl.Bool:      "boolean",
	ddl.Bytes:     "blob",
	ddl.Date:      "date",
	ddl.Float32:   "float",
	ddl.Float64:   "double",
	ddl.Int64:     "bigint",
	ddl.Numeric:   "decimal",
	ddl.String:    "text",
	ddl.Timestamp: "timestamp",
}

// GetCassandraType returns default cassandra type for specified Spanner type
func GetCassandraType(spannerType string) string {
	if cassandraType, ok := SpannerToCassandra[spannerType]; ok {
		return cassandraType
	}
	return ""
}

// IsColumnPresentInColNames check column is present in colnames.
func IsColumnPresentInColNames(colIds []string, colId string) bool {

	for _, id := range colIds {
		if id == colId {
			return true
		}
	}

	return false
}

// GetSpannerTableDDL return Spanner Table DDL as string.
func GetSpannerTableDDL(spannerTable ddl.CreateTable, spDialect string, driver string) string {
	sessionState := session.GetSessionState()
	c := ddl.Config{Comments: true, ProtectIds: false, SpDialect: spDialect, Source: driver}

	ddl := spannerTable.PrintCreateTable(sessionState.Conv.SpSchema, c)

	return ddl
}

func UpdateNotNull(notNullChange, tableId, colId string, conv *internal.Conv) {

	sp := conv.SpSchema[tableId]

	switch notNullChange {
	case NotNullAdded:
		spColDef := sp.ColDefs[colId]
		spColDef.NotNull = true
		sp.ColDefs[colId] = spColDef
	case NotNullRemoved:
		spColDef := sp.ColDefs[colId]
		spColDef.NotNull = false
		sp.ColDefs[colId] = spColDef
	}
}

func UpdateAutoGenCol(autoGen ddl.AutoGenCol, tableId, colId string, conv *internal.Conv) map[string]ddl.Sequence {
	sp := conv.SpSchema[tableId]
	sequences := conv.SpSequences
	spColDef := sp.ColDefs[colId]
	if spColDef.AutoGen.GenerationType == constants.SEQUENCE {
		seqId := getSequenceId(spColDef.AutoGen.Name, conv.SpSequences)
		sequences = deleteColumnFromSequence(seqId, tableId, colId, sequences)
	}
	spColDef.AutoGen = autoGen
	sp.ColDefs[colId] = spColDef
	if autoGen.GenerationType == constants.SEQUENCE {
		seqId := getSequenceId(autoGen.Name, conv.SpSequences)
		sequences = addColumnToSequence(seqId, tableId, colId, sequences)
	}
	conv.SpSchema[tableId] = sp
	return sequences
}

func getFkColumnPosition(colIds []string, colId string) int {
	for i, id := range colIds {
		if colId == id {
			return i
		}
	}
	return -1
}

func deleteColumnFromSequence(sequenceId string, tableId, colId string, sequences map[string]ddl.Sequence) map[string]ddl.Sequence {
	if _, ok := sequences[sequenceId].ColumnsUsingSeq[tableId]; ok {
		cols := sequences[sequenceId].ColumnsUsingSeq[tableId]
		for i, col := range cols {
			if col == colId {
				sequences[sequenceId].ColumnsUsingSeq[tableId] = append(cols[:i], cols[i+1:]...)
				return sequences
			}
		}
	}
	return sequences
}

func addColumnToSequence(sequenceId string, tableId, colId string, sequences map[string]ddl.Sequence) map[string]ddl.Sequence {
	if _, ok := sequences[sequenceId].ColumnsUsingSeq[tableId]; ok {
		cols := sequences[sequenceId].ColumnsUsingSeq[tableId]
		found := false
		for _, c := range cols {
			if c == colId {
				found = true
				fmt.Println(found)
				return sequences
			}
		}
		if !found {
			sequences[sequenceId].ColumnsUsingSeq[tableId] = append(cols, colId)
			return sequences
		}
	} else {
		cols := []string{colId}
		seq := sequences[sequenceId]
		if seq.ColumnsUsingSeq == nil {
			seq.ColumnsUsingSeq = make(map[string][]string)
		}
		seq.ColumnsUsingSeq[tableId] = cols
		sequences[sequenceId] = seq
	}
	return sequences
}

func getSequenceId(sequenceName string, spSeq map[string]ddl.Sequence) string {
	for seqId, seq := range spSeq {
		if seq.Name == sequenceName {
			return seqId
		}
	}
	return ""
}

// Add, deletes and updates generated column associated with a column during edit column functionality
func UpdateGeneratedCol(gc ddl.GeneratedColumn, tableId, colId string, conv *internal.Conv) {
	col := conv.SpSchema[tableId].ColDefs[colId]
	if !gc.IsPresent {
		col.GeneratedColumn = ddl.GeneratedColumn{}
		conv.SpSchema[tableId].ColDefs[colId] = col
		return
	}

	var expressionId string
	if gc.Value.ExpressionId == "" {
		if _, exists := conv.SrcSchema[tableId]; exists {
			if column, exists := conv.SrcSchema[tableId].ColDefs[colId]; exists {
				if column.GeneratedColumn.Value.ExpressionId != "" {
					expressionId = column.GeneratedColumn.Value.ExpressionId
				}
			}
		}
		if expressionId == "" {
			expressionId = internal.GenerateExpressionId()
		}
	} else {
		expressionId = gc.Value.ExpressionId
	}
	re := regexp.MustCompile(`\([^)]*\)`)
	col.GeneratedColumn = ddl.GeneratedColumn{
		Value: ddl.Expression{
			ExpressionId: expressionId,
			Statement:    common.SanitizeExpressionsValue(gc.Value.Statement, col.T.Name, re.MatchString(gc.Value.Statement)),
		},
		IsPresent: true,
		Type:      gc.Type,
	}
	conv.SpSchema[tableId].ColDefs[colId] = col
}

// Add, deletes and updates default value associated with a column during edit column functionality
func UpdateDefaultValue(dv ddl.DefaultValue, tableId, colId string, conv *internal.Conv) {
	col := conv.SpSchema[tableId].ColDefs[colId]
	if !dv.IsPresent {
		col.DefaultValue = ddl.DefaultValue{}
		conv.SpSchema[tableId].ColDefs[colId] = col
		return
	}

	var expressionId string
	if dv.Value.ExpressionId == "" {
		if _, exists := conv.SrcSchema[tableId]; exists {
			if column, exists := conv.SrcSchema[tableId].ColDefs[colId]; exists {
				if column.DefaultValue.Value.ExpressionId != "" {
					expressionId = column.DefaultValue.Value.ExpressionId
				}
			}
		}
		if expressionId == "" {
			expressionId = internal.GenerateExpressionId()
		}
	} else {
		expressionId = dv.Value.ExpressionId
	}
	re := regexp.MustCompile(`\([^)]*\)`)
	col.DefaultValue = ddl.DefaultValue{
		Value: ddl.Expression{
			ExpressionId: expressionId,
			Statement:    common.SanitizeExpressionsValue(dv.Value.Statement, col.T.Name, re.MatchString(dv.Value.Statement)),
		},
		IsPresent: true,
	}
	conv.SpSchema[tableId].ColDefs[colId] = col
}

func IsInterleavingImpacted(v updateCol, tableId string, colId string, conv *internal.Conv) string {
	isPkColumn := false
	pkOrder := -1
	for _, pk := range conv.SpSchema[tableId].PrimaryKeys {
		if pk.ColId == colId {
			isPkColumn = true
			pkOrder = pk.Order
			break
		}
	}

	if isPkColumn {
		isModification := false
		isRename := v.Rename != "" && v.Rename != conv.SpSchema[tableId].ColDefs[colId].Name
		isTypeChange, _ := utilities.IsTypeChanged(v.ToType, tableId, colId, conv)
		isNullChange := v.NotNull != "" && ((v.NotNull == "ADDED" && !conv.SpSchema[tableId].ColDefs[colId].NotNull) || (v.NotNull == "REMOVED" && conv.SpSchema[tableId].ColDefs[colId].NotNull))

		var isSizeChange bool
		if v.MaxColLength != "" {
			var colMaxLength int64
			if strings.ToLower(v.MaxColLength) == "max" {
				colMaxLength = ddl.MaxLength
			} else {
				colMaxLength, _ = strconv.ParseInt(v.MaxColLength, 10, 64)
			}
			if conv.SpSchema[tableId].ColDefs[colId].T.Len != colMaxLength {
				isSizeChange = true
			}
		}
		if v.Removed || isRename || isTypeChange || isSizeChange || isNullChange {
			isModification = true
		}

		if isModification {
			isParent, _ := utilities.IsParent(tableId)
			isChild := conv.SpSchema[tableId].ParentTable.Id != ""

			// Rule 1: If it's a parent table, any change to a PK column is disallowed.
			if isParent {
				return fmt.Sprintf("Modifying primary key column '%s' is not allowed because table '%s' is a parent in an interleave relationship. Please remove the interleave relationship first.", conv.SpSchema[tableId].ColDefs[colId].Name, conv.SpSchema[tableId].Name)
			}

			// Rule 2: If it's a child table, check if the PK column is part of the parent's key.
			if isChild {
				parentTableId := conv.SpSchema[tableId].ParentTable.Id
				parentTable, parentExists := conv.SpSchema[parentTableId]
				if !parentExists {
					// This would be an inconsistent state, but handle it.
					return fmt.Sprintf("Internal server error: Parent table with ID %s not found for interleaved table %s", parentTableId, conv.SpSchema[tableId].Name)
				}
				numParentPKs := len(parentTable.PrimaryKeys)

				// If the column's order in the PK is within the count of parent PKs, it's an inherited key.
				if pkOrder != -1 && pkOrder <= numParentPKs {
					return fmt.Sprintf("Modifying column '%s' is not allowed because it is part of the interleaved primary key from parent table '%s'. Please remove the interleave relationship first.", conv.SpSchema[tableId].ColDefs[colId].Name, parentTable.Name)
				}
			}
		}
	}
	return ""
}

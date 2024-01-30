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
	"reflect"
	"strings"

	"time"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/utils"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/schema"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/session"
)

func InitObjectId() {

	sessionState := session.GetSessionState()
	sessionState.Counter.ObjectId = "0"
}

func GetSpannerUri(projectId string, instanceId string) string {
	return fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectId, instanceId, constants.METADATA_DB)
}

// DuplicateInArray checks if there is any duplicate element present in the list.
func DuplicateInArray(element []int) int {
	visited := make(map[int]bool, 0)
	for i := 0; i < len(element); i++ {
		if visited[element[i]] == true {
			return element[i]
		} else {
			visited[element[i]] = true
		}
	}
	return -1
}

// Difference gives list of element that are only present in first list.
func Difference(listone, listtwo []string) []string {

	hashmap := make(map[string]int, len(listtwo))

	for _, val := range listtwo {
		hashmap[val]++
	}

	var diff []string

	for _, val := range listone {

		_, found := hashmap[val]
		if !found {
			diff = append(diff, val)
		}
	}
	return diff
}

// IsColumnPresent check string is present in given list.
func IsColumnPresent(columns []string, col string) int {

	for i, c := range columns {
		if c == col {
			return i
		}
	}
	return -1
}

// RemoveSchemaIssue removes issue from the given list.
func RemoveSchemaIssue(schemaissue []internal.SchemaIssue, issue internal.SchemaIssue) []internal.SchemaIssue {

	k := 0
	for i := 0; i < len(schemaissue); {
		if schemaissue[i] != issue {
			schemaissue[k] = schemaissue[i]
			k++
		}
		i++
	}
	return schemaissue[0:k]
}

// IsSchemaIssuePresent checks if issue is present in the given schemaissue list.
func IsSchemaIssuePresent(schemaissue []internal.SchemaIssue, issue internal.SchemaIssue) bool {

	for _, s := range schemaissue {
		if s == issue {
			return true
		}
	}
	return false
}

// RemoveSchemaIssues remove all  hotspot and interleaved from given list.
// RemoveSchemaIssues is used when we are adding or removing primary key column from primary key.
func RemoveSchemaIssues(schemaissue []internal.SchemaIssue) []internal.SchemaIssue {

	switch {

	case IsSchemaIssuePresent(schemaissue, internal.HotspotAutoIncrement):
		schemaissue = RemoveSchemaIssue(schemaissue, internal.HotspotAutoIncrement)
		fallthrough

	case IsSchemaIssuePresent(schemaissue, internal.HotspotTimestamp):
		schemaissue = RemoveSchemaIssue(schemaissue, internal.HotspotTimestamp)
		fallthrough

	case IsSchemaIssuePresent(schemaissue, internal.UniqueIndexPrimaryKey):
		schemaissue = RemoveSchemaIssue(schemaissue, internal.UniqueIndexPrimaryKey)
		fallthrough

	case IsSchemaIssuePresent(schemaissue, internal.InterleavedOrder):
		schemaissue = RemoveSchemaIssue(schemaissue, internal.InterleavedOrder)

	case IsSchemaIssuePresent(schemaissue, internal.InterleavedNotInOrder):
		schemaissue = RemoveSchemaIssue(schemaissue, internal.InterleavedNotInOrder)
		fallthrough

	case IsSchemaIssuePresent(schemaissue, internal.InterleavedAddColumn):
		schemaissue = RemoveSchemaIssue(schemaissue, internal.InterleavedAddColumn)
		fallthrough

	case IsSchemaIssuePresent(schemaissue, internal.InterleavedRenameColumn):
		schemaissue = RemoveSchemaIssue(schemaissue, internal.InterleavedRenameColumn)
		fallthrough

	case IsSchemaIssuePresent(schemaissue, internal.InterleavedChangeColumnSize):
		schemaissue = RemoveSchemaIssue(schemaissue, internal.InterleavedChangeColumnSize)
	}

	return schemaissue
}

// RemoveIndex removes Primary Key from the given Primary Key list.
func RemoveIndex(PrimaryKeys []ddl.IndexKey, index int) []ddl.IndexKey {

	list := append(PrimaryKeys[:index], PrimaryKeys[index+1:]...)

	return list
}

// removeFkReferColumns remove given column from Spanner FkReferColumns Columns List.
func RemoveFkReferColumns(slice []string, s int) []string {
	return append(slice[:s], slice[s+1:]...)
}

func IsTypeChanged(newType, tableId, colId string, conv *internal.Conv) (bool, error) {

	sp, ty, err := GetType(conv, newType, tableId, colId)
	if err != nil {
		return false, err
	}
	colDef := sp.ColDefs[colId]
	return !reflect.DeepEqual(colDef.T, ty), nil
}

func IsPartOfPK(col, table string) bool {
	sessionState := session.GetSessionState()

	for _, pk := range sessionState.Conv.SpSchema[table].PrimaryKeys {
		if pk.ColId == col {
			return true
		}
	}
	return false
}

func IsPartOfSecondaryIndex(col, table string) (bool, string) {
	sessionState := session.GetSessionState()

	for _, index := range sessionState.Conv.SpSchema[table].Indexes {
		for _, key := range index.Keys {
			if key.ColId == col {
				return true, index.Name
			}
		}
	}
	return false, ""
}

func IsPartOfFK(col, table string) bool {
	sessionState := session.GetSessionState()

	for _, fk := range sessionState.Conv.SpSchema[table].ForeignKeys {
		for _, column := range fk.ColIds {
			if column == col {
				return true
			}
		}
	}
	return false
}

func IsReferencedByFK(col, table string) (bool, string) {
	sessionState := session.GetSessionState()

	for _, spSchema := range sessionState.Conv.SpSchema {
		if table != spSchema.Name {
			for _, fk := range spSchema.ForeignKeys {
				if fk.ReferTableId == table {
					for _, column := range fk.ReferColumnIds {
						if column == col {
							return true, spSchema.Name
						}
					}
				}
			}
		}
	}
	return false, ""
}

func Remove(slice []string, s int) []string {
	return append(slice[:s], slice[s+1:]...)
}

func RemovePk(slice []ddl.IndexKey, s int) []ddl.IndexKey {
	return append(slice[:s], slice[s+1:]...)
}

func RemoveFk(slice []ddl.Foreignkey, fkId string) []ddl.Foreignkey {
	pos := -1
	for i, fk := range slice {
		if fk.Id == fkId {
			pos = i
			break
		}
	}
	return append(slice[:pos], slice[pos+1:]...)
}

func RemoveSecondaryIndex(slice []ddl.CreateIndex, s int) []ddl.CreateIndex {
	return append(slice[:s], slice[s+1:]...)
}

// RemoveFkColumn remove given column from Spanner Foreignkey Columns List.
func RemoveFkColumn(slice []string, s int) []string {
	return append(slice[:s], slice[s+1:]...)
}

// RemoveColumnFromSecondaryIndexKey remove given column from SpannerSecondary Index Key List.
func RemoveColumnFromSecondaryIndexKey(slice []ddl.IndexKey, s int) []ddl.IndexKey {
	return append(slice[:s], slice[s+1:]...)
}

func CheckSpannerNamesValidity(input []string) (bool, []string) {
	status := true
	var invalidNewNames []string
	for _, changed := range input {
		if _, status := internal.FixName(changed); status {
			status = false
			invalidNewNames = append(invalidNewNames, changed)
		}
	}
	return status, invalidNewNames
}

func CanRename(names []string, table string) (bool, error) {
	sessionState := session.GetSessionState()
	for _, name := range names {
		if _, ok := sessionState.Conv.UsedNames[strings.ToLower(name)]; ok {
			return false, fmt.Errorf("new name : '%s' is used by another entity", name)
		}
	}
	return true, nil
}

func GetPrimaryKeyIndexFromOrder(pk []ddl.IndexKey, order int) int {

	for i := 0; i < len(pk); i++ {
		if pk[i].Order == order {
			return i
		}
	}
	return -1
}

func GetRefColIndexFromFk(fk ddl.Foreignkey, colId string) int {
	for i, id := range fk.ReferColumnIds {
		if colId == id {
			return i
		}
	}
	return -1
}

func GetFilePrefix(now time.Time) (string, error) {
	sessionState := session.GetSessionState()

	dbName := sessionState.DbName
	var err error
	if dbName == "" {
		g := utils.GetUtilInfo{}
		dbName, err = g.GetDatabaseName(sessionState.Driver, now)
		if err != nil {
			return "", fmt.Errorf("Can not create database name : %v", err)
		}
	}
	return dbName, nil
}

func UpdateDataType(conv *internal.Conv, newType, tableId, colId string) error {
	sp, ty, err := GetType(conv, newType, tableId, colId)
	if err != nil {
		return err
	}
	colDef := sp.ColDefs[colId]
	colDef.T = ty
	sp.ColDefs[colId] = colDef
	return nil
}

// Update the column length with the default mapping length in case its same as the length in the rule added
func updateColLen(conv *internal.Conv, dataType, tableId, colId string, spColLen int64) error {
	sp, ty, err := GetType(conv, dataType, tableId, colId)
	if err != nil {
		return err
	}
	colDef := sp.ColDefs[colId]
	if colDef.T.Len == spColLen {
		colDef.T.Len = ty.Len
		sp.ColDefs[colId] = colDef
	}
	return nil
}

func UpdateMaxColumnLen(conv *internal.Conv, dataType, tableId, colId string, spColLen int64) error {

	err := updateColLen(conv, dataType, tableId, colId, spColLen)
	if err != nil {
		return err
	}
	sp := conv.SpSchema[tableId]
	// update column size of child table.
	isParent, childTableId := IsParent(tableId)
	if isParent {
		childColId, err := GetColIdFromSpannerName(conv, childTableId, sp.ColDefs[colId].Name)
		if err == nil {
			err = updateColLen(conv, dataType, childTableId, childColId, spColLen)
			if err != nil {
				return err
			}
		}
	}

	// update column size of parent table.
	parentTableId := conv.SpSchema[tableId].ParentId
	if parentTableId != "" {
		parentColId, err := GetColIdFromSpannerName(conv, parentTableId, sp.ColDefs[colId].Name)
		if err == nil {
			err = updateColLen(conv, dataType, parentTableId, parentColId, spColLen)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func GetColIdFromSpannerName(conv *internal.Conv, tableId, colName string) (string, error) {
	for _, col := range conv.SpSchema[tableId].ColDefs {
		if col.Name == colName {
			return col.Id, nil
		}
	}
	return "", fmt.Errorf("column id not found for spaner column %v", colName)
}

func IsParent(tableId string) (bool, string) {
	sessionState := session.GetSessionState()

	for _, spSchema := range sessionState.Conv.SpSchema {
		if spSchema.ParentId == tableId {
			return true, spSchema.Id
		}
	}
	return false, ""
}

func GetInterleavedFk(conv *internal.Conv, tableId string, srcColId string) (schema.ForeignKey, error) {
	for _, fk := range conv.SrcSchema[tableId].ForeignKeys {
		for _, colId := range fk.ColIds {
			if srcColId == colId {
				return fk, nil
			}
		}
	}
	return schema.ForeignKey{}, fmt.Errorf("interleaved Foreign key not found")
}

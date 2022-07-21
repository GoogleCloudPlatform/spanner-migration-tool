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
	"context"
	"fmt"
	"net/http"
	"reflect"
	"regexp"
	"time"

	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"github.com/cloudspannerecosystem/harbourbridge/common/utils"
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
	"github.com/cloudspannerecosystem/harbourbridge/webv2/session"
	adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
)

const metadataDbName string = "harbourbridge_metadata"

func GetMetadataDbName() string {
	return metadataDbName
}

func GetSpannerUri(projectId string, instanceId string) string {
	return fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectId, instanceId, GetMetadataDbName())
}

func createDatabase(ctx context.Context, uri string) error {

	// Spanner uri will be in this format 'projects/project-id/instances/spanner-instance-id/databases/db-name'
	matches := regexp.MustCompile("^(.*)/databases/(.*)$").FindStringSubmatch(uri)
	spInstance := matches[1]
	dbName := matches[2]

	adminClient, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		return err
	}
	defer adminClient.Close()
	fmt.Println("Creating database to store session metadata...")

	op, err := adminClient.CreateDatabase(ctx, &adminpb.CreateDatabaseRequest{
		Parent:          spInstance,
		CreateStatement: "CREATE DATABASE `" + dbName + "`",
		ExtraStatements: []string{
			`CREATE TABLE SchemaConversionSession (
				VersionId STRING(36) NOT NULL,
				PreviousVersionId ARRAY<STRING(36)>,
				SessionName STRING(50) NOT NULL,
				EditorName STRING(100) NOT NULL,
				DatabaseType STRING(50) NOT NULL,
				DatabaseName STRING(50) NOT NULL,
				Notes ARRAY<STRING(MAX)> NOT NULL,
				Tags ARRAY<STRING(20)>,
				SchemaChanges STRING(MAX),
				SchemaConversionObject JSON NOT NULL,
				CreateTimestamp TIMESTAMP NOT NULL,
			  ) PRIMARY KEY(VersionId)`,
		},
	})
	if err != nil {
		return err
	}
	if _, err := op.Wait(ctx); err != nil {
		return err
	}

	fmt.Printf("Created database [%s]\n", matches[2])
	return nil
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
func IsColumnPresent(columns []string, col string) string {

	for _, c := range columns {
		if c == col {
			return col
		}
	}
	return ""
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

	case IsSchemaIssuePresent(schemaissue, internal.InterleavedOrder):
		schemaissue = RemoveSchemaIssue(schemaissue, internal.InterleavedOrder)

	case IsSchemaIssuePresent(schemaissue, internal.InterleavedNotInOrder):
		schemaissue = RemoveSchemaIssue(schemaissue, internal.InterleavedNotInOrder)
		fallthrough

	case IsSchemaIssuePresent(schemaissue, internal.InterleavedAddColumn):
		schemaissue = RemoveSchemaIssue(schemaissue, internal.InterleavedAddColumn)
	}

	return schemaissue
}

// RemoveIndex removes Primary Key from the given Primary Key list.
func RemoveIndex(Pks []ddl.IndexKey, index int) []ddl.IndexKey {

	list := append(Pks[:index], Pks[index+1:]...)

	return list
}

func IsTypeChanged(newType, table, colName string, Conv *internal.Conv) (bool, error) {

	//sessionState := session.GetSessionState()

	srcTableName := Conv.ToSource[table].Name

	sp, ty, err := GetType(newType, table, colName, srcTableName)
	if err != nil {
		return false, err
	}
	colDef := sp.ColDefs[colName]
	return !reflect.DeepEqual(colDef.T, ty), nil
}

func IsPartOfPK(col, table string) bool {
	sessionState := session.GetSessionState()

	for _, pk := range sessionState.Conv.SpSchema[table].Pks {
		if pk.Col == col {
			return true
		}
	}
	return false
}

func IsPartOfSecondaryIndex(col, table string) (bool, string) {
	sessionState := session.GetSessionState()

	for _, index := range sessionState.Conv.SpSchema[table].Indexes {
		for _, key := range index.Keys {
			if key.Col == col {
				return true, index.Name
			}
		}
	}
	return false, ""
}

func IsPartOfFK(col, table string) bool {
	sessionState := session.GetSessionState()

	for _, fk := range sessionState.Conv.SpSchema[table].Fks {
		for _, column := range fk.Columns {
			if column == col {
				return true
			}
		}
	}
	return false
}

// TODO: create a map to store referenced column to get
// this information in O(1).
// TODO:(searce) can have foreign key constraints between columns of the same table, as well as between same column on a given table.
func IsReferencedByFK(col, table string) (bool, string) {
	sessionState := session.GetSessionState()

	for _, spSchema := range sessionState.Conv.SpSchema {
		if table != spSchema.Name {
			for _, fk := range spSchema.Fks {
				if fk.ReferTable == table {
					for _, column := range fk.ReferColumns {
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

func RemoveFk(slice []ddl.Foreignkey, s int) []ddl.Foreignkey {
	return append(slice[:s], slice[s+1:]...)
}

func RemoveSecondaryIndex(slice []ddl.CreateIndex, s int) []ddl.CreateIndex {
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

	namesMap := map[string]bool{}
	// Check that this name isn't already used by another table.
	for _, name := range names {
		namesMap[name] = true
		if _, ok := sessionState.Conv.SpSchema[name]; ok {
			return false, fmt.Errorf("new name : '%s' is used by another table", name)
		}
	}

	// Check that this name isn't already used by another foreign key.
	for _, sp := range sessionState.Conv.SpSchema {
		for _, foreignKey := range sp.Fks {
			if _, ok := namesMap[foreignKey.Name]; ok {
				return false, fmt.Errorf("new name : '%s' is used by another foreign key in table : '%s'", foreignKey.Name, sp.Name)
			}

		}
	}

	// Check that this name isn't already used by another secondary index.
	for _, sp := range sessionState.Conv.SpSchema {
		for _, index := range sp.Indexes {
			if _, ok := namesMap[index.Name]; ok {
				return false, fmt.Errorf("new name : '%s' is used by another index in table : '%s'", index.Name, sp.Name)
			}
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

func IsUniqueName(name string) bool {
	sessionState := session.GetSessionState()

	for table := range sessionState.Conv.SpSchema {
		if table == name {
			return false
		}
	}
	for _, spSchema := range sessionState.Conv.SpSchema {
		for _, fk := range spSchema.Fks {
			if fk.Name == name {
				return false
			}
		}
		for _, index := range spSchema.Indexes {
			if index.Name == name {
				return false
			}
		}
	}
	return true
}

func GetFilePrefix(now time.Time) (string, error) {
	sessionState := session.GetSessionState()

	dbName := sessionState.DbName
	var err error
	if dbName == "" {
		dbName, err = utils.GetDatabaseName(sessionState.Driver, now)
		if err != nil {
			return "", fmt.Errorf("Can not create database name : %v", err)
		}
	}
	return dbName + ".", nil
}

func UpdateType(newType, table, colName, srcTableName string, w http.ResponseWriter) {
	sp, ty, err := GetType(newType, table, colName, srcTableName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	colDef := sp.ColDefs[colName]
	colDef.T = ty
	sp.ColDefs[colName] = colDef
}

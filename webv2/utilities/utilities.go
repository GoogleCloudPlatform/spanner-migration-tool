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

// Package web defines web APIs to be used with harbourbridge frontend.
// Apart from schema conversion, this package involves API to update
// converted schema.

package common

import (
	"context"
	"fmt"
	"regexp"

	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"github.com/cloudspannerecosystem/harbourbridge/conversion"
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
	adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
)

const metadataDbName string = "harbourbridge_metadata"

func GetMetadataDbName() string {
	return metadataDbName
}

func GetSpannerUri(projectId string, instanceId string) string {
	return fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectId, instanceId, GetMetadataDbName())
}

func CheckOrCreateMetadataDb(projectId string, instanceId string) bool {
	uri := GetSpannerUri(projectId, instanceId)
	if uri == "" {
		fmt.Println("Invalid spanner uri")
		return false
	}

	ctx := context.Background()
	adminClient, err := database.NewDatabaseAdminClient(ctx)
	defer adminClient.Close()

	dbExists, err := conversion.CheckExistingDb(ctx, adminClient, uri)
	if err != nil {
		fmt.Println(err)
		return false
	}
	if dbExists {
		return true
	}

	fmt.Println("No existing database found to store session metadata.")
	err = createDatabase(ctx, uri)
	if err != nil {
		fmt.Println(err)
		return false
	}
	return true
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

	for i := 0; i < len(schemaissue); i++ {
		if schemaissue[i] == issue {
			schemaissue = append(schemaissue[:i], schemaissue[i+1:]...)
		}
	}
	return schemaissue
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

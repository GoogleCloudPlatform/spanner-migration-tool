// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package dao

import (
	"context"
	"fmt"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/iterator"
)

func InsertSMTResourceEntry(ctx context.Context, resourceId, jobId, externalId, resourceName, resourceType string, resourceData spanner.NullJSON) error {
	_, err := GetClient().ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		resourceStmt := spanner.Statement{
			SQL: `INSERT INTO SMT_RESOURCE
			(ResourceId, JobId, ExternalId, ResourceName, ResourceType, ResourceStateData, ResourceData, CreatedAt, UpdatedAt)
			VALUES(
			 @resourceId, @jobId, @externalId, @resourceName, @resourceType, @resourceStateData, @resourceData, PENDING_COMMIT_TIMESTAMP(), PENDING_COMMIT_TIMESTAMP()
			);`,
			Params: map[string]interface{}{
				"resourceId":        resourceId,
				"jobId":             jobId,
				"externalId":        externalId,
				"resourceName":      resourceName,
				"resourceType":      resourceType,
				"resourceStateData": spanner.NullJSON{Valid: true, Value: StateData{State: "CREATING"}},
				"resourceData":      resourceData,
			},
		}
		_, err := txn.Update(ctx, resourceStmt)
		if err != nil {
			return err
		}
		_, err = updateResourceHistoryWithinTxn(ctx, txn, resourceId)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("error inserting smt resource entry: %v", err)
	}
	return nil
}

func updateResourceHistoryWithinTxn(ctx context.Context, txn *spanner.ReadWriteTransaction, resourceId string) (int64, error) {
	// Fetch the latest version for job.
	version, err := getLatestResourceVersionWithinTxn(ctx, txn, resourceId)
	if err != nil {
		return 0, fmt.Errorf("error fetching latest resource version: %v", err)
	}
	stmt := spanner.Statement{SQL: `
		SELECT 
			JobId, ExternalId, ResourceName, ResourceType, ResourceStateData, ResourceData 
		FROM SMT_RESOURCE WHERE ResourceId = @resourceId;`,
		Params: map[string]interface{}{"resourceId": resourceId},
	}
	iter := txn.Query(ctx, stmt)
	defer iter.Stop()
	var jobId, externalId, resourceName, resourceType spanner.NullString
	var resourceStateData, resourceData spanner.NullJSON
	row, err := iter.Next()
	if err == iterator.Done || err != nil {
		return 0, err
	}
	if err := row.Columns(&jobId, &externalId, &resourceName, &resourceType, &resourceStateData, &resourceData); err != nil {
		return 0, fmt.Errorf("error reading smt resource row: %v", err)
	}
	jobStmt := spanner.Statement{
		SQL: `INSERT INTO SMT_RESOURCE_HISTORY 
		(ResourceId, Version, JobId, ExternalId, ResourceName, ResourceType, ResourceStateData, ResourceData, CreatedAt)
		VALUES(
		 @resourceId, @version, @jobId, @externalId, @resourceName, @resourceType, @resourceStateData, @resourceData, PENDING_COMMIT_TIMESTAMP()
		);`,
		Params: map[string]interface{}{
			"resourceId":        resourceId,
			"version":           version + 1,
			"jobId":             jobId,
			"externalId":        externalId,
			"resourceName":      resourceName,
			"resourceType":      resourceType,
			"resourceStateData": resourceStateData,
			"resourceData":      resourceData,
		},
	}
	return txn.Update(ctx, jobStmt)
}

func getLatestResourceVersionWithinTxn(ctx context.Context, txn *spanner.ReadWriteTransaction, resourceId string) (int64, error) {
	// Fetch rows for the resource with latest version.
	stmt := spanner.Statement{SQL: `SELECT MAX(Version) FROM SMT_RESOURCE_HISTORY WHERE ResourceId = @resourceId;`,
		Params: map[string]interface{}{"resourceId": resourceId},
	}
	iter := txn.Query(ctx, stmt)
	defer iter.Stop()
	version := spanner.NullInt64{}
	row, err := iter.Next()
	if err == iterator.Done || err != nil {
		return 0, err
	}
	if err := row.Columns(&version); err != nil {
		return 0, err
	}
	if version.Valid {
		return version.Int64, nil
	}
	return 0, nil
}

func UpdateSMTResourceState(ctx context.Context, resourceId, state string) error {
	_, err := GetClient().ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		jobStmt := spanner.Statement{
			SQL: `UPDATE SMT_RESOURCE SET ResourceStateData = @resourceStateData, UpdatedAt = PENDING_COMMIT_TIMESTAMP()
			WHERE ResourceId = @resourceId;`,
			Params: map[string]interface{}{
				"resourceId":        resourceId,
				"resourceStateData": spanner.NullJSON{Valid: true, Value: StateData{State: state}},
			},
		}
		_, err := txn.Update(ctx, jobStmt)
		if err != nil {
			return err
		}
		_, err = updateResourceHistoryWithinTxn(ctx, txn, resourceId)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("error updating smt resource state: %v", err)
	}
	return nil
}

func UpdateSMTResourceExternalId(ctx context.Context, resourceId, externalId string) error {
	_, err := GetClient().ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		jobStmt := spanner.Statement{
			SQL: `UPDATE SMT_RESOURCE SET ExternalId = @externalId, UpdatedAt = PENDING_COMMIT_TIMESTAMP()
			WHERE ResourceId = @resourceId;`,
			Params: map[string]interface{}{
				"resourceId": resourceId,
				"externalId": externalId,
			},
		}
		_, err := txn.Update(ctx, jobStmt)
		if err != nil {
			return err
		}
		_, err = updateResourceHistoryWithinTxn(ctx, txn, resourceId)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("error updating smt resource external id: %v", err)
	}
	return nil
}

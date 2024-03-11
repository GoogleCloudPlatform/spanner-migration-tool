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

type StateData struct {
	State string `json:"state"`
}

type DAO interface {
	InsertJobEntry(ctx context.Context, jobId, jobName, jobType, dialect, dbName string, jobData spanner.NullJSON) error
	UpdateJobState(ctx context.Context, jobId, state string) error
	InsertResourceEntry(ctx context.Context, resourceId, jobId, externalId, resourceName, resourceType string, resourceData spanner.NullJSON) error
	UpdateResourceState(ctx context.Context, resourceId, state string) error
	UpdateResourceExternalId(ctx context.Context, resourceId, externalId string) error
}

type DAOImpl struct{}

// Insert a job entry into the SMT_JOB table.
func (dao *DAOImpl) InsertJobEntry(ctx context.Context, jobId, jobName, jobType, dialect, dbName string, jobData spanner.NullJSON) error {
	_, err := GetClient().ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		jobStmt := spanner.Statement{
			SQL: `INSERT INTO SMT_JOB 
			(JobId, JobName, JobType, JobStateData, JobData, Dialect, SpannerDatabaseName, CreatedAt, UpdatedAt)
			VALUES(
			 @jobId, @jobName, @jobType, @jobStateData, @jobData, @dialect, @dbName, PENDING_COMMIT_TIMESTAMP(), PENDING_COMMIT_TIMESTAMP()
			);`,
			Params: map[string]interface{}{
				"jobId":        jobId,
				"jobName":      jobName,
				"jobType":      jobType,
				"jobStateData": spanner.NullJSON{Valid: true, Value: StateData{State: "CREATING"}},
				"jobData":      jobData,
				"dialect":      dialect,
				"dbName":       dbName,
			},
		}
		_, err := txn.Update(ctx, jobStmt)
		if err != nil {
			return err
		}
		// Update job history table within the same txn.
		_, err = updateJobHistoryWithinTxn(ctx, txn, jobId)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("could not insert SMT job entry: %v", err)
	}
	return nil
}

// Update the state of the SMT job.
func (dao *DAOImpl) UpdateJobState(ctx context.Context, jobId, state string) error {
	_, err := GetClient().ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		jobStmt := spanner.Statement{
			SQL: `UPDATE SMT_JOB SET JobStateData = @jobStateData, UpdatedAt = PENDING_COMMIT_TIMESTAMP()
			WHERE JobId = @jobId;`,
			Params: map[string]interface{}{
				"jobId":        jobId,
				"jobStateData": spanner.NullJSON{Valid: true, Value: StateData{State: state}},
			},
		}
		_, err := txn.Update(ctx, jobStmt)
		if err != nil {
			return err
		}
		_, err = updateJobHistoryWithinTxn(ctx, txn, jobId)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("error updating smt job state: %v", err)
	}
	return nil
}

// Insert an entry into the SMT_RESOURCE table.
func (dao *DAOImpl) InsertResourceEntry(ctx context.Context, resourceId, jobId, externalId, resourceName, resourceType string, resourceData spanner.NullJSON) error {
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
		// Update the resource history table in the same transaction.
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

// Update the state of the SMT resource.
func (dao *DAOImpl) UpdateResourceState(ctx context.Context, resourceId, state string) error {
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

// Update the external of the SMT resource.
func (dao *DAOImpl) UpdateResourceExternalId(ctx context.Context, resourceId, externalId string) error {
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

func updateJobHistoryWithinTxn(ctx context.Context, txn *spanner.ReadWriteTransaction, jobId string) (int64, error) {
	// Fetch the newly updated row from SMT_JOB table.
	stmt := spanner.Statement{SQL: `
		SELECT 
			JobName, JobType, JobStateData, JobData, Dialect, SpannerDatabaseName 
		FROM SMT_JOB WHERE JobId = @jobId;`,
		Params: map[string]interface{}{"jobId": jobId},
	}
	iter := txn.Query(ctx, stmt)
	defer iter.Stop()
	var jobName, jobType, dialect, spannerDatabaseName spanner.NullString
	var jobStateData, jobData spanner.NullJSON
	row, err := iter.Next()
	if err == iterator.Done || err != nil {
		return 0, err
	}
	if err := row.Columns(&jobName, &jobType, &jobStateData, &jobData, &dialect, &spannerDatabaseName); err != nil {
		return 0, fmt.Errorf("error reading smt job row: %v", err)
	}

	// Insert entry to SMT_JOB_HISTORY table.
	jobStmt := spanner.Statement{
		SQL: `INSERT INTO SMT_JOB_HISTORY 
		(JobId, JobName, JobType, JobStateData, JobData, Dialect, SpannerDatabaseName, CreatedAt)
		VALUES(
		 @jobId, @jobName, @jobType, @jobStateData, @jobData, @dialect, @spannerDatabaseName, PENDING_COMMIT_TIMESTAMP()
		);`,
		Params: map[string]interface{}{
			"jobId":               jobId,
			"jobName":             jobName,
			"jobType":             jobType,
			"jobStateData":        jobStateData,
			"jobData":             jobData,
			"dialect":             dialect,
			"spannerDatabaseName": spannerDatabaseName,
		},
	}
	return txn.Update(ctx, jobStmt)
}

func updateResourceHistoryWithinTxn(ctx context.Context, txn *spanner.ReadWriteTransaction, resourceId string) (int64, error) {
	// Fetch the newly updated row from SMT_RESOURCE table.
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
	// Create new entry into the SMT_RESOURCE_HISTORY table.
	jobStmt := spanner.Statement{
		SQL: `INSERT INTO SMT_RESOURCE_HISTORY 
		(ResourceId, JobId, ExternalId, ResourceName, ResourceType, ResourceStateData, ResourceData, CreatedAt)
		VALUES(
		 @resourceId, @jobId, @externalId, @resourceName, @resourceType, @resourceStateData, @resourceData, PENDING_COMMIT_TIMESTAMP()
		);`,
		Params: map[string]interface{}{
			"resourceId":        resourceId,
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

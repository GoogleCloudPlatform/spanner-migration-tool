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

func InsertSMTJobEntry(ctx context.Context, jobId, jobName, jobType, dialect, dbName string, jobData spanner.NullJSON) error {
	_, err := GetClient().ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		jobStmt := spanner.Statement{
			SQL: `INSERT INTO SMT_JOB 
			(JobId, JobName, JobType, JobStateData, JobData, Dialect, SpannerDatabaseName, CreatedAt,UpdatedAt)
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

func updateJobHistoryWithinTxn(ctx context.Context, txn *spanner.ReadWriteTransaction, jobId string) (int64, error) {
	// Fetch the latest version for job.
	version, err := getLatestJobVersionWithinTxn(ctx, txn, jobId)
	if err != nil {
		return 0, fmt.Errorf("error fetching latest job version: %v", err)
	}
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

	jobStmt := spanner.Statement{
		SQL: `INSERT INTO SMT_JOB_HISTORY 
		(JobId, Version, JobName, JobType, JobStateData, JobData, Dialect, SpannerDatabaseName, CreatedAt)
		VALUES(
		 @jobId, @version, @jobName, @jobType, @jobStateData, @jobData, @dialect, @spannerDatabaseName, PENDING_COMMIT_TIMESTAMP()
		);`,
		Params: map[string]interface{}{
			"jobId":               jobId,
			"version":             version + 1,
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

func getLatestJobVersionWithinTxn(ctx context.Context, txn *spanner.ReadWriteTransaction, jobId string) (int64, error) {
	// Fetch rows for the resource with latest version.
	stmt := spanner.Statement{SQL: `SELECT MAX(Version) FROM SMT_JOB_HISTORY WHERE JobId = @jobId;`,
		Params: map[string]interface{}{"jobId": jobId},
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

func UpdateSMTJobState(ctx context.Context, jobId, state string) error {
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

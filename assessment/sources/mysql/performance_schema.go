// Copyright 2025 Google LLC
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

package mysql

import (
	"database/sql"
	"fmt"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/assessment/utils"
)

type PerformanceSchemaImpl struct {
	Db     *sql.DB
	DbName string
}

func (psi PerformanceSchemaImpl) GetAllQueryAssessments() ([]utils.QueryAssessmentInfo, error) {
	q := `SELECT
    DIGEST_TEXT,
    SUM(COUNT_STAR) AS total_count
FROM
    performance_schema.events_statements_summary_by_digest
WHERE
  SCHEMA_NAME = ?
  AND DIGEST_TEXT NOT LIKE 'COMMIT%'
  AND DIGEST_TEXT NOT LIKE 'ROLLBACK%'
  AND DIGEST_TEXT NOT LIKE 'SET%'
  AND DIGEST_TEXT NOT LIKE 'SHOW%'
  AND DIGEST_TEXT NOT LIKE 'PREPARE%'
  AND DIGEST_TEXT NOT LIKE 'EXECUTE%stmt%'
GROUP BY
    DIGEST_TEXT
ORDER BY
  total_count DESC;`
	rows, err := psi.Db.Query(q, psi.DbName)
	if err != nil {
		return nil, fmt.Errorf("couldn't read events_statements_summary_by_digest from performance schema : %s", err)
	}
	defer rows.Close()
	var digestText, errString string
	var totalCount int
	var queryInfo []utils.QueryAssessmentInfo
	for rows.Next() {
		if err := rows.Scan(&digestText, &totalCount); err != nil {
			errString = errString + fmt.Sprintf("Can't scan: %v", err)
			continue
		}
		queryInfo = append(queryInfo, utils.QueryAssessmentInfo{
			Query: digestText,
			Db: utils.DbIdentifier{
				DatabaseName: psi.DbName,
			},
			Count: totalCount,
		})
	}
	if errString != "" {
		return queryInfo, fmt.Errorf(errString)
	}
	return queryInfo, nil
}

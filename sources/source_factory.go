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

package sources

import (
	"database/sql"
	"fmt"
	"os"

	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/sources/common"
	"github.com/cloudspannerecosystem/harbourbridge/sources/mysql"
	"github.com/cloudspannerecosystem/harbourbridge/sources/postgres"
)

//TODO - move the db constants into this folder
func GetInfoSchema(driver string, conv *internal.Conv, db *sql.DB) (common.BaseInfoSchema, error) {
	switch driver {
	case "mysql":
		return mysql.MySQLInfoSchema{os.Getenv("MYSQLDATABASE")}, nil
	case "postgres":
		dbName, err := postgres.GetCurrentDbName(db)
		if err != nil {
			return nil, err
		}
		return postgres.PostgresInfoSchema{dbName}, nil
	default:
		return nil, fmt.Errorf("Could not set rows stats for '%s' driver", driver)
	}
}

func GetDbDump(driver string, conv *internal.Conv) (common.BaseDbDump, error) {
	switch driver {
	case "mysqldump":
		return mysql.MysqlDbDump{}, nil
	case "pg_dump":
		return postgres.PostgresDbDump{}, nil
	default:
		return nil, fmt.Errorf("process dump for driver %s not supported", driver)
	}
}

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

package profiles

import (
	"fmt"
	"os"
	"time"

	"github.com/cloudspannerecosystem/harbourbridge/common/constants"
	"github.com/cloudspannerecosystem/harbourbridge/common/utils"
	"golang.org/x/net/context"
	adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
)

type TargetProfileType int

const (
	TargetProfileTypeUnset = iota
	TargetProfileTypeConnection
)

type TargetProfileConnectionType int

const (
	TargetProfileConnectionTypeUnset = iota
	TargetProfileConnectionTypeSpanner
)

type TargetProfileConnectionSpanner struct {
	Endpoint string // Same as SPANNER_API_ENDPOINT environment variable
	Project  string // Same as GCLOUD_PROJECT environment variable
	Instance string
	Dbname   string
	Dialect  string
}

type TargetProfileConnection struct {
	Ty TargetProfileConnectionType
	Sp TargetProfileConnectionSpanner
}

type TargetProfile struct {
	TargetDb string
	Ty       TargetProfileType
	Conn     TargetProfileConnection
}

// ToLegacyTargetDb converts source-profile to equivalent legacy global flag
// -target-db etc since the rest of the codebase still uses the same.
// TODO: Deprecate this function and pass around TargetProfile across the
// codebase wherever information about target connection is required.
func (trg TargetProfile) ToLegacyTargetDb() string {
	switch trg.Ty {
	case TargetProfileTypeConnection:
		{
			conn := trg.Conn
			switch conn.Ty {
			case TargetProfileConnectionTypeSpanner:
				{
					sp := conn.Sp
					return utils.DialectToTarget(sp.Dialect)
				}
			default:
				return constants.TargetSpanner
			}
		}
	default:
		return constants.TargetSpanner
	}
}

// This expects that GetResourceIds has already been called once and the project, instance and dbName
// fields in target profile are populated.
func (trg TargetProfile) FetchTargetDialect(ctx context.Context) (string, error) {
	// TODO: consider moving all clients to target profile instead of passing them around the codebase.
	// Ideally we should use the client we create at the beginning, but we can fix that with the refactoring.
	adminClient, _ := utils.NewDatabaseAdminClient(ctx)
	// The parameters are irrelevant because the results are already cached when called the first time.
	project, instance, dbName, _ := trg.GetResourceIds(ctx, time.Now(), "", nil)
	result, err := adminClient.GetDatabase(ctx, &adminpb.GetDatabaseRequest{Name: fmt.Sprintf("projects/%s/instances/%s/databases/%s", project, instance, dbName)})
	if err != nil {
		return "", fmt.Errorf("cannot connect to target: %v", err)
	}
	return result.DatabaseDialect.String(), nil
}

func (targetProfile *TargetProfile) GetResourceIds(ctx context.Context, now time.Time, driverName string, out *os.File) (string, string, string, error) {
	var err error
	project := targetProfile.Conn.Sp.Project
	if project == "" {
		project, err = utils.GetProject()
		if err != nil {
			return "", "", "", fmt.Errorf("can't get project: %v", err)
		}
		targetProfile.Conn.Sp.Project = project
	}

	instance := targetProfile.Conn.Sp.Instance
	if instance == "" {
		instance, err = utils.GetInstance(ctx, project, out)
		if err != nil {
			return "", "", "", fmt.Errorf("can't get instance: %v", err)
		}
		targetProfile.Conn.Sp.Instance = instance
	}

	dbName := targetProfile.Conn.Sp.Dbname
	if dbName == "" {
		dbName, err = utils.GetDatabaseName(driverName, now)
		if err != nil {
			return "", "", "", fmt.Errorf("can't get database name: %v", err)
		}
		targetProfile.Conn.Sp.Dbname = dbName
	}
	return project, instance, dbName, err
}

// Target profile is passed as a list of key value pairs on the command line.
// Today we support only direct connection as a valid target profile type, but
// in future we can support writing to CSV or AVRO as valid targets.
//
// Among direct connection targets, today we only support Spanner database.
// TargetProfileConnectionType can be extended to add more databases.
// Users can specify the database dialect, instance, database name etc when
// connecting to Spanner.
//
// Database dialect can take 2 values: GoogleSQL or PostgreSQL and the same
// correspond to regular Cloud Spanner database and PG Cloud Spanner database
// respectively.
//
// If dbName is not specified, then HarbourBridge will autogenerate the same
// and create a database with the same name.
//
// Example: -target-profile="instance=my-instance1,dbName=my-new-db1"
// Example: -target-profile="instance=my-instance1,dbName=my-new-db1,dialect=PostgreSQL"
//
func NewTargetProfile(s string) (TargetProfile, error) {
	params, err := parseProfile(s)
	if err != nil {
		return TargetProfile{}, fmt.Errorf("could not parse target profile, error = %v", err)
	}

	sp := TargetProfileConnectionSpanner{}
	if endpoint, ok := params["endpoint"]; ok {
		sp.Endpoint = endpoint
	}
	if project, ok := params["project"]; ok {
		sp.Project = project
	}
	if instance, ok := params["instance"]; ok {
		sp.Instance = instance
	}
	if dbName, ok := params["dbName"]; ok {
		sp.Dbname = dbName
	}
	if dialect, ok := params["dialect"]; ok {
		sp.Dialect = dialect
	}

	conn := TargetProfileConnection{Ty: TargetProfileConnectionTypeSpanner, Sp: sp}
	return TargetProfile{Ty: TargetProfileTypeConnection, Conn: conn}, nil
}

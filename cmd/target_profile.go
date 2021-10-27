package cmd

import (
	"fmt"
	"strings"
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
	endpoint string // Same as SPANNER_API_ENDPOINT environment variable
	project  string // Same as GCLOUD_PROJECT environment variable
	instance string
	dbname   string
	dialect  string
}

type TargetProfileConnection struct {
	ty TargetProfileConnectionType
	sp TargetProfileConnectionSpanner
}

type TargetProfile struct {
	ty   TargetProfileType
	conn TargetProfileConnection
}

func (trg TargetProfile) ToLegacyTargetDb() string {
	switch trg.ty {
	case TargetProfileTypeConnection:
		{
			conn := trg.conn
			switch conn.ty {
			case TargetProfileConnectionTypeSpanner:
				{
					sp := conn.sp
					if len(sp.dialect) > 0 && strings.ToLower(sp.dialect) == "postgresql" {
						return "experimental_postgres"
					}
					return "spanner"
				}
			default:
				return "spanner"
			}
		}
	default:
		return "spanner"
	}
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
// If dbname is not specified, then HarbourBridge will autogenerate the same
// and create a database with the same name.
//
// Example: -target-profile="instance=my-instance1,dbname=my-new-db1"
// Example: -target-profile="instance=my-instance1,dbname=my-new-db1,dialect=PostgreSQL"
//
func NewTargetProfile(str string) (TargetProfile, error) {
	params := make(map[string]string)
	if len(str) > 0 {
		kvs := strings.Split(str, ",")
		for _, kv := range kvs {
			s := strings.Split(strings.TrimSpace(kv), "=")
			if len(s) != 2 {
				return TargetProfile{}, fmt.Errorf("invalid key, value in source profile (expected format: key1=value1): %v", kv)
			}
			if _, ok := params[s[0]]; ok {
				return TargetProfile{}, fmt.Errorf("duplicate key in source profile: %v", s[0])
			}
			params[s[0]] = s[1]
		}
	}

	sp := TargetProfileConnectionSpanner{}
	if endpoint, ok := params["endpoint"]; ok {
		sp.endpoint = endpoint
	}
	if project, ok := params["project"]; ok {
		sp.project = project
	}
	if instance, ok := params["instance"]; ok {
		sp.instance = instance
	}
	if dbname, ok := params["dbname"]; ok {
		sp.dbname = dbname
	}
	if dialect, ok := params["dialect"]; ok {
		sp.dialect = dialect
	}

	conn := TargetProfileConnection{ty: TargetProfileConnectionTypeSpanner, sp: sp}
	return TargetProfile{ty: TargetProfileTypeConnection, conn: conn}, nil
}

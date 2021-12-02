package cmd

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/cloudspannerecosystem/harbourbridge/conversion"
)

// Parses input string `s` as a map of key-value pairs. It's expected that the
// input string is of the form "key1=value1,key2=value2,..." etc. Return error
// otherwise.
func parseProfile(s string) (map[string]string, error) {
	params := make(map[string]string)
	if len(s) == 0 {
		return params, nil
	}

	// We use CSV reader to parse key=value pairs separated by a comma to
	// handle the case where a value may contain a comma within a quote. We
	// expect exactly one record to be returned.
	r := csv.NewReader(strings.NewReader(s))
	r.Comma = ','
	r.TrimLeadingSpace = true
	records, err := r.ReadAll()
	if err != nil {
		return params, err
	}
	if len(records) > 1 {
		return params, fmt.Errorf("contains invalid newline characters")
	}

	for _, kv := range records[0] {
		s := strings.Split(strings.TrimSpace(kv), "=")
		if len(s) != 2 {
			return params, fmt.Errorf("invalid key=value pair (expected format: key1=value1): %v", kv)
		}
		if _, ok := params[s[0]]; ok {
			return params, fmt.Errorf("duplicate key found: %v", s[0])
		}
		params[s[0]] = s[1]
	}
	return params, nil
}

func getResourceIds(ctx context.Context, targetProfile TargetProfile, now time.Time, driverName string, out *os.File) (string, string, string, error) {
	var err error
	project := targetProfile.conn.sp.project
	if project == "" {
		project, err = conversion.GetProject()
		if err != nil {
			return "", "", "", fmt.Errorf("can't get project: %v", err)
		}
	}
	fmt.Println("Using Google Cloud project:", project)

	instance := targetProfile.conn.sp.instance
	if instance == "" {
		instance, err = conversion.GetInstance(ctx, project, out)
		if err != nil {
			return "", "", "", fmt.Errorf("can't get instance: %v", err)
		}
	}
	fmt.Println("Using Cloud Spanner instance:", instance)
	conversion.PrintPermissionsWarning(driverName, out)

	dbName := targetProfile.conn.sp.dbname
	if dbName == "" {
		dbName, err = conversion.GetDatabaseName(driverName, now)
		if err != nil {
			return "", "", "", fmt.Errorf("can't get database name: %v", err)
		}
	}
	return project, instance, dbName, err
}

func getSQLConnectionStr(sourceProfile SourceProfile) string {
	sqlConnectionStr := ""
	if sourceProfile.ty == SourceProfileTypeConnection {
		switch sourceProfile.conn.ty {
		case SourceProfileConnectionTypeMySQL:
			connParams := sourceProfile.conn.mysql
			return conversion.GetMYSQLConnectionStr(connParams.host, connParams.port, connParams.user, connParams.pwd, connParams.db)
		case SourceProfileConnectionTypePostgreSQL:
			connParams := sourceProfile.conn.pg
			return conversion.GetPGSQLConnectionStr(connParams.host, connParams.port, connParams.user, connParams.pwd, connParams.db)
		case SourceProfileConnectionTypeSqlServer:
			connParams := sourceProfile.conn.sqlserver
			return conversion.GetSQLSERVERConnectionStr(connParams.host, connParams.port, connParams.user, connParams.pwd, connParams.db)
		case SourceProfileConnectionTypeDynamoDB:
			// For DynamoDB, client provided by aws-sdk reads connection credentials from env variables only.
			// Thus, there is no need to create sqlConnectionStr for the same. We instead set the env variables
			// programmatically if not set.
			return ""
		}
	}
	return sqlConnectionStr
}

func getSchemaSampleSize(sourceProfile SourceProfile) int64 {
	schemaSampleSize := int64(100000)
	if sourceProfile.ty == SourceProfileTypeConnection {
		if sourceProfile.conn.ty == SourceProfileConnectionTypeDynamoDB {
			if sourceProfile.conn.dydb.schemaSampleSize != 0 {
				schemaSampleSize = sourceProfile.conn.dydb.schemaSampleSize
			}
		}
	}
	return schemaSampleSize
}

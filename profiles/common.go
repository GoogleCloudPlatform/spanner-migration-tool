package profiles

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/cloudspannerecosystem/harbourbridge/common/utils"
	go_ora "github.com/sijms/go-ora/v2"
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

func GetResourceIds(ctx context.Context, targetProfile TargetProfile, now time.Time, driverName string, out *os.File) (string, string, string, error) {
	var err error
	project := targetProfile.Conn.Sp.Project
	if project == "" {
		project, err = utils.GetProject()
		if err != nil {
			return "", "", "", fmt.Errorf("can't get project: %v", err)
		}
	}
	fmt.Println("Using Google Cloud project:", project)

	instance := targetProfile.Conn.Sp.Instance
	if instance == "" {
		instance, err = utils.GetInstance(ctx, project, out)
		if err != nil {
			return "", "", "", fmt.Errorf("can't get instance: %v", err)
		}
	}
	fmt.Println("Using Cloud Spanner instance:", instance)
	utils.PrintPermissionsWarning(driverName, out)

	dbName := targetProfile.Conn.Sp.Dbname
	if dbName == "" {
		dbName, err = utils.GetDatabaseName(driverName, now)
		if err != nil {
			return "", "", "", fmt.Errorf("can't get database name: %v", err)
		}
	}
	return project, instance, dbName, err
}

func GetSQLConnectionStr(sourceProfile SourceProfile) string {
	sqlConnectionStr := ""
	if sourceProfile.Ty == SourceProfileTypeConnection {
		switch sourceProfile.Conn.Ty {
		case SourceProfileConnectionTypeMySQL:
			connParams := sourceProfile.Conn.Mysql
			return getMYSQLConnectionStr(connParams.Host, connParams.Port, connParams.User, connParams.Pwd, connParams.Db)
		case SourceProfileConnectionTypePostgreSQL:
			connParams := sourceProfile.Conn.Pg
			return getPGSQLConnectionStr(connParams.Host, connParams.Port, connParams.User, connParams.Pwd, connParams.Db)
		case SourceProfileConnectionTypeDynamoDB:
			// For DynamoDB, client provided by aws-sdk reads connection credentials from env variables only.
			// Thus, there is no need to create sqlConnectionStr for the same. We instead set the env variables
			// programmatically if not set.
			return ""
		case SourceProfileConnectionTypeSqlServer:
			connParams := sourceProfile.Conn.SqlServer
			return getSQLSERVERConnectionStr(connParams.Host, connParams.Port, connParams.User, connParams.Pwd, connParams.Db)
		case SourceProfileConnectionTypeOracle:
			connParams := sourceProfile.Conn.Oracle
			return getORACLEConnectionStr(connParams.Host, connParams.Port, connParams.User, connParams.Pwd, connParams.Db)
		}
	}
	return sqlConnectionStr
}

func GeneratePGSQLConnectionStr() (string, error) {
	server := os.Getenv("PGHOST")
	port := os.Getenv("PGPORT")
	user := os.Getenv("PGUSER")
	dbname := os.Getenv("PGDATABASE")
	if server == "" || port == "" || user == "" || dbname == "" {
		fmt.Printf("Please specify host, port, user and database using PGHOST, PGPORT, PGUSER and PGDATABASE environment variables\n")
		return "", fmt.Errorf("could not connect to source database")
	}
	password := os.Getenv("PGPASSWORD")
	if password == "" {
		password = utils.GetPassword()
	}
	return getPGSQLConnectionStr(server, port, user, password, dbname), nil
}

func getPGSQLConnectionStr(server, port, user, password, dbname string) string {
	return fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", server, port, user, password, dbname)
}

func GenerateMYSQLConnectionStr() (string, error) {
	server := os.Getenv("MYSQLHOST")
	port := os.Getenv("MYSQLPORT")
	user := os.Getenv("MYSQLUSER")
	dbname := os.Getenv("MYSQLDATABASE")
	if server == "" || port == "" || user == "" || dbname == "" {
		fmt.Printf("Please specify host, port, user and database using MYSQLHOST, MYSQLPORT, MYSQLUSER and MYSQLDATABASE environment variables\n")
		return "", fmt.Errorf("could not connect to source database")
	}
	password := os.Getenv("MYSQLPWD")
	if password == "" {
		password = utils.GetPassword()
	}
	return getMYSQLConnectionStr(server, port, user, password, dbname), nil
}

func getMYSQLConnectionStr(server, port, user, password, dbname string) string {
	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", user, password, server, port, dbname)
}

func getSQLSERVERConnectionStr(server, port, user, password, dbname string) string {
	return fmt.Sprintf(`sqlserver://%s:%s@%s:%s?database=%s`, user, password, server, port, dbname)
}

func GetSchemaSampleSize(sourceProfile SourceProfile) int64 {
	schemaSampleSize := int64(100000)
	if sourceProfile.Ty == SourceProfileTypeConnection {
		if sourceProfile.Conn.Ty == SourceProfileConnectionTypeDynamoDB {
			if sourceProfile.Conn.Dydb.SchemaSampleSize != 0 {
				schemaSampleSize = sourceProfile.Conn.Dydb.SchemaSampleSize
			}
		}
	}
	return schemaSampleSize
}

func getORACLEConnectionStr(server, port, user, password, dbname string) string {
	portNumber, _ := strconv.Atoi(port)
	return go_ora.BuildUrl(server, portNumber, dbname, user, password, nil)
}

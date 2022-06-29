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
	"strconv"
	"strings"

	"github.com/cloudspannerecosystem/harbourbridge/common/constants"
	"github.com/cloudspannerecosystem/harbourbridge/common/utils"
)

type SourceProfileType int

const (
	SourceProfileTypeUnset = iota
	SourceProfileTypeFile
	SourceProfileTypeConnection
	SourceProfileTypeConfig
	SourceProfileTypeCsv
)

type SourceProfileFile struct {
	Path   string
	Format string
}

func NewSourceProfileFile(params map[string]string) SourceProfileFile {
	profile := SourceProfileFile{}
	if !filePipedToStdin() {
		profile.Path = params["file"]
	}
	if format, ok := params["format"]; ok {
		profile.Format = format
		// TODO: Add check that format takes values from ["dump", "csv", "avro", ... etc]
	} else {
		fmt.Printf("source-profile format defaulting to `dump`\n")
		profile.Format = "dump"
	}
	return profile
}

type SourceProfileConnectionType int

const (
	SourceProfileConnectionTypeUnset = iota
	SourceProfileConnectionTypeMySQL
	SourceProfileConnectionTypePostgreSQL
	SourceProfileConnectionTypeDynamoDB
	SourceProfileConnectionTypeSqlServer
	SourceProfileConnectionTypeOracle
)

type SourceProfileConnectionMySQL struct {
	Host            string // Same as MYSQLHOST environment variable
	Port            string // Same as MYSQLPORT environment variable
	User            string // Same as MYSQLUSER environment variable
	Db              string // Same as MYSQLDATABASE environment variable
	Pwd             string // Same as MYSQLPWD environment variable
	StreamingConfig string
}

func NewSourceProfileConnectionMySQL(params map[string]string) (SourceProfileConnectionMySQL, error) {
	mysql := SourceProfileConnectionMySQL{}

	host, hostOk := params["host"]
	user, userOk := params["user"]
	db, dbOk := params["dbName"]
	port, portOk := params["port"]
	pwd, pwdOk := params["password"]

	streamingConfig, cfgOk := params["streamingCfg"]
	if cfgOk && streamingConfig == "" {
		return mysql, fmt.Errorf("specify a non-empty streaming config file path")
	}
	mysql.StreamingConfig = streamingConfig

	// We don't users to mix and match params from source-profile and environment variables.
	// We either try to get all params from the source-profile and if none are set, we read from the env variables.
	if !(hostOk || userOk || dbOk || portOk || pwdOk) {
		// No connection params provided through source-profile. Fetching from env variables.
		fmt.Printf("Connection parameters not specified in source-profile. Reading from " +
			"environment variables MYSQLHOST, MYSQLUSER, MYSQLDATABASE, MYSQLPORT, MYSQLPWD...\n")
		mysql.Host = os.Getenv("MYSQLHOST")
		mysql.User = os.Getenv("MYSQLUSER")
		mysql.Db = os.Getenv("MYSQLDATABASE")
		mysql.Port = os.Getenv("MYSQLPORT")
		mysql.Pwd = os.Getenv("MYSQLPWD")
		// Throw error if the input entered is empty.
		if mysql.Host == "" || mysql.User == "" || mysql.Db == "" {
			return mysql, fmt.Errorf("found empty string for MYSQLHOST/MYSQLUSER/MYSQLDATABASE. Please specify these environment variables with correct values")
		}
	} else if hostOk && userOk && dbOk {
		// If atleast host, username and dbName are provided through source-profile,
		// go ahead and use source-profile. Port and password handled later even if they are empty.
		mysql.Host, mysql.User, mysql.Db, mysql.Port, mysql.Pwd = host, user, db, port, pwd
		// Throw error if the input entered is empty.
		if mysql.Host == "" || mysql.User == "" || mysql.Db == "" {
			return mysql, fmt.Errorf("found empty string for host/user/dbName. Please specify host, port, user and dbName in the source-profile")
		}
	} else {
		// Partial params provided through source-profile. Ask user to provide all through the source-profile.
		return mysql, fmt.Errorf("please specify host, port, user and dbName in the source-profile")
	}

	// Throw same error if the input entered is empty.
	if mysql.Host == "" || mysql.User == "" || mysql.Db == "" {
		return mysql, fmt.Errorf("found empty string for host/user/db. please specify host, port, user and dbName in the source-profile")
	}

	if mysql.Port == "" {
		// Set default port for mysql, which rarely changes.
		mysql.Port = "3306"
	}
	if mysql.Pwd == "" {
		mysql.Pwd = utils.GetPassword()
	}

	return mysql, nil
}

type SourceProfileConnectionPostgreSQL struct {
	Host string // Same as PGHOST environment variable
	Port string // Same as PGPORT environment variable
	User string // Same as PGUSER environment variable
	Db   string // Same as PGDATABASE environment variable
	Pwd  string // Same as PGPASSWORD environment variable
}

func NewSourceProfileConnectionPostgreSQL(params map[string]string) (SourceProfileConnectionPostgreSQL, error) {
	pg := SourceProfileConnectionPostgreSQL{}
	host, hostOk := params["host"]
	user, userOk := params["user"]
	db, dbOk := params["dbName"]
	port, portOk := params["port"]
	pwd, pwdOk := params["password"]
	// We don't users to mix and match params from source-profile and environment variables.
	// We either try to get all params from the source-profile and if none are set, we read from the env variables.
	if !(hostOk || userOk || dbOk || portOk || pwdOk) {
		// No connection params provided through source-profile. Fetching from env variables.
		fmt.Printf("Connection parameters not specified in source-profile. Reading from " +
			"environment variables PGHOST, PGUSER, PGDATABASE, PGPORT, PGPASSWORD...\n")
		pg.Host = os.Getenv("PGHOST")
		pg.User = os.Getenv("PGUSER")
		pg.Db = os.Getenv("PGDATABASE")
		pg.Port = os.Getenv("PGPORT")
		pg.Pwd = os.Getenv("PGPASSWORD")
		// Throw error if the input entered is empty.
		if pg.Host == "" || pg.User == "" || pg.Db == "" {
			return pg, fmt.Errorf("found empty string for PGHOST/PGUSER/PGDATABASE. Please specify these environment variables with correct values")
		}
	} else if hostOk && userOk && dbOk {
		// All connection params provided through source-profile. Port and password handled later.
		pg.Host, pg.User, pg.Db, pg.Port, pg.Pwd = host, user, db, port, pwd
		// Throw error if the input entered is empty.
		if pg.Host == "" || pg.User == "" || pg.Db == "" {
			return pg, fmt.Errorf("found empty string for host/user/dbName. Please specify host, port, user and dbName in the source-profile")
		}
	} else {
		// Partial params provided through source-profile. Ask user to provide all through the source-profile.
		return pg, fmt.Errorf("please specify host, port, user and dbName in the source-profile")
	}

	if pg.Port == "" {
		// Set default port for postgresql, which rarely changes.
		pg.Port = "5432"
	}
	if pg.Pwd == "" {
		pg.Pwd = utils.GetPassword()
	}

	return pg, nil
}

type SourceProfileConnectionSqlServer struct {
	Host string
	Port string
	User string
	Db   string
	Pwd  string
}

func NewSourceProfileConnectionSqlServer(params map[string]string) (SourceProfileConnectionSqlServer, error) {
	ss := SourceProfileConnectionSqlServer{}
	host, hostOk := params["host"]
	user, userOk := params["user"]
	db, dbOk := params["dbName"]
	port, portOk := params["port"]
	pwd, pwdOk := params["password"]

	// We don't allow users to mix and match params from source-profile and environment variables.
	// We either try to get all params from the source-profile and if none are set, we read from the env variables.
	if !(hostOk || userOk || dbOk || portOk || pwdOk) {
		// No connection params provided through source-profile. Fetching from env variables.
		fmt.Printf("Connection parameters not specified in source-profile. Reading from " +
			"environment variables MSSQL_IP_ADDRESS, MSSQL_USER, MSSQL_DATABASE, MSSQL_TCP_PORT, MSSQL_SA_PASSWORD...\n")
		ss.Host = os.Getenv("MSSQL_IP_ADDRESS") //For default SQL Server instances.
		ss.Port = os.Getenv("MSSQL_TCP_PORT")
		ss.Pwd = os.Getenv("MSSQL_SA_PASSWORD")

		ss.Db = os.Getenv("MSSQL_DATABASE")  //Non standard env variable. Defined for HarbourBridge.
		ss.User = os.Getenv("MSSQL_SA_USER") //Non standard env variable. Defined for HarbourBridge.
		if ss.User == "" {
			fmt.Printf("MSSQL_SA_USER environment variable is not set. Default admin user 'SA' will be used for further processing.\n")
			ss.User = "SA"
		}
		// Throw error if the input entered is empty.
		if ss.Host == "" || ss.Db == "" {
			return ss, fmt.Errorf("found empty string for MSSQL_IP_ADDRESS/MSSQL_DATABASE. Please specify these environment variables with correct values")
		}
	} else if hostOk && userOk && dbOk {
		// All connection params provided through source-profile. Port and password handled later.
		ss.Host, ss.User, ss.Db, ss.Port, ss.Pwd = host, user, db, port, pwd
		// Throw error if the input entered is empty.
		if ss.Host == "" || ss.User == "" || ss.Db == "" {
			return ss, fmt.Errorf("found empty string for host/user/dbName. Please specify host, port, user and dbName in the source-profile")
		}
	} else {
		// Partial params provided through source-profile. Ask user to provide all through the source-profile.
		return ss, fmt.Errorf("please specify host, port, user and dbName in the source-profile")
	}

	if ss.Port == "" {
		// Set default port for sql server, which rarely changes.
		ss.Port = "1433"
	}

	// Try to get admin password from env
	if saPas := os.Getenv("MSSQL_SA_PASSWORD"); saPas != "" {
		ss.Pwd = saPas
	}

	// If source profile and env do not have password then get password via prompt.
	if ss.Pwd == "" {
		ss.Pwd = utils.GetPassword()
	}

	return ss, nil
}

type SourceProfileConnectionDynamoDB struct {
	// These connection params are not used currently because the SDK reads directly from the env variables.
	// These are still kept around as reference when we refactor passing
	// SourceProfile instead of sqlConnectionStr around.
	AwsAccessKeyID     string // Same as AWS_ACCESS_KEY_ID environment variable
	AwsSecretAccessKey string // Same as AWS_SECRET_ACCESS_KEY environment variable
	AwsRegion          string // Same as AWS_REGION environment variable
	DydbEndpoint       string // Same as DYNAMODB_ENDPOINT_OVERRIDE environment variable
	SchemaSampleSize   int64  // Number of rows to use for inferring schema (default 100,000)
	Streaming          string // Confirmation for streaming migration (required yes)
}

func NewSourceProfileConnectionDynamoDB(params map[string]string) (SourceProfileConnectionDynamoDB, error) {
	dydb := SourceProfileConnectionDynamoDB{}
	if schemaSampleSize, ok := params["schema-sample-size"]; ok {
		schemaSampleSizeInt, err := strconv.Atoi(schemaSampleSize)
		if err != nil {
			return dydb, fmt.Errorf("could not parse schema-sample-size = %v as a valid int64", schemaSampleSize)
		}
		dydb.SchemaSampleSize = int64(schemaSampleSizeInt)
	}
	// For DynamoDB, the preferred way to provide connection params is through env variables.
	// Unlike postgres and mysql, there may not be deprecation of env variables, hence it
	// is better to override env variables optionally via source profile params.
	var ok bool
	if dydb.AwsAccessKeyID, ok = params["aws-access-key-id"]; ok {
		os.Setenv("AWS_ACCESS_KEY_ID", dydb.AwsAccessKeyID)
	}
	if dydb.AwsSecretAccessKey, ok = params["aws-secret-access-key"]; ok {
		os.Setenv("AWS_SECRET_ACCESS_KEY", dydb.AwsSecretAccessKey)
	}
	if dydb.AwsRegion, ok = params["aws-region"]; ok {
		os.Setenv("AWS_REGION", dydb.AwsRegion)
	}
	if dydb.DydbEndpoint, ok = params["dydb-endpoint"]; ok {
		os.Setenv("DYNAMODB_ENDPOINT_OVERRIDE", dydb.DydbEndpoint)
	}
	streaming, ok := params["streaming"]
	if ok {
		dydb.Streaming = streaming
	}
	return dydb, nil
}

type SourceProfileConnectionOracle struct {
	Host            string
	Port            string
	User            string
	Db              string
	Pwd             string
	StreamingConfig string
}

func NewSourceProfileConnectionOracle(params map[string]string) (SourceProfileConnectionOracle, error) {
	ss := SourceProfileConnectionOracle{}
	host, hostOk := params["host"]
	user, userOk := params["user"]
	db, dbOk := params["dbName"]
	port, _ := params["port"]
	pwd, _ := params["password"]

	streamingConfig, cfgOk := params["streamingCfg"]
	if cfgOk && streamingConfig == "" {
		return ss, fmt.Errorf("specify a non-empty streaming config file path")
	}
	ss.StreamingConfig = streamingConfig

	if hostOk && userOk && dbOk {
		// All connection params provided through source-profile. Port and password handled later.
		ss.Host, ss.User, ss.Db, ss.Port, ss.Pwd = host, user, db, port, pwd
		// Throw error if the input entered is empty.
		if ss.Host == "" || ss.User == "" || ss.Db == "" {
			return ss, fmt.Errorf("found empty string for host/user/dbName. Please specify host, port, user and dbName in the source-profile")
		}
	} else {
		// Partial params provided through source-profile. Ask user to provide all through the source-profile.
		return ss, fmt.Errorf("please specify host, port, user and dbName in the source-profile")
	}

	if ss.Port == "" {
		// Set default port for oracle, which rarely changes.
		ss.Port = "1521"
	}
	if ss.Pwd == "" {
		ss.Pwd = utils.GetPassword()
	}

	return ss, nil
}

type SourceProfileConnection struct {
	Ty        SourceProfileConnectionType
	Streaming bool
	Mysql     SourceProfileConnectionMySQL
	Pg        SourceProfileConnectionPostgreSQL
	Dydb      SourceProfileConnectionDynamoDB
	SqlServer SourceProfileConnectionSqlServer
	Oracle    SourceProfileConnectionOracle
}

func NewSourceProfileConnection(source string, params map[string]string) (SourceProfileConnection, error) {
	conn := SourceProfileConnection{}
	var err error
	switch strings.ToLower(source) {
	case "mysql":
		{
			conn.Ty = SourceProfileConnectionTypeMySQL
			conn.Mysql, err = NewSourceProfileConnectionMySQL(params)
			if err != nil {
				return conn, err
			}
			if conn.Mysql.StreamingConfig != "" {
				conn.Streaming = true
			}
		}
	case "postgresql", "postgres", "pg":
		{
			conn.Ty = SourceProfileConnectionTypePostgreSQL
			conn.Pg, err = NewSourceProfileConnectionPostgreSQL(params)
			if err != nil {
				return conn, err
			}
		}
	case "dynamodb":
		{
			conn.Ty = SourceProfileConnectionTypeDynamoDB
			conn.Dydb, err = NewSourceProfileConnectionDynamoDB(params)
			if err != nil {
				return conn, err
			}
			if conn.Dydb.Streaming == "yes" {
				conn.Streaming = true
			}
		}

	case "sqlserver", "mssql":
		{
			conn.Ty = SourceProfileConnectionTypeSqlServer
			conn.SqlServer, err = NewSourceProfileConnectionSqlServer(params)
			if err != nil {
				return conn, err
			}
		}
	case "oracle":
		{
			conn.Ty = SourceProfileConnectionTypeOracle
			conn.Oracle, err = NewSourceProfileConnectionOracle(params)
			if err != nil {
				return conn, err
			}
			if conn.Oracle.StreamingConfig != "" {
				conn.Streaming = true
			}
		}
	default:
		return conn, fmt.Errorf("please specify a valid source database using -source flag, received source = %v", source)
	}
	return conn, nil
}

type SourceProfileConfig struct {
	path string
}

func NewSourceProfileConfig(path string) SourceProfileConfig {
	return SourceProfileConfig{path: path}
}

type SourceProfileCsv struct {
	Manifest  string
	Delimiter string
	NullStr   string
}

func NewSourceProfileCsv(params map[string]string) SourceProfileCsv {
	csvProfile := SourceProfileCsv{}
	csvProfile.Manifest = params["manifest"]
	csvProfile.Delimiter = ","
	csvProfile.NullStr = ""
	if delimiter, ok := params["delimiter"]; ok {
		csvProfile.Delimiter = delimiter
	}
	if nullStr, ok := params["nullStr"]; ok {
		csvProfile.NullStr = nullStr
	}
	return csvProfile
}

type SourceProfile struct {
	Driver string
	Ty     SourceProfileType
	File   SourceProfileFile
	Conn   SourceProfileConnection
	Config SourceProfileConfig
	Csv    SourceProfileCsv
}

// UseTargetSchema returns true if the driver expects an existing schema
// to use in the target database.
func (src SourceProfile) UseTargetSchema() bool {
	return (src.Driver == constants.CSV)
}

// ToLegacyDriver converts source-profile to equivalent legacy global flags
// e.g., -driver, -dump-file etc since the rest of the codebase still uses the
// same. TODO: Deprecate this function and pass around SourceProfile across the
// codebase wherever information about source connection is required.
func (src SourceProfile) ToLegacyDriver(source string) (string, error) {
	switch src.Ty {
	case SourceProfileTypeFile:
		{
			switch strings.ToLower(source) {
			case "mysql":
				return constants.MYSQLDUMP, nil
			case "postgresql", "postgres", "pg":
				return constants.PGDUMP, nil
			case "dynamodb":
				return "", fmt.Errorf("dump files are not supported with DynamoDB")
			default:
				return "", fmt.Errorf("please specify a valid source database using -source flag, received source = %v", source)
			}
		}
	// No need to handle unsupported streaming source specified as it is already covered during source profile creation.
	case SourceProfileTypeConnection:
		{
			switch strings.ToLower(source) {
			case "mysql":
				return constants.MYSQL, nil
			case "postgresql", "postgres", "pg":
				return constants.POSTGRES, nil
			case "dynamodb":
				return constants.DYNAMODB, nil
			case "sqlserver", "mssql":
				return constants.SQLSERVER, nil
			case "oracle":
				return constants.ORACLE, nil
			default:
				return "", fmt.Errorf("please specify a valid source database using -source flag, received source = %v", source)
			}
		}
	case SourceProfileTypeConfig:
		return "", fmt.Errorf("specifying source-profile using config not implemented")
	case SourceProfileTypeCsv:
		return constants.CSV, nil
	default:
		return "", fmt.Errorf("invalid source-profile, could not infer type")
	}
}

// Flag source-profile is passed as a list of key value pairs on the command line.
// Following 3 formats are supported as a valid source-profile.
//
// Format 1. Specify file path and file format.
// File path can be a local file path or a gcs file path. Support for more file
// path types can be added in future.
// File format can be "dump" e.g., when specifying a mysqldump or pgdump etc.
// Support for more formats e.g., "csv", "avro" etc can be added in future.
//
// Example: -source-profile="file=/tmp/abc, format=dump"
// Example: -source-profile="file=gcs://bucket_name/cart.txt, format=dump"
//
// Format 2. Specify source connection parameters. If none specified, then read
// from envrironment variables.
//
// Format 3. Specify a config file that specifies source connection profile.
//
func NewSourceProfile(s string, source string) (SourceProfile, error) {
	if source == "" {
		return SourceProfile{}, fmt.Errorf("cannot leave -source flag empty, please specify source databases e.g., -source=postgres etc")
	}
	params, err := parseProfile(s)
	if err != nil {
		return SourceProfile{}, fmt.Errorf("could not parse source-profile, error = %v", err)
	}
	if strings.ToLower(source) == constants.CSV {
		return SourceProfile{Ty: SourceProfileTypeCsv, Csv: NewSourceProfileCsv(params)}, nil
	}

	if _, ok := params["file"]; ok || filePipedToStdin() {
		profile := NewSourceProfileFile(params)
		return SourceProfile{Ty: SourceProfileTypeFile, File: profile}, nil
	} else if format, ok := params["format"]; ok {
		// File is not passed in from stdin or specified using "file" flag.
		return SourceProfile{Ty: SourceProfileTypeFile}, fmt.Errorf("file not specified, but format set to %v", format)
	} else if file, ok := params["config"]; ok {
		config := NewSourceProfileConfig(file)
		return SourceProfile{Ty: SourceProfileTypeConfig, Config: config}, fmt.Errorf("source-profile type config not yet implemented")
	} else {
		// Assume connection profile type connection by default, since
		// connection parameters could be specified as part of environment
		// variables.
		conn, err := NewSourceProfileConnection(source, params)
		return SourceProfile{Ty: SourceProfileTypeConnection, Conn: conn}, err
	}
}

var filePipedToStdin = func() bool {
	stat, _ := os.Stdin.Stat()
	// Data is being piped to stdin, if true. Else, stdin is from a terminal.
	return (stat.Mode() & os.ModeCharDevice) == 0
}

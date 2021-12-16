package cmd

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/cloudspannerecosystem/harbourbridge/common/constants"
	"github.com/cloudspannerecosystem/harbourbridge/conversion"
)

type SourceProfileType int

const (
	SourceProfileTypeUnset = iota
	SourceProfileTypeFile
	SourceProfileTypeConnection
	SourceProfileTypeConfig
)

type SourceProfileFile struct {
	path   string
	format string
}

func NewSourceProfileFile(params map[string]string) SourceProfileFile {
	profile := SourceProfileFile{}
	if !filePipedToStdin() {
		profile.path = params["file"]
	}
	if format, ok := params["format"]; ok {
		profile.format = format
		// TODO: Add check that format takes values from ["dump", "csv", "avro", ... etc]
	} else {
		fmt.Printf("source-profile format defaulting to `dump`\n")
		profile.format = "dump"
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
)

type SourceProfileConnectionMySQL struct {
	host string // Same as MYSQLHOST environment variable
	port string // Same as MYSQLPORT environment variable
	user string // Same as MYSQLUSER environment variable
	db   string // Same as MYSQLDATABASE environment variable
	pwd  string // Same as MYSQLPWD environment variable
}

func NewSourceProfileConnectionMySQL(params map[string]string) (SourceProfileConnectionMySQL, error) {
	mysql := SourceProfileConnectionMySQL{}
	host, hostOk := params["host"]
	user, userOk := params["user"]
	db, dbOk := params["db_name"]
	port, portOk := params["port"]
	pwd, pwdOk := params["password"]
	// We don't users to mix and match params from source-profile and environment variables.
	// We either try to get all params from the source-profile and if none are set, we read from the env variables.
	if !(hostOk || userOk || dbOk || portOk || pwdOk) {
		// No connection params provided through source-profile. Fetching from env variables.
		fmt.Printf("Connection parameters not specified in source-profile. Reading from " +
			"environment variables MYSQLHOST, MYSQLUSER, MYSQLDATABASE, MYSQLPORT, MYSQLPWD...\n")
		mysql.host = os.Getenv("MYSQLHOST")
		mysql.user = os.Getenv("MYSQLUSER")
		mysql.db = os.Getenv("MYSQLDATABASE")
		mysql.port = os.Getenv("MYSQLPORT")
		mysql.pwd = os.Getenv("MYSQLPWD")
		// Throw error if the input entered is empty.
		if mysql.host == "" || mysql.user == "" || mysql.db == "" {
			return mysql, fmt.Errorf("found empty string for MYSQLHOST/MYSQLUSER/MYSQLDATABASE. Please specify these environment variables with correct values")
		}
	} else if hostOk && userOk && dbOk {
		// If atleast host, username and dbname are provided through source-profile,
		// go ahead and use source-profile. Port and password handled later even if they are empty.
		mysql.host, mysql.user, mysql.db, mysql.port, mysql.pwd = host, user, db, port, pwd
		// Throw error if the input entered is empty.
		if mysql.host == "" || mysql.user == "" || mysql.db == "" {
			return mysql, fmt.Errorf("found empty string for host/user/db_name. Please specify host, port, user and db_name in the source-profile")
		}
	} else {
		// Partial params provided through source-profile. Ask user to provide all through the source-profile.
		return mysql, fmt.Errorf("please specify host, port, user and db_name in the source-profile")
	}

	// Throw same error if the input entered is empty.
	if mysql.host == "" || mysql.user == "" || mysql.db == "" {
		return mysql, fmt.Errorf("found empty string for host/user/db. please specify host, port, user and db_name in the source-profile")
	}

	if mysql.port == "" {
		// Set default port for mysql, which rarely changes.
		mysql.port = "3306"
	}
	if mysql.pwd == "" {
		mysql.pwd = conversion.GetPassword()
	}

	return mysql, nil
}

type SourceProfileConnectionPostgreSQL struct {
	host string // Same as PGHOST environment variable
	port string // Same as PGPORT environment variable
	user string // Same as PGUSER environment variable
	db   string // Same as PGDATABASE environment variable
	pwd  string // Same as PGPASSWORD environment variable
}

func NewSourceProfileConnectionPostgreSQL(params map[string]string) (SourceProfileConnectionPostgreSQL, error) {
	pg := SourceProfileConnectionPostgreSQL{}
	host, hostOk := params["host"]
	user, userOk := params["user"]
	db, dbOk := params["db_name"]
	port, portOk := params["port"]
	pwd, pwdOk := params["password"]
	// We don't users to mix and match params from source-profile and environment variables.
	// We either try to get all params from the source-profile and if none are set, we read from the env variables.
	if !(hostOk || userOk || dbOk || portOk || pwdOk) {
		// No connection params provided through source-profile. Fetching from env variables.
		fmt.Printf("Connection parameters not specified in source-profile. Reading from " +
			"environment variables PGHOST, PGUSER, PGDATABASE, PGPORT, PGPASSWORD...\n")
		pg.host = os.Getenv("PGHOST")
		pg.user = os.Getenv("PGUSER")
		pg.db = os.Getenv("PGDATABASE")
		pg.port = os.Getenv("PGPORT")
		pg.pwd = os.Getenv("PGPASSWORD")
		// Throw error if the input entered is empty.
		if pg.host == "" || pg.user == "" || pg.db == "" {
			return pg, fmt.Errorf("found empty string for PGHOST/PGUSER/PGDATABASE. Please specify these environment variables with correct values")
		}
	} else if hostOk && userOk && dbOk {
		// All connection params provided through source-profile. Port and password handled later.
		pg.host, pg.user, pg.db, pg.port, pg.pwd = host, user, db, port, pwd
		// Throw error if the input entered is empty.
		if pg.host == "" || pg.user == "" || pg.db == "" {
			return pg, fmt.Errorf("found empty string for host/user/db_name. Please specify host, port, user and db_name in the source-profile")
		}
	} else {
		// Partial params provided through source-profile. Ask user to provide all through the source-profile.
		return pg, fmt.Errorf("please specify host, port, user and db_name in the source-profile")
	}

	if pg.port == "" {
		// Set default port for postgresql, which rarely changes.
		pg.port = "5432"
	}
	if pg.pwd == "" {
		pg.pwd = conversion.GetPassword()
	}

	return pg, nil
}

type SourceProfileConnectionSqlServer struct {
	host string
	port string
	user string
	db   string
	pwd  string
}

func NewSourceProfileConnectionSqlServer(params map[string]string) (SourceProfileConnectionSqlServer, error) {
	ss := SourceProfileConnectionSqlServer{}
	host, hostOk := params["host"]
	user, userOk := params["user"]
	db, dbOk := params["db_name"]
	port, _ := params["port"]
	pwd, _ := params["password"]

	if hostOk && userOk && dbOk {
		// All connection params provided through source-profile. Port and password handled later.
		ss.host, ss.user, ss.db, ss.port, ss.pwd = host, user, db, port, pwd
		// Throw error if the input entered is empty.
		if ss.host == "" || ss.user == "" || ss.db == "" {
			return ss, fmt.Errorf("found empty string for host/user/db_name. Please specify host, port, user and db_name in the source-profile")
		}
	} else {
		// Partial params provided through source-profile. Ask user to provide all through the source-profile.
		return ss, fmt.Errorf("please specify host, port, user and db_name in the source-profile")
	}

	if ss.port == "" {
		// Set default port for sql server, which rarely changes.
		ss.port = "1433"
	}
	if ss.pwd == "" {
		ss.pwd = conversion.GetPassword()
	}

	return ss, nil
}

type SourceProfileConnectionDynamoDB struct {
	// These connection params are not used currently because the SDK reads directly from the env variables.
	// These are still kept around as reference when we refactor passing
	// SourceProfile instead of sqlConnectionStr around.
	awsAccessKeyID     string // Same as AWS_ACCESS_KEY_ID environment variable
	awsSecretAccessKey string // Same as AWS_SECRET_ACCESS_KEY environment variable
	awsRegion          string // Same as AWS_REGION environment variable
	dydbEndpoint       string // Same as DYNAMODB_ENDPOINT_OVERRIDE environment variable
	schemaSampleSize   int64  // Number of rows to use for inferring schema (default 100,000)
}

func NewSourceProfileConnectionDynamoDB(params map[string]string) (SourceProfileConnectionDynamoDB, error) {
	dydb := SourceProfileConnectionDynamoDB{}
	if schemaSampleSize, ok := params["schema-sample-size"]; ok {
		schemaSampleSizeInt, err := strconv.Atoi(schemaSampleSize)
		if err != nil {
			return dydb, fmt.Errorf("could not parse schema-sample-size = %v as a valid int64", schemaSampleSize)
		}
		dydb.schemaSampleSize = int64(schemaSampleSizeInt)
	}
	// For DynamoDB, the preferred way to provide connection params is through env variables.
	// Unlike postgres and mysql, there may not be deprecation of env variables, hence it
	// is better to override env variables optionally via source profile params.
	var ok bool
	if dydb.awsAccessKeyID, ok = params["aws-access-key-id"]; ok {
		os.Setenv("AWS_ACCESS_KEY_ID", dydb.awsAccessKeyID)
	}
	if dydb.awsSecretAccessKey, ok = params["aws-secret-access-key"]; ok {
		os.Setenv("AWS_SECRET_ACCESS_KEY", dydb.awsAccessKeyID)
	}
	if dydb.awsRegion, ok = params["aws-region"]; ok {
		os.Setenv("AWS_REGION", dydb.awsAccessKeyID)
	}
	if dydb.dydbEndpoint, ok = params["dydb-endpoint"]; ok {
		os.Setenv("DYNAMODB_ENDPOINT_OVERRIDE", dydb.awsAccessKeyID)
	}
	return dydb, nil
}

type SourceProfileConnection struct {
	ty        SourceProfileConnectionType
	mysql     SourceProfileConnectionMySQL
	pg        SourceProfileConnectionPostgreSQL
	dydb      SourceProfileConnectionDynamoDB
	sqlserver SourceProfileConnectionSqlServer
}

func NewSourceProfileConnection(source string, params map[string]string) (SourceProfileConnection, error) {
	conn := SourceProfileConnection{}
	var err error
	switch strings.ToLower(source) {
	case "mysql":
		{
			conn.ty = SourceProfileConnectionTypeMySQL
			conn.mysql, err = NewSourceProfileConnectionMySQL(params)
			if err != nil {
				return conn, err
			}
		}
	case "postgresql", "postgres", "pg":
		{
			conn.ty = SourceProfileConnectionTypePostgreSQL
			conn.pg, err = NewSourceProfileConnectionPostgreSQL(params)
			if err != nil {
				return conn, err
			}
		}
	case "dynamodb":
		{
			conn.ty = SourceProfileConnectionTypeDynamoDB
			conn.dydb, err = NewSourceProfileConnectionDynamoDB(params)
			if err != nil {
				return conn, err
			}
		}
	case "sqlserver":
		{
			conn.ty = SourceProfileConnectionTypeSqlServer
			conn.sqlserver, err = NewSourceProfileConnectionSqlServer(params)
			if err != nil {
				return conn, err
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

type SourceProfile struct {
	ty     SourceProfileType
	file   SourceProfileFile
	conn   SourceProfileConnection
	config SourceProfileConfig
}

// ToLegacyDriver converts source-profile to equivalent legacy global flags
// e.g., -driver, -dump-file etc since the rest of the codebase still uses the
// same. TODO: Deprecate this function and pass around SourceProfile across the
// codebase wherever information about source connection is required.
func (src SourceProfile) ToLegacyDriver(source string) (string, error) {
	switch src.ty {
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
			default:
				return "", fmt.Errorf("please specify a valid source database using -source flag, received source = %v", source)
			}
		}
	case SourceProfileTypeConfig:
		return "", fmt.Errorf("specifying source-profile using config not implemented")
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

	if _, ok := params["file"]; ok || filePipedToStdin() {
		profile := NewSourceProfileFile(params)
		return SourceProfile{ty: SourceProfileTypeFile, file: profile}, nil
	} else if format, ok := params["format"]; ok {
		// File is not passed in from stdin or specified using "file" flag.
		return SourceProfile{ty: SourceProfileTypeFile}, fmt.Errorf("file not specified, but format set to %v", format)
	} else if file, ok := params["config"]; ok {
		config := NewSourceProfileConfig(file)
		return SourceProfile{ty: SourceProfileTypeConfig, config: config}, fmt.Errorf("source-profile type config not yet implemented")
	} else {
		// Assume connection profile type connection by default, since
		// connection parameters could be specified as part of environment
		// variables.
		conn, err := NewSourceProfileConnection(source, params)
		return SourceProfile{ty: SourceProfileTypeConnection, conn: conn}, err
	}
}

var filePipedToStdin = func() bool {
	stat, _ := os.Stdin.Stat()
	// Data is being piped to stdin, if true. Else, stdin is from a terminal.
	return (stat.Mode() & os.ModeCharDevice) == 0
}

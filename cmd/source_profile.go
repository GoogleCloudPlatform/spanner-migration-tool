package cmd

import (
	"fmt"
	"os"
	"strconv"
	"strings"
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
		fmt.Printf("User did not specify `format` in source-profile, defaulting to \"dump\"\n")
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
)

type SourceProfileConnectionMySQL struct {
	host string // Same as MYSQLHOST environment variable
	port string // Same as MYSQLPORT environment variable
	user string // Same as MYSQLUSER environment variable
	db   string // Same as MYSQLDATABASE environment variable
	pwd  string // Same as MYSQLPWD environment variable
}

func NewSourceProfileConnectionMySQL(params map[string]string) SourceProfileConnectionMySQL {
	// TODO: Move parsing of environment variables in this function.
	mysql := SourceProfileConnectionMySQL{}
	if host, ok := params["host"]; ok {
		mysql.host = host
	}
	if port, ok := params["port"]; ok {
		mysql.port = port
	}
	if user, ok := params["user"]; ok {
		mysql.user = user
	}
	if db, ok := params["db_name"]; ok {
		mysql.db = db
	}
	if pwd, ok := params["password"]; ok {
		mysql.pwd = pwd
	}
	return mysql
}

type SourceProfileConnectionPostgreSQL struct {
	host string // Same as PGHOST environment variable
	port string // Same as PGPORT environment variable
	user string // Same as PGUSER environment variable
	db   string // Same as PGDATABASE environment variable
	pwd  string // Same as PGPASSWORD environment variable
}

func NewSourceProfileConnectionPostgreSQL(params map[string]string) SourceProfileConnectionPostgreSQL {
	pg := SourceProfileConnectionPostgreSQL{}
	if host, ok := params["host"]; ok {
		pg.host = host
	}
	if port, ok := params["port"]; ok {
		pg.port = port
	}
	if user, ok := params["user"]; ok {
		pg.user = user
	}
	if db, ok := params["db_name"]; ok {
		pg.db = db
	}
	if pwd, ok := params["password"]; ok {
		pg.pwd = pwd
	}
	return pg
}

type SourceProfileConnectionDynamoDB struct {
	awsAccessKeyID     string // Same as AWS_ACCESS_KEY_ID environment variable
	awsSecretAccessKey string // Same as AWS_SECRET_ACCESS_KEY environment variable
	awsRegion          string // Same as AWS_REGION environment variable
	dydbEndpoint       string // Same as DYNAMODB_ENDPOINT_OVERRIDE environment variable
	schemaSampleSize   int64  // Number of rows to use for inferring schema (default 100,000)
}

func NewSourceProfileConnectionDynamoDB(params map[string]string) (SourceProfileConnectionDynamoDB, error) {
	dydb := SourceProfileConnectionDynamoDB{}
	if awsAccessKeyID, ok := params["awsAccessKeyID"]; ok {
		dydb.awsAccessKeyID = awsAccessKeyID
	}
	if awsSecretAccessKey, ok := params["awsSecretAccessKey"]; ok {
		dydb.awsSecretAccessKey = awsSecretAccessKey
	}
	if awsRegion, ok := params["awsRegion"]; ok {
		dydb.awsRegion = awsRegion
	}
	if dydbEndpoint, ok := params["dydbEndpoint"]; ok {
		dydb.dydbEndpoint = dydbEndpoint
	}
	if schemaSampleSize, ok := params["schemaSampleSize"]; ok {
		schemaSampleSizeInt, err := strconv.Atoi(schemaSampleSize)
		if err != nil {
			return dydb, fmt.Errorf("could not parse schemaSampleSize = %v as a valid int64", schemaSampleSize)
		}
		dydb.schemaSampleSize = int64(schemaSampleSizeInt)
	}
	return dydb, nil
}

type SourceProfileConnection struct {
	ty    SourceProfileConnectionType
	mysql SourceProfileConnectionMySQL
	pg    SourceProfileConnectionPostgreSQL
	dydb  SourceProfileConnectionDynamoDB
}

func NewSourceProfileConnection(source string, params map[string]string) (SourceProfileConnection, error) {
	conn := SourceProfileConnection{}
	switch strings.ToLower(source) {
	case "mysql":
		{
			conn.ty = SourceProfileConnectionTypeMySQL
			conn.mysql = NewSourceProfileConnectionMySQL(params)
		}
	case "postgresql", "postgres", "pg":
		{
			conn.ty = SourceProfileConnectionTypePostgreSQL
			conn.pg = NewSourceProfileConnectionPostgreSQL(params)
		}
	case "dynamodb":
		{
			conn.ty = SourceProfileConnectionTypeDynamoDB
			dydb, err := NewSourceProfileConnectionDynamoDB(params)
			conn.dydb = dydb
			if err != nil {
				return conn, err
			}
		}
	default:
		return conn, fmt.Errorf("invalid source database: %v", source)
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

func (src SourceProfile) ToLegacyDriver(source string) (string, error) {
	switch src.ty {
	case SourceProfileTypeFile:
		{
			switch strings.ToLower(source) {
			case "mysql":
				return "mysqldump", nil
			case "postgresql", "postgres", "pg":
				return "pg_dump", nil
			default:
				return "", fmt.Errorf("invalid source database %v for source-profile type file", strings.ToLower(source))
			}
		}
	case SourceProfileTypeConnection:
		{
			switch strings.ToLower(source) {
			case "mysql":
				return "mysql", nil
			case "postgresql", "postgres", "pg":
				return "postgres", nil
			case "dynamodb":
				return "dynamodb", nil
			default:
				return "", fmt.Errorf("invalid source database %v for source-profile direct connect", strings.ToLower(source))
			}
		}
	case SourceProfileTypeConfig:
		return "", fmt.Errorf("specifying source-profile using config not implemented")
	default:
		return "", fmt.Errorf("invalid source-profile, could not infer type")
	}
}

// Source profile is passed as a list of key value pairs on the command line.
// Following 3 formats are supported as valid source profiles.
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
func NewSourceProfile(str string, source string) (SourceProfile, error) {
	params := make(map[string]string)

	if len(str) > 0 {
		kvs := strings.Split(str, ",")
		for _, kv := range kvs {
			s := strings.Split(strings.TrimSpace(kv), "=")
			if len(s) != 2 {
				return SourceProfile{}, fmt.Errorf("invalid key, value in source profile (expected format: key1=value1): %v", kv)
			}
			if _, ok := params[s[0]]; ok {
				return SourceProfile{}, fmt.Errorf("duplicate key in source profile: %v", s[0])
			}
			params[s[0]] = s[1]
		}
	}

	if _, ok := params["file"]; ok || filePipedToStdin() {
		profile := NewSourceProfileFile(params)
		return SourceProfile{ty: SourceProfileTypeFile, file: profile}, nil
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

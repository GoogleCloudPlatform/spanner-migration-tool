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
	"crypto/tls"
	"crypto/x509"
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/utils"
	mysqldriver "github.com/go-sql-driver/mysql"
	go_ora "github.com/sijms/go-ora/v2"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
)

// Parses input string `s` as a map of key-value pairs. It's expected that the
// input string is of the form "key1=value1,key2=value2,..." etc. Return error
// otherwise.
func ParseMap(s string) (map[string]string, error) {
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
		equalSignIndex := strings.Index(kv, "=")
		if equalSignIndex == -1 {
			return params, fmt.Errorf("invalid key=value pair (expected format: key1=value1): %v", kv)
		}

		key := strings.TrimSpace(kv[:equalSignIndex])
		value := kv[equalSignIndex+1:]

		if len(key) == 0 {
			return params, fmt.Errorf("empty key found in pair: %v", kv)
		}

		if _, ok := params[key]; ok {
			return params, fmt.Errorf("duplicate key found: %v", key)
		}
		params[key] = value
	}
	return params, nil
}

func ParseList(s string) ([]string, error) {
	if len(s) == 0 {
		return nil, nil
	}
	r := csv.NewReader(strings.NewReader(s))
	r.Comma = ','
	r.TrimLeadingSpace = true
	records, err := r.ReadAll()
	if err != nil {
		return nil, err
	}
	if len(records) > 1 {
		return nil, fmt.Errorf("contains invalid newline characters")
	}
	return records[0], nil
}

func registerMySQLTLSConfig(sslrootcert, sslcert, sslkey, sslmode string) (string, error) {
	tlsConfig := &tls.Config{}

	if sslrootcert != "" {
		rootCertPool := x509.NewCertPool()
		pem, err := os.ReadFile(sslrootcert)
		if err != nil {
			return "", fmt.Errorf("failed to read root CA cert: %w", err)
		}
		if ok := rootCertPool.AppendCertsFromPEM(pem); !ok {
			return "", fmt.Errorf("failed to append root CA cert")
		}
		tlsConfig.RootCAs = rootCertPool
	}

	if sslcert != "" && sslkey != "" {
		certs, err := tls.LoadX509KeyPair(sslcert, sslkey)
		if err != nil {
			return "", fmt.Errorf("failed to load client cert/key: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{certs}
	}

	if sslmode == "verify-ca" {
		tlsConfig.InsecureSkipVerify = true
		tlsConfig.VerifyConnection = func(cs tls.ConnectionState) error {
			opts := x509.VerifyOptions{
				Roots:         tlsConfig.RootCAs,
				CurrentTime:   time.Now(),
				Intermediates: x509.NewCertPool(),
			}
			for _, cert := range cs.PeerCertificates[1:] {
				opts.Intermediates.AddCert(cert)
			}
			_, err := cs.PeerCertificates[0].Verify(opts)
			return err
		}
	} else if sslmode == "skip-verify" {
		tlsConfig.InsecureSkipVerify = true
	}

	configName := fmt.Sprintf("custom_%d", time.Now().UnixNano())
	err := mysqldriver.RegisterTLSConfig(configName, tlsConfig)
	if err != nil {
		return "", err
	}
	return configName, nil
}

func GetSQLConnectionStr(sourceProfile SourceProfile) string {
	sqlConnectionStr := ""
	if sourceProfile.Ty == SourceProfileTypeConnection {
		switch sourceProfile.Conn.Ty {
		case SourceProfileConnectionTypeMySQL:
			connParams := sourceProfile.Conn.Mysql
			return getMYSQLConnectionStr(connParams.Host, connParams.Port, connParams.User, connParams.Pwd, connParams.Db, connParams.Sslmode, connParams.Sslrootcert, connParams.Sslcert, connParams.Sslkey)
		case SourceProfileConnectionTypePostgreSQL:
			connParams := sourceProfile.Conn.Pg
			return getPGSQLConnectionStr(connParams.Host, connParams.Port, connParams.User, connParams.Pwd, connParams.Db, connParams.Sslmode, connParams.Sslrootcert, connParams.Sslcert, connParams.Sslkey)
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
	dbName := os.Getenv("PGDATABASE")
	if server == "" || port == "" || user == "" || dbName == "" {
		logger.Log.Info(fmt.Sprintf("Please specify host, port, user and database using PGHOST, PGPORT, PGUSER and PGDATABASE environment variables\n"))
		return "", fmt.Errorf("could not connect to source database")
	}
	password := os.Getenv("PGPASSWORD")
	if password == "" {
		getInfo := utils.GetUtilInfoImpl{}
		password = getInfo.GetPassword()
	}
	return getPGSQLConnectionStr(server, port, user, password, dbName, os.Getenv("PGSSLMODE"), os.Getenv("PGSSLROOTCERT"), os.Getenv("PGSSLCERT"), os.Getenv("PGSSLKEY")), nil
}

func getPGSQLConnectionStr(server, port, user, password, dbName string, sslmode, sslrootcert, sslcert, sslkey string) string {
	str := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s", server, port, user, password, dbName)
	if sslmode != "" {
		str = fmt.Sprintf("%s sslmode=%s", str, sslmode)
	} else {
		str = fmt.Sprintf("%s sslmode=disable", str)
	}
	if sslrootcert != "" {
		str = fmt.Sprintf("%s sslrootcert=%s", str, sslrootcert)
	}
	if sslcert != "" {
		str = fmt.Sprintf("%s sslcert=%s", str, sslcert)
	}
	if sslkey != "" {
		str = fmt.Sprintf("%s sslkey=%s", str, sslkey)
	}
	return str
}

func GenerateMYSQLConnectionStr() (string, error) {
	server := os.Getenv("MYSQLHOST")
	port := os.Getenv("MYSQLPORT")
	user := os.Getenv("MYSQLUSER")
	dbName := os.Getenv("MYSQLDATABASE")
	if server == "" || port == "" || user == "" || dbName == "" {
		logger.Log.Info(fmt.Sprintf("Please specify host, port, user and database using MYSQLHOST, MYSQLPORT, MYSQLUSER and MYSQLDATABASE environment variables\n"))
		return "", fmt.Errorf("could not connect to source database")
	}
	password := os.Getenv("MYSQLPWD")
	if password == "" {
		getInfo := utils.GetUtilInfoImpl{}
		password = getInfo.GetPassword()
	}
	return getMYSQLConnectionStr(server, port, user, password, dbName, os.Getenv("MYSQL_SSL_MODE"), os.Getenv("MYSQL_SSL_ROOT_CERT"), os.Getenv("MYSQL_SSL_CERT"), os.Getenv("MYSQL_SSL_KEY")), nil
}

func getMYSQLConnectionStr(server, port, user, password, dbName string, sslmode, sslrootcert, sslcert, sslkey string) string {
	base := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", user, password, server, port, dbName)
	tlsParam := ""
	if sslrootcert != "" || sslcert != "" || sslkey != "" {
		configName, err := registerMySQLTLSConfig(sslrootcert, sslcert, sslkey, sslmode)
		if err == nil {
			tlsParam = configName
		}
	} else if sslmode != "" {
		switch sslmode {
		case "disable":
			tlsParam = "false"
		case "skip-verify":
			tlsParam = "skip-verify"
		case "require":
			tlsParam = "true"
		default:
			tlsParam = "preferred"
		}
	}

	if tlsParam != "" {
		return fmt.Sprintf("%s?tls=%s", base, tlsParam)
	}
	return base
}

func getSQLSERVERConnectionStr(server, port, user, password, dbName string) string {
	return fmt.Sprintf(`sqlserver://%s:%s@%s:%s?database=%s`, user, password, server, port, dbName)
}

func GetSchemaSampleSize(sourceProfile SourceProfile) int64 {
	return int64(100000)
}

func getORACLEConnectionStr(server, port, user, password, dbName string) string {
	portNumber, _ := strconv.Atoi(port)
	return go_ora.BuildUrl(server, portNumber, dbName, user, password, nil)
}

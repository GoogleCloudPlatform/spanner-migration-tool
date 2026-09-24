// Copyright 2024 Google LLC
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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// code for testing parse map
func TestParseMap(t *testing.T) {
	// Avoid getting/setting env variables in the unit tests.
	testCases := []struct {
		name           string
		inputString    string
		expectedParams map[string]string
		errorExpected  bool
	}{
		{
			name:           "empty params",
			inputString:    "",
			expectedParams: map[string]string{},
			errorExpected:  false,
		},
		{
			name:           "valid params",
			inputString:    "instance=instance",
			expectedParams: map[string]string{"instance": "instance"},
			errorExpected:  false,
		},
		{
			name:           "valid params with = in value",
			inputString:    "password=pass=word",
			expectedParams: map[string]string{"password": "pass=word"},
			errorExpected:  false,
		},
		{
			name:           "valid params with special characters in value",
			inputString:    "\"password=password`~!@#$%^&*()-_=+[]{}|;:<,>.?/'\\\"",
			expectedParams: map[string]string{"password": "password`~!@#$%^&*()-_=+[]{}|;:<,>.?/'\\"},
			errorExpected:  false,
		},
		{
			name:           "invalid params incorrect format",
			inputString:    "uuwy",
			expectedParams: map[string]string{},
			errorExpected:  true,
		},
		{
			name:           "invalid params new line char",
			inputString:    "uuwy\n hjgse",
			expectedParams: map[string]string{},
			errorExpected:  true,
		},
		{
			name:           "invalid params duplicates",
			inputString:    "instance=instance, instance=instance",
			expectedParams: map[string]string{"instance": "instance"},
			errorExpected:  true,
		},
	}

	for _, tc := range testCases {
		res, err := ParseMap(tc.inputString)
		assert.Equal(t, tc.expectedParams, res, tc.name)
		assert.Equal(t, tc.errorExpected, err != nil, tc.name)
	}
}

// code for testing parse list
func TestParseList(t *testing.T) {
	// Avoid getting/setting env variables in the unit tests.
	testCases := []struct {
		name           string
		inputString    string
		expectedParams []string
		errorExpected  bool
	}{
		{
			name:           "empty input string",
			inputString:    "",
			expectedParams: nil,
			errorExpected:  false,
		},
		{
			name:           "valid input string",
			inputString:    "hello, world",
			expectedParams: []string{"hello", "world"},
			errorExpected:  false,
		},
		{
			name:           "invalid input string new line char",
			inputString:    "hello, world\n, !",
			expectedParams: nil,
			errorExpected:  true,
		},
	}

	for _, tc := range testCases {
		res, err := ParseList(tc.inputString)
		assert.Equal(t, tc.expectedParams, res, tc.name)
		assert.Equal(t, tc.errorExpected, err != nil, tc.name)
	}
}

// code for testing sql connection string
func TestGetSQLConnectionStr(t *testing.T) {
	// Avoid getting/setting env variables in the unit tests.
	host := "0.0.0.0"
	port := "3306"
	user := "user"
	pwd := "password"
	db := "database"

	// Generate a valid dummy CA certificate PEM
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	assert.NoError(t, err)
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"Dummy CA"},
		},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	assert.NoError(t, err)
	certPem := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: derBytes})

	tmpDir := t.TempDir()
	caFile := filepath.Join(tmpDir, "ca.pem")
	err = os.WriteFile(caFile, certPem, 0644)
	assert.NoError(t, err)

	testCases := []struct {
		name                   string
		inputSourceProfileConn SourceProfileConnection
		expectedOutput         string
	}{
		{
			name:                   "source profile connection type mysql",
			inputSourceProfileConn: SourceProfileConnection{Ty: SourceProfileConnectionTypeMySQL, Mysql: SourceProfileConnectionMySQL{Host: host, Port: port, User: user, Pwd: pwd, Db: db}},
			expectedOutput:         "user:password@tcp(0.0.0.0:3306)/database",
		},
		{
			name:                   "source profile connection type mysql with sslmode require",
			inputSourceProfileConn: SourceProfileConnection{Ty: SourceProfileConnectionTypeMySQL, Mysql: SourceProfileConnectionMySQL{Host: host, Port: port, User: user, Pwd: pwd, Db: db, Sslmode: "require"}},
			expectedOutput:         "user:password@tcp(0.0.0.0:3306)/database?tls=true",
		},
		{
			name:                   "source profile connection type mysql with custom certs",
			inputSourceProfileConn: SourceProfileConnection{Ty: SourceProfileConnectionTypeMySQL, Mysql: SourceProfileConnectionMySQL{Host: host, Port: port, User: user, Pwd: pwd, Db: db, Sslmode: "verify-ca", Sslrootcert: caFile}},
			expectedOutput:         "user:password@tcp(0.0.0.0:3306)/database?tls=custom_",
		},
		{
			name:                   "source profile connection type postgres",
			inputSourceProfileConn: SourceProfileConnection{Ty: SourceProfileConnectionTypePostgreSQL, Pg: SourceProfileConnectionPostgreSQL{Host: host, Port: port, User: user, Pwd: pwd, Db: db}},
			expectedOutput:         "host=0.0.0.0 port=3306 user=user password=password dbname=database sslmode=disable",
		},
		{
			name:                   "source profile connection type postgres with TLS verify-full",
			inputSourceProfileConn: SourceProfileConnection{Ty: SourceProfileConnectionTypePostgreSQL, Pg: SourceProfileConnectionPostgreSQL{Host: host, Port: port, User: user, Pwd: pwd, Db: db, Sslmode: "verify-full", Sslrootcert: "ca.pem", Sslcert: "cert.pem", Sslkey: "key.pem"}},
			expectedOutput:         "host=0.0.0.0 port=3306 user=user password=password dbname=database sslmode=verify-full sslrootcert=ca.pem sslcert=cert.pem sslkey=key.pem",
		},
		{
			name:                   "source profile connection type sql server",
			inputSourceProfileConn: SourceProfileConnection{Ty: SourceProfileConnectionTypeSqlServer, SqlServer: SourceProfileConnectionSqlServer{Host: host, Port: port, User: user, Pwd: pwd, Db: db}},
			expectedOutput:         "sqlserver://user:password@0.0.0.0:3306?database=database",
		},
		{
			name:                   "source profile connection type oracle",
			inputSourceProfileConn: SourceProfileConnection{Ty: SourceProfileConnectionTypeOracle, Oracle: SourceProfileConnectionOracle{Host: host, Port: port, User: user, Pwd: pwd, Db: db}},
			expectedOutput:         "oracle://user:password@0.0.0.0:3306/database",
		},
	}

	for _, tc := range testCases {
		res := GetSQLConnectionStr(SourceProfile{Ty: SourceProfileType(SourceProfileTypeConnection), Conn: tc.inputSourceProfileConn})
		if tc.name == "source profile connection type mysql with custom certs" {
			assert.Contains(t, res, tc.expectedOutput)
		} else {
			assert.Equal(t, tc.expectedOutput, res, tc.name)
		}
	}
}

func TestGenerateConnectionStr(t *testing.T) {
	// Avoid getting/setting env variables in the unit tests.
	before := func() {
		setEnvVariables()
	}

	after := func() {
		unsetEnvVariables()
	}
	testCases := []struct {
		name                string
		expectedOutputPg    string
		expectedOutputMysql string
		errorExpected       bool
	}{
		{
			name:                "valid get mysql and postgres conn string",
			expectedOutputPg:    "host=0.0.0.0 port=3306 user=user password=password dbname=db sslmode=disable",
			expectedOutputMysql: "user:password@tcp(0.0.0.0:3306)/db",
			errorExpected:       false,
		},
	}

	for _, tc := range testCases {
		before()
		res, err := GeneratePGSQLConnectionStr()
		assert.Equal(t, tc.expectedOutputPg, res, tc.name)
		assert.Equal(t, tc.errorExpected, err != nil, tc.name)
		res, err = GenerateMYSQLConnectionStr()
		assert.Equal(t, tc.expectedOutputMysql, res, tc.name)
		assert.Equal(t, tc.errorExpected, err != nil, tc.name)
		after()
	}
}

func TestGetSchemaSampleSize(t *testing.T) {
	// Avoid getting/setting env variables in the unit tests.
	testCases := []struct {
		name               string
		inputSourceProfile SourceProfile
		expectedOutput     int64
	}{
		{
			name:               "mysql source profile type",
			inputSourceProfile: SourceProfile{Ty: SourceProfileType(SourceProfileTypeConnection), Conn: SourceProfileConnection{Ty: SourceProfileConnectionTypeMySQL}},
			expectedOutput:     int64(100000),
		},
	}

	for _, tc := range testCases {
		res := GetSchemaSampleSize(tc.inputSourceProfile)
		assert.Equal(t, tc.expectedOutput, res, tc.name)
	}
}

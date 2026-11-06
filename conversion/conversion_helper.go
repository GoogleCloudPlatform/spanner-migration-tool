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

package conversion

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"syscall"
	"unicode"

	sp "cloud.google.com/go/spanner"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/metrics"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/utils"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/expressions_api"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/profiles"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/common"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/mysql"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/postgres"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/writer"
	mysqldriver "github.com/go-sql-driver/mysql"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

type ProcessDumpByDialectInterface interface {
	ProcessDump(driver string, conv *internal.Conv, r *internal.Reader) error
}

type ProcessDumpByDialectImpl struct {
	ExpressionVerificationAccessor expressions_api.ExpressionVerificationAccessor
	DdlVerifier                    expressions_api.DDLVerifier
}

type PopulateDataConvInterface interface {
	populateDataConv(conv *internal.Conv, config writer.BatchWriterConfig, client *sp.Client) *writer.BatchWriter
}

type PopulateDataConvImpl struct{}

// getSeekable returns a seekable file (with same content as f) and the size of the content (in bytes).
func getSeekable(f *os.File) (*os.File, int64, error) {
	_, err := f.Seek(0, 0)
	if err == nil { // Stdin is seekable, let's just use that. This happens when you run 'cmd < file'.
		n, err := utils.GetFileSize(f)
		return f, n, err
	}
	internal.VerbosePrintln("Creating a tmp file with a copy of stdin because stdin is not seekable.")
	logger.Log.Debug("Creating a tmp file with a copy of stdin because stdin is not seekable.")

	// Create file in os.TempDir. Its not clear this is a good idea e.g. if the
	// pg_dump/mysqldump output is large (tens of GBs) and os.TempDir points to a directory
	// (such as /tmp) that's configured with a small amount of disk space.
	// To workaround such limits on Unix, set $TMPDIR to a directory with lots
	// of disk space.
	fcopy, err := ioutil.TempFile("", "spanner-migration-tool.data")
	if err != nil {
		return nil, 0, err
	}
	syscall.Unlink(fcopy.Name()) // File will be deleted when this process exits.
	_, err = io.Copy(fcopy, f)
	if err != nil {
		return nil, 0, fmt.Errorf("can't write stdin to tmp file: %w", err)
	}
	_, err = fcopy.Seek(0, 0)
	if err != nil {
		return nil, 0, fmt.Errorf("can't reset file offset: %w", err)
	}
	n, _ := utils.GetFileSize(fcopy)
	return fcopy, n, nil
}

// ProcessDump invokes process dump function from a sql package based on driver selected.
func (pdd *ProcessDumpByDialectImpl) ProcessDump(driver string, conv *internal.Conv, r *internal.Reader) error {
	switch driver {
	case constants.MYSQLDUMP:
		return common.ProcessDbDump(conv, r, mysql.DbDumpImpl{}, pdd.DdlVerifier, pdd.ExpressionVerificationAccessor)
	case constants.PGDUMP:
		return common.ProcessDbDump(conv, r, postgres.DbDumpImpl{}, pdd.DdlVerifier, pdd.ExpressionVerificationAccessor)
	default:
		return fmt.Errorf("process dump for driver %s not supported", driver)
	}
}

func (pdc *PopulateDataConvImpl) populateDataConv(conv *internal.Conv, config writer.BatchWriterConfig, client *sp.Client) *writer.BatchWriter {
	rows := int64(0)
	config.Write = func(m []*sp.Mutation) error {
		ctx := context.Background()
		if !conv.Audit.SkipMetricsPopulation {
			migrationData := metrics.GetMigrationData(conv, "", constants.DataConv)
			serializedMigrationData, _ := proto.Marshal(migrationData)
			migrationMetadataValue := base64.StdEncoding.EncodeToString(serializedMigrationData)
			ctx = metadata.AppendToOutgoingContext(context.Background(), constants.MigrationMetadataKey, migrationMetadataValue)
		}
		_, err := client.Apply(ctx, m)
		if err != nil {
			return err
		}
		atomic.AddInt64(&rows, int64(len(m)))
		conv.Audit.Progress.MaybeReport(atomic.LoadInt64(&rows))
		return nil
	}
	batchWriter := writer.NewBatchWriter(config)
	conv.SetDataMode()
	if !conv.Audit.DryRun {
		conv.SetDataSink(
			func(table string, cols []string, vals []interface{}) {
				batchWriter.AddRow(table, cols, vals)
			})
		conv.DataFlush = func() {
			batchWriter.Flush()
		}
	}

	return batchWriter
}

func ConnectionConfig(sourceProfile profiles.SourceProfile) (interface{}, error) {
	switch sourceProfile.Driver {
	// For PG and MYSQL, When called as part of the subcommand flow, host/user/db etc will
	// never be empty as we error out right during source profile creation. If any of them
	// are empty, that means this was called through the legacy cmd flow and we create the
	// string using env vars.
	case constants.POSTGRES:
		pgConn := sourceProfile.Conn.Pg
		if !(pgConn.Host != "" && pgConn.User != "" && pgConn.Db != "") {
			return profiles.GeneratePGSQLConnectionStr()
		} else {
			return profiles.GetSQLConnectionStr(sourceProfile), nil
		}
	case constants.MYSQL:
		// If empty, this is called as part of the legacy mode witih global CLI flags.
		// When using source-profile mode is used, the sqlConnectionStr is already populated.
		mysqlConn := sourceProfile.Conn.Mysql
		if !(mysqlConn.Host != "" && mysqlConn.User != "" && mysqlConn.Db != "") {
			return profiles.GenerateMYSQLConnectionStr()
		} else {
			return profiles.GetSQLConnectionStr(sourceProfile), nil
		}
	case constants.SQLSERVER:
		return profiles.GetSQLConnectionStr(sourceProfile), nil
	case constants.ORACLE:
		return profiles.GetSQLConnectionStr(sourceProfile), nil
	// Returns an empty string as Cassandra connections are managed directly by the gocql session.	
	case constants.CASSANDRA:
		return "", nil
	default:
		return "", fmt.Errorf("driver %s not supported", sourceProfile.Driver)
	}
}

func extractLibpqParam(dsn, targetKey string) string {
	runes := []rune(dsn)
	n := len(runes)
	i := 0
	var result string
	for i < n {
		for i < n && unicode.IsSpace(runes[i]) {
			i++
		}
		if i >= n {
			break
		}
		keyStart := i
		for i < n && !unicode.IsSpace(runes[i]) && runes[i] != '=' {
			i++
		}
		key := string(runes[keyStart:i])
		for i < n && unicode.IsSpace(runes[i]) {
			i++
		}
		if i >= n || runes[i] != '=' {
			return result
		}
		i++
		for i < n && unicode.IsSpace(runes[i]) {
			i++
		}
		if i >= n {
			if key == targetKey {
				result = ""
			}
			break
		}
		var val []rune
		if runes[i] == '\'' {
			i++
			closed := false
			for i < n {
				if runes[i] == '\'' {
					i++
					closed = true
					break
				}
				if runes[i] == '\\' {
					i++
					if i >= n {
						return result
					}
				}
				val = append(val, runes[i])
				i++
			}
			if !closed {
				return result
			}
		} else {
			for i < n && !unicode.IsSpace(runes[i]) {
				if runes[i] == '\\' {
					i++
					if i >= n {
						return result
					}
				}
				val = append(val, runes[i])
				i++
			}
		}
		if key == targetKey {
			result = string(val)
		}
	}
	return result
}

func getDbNameFromSQLConnectionStr(driver, sqlConnectionStr string) string {
	switch driver {
	case constants.POSTGRES:
		if strings.HasPrefix(sqlConnectionStr, "postgres://") || strings.HasPrefix(sqlConnectionStr, "postgresql://") {
			u, err := url.Parse(sqlConnectionStr)
			if err != nil {
				return ""
			}
			return strings.TrimPrefix(u.Path, "/")
		}
		return extractLibpqParam(sqlConnectionStr, "dbname")
	case constants.MYSQL:
		if cfg, err := mysqldriver.ParseDSN(sqlConnectionStr); err == nil {
			return cfg.DBName
		}
		if parts := strings.SplitN(sqlConnectionStr, ")/", 2); len(parts) == 2 {
			return strings.SplitN(parts[1], "?", 2)[0]
		}
		return ""
	case constants.SQLSERVER:
		if u, err := url.Parse(sqlConnectionStr); err == nil {
			return u.Query().Get("database")
		}
		idx := strings.Index(sqlConnectionStr, "?database=")
		if idx == -1 {
			return ""
		}
		dbParam := sqlConnectionStr[idx+len("?database="):]
		dbName := strings.SplitN(dbParam, "&", 2)[0]
		if unescaped, err := url.QueryUnescape(dbName); err == nil {
			dbName = unescaped
		}
		return dbName
	case constants.ORACLE:
		// connection string format: "oracle://user:password@104.108.154.85:1521/XE"
		// User name is used as the schema owner in Oracle.
		if u, err := url.Parse(sqlConnectionStr); err == nil && u.User != nil {
			return u.User.Username()
		}
		if !strings.HasPrefix(sqlConnectionStr, "oracle://") {
			return ""
		}
		substr := sqlConnectionStr[len("oracle://"):]
		dbName := strings.SplitN(substr, ":", 2)[0]
		if unescaped, err := url.QueryUnescape(dbName); err == nil {
			dbName = unescaped
		}
		return dbName
	}
	return ""
}



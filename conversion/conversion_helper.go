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
	"os"
	"strings"
	"sync/atomic"
	"syscall"

	sp "cloud.google.com/go/spanner"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/metrics"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/utils"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/profiles"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/common"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/mysql"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/postgres"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/writer"
	"github.com/aws/aws-sdk-go/aws"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

type ProcessDumpByDialectInterface interface{
	ProcessDump(driver string, conv *internal.Conv, r *internal.Reader) error
}

type ProcessDumpByDialectImpl struct{}

type PopulateDataConvInterface interface{
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
		return common.ProcessDbDump(conv, r, mysql.DbDumpImpl{}, driver)
	case constants.PGDUMP:
		return common.ProcessDbDump(conv, r, postgres.DbDumpImpl{}, driver)
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


func connectionConfig(sourceProfile profiles.SourceProfile) (interface{}, error) {
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
	// For Dynamodb, both legacy and new flows use env vars.
	case constants.DYNAMODB:
		return getDynamoDBClientConfig()
	case constants.SQLSERVER:
		return profiles.GetSQLConnectionStr(sourceProfile), nil
	case constants.ORACLE:
		return profiles.GetSQLConnectionStr(sourceProfile), nil
	default:
		return "", fmt.Errorf("driver %s not supported", sourceProfile.Driver)
	}
}

func getDbNameFromSQLConnectionStr(driver, sqlConnectionStr string) string {
	switch driver {
	case constants.POSTGRES:
		dbParam := strings.Split(sqlConnectionStr, " ")[4]
		return strings.Split(dbParam, "=")[1]
	case constants.MYSQL:
		return strings.Split(sqlConnectionStr, ")/")[1]
	case constants.SQLSERVER:
		splts := strings.Split(sqlConnectionStr, "?database=")
		return splts[len(splts)-1]
	case constants.ORACLE:
		// connection string formate : "oracle://user:password@104.108.154.85:1521/XE"
		substr := sqlConnectionStr[9:]
		dbName := strings.Split(substr, ":")[0]
		return dbName
	}
	return ""
}

func updateShardsWithTuningConfigs(shardedTuningConfig profiles.ShardConfigurationDataflow) {
	for _, dataShard := range shardedTuningConfig.DataShards {
		dataShard.DatastreamConfig = shardedTuningConfig.DatastreamConfig
		dataShard.GcsConfig = shardedTuningConfig.GcsConfig
		dataShard.DataflowConfig = shardedTuningConfig.DataflowConfig
	}
}

func getDynamoDBClientConfig() (*aws.Config, error) {
	cfg := aws.Config{}
	endpointOverride := os.Getenv("DYNAMODB_ENDPOINT_OVERRIDE")
	if endpointOverride != "" {
		cfg.Endpoint = aws.String(endpointOverride)
	}
	return &cfg, nil
}
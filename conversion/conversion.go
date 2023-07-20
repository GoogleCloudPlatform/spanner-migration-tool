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

// Package conversion handles initial setup for the command line tool
// and web APIs.

// TODO:(searce) Organize code in go style format to make this file more readable.
//
//	public constants first
//	key public type definitions next (although often it makes sense to put them next to public functions that use them)
//	then public functions (and relevant type definitions)
//	and helper functions and other non-public definitions last (generally in order of importance)
package conversion

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	sp "cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	dydb "github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/metrics"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/utils"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal/reports"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/profiles"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/common"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/csv"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/dynamodb"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/mysql"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/oracle"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/postgres"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/spanner"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/sqlserver"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/ddl"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/writer"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/streaming"
	"go.uber.org/zap"
	adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

var (
	// Set the maximum number of concurrent workers during foreign key creation.
	// This number should not be too high so as to not hit the AdminQuota limit.
	// AdminQuota limits are mentioned here: https://cloud.google.com/spanner/quotas#administrative_limits
	// If facing a quota limit error, consider reducing this value.
	MaxWorkers = 50
)

// SchemaConv performs the schema conversion
// The SourceProfile param provides the connection details to use the go SQL library.
func SchemaConv(sourceProfile profiles.SourceProfile, targetProfile profiles.TargetProfile, ioHelper *utils.IOStreams) (*internal.Conv, error) {
	switch sourceProfile.Driver {
	case constants.POSTGRES, constants.MYSQL, constants.DYNAMODB, constants.SQLSERVER, constants.ORACLE:
		return schemaFromDatabase(sourceProfile, targetProfile)
	case constants.PGDUMP, constants.MYSQLDUMP:
		return schemaFromDump(sourceProfile.Driver, targetProfile.Conn.Sp.Dialect, ioHelper)
	default:
		return nil, fmt.Errorf("schema conversion for driver %s not supported", sourceProfile.Driver)
	}
}

// DataConv performs the data conversion
// The SourceProfile param provides the connection details to use the go SQL library.
func DataConv(ctx context.Context, sourceProfile profiles.SourceProfile, targetProfile profiles.TargetProfile, ioHelper *utils.IOStreams, client *sp.Client, conv *internal.Conv, dataOnly bool, writeLimit int64) (*writer.BatchWriter, error) {
	config := writer.BatchWriterConfig{
		BytesLimit: 100 * 1000 * 1000,
		WriteLimit: writeLimit,
		RetryLimit: 1000,
		Verbose:    internal.Verbose(),
	}
	switch sourceProfile.Driver {
	case constants.POSTGRES, constants.MYSQL, constants.DYNAMODB, constants.SQLSERVER, constants.ORACLE:
		return dataFromDatabase(ctx, sourceProfile, targetProfile, config, conv, client)
	case constants.PGDUMP, constants.MYSQLDUMP:
		if conv.SpSchema.CheckInterleaved() {
			return nil, fmt.Errorf("harbourBridge does not currently support data conversion from dump files\nif the schema contains interleaved tables. Suggest using direct access to source database\ni.e. using drivers postgres and mysql")
		}
		return dataFromDump(sourceProfile.Driver, config, ioHelper, client, conv, dataOnly)
	case constants.CSV:
		return dataFromCSV(ctx, sourceProfile, targetProfile, config, conv, client)
	default:
		return nil, fmt.Errorf("data conversion for driver %s not supported", sourceProfile.Driver)
	}
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

func getInfoSchemaForShard(shardConnInfo profiles.DirectConnectionConfig, driver string, targetProfile profiles.TargetProfile) (common.InfoSchema, error) {
	params := make(map[string]string)
	params["host"] = shardConnInfo.Host
	params["user"] = shardConnInfo.User
	params["dbName"] = shardConnInfo.DbName
	params["port"] = shardConnInfo.Port
	params["password"] = shardConnInfo.Password
	//while adding other sources, a switch-case will be added here on the basis of the driver input param passed.
	//pased on the driver name, profiles.NewSourceProfileConnection<DBName> will need to be called to create
	//the source profile information.
	sourceProfileConnectionMySQL, err := profiles.NewSourceProfileConnectionMySQL(params)
	if err != nil {
		return nil, fmt.Errorf("cannot parse connection configuration for the primary shard")
	}
	sourceProfileConnection := profiles.SourceProfileConnection{Mysql: sourceProfileConnectionMySQL, Ty: profiles.SourceProfileConnectionTypeMySQL}
	//create a source profile which contains the sourceProfileConnection object for the primary shard
	//this is done because GetSQLConnectionStr() should not be aware of sharding
	newSourceProfile := profiles.SourceProfile{Conn: sourceProfileConnection, Ty: profiles.SourceProfileTypeConnection}
	newSourceProfile.Driver = driver
	infoSchema, err := GetInfoSchema(newSourceProfile, targetProfile)
	if err != nil {
		return nil, err
	}
	return infoSchema, nil
}

func schemaFromDatabase(sourceProfile profiles.SourceProfile, targetProfile profiles.TargetProfile) (*internal.Conv, error) {
	conv := internal.MakeConv()
	conv.SpDialect = targetProfile.Conn.Sp.Dialect
	//handle fetching schema differently for sharded migrations, we only connect to the primary shard to
	//fetch the schema. We reuse the SourceProfileConnection object for this purpose.
	var infoSchema common.InfoSchema
	var err error
	isSharded := false
	switch sourceProfile.Ty {
	case profiles.SourceProfileTypeConfig:
		isSharded = true
		//Find Primary Shard Name
		if sourceProfile.Config.ConfigType == constants.BULK_MIGRATION {
			schemaSource := sourceProfile.Config.ShardConfigurationBulk.SchemaSource
			infoSchema, err = getInfoSchemaForShard(schemaSource, sourceProfile.Driver, targetProfile)
			if err != nil {
				return conv, err
			}
		} else if sourceProfile.Config.ConfigType == constants.DATAFLOW_MIGRATION {
			schemaSource := sourceProfile.Config.ShardConfigurationDataflow.SchemaSource
			infoSchema, err = getInfoSchemaForShard(schemaSource, sourceProfile.Driver, targetProfile)
			if err != nil {
				return conv, err
			}
		} else if sourceProfile.Config.ConfigType == constants.DMS_MIGRATION {
			// TODO: Define the schema processing logic for DMS migrations here.
			return conv, fmt.Errorf("dms based migrations are not implemented yet")
		} else {
			return conv, fmt.Errorf("unknown type of migration, please select one of bulk, dataflow or dms")
		}
	default:
		infoSchema, err = GetInfoSchema(sourceProfile, targetProfile)
		if err != nil {
			return conv, err
		}
	}
	additionalSchemaAttributes := internal.AdditionalSchemaAttributes{
		IsSharded: isSharded,
	}
	return conv, common.ProcessSchema(conv, infoSchema, common.DefaultWorkers, additionalSchemaAttributes)
}

func performSnapshotMigration(config writer.BatchWriterConfig, conv *internal.Conv, client *sp.Client, infoSchema common.InfoSchema, additionalAttributes internal.AdditionalDataAttributes) *writer.BatchWriter {
	common.SetRowStats(conv, infoSchema)
	totalRows := conv.Rows()
	if !conv.Audit.DryRun {
		conv.Audit.Progress = *internal.NewProgress(totalRows, "Writing data to Spanner", internal.Verbose(), false, int(internal.DataWriteInProgress))
	}
	batchWriter := populateDataConv(conv, config, client)
	common.ProcessData(conv, infoSchema, additionalAttributes)
	batchWriter.Flush()
	return batchWriter
}

func snapshotMigrationHandler(sourceProfile profiles.SourceProfile, config writer.BatchWriterConfig, conv *internal.Conv, client *sp.Client, infoSchema common.InfoSchema) (*writer.BatchWriter, error) {
	switch sourceProfile.Driver {
	// Skip snapshot migration via harbourbridge for mysql and oracle since dataflow job will job will handle this from backfilled data.
	case constants.MYSQL, constants.ORACLE, constants.POSTGRES:
		return &writer.BatchWriter{}, nil
	case constants.DYNAMODB:
		return performSnapshotMigration(config, conv, client, infoSchema, internal.AdditionalDataAttributes{ShardId: ""}), nil
	default:
		return &writer.BatchWriter{}, fmt.Errorf("streaming migration not supported for driver %s", sourceProfile.Driver)
	}
}

func updateShardsWithDataflowConfig(shardedDataflowConfig profiles.ShardConfigurationDataflow) {
	for _, dataShard := range shardedDataflowConfig.DataShards {
		dataShard.DataflowConfig = shardedDataflowConfig.DataflowConfig
	}
}

func dataFromDatabase(ctx context.Context, sourceProfile profiles.SourceProfile, targetProfile profiles.TargetProfile, config writer.BatchWriterConfig, conv *internal.Conv, client *sp.Client) (*writer.BatchWriter, error) {
	//handle migrating data for sharded migrations differently
	//sharded migrations are identified via the config= flag, if that flag is not present
	//carry on with the existing code path in the else block
	switch sourceProfile.Ty {
	case profiles.SourceProfileTypeConfig:
		////There are three cases to cover here, bulk migrations and sharded migrations (and later DMS)
		//We provide an if-else based handling for each within the sharded code branch
		//This will be determined via the configType, which can be "bulk", "dataflow" or "dms"
		if sourceProfile.Config.ConfigType == constants.BULK_MIGRATION {
			return dataFromDatabaseForBulkMigration(sourceProfile, targetProfile, config, conv, client)
		} else if sourceProfile.Config.ConfigType == constants.DATAFLOW_MIGRATION {
			return dataFromDatabaseForDataflowMigration(targetProfile, ctx, sourceProfile, conv)
		} else if sourceProfile.Config.ConfigType == constants.DMS_MIGRATION {
			return dataFromDatabaseForDMSMigration()
		} else {
			return nil, fmt.Errorf("configType should be one of 'bulk', 'dataflow' or 'dms'")
		}
	default:
		infoSchema, err := GetInfoSchema(sourceProfile, targetProfile)
		if err != nil {
			return nil, err
		}
		var streamInfo map[string]interface{}
		if sourceProfile.Conn.Streaming {
			streamInfo, err = infoSchema.StartChangeDataCapture(ctx, conv)
			if err != nil {
				return nil, err
			}
			bw, err := snapshotMigrationHandler(sourceProfile, config, conv, client, infoSchema)
			if err != nil {
				return nil, err
			}
			err = infoSchema.StartStreamingMigration(ctx, client, conv, streamInfo)
			if err != nil {
				return nil, err
			}
			return bw, nil
		}
		return performSnapshotMigration(config, conv, client, infoSchema, internal.AdditionalDataAttributes{ShardId: ""}), nil
	}
}

// TODO: Define the data processing logic for DMS migrations here.
func dataFromDatabaseForDMSMigration() (*writer.BatchWriter, error) {
	return nil, fmt.Errorf("dms configType is not implemented yet, please use one of 'bulk' or 'dataflow'")
}

// 1. Create batch for each physical shard
// 2. Create streaming cfg from the config source type.
// 3. Verify the CFG and update it with HB defaults
// 4. Launch the stream for the physical shard
// 5. Perform streaming migration via dataflow
func dataFromDatabaseForDataflowMigration(targetProfile profiles.TargetProfile, ctx context.Context, sourceProfile profiles.SourceProfile, conv *internal.Conv) (*writer.BatchWriter, error) {
	updateShardsWithDataflowConfig(sourceProfile.Config.ShardConfigurationDataflow)
	conv.Audit.StreamingStats.ShardToDataStreamNameMap = make(map[string]string)
	conv.Audit.StreamingStats.ShardToDataflowJobMap = make(map[string]string)
	tableList, err := common.GetIncludedSrcTablesFromConv(conv)
	if err != nil {
		fmt.Printf("unable to determine tableList from schema, falling back to full database")
		tableList = []string{}
	}
	asyncProcessShards := func(p *profiles.DataShard, mutex *sync.Mutex) common.TaskResult[*profiles.DataShard] {
		dbNameToShardIdMap := make(map[string]string)
		for _, l := range p.LogicalShards {
			dbNameToShardIdMap[l.DbName] = l.LogicalShardId
		}
		streamingCfg := streaming.CreateStreamingConfig(*p)
		err := streaming.VerifyAndUpdateCfg(&streamingCfg, targetProfile.Conn.Sp.Dbname, tableList)
		if err != nil {
			err = fmt.Errorf("failed to process shard: %s, there seems to be an error in the sharding configuration, error: %v", p.DataShardId, err)
			return common.TaskResult[*profiles.DataShard]{Result: p, Err: err}
		}
		fmt.Printf("Initiating migration for shard: %v\n", p.DataShardId)

		err = streaming.LaunchStream(ctx, sourceProfile, p.LogicalShards, targetProfile.Conn.Sp.Project, streamingCfg.DatastreamCfg)
		if err != nil {
			return common.TaskResult[*profiles.DataShard]{Result: p, Err: err}
		}
		streamingCfg.DataflowCfg.DbNameToShardIdMap = dbNameToShardIdMap
		err = streaming.StartDataflow(ctx, targetProfile, streamingCfg, conv)
		return common.TaskResult[*profiles.DataShard]{Result: p, Err: err}
	}
	_, err = common.RunParallelTasks(sourceProfile.Config.ShardConfigurationDataflow.DataShards, 5, asyncProcessShards, true)
	if err != nil {
		return nil, fmt.Errorf("unable to start minimal downtime migrations: %v", err)
	}
	return &writer.BatchWriter{}, nil
}

// 1. Migrate the data from the data shards, the schema shard needs to be specified here again.
// 2. Create a connection profile object for it
// 3. Perform a snapshot migration for the shard
// 4. Once all shard migrations are complete, return the batch writer object
func dataFromDatabaseForBulkMigration(sourceProfile profiles.SourceProfile, targetProfile profiles.TargetProfile, config writer.BatchWriterConfig, conv *internal.Conv, client *sp.Client) (*writer.BatchWriter, error) {
	var bw *writer.BatchWriter
	for _, dataShard := range sourceProfile.Config.ShardConfigurationBulk.DataShards {

		fmt.Printf("Initiating migration for shard: %v\n", dataShard.DbName)
		infoSchema, err := getInfoSchemaForShard(dataShard, sourceProfile.Driver, targetProfile)
		if err != nil {
			return nil, err
		}
		additionalDataAttributes := internal.AdditionalDataAttributes{
			ShardId: dataShard.DataShardId,
		}
		bw = performSnapshotMigration(config, conv, client, infoSchema, additionalDataAttributes)
	}

	return bw, nil
}

func getDynamoDBClientConfig() (*aws.Config, error) {
	cfg := aws.Config{}
	endpointOverride := os.Getenv("DYNAMODB_ENDPOINT_OVERRIDE")
	if endpointOverride != "" {
		cfg.Endpoint = aws.String(endpointOverride)
	}
	return &cfg, nil
}

func schemaFromDump(driver string, spDialect string, ioHelper *utils.IOStreams) (*internal.Conv, error) {
	f, n, err := getSeekable(ioHelper.In)
	if err != nil {
		utils.PrintSeekError(driver, err, ioHelper.Out)
		return nil, fmt.Errorf("can't get seekable input file")
	}
	ioHelper.SeekableIn = f
	ioHelper.BytesRead = n
	conv := internal.MakeConv()
	conv.SpDialect = spDialect
	p := internal.NewProgress(n, "Generating schema", internal.Verbose(), false, int(internal.SchemaCreationInProgress))
	r := internal.NewReader(bufio.NewReader(f), p)
	conv.SetSchemaMode() // Build schema and ignore data in dump.
	conv.SetDataSink(nil)
	err = ProcessDump(driver, conv, r)
	if err != nil {
		fmt.Fprintf(ioHelper.Out, "Failed to parse the data file: %v", err)
		return nil, fmt.Errorf("failed to parse the data file")
	}
	p.Done()
	return conv, nil
}

func dataFromDump(driver string, config writer.BatchWriterConfig, ioHelper *utils.IOStreams, client *sp.Client, conv *internal.Conv, dataOnly bool) (*writer.BatchWriter, error) {
	// TODO: refactor of the way we handle getSeekable
	// to avoid the code duplication here
	if !dataOnly {
		_, err := ioHelper.SeekableIn.Seek(0, 0)
		if err != nil {
			fmt.Printf("\nCan't seek to start of file (preparation for second pass): %v\n", err)
			return nil, fmt.Errorf("can't seek to start of file")
		}
	} else {
		// Note: input file is kept seekable to plan for future
		// changes in showing progress for data migration.
		f, n, err := getSeekable(ioHelper.In)
		if err != nil {
			utils.PrintSeekError(driver, err, ioHelper.Out)
			return nil, fmt.Errorf("can't get seekable input file")
		}
		ioHelper.SeekableIn = f
		ioHelper.BytesRead = n
	}
	totalRows := conv.Rows()

	conv.Audit.Progress = *internal.NewProgress(totalRows, "Writing data to Spanner", internal.Verbose(), false, int(internal.DataWriteInProgress))
	r := internal.NewReader(bufio.NewReader(ioHelper.SeekableIn), nil)
	batchWriter := populateDataConv(conv, config, client)
	ProcessDump(driver, conv, r)
	batchWriter.Flush()
	conv.Audit.Progress.Done()

	return batchWriter, nil
}

func dataFromCSV(ctx context.Context, sourceProfile profiles.SourceProfile, targetProfile profiles.TargetProfile, config writer.BatchWriterConfig, conv *internal.Conv, client *sp.Client) (*writer.BatchWriter, error) {
	if targetProfile.Conn.Sp.Dbname == "" {
		return nil, fmt.Errorf("dbName is mandatory in target-profile for csv source")
	}
	conv.SpDialect = targetProfile.Conn.Sp.Dialect
	dialect, err := targetProfile.FetchTargetDialect(ctx)
	if err != nil {
		return nil, fmt.Errorf("could not fetch dialect: %v", err)
	}
	if strings.ToLower(dialect) != constants.DIALECT_POSTGRESQL {
		dialect = constants.DIALECT_GOOGLESQL
	}

	if dialect != conv.SpDialect {
		return nil, fmt.Errorf("dialect specified in target profile does not match spanner dialect")
	}

	delimiterStr := sourceProfile.Csv.Delimiter
	if len(delimiterStr) != 1 {
		return nil, fmt.Errorf("delimiter should only be a single character long, found '%s'", delimiterStr)
	}

	delimiter := rune(delimiterStr[0])

	err = utils.ReadSpannerSchema(ctx, conv, client)
	if err != nil {
		return nil, fmt.Errorf("error trying to read and convert spanner schema: %v", err)
	}

	tables, err := csv.GetCSVFiles(conv, sourceProfile)
	if err != nil {
		return nil, fmt.Errorf("error finding csv files: %v", err)
	}

	// Find the number of rows in each csv file for generating stats.
	err = csv.SetRowStats(conv, tables, delimiter)
	if err != nil {
		return nil, err
	}

	totalRows := conv.Rows()
	conv.Audit.Progress = *internal.NewProgress(totalRows, "Writing data to Spanner", internal.Verbose(), false, int(internal.DataWriteInProgress))
	batchWriter := populateDataConv(conv, config, client)
	err = csv.ProcessCSV(conv, tables, sourceProfile.Csv.NullStr, delimiter)
	if err != nil {
		return nil, fmt.Errorf("can't process csv: %v", err)
	}
	batchWriter.Flush()
	conv.Audit.Progress.Done()
	return batchWriter, nil
}

func populateDataConv(conv *internal.Conv, config writer.BatchWriterConfig, client *sp.Client) *writer.BatchWriter {
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

// Report generates a report of schema and data conversion.
func Report(driver string, badWrites map[string]int64, BytesRead int64, banner string, conv *internal.Conv, reportFileName string, dbName string, out *os.File) {

	//Write the structured report file
	structuredReportFileName := fmt.Sprintf("%s.%s", reportFileName, "structured_report.json")
	structuredReport := reports.GenerateStructuredReport(driver, dbName, conv, badWrites, true, true)
	fBytes, _ := json.MarshalIndent(structuredReport, "", " ")
	f, err := os.Create(structuredReportFileName)
	if err != nil {
		fmt.Fprintf(out, "Can't write out structured report file %s: %v\n", reportFileName, err)
		fmt.Fprintf(out, "Writing report to stdout\n")
		f = out
	} else {
		defer f.Close()
	}
	f.Write(fBytes)

	//Write the text report file from the structured report
	textReportFileName := fmt.Sprintf("%s.%s", reportFileName, "report.txt")
	f, err = os.Create(textReportFileName)
	if err != nil {
		fmt.Fprintf(out, "Can't write out report file %s: %v\n", reportFileName, err)
		fmt.Fprintf(out, "Writing report to stdout\n")
		f = out
	} else {
		defer f.Close()
	}
	w := bufio.NewWriter(f)
	w.WriteString(banner)
	reports.GenerateTextReport(structuredReport, w)
	w.Flush()

	var isDump bool
	if strings.Contains(driver, "dump") {
		isDump = true
	}
	if isDump {
		fmt.Fprintf(out, "Processed %d bytes of %s data (%d statements, %d rows of data, %d errors, %d unexpected conditions).\n",
			BytesRead, driver, conv.Statements(), conv.Rows(), conv.StatementErrors(), conv.Unexpecteds())
	} else {
		fmt.Fprintf(out, "Processed source database via %s driver (%d rows of data, %d unexpected conditions).\n",
			driver, conv.Rows(), conv.Unexpecteds())
	}
	// We've already written summary to f (as part of GenerateReport).
	// In the case where f is stdout, don't write a duplicate copy.
	if f != out {
		fmt.Fprint(out, structuredReport.Summary.Text)
		fmt.Fprintf(out, "See file '%s' for details of the schema and data conversions.\n", reportFileName)
	}
}

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
	fcopy, err := ioutil.TempFile("", "harbourbridge.data")
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

// VerifyDb checks whether the db exists and if it does, verifies if the schema is what we currently support.
func VerifyDb(ctx context.Context, adminClient *database.DatabaseAdminClient, dbURI string) (dbExists bool, err error) {
	dbExists, err = CheckExistingDb(ctx, adminClient, dbURI)
	if err != nil {
		return dbExists, err
	}
	if dbExists {
		err = ValidateDDL(ctx, adminClient, dbURI)
	}
	return dbExists, err
}

// CheckExistingDb checks whether the database with dbURI exists or not.
// If API call doesn't respond then user is informed after every 5 minutes on command line.
func CheckExistingDb(ctx context.Context, adminClient *database.DatabaseAdminClient, dbURI string) (bool, error) {
	gotResponse := make(chan bool)
	var err error
	go func() {
		_, err = adminClient.GetDatabase(ctx, &adminpb.GetDatabaseRequest{Name: dbURI})
		gotResponse <- true
	}()
	for {
		select {
		case <-time.After(5 * time.Minute):
			fmt.Println("WARNING! API call not responding: make sure that spanner api endpoint is configured properly")
		case <-gotResponse:
			if err != nil {
				if utils.ContainsAny(strings.ToLower(err.Error()), []string{"database not found"}) {
					return false, nil
				}
				return false, fmt.Errorf("can't get database info: %s", err)
			}
			return true, nil
		}
	}
}

// ValidateTables validates that all the tables in the database are empty.
// It returns the name of the first non-empty table if found, and an empty string otherwise.
func ValidateTables(ctx context.Context, client *sp.Client, spDialect string) (string, error) {
	infoSchema := spanner.InfoSchemaImpl{Client: client, Ctx: ctx, SpDialect: spDialect}
	tables, err := infoSchema.GetTables()
	if err != nil {
		return "", err
	}
	for _, table := range tables {
		count, err := infoSchema.GetRowCount(table)
		if err != nil {
			return "", err
		}
		if count != 0 {
			return table.Name, nil
		}
	}
	return "", nil
}

// ValidateDDL verifies if an existing DB's ddl follows what is supported by harbourbridge. Currently,
// we only support empty schema when db already exists.
func ValidateDDL(ctx context.Context, adminClient *database.DatabaseAdminClient, dbURI string) error {
	dbDdl, err := adminClient.GetDatabaseDdl(ctx, &adminpb.GetDatabaseDdlRequest{Database: dbURI})
	if err != nil {
		return fmt.Errorf("can't fetch database ddl: %v", err)
	}
	if len(dbDdl.Statements) != 0 {
		return fmt.Errorf("harbourBridge supports writing to existing databases only if they have an empty schema")
	}
	return nil
}

// CreatesOrUpdatesDatabase updates an existing Spanner database or creates a new one if one does not exist.
func CreateOrUpdateDatabase(ctx context.Context, adminClient *database.DatabaseAdminClient, dbURI, driver string, conv *internal.Conv, out *os.File, migrationType string) error {
	dbExists, err := VerifyDb(ctx, adminClient, dbURI)
	if err != nil {
		return err
	}
	if !conv.Audit.SkipMetricsPopulation {
		// Adding migration metadata to the outgoing context.
		migrationData := metrics.GetMigrationData(conv, driver, constants.SchemaConv)
		serializedMigrationData, _ := proto.Marshal(migrationData)
		migrationMetadataValue := base64.StdEncoding.EncodeToString(serializedMigrationData)
		ctx = metadata.AppendToOutgoingContext(ctx, constants.MigrationMetadataKey, migrationMetadataValue)
	}
	if dbExists {
		if conv.SpDialect != constants.DIALECT_POSTGRESQL && migrationType == constants.DATAFLOW_MIGRATION {
			return fmt.Errorf("Harbourbridge does not support minimal downtime schema/schema-and-data migrations to an existing database.")
		}
		err := UpdateDatabase(ctx, adminClient, dbURI, conv, out, driver)
		if err != nil {
			return fmt.Errorf("can't update database schema: %v", err)
		}
	} else {
		err := CreateDatabase(ctx, adminClient, dbURI, conv, out, driver, migrationType)
		if err != nil {
			return fmt.Errorf("can't create database: %v", err)
		}
	}
	return nil
}

// CreateDatabase returns a newly create Spanner DB.
// It automatically determines an appropriate project, selects a
// Spanner instance to use, generates a new Spanner DB name,
// and call into the Spanner admin interface to create the new DB.
func CreateDatabase(ctx context.Context, adminClient *database.DatabaseAdminClient, dbURI string, conv *internal.Conv, out *os.File, driver string, migrationType string) error {
	project, instance, dbName := utils.ParseDbURI(dbURI)
	fmt.Fprintf(out, "Creating new database %s in instance %s with default permissions ... \n", dbName, instance)
	// The schema we send to Spanner excludes comments (since Cloud
	// Spanner DDL doesn't accept them), and protects table and col names
	// using backticks (to avoid any issues with Spanner reserved words).
	// Foreign Keys are set to false since we create them post data migration.
	req := &adminpb.CreateDatabaseRequest{
		Parent: fmt.Sprintf("projects/%s/instances/%s", project, instance),
	}
	if conv.SpDialect == constants.DIALECT_POSTGRESQL {
		// PostgreSQL dialect doesn't support:
		// a) backticks around the database name, and
		// b) DDL statements as part of a CreateDatabase operation (so schema
		// must be set using a separate UpdateDatabase operation).
		req.CreateStatement = "CREATE DATABASE \"" + dbName + "\""
		req.DatabaseDialect = adminpb.DatabaseDialect_POSTGRESQL
	} else {
		req.CreateStatement = "CREATE DATABASE `" + dbName + "`"
		if migrationType == constants.DATAFLOW_MIGRATION {
			req.ExtraStatements = conv.SpSchema.GetDDL(ddl.Config{Comments: false, ProtectIds: true, Tables: true, ForeignKeys: true, SpDialect: conv.SpDialect, Source: driver})
		} else {
			req.ExtraStatements = conv.SpSchema.GetDDL(ddl.Config{Comments: false, ProtectIds: true, Tables: true, ForeignKeys: false, SpDialect: conv.SpDialect, Source: driver})
		}

	}

	op, err := adminClient.CreateDatabase(ctx, req)
	if err != nil {
		return fmt.Errorf("can't build CreateDatabaseRequest: %w", utils.AnalyzeError(err, dbURI))
	}
	if _, err := op.Wait(ctx); err != nil {
		return fmt.Errorf("createDatabase call failed: %w", utils.AnalyzeError(err, dbURI))
	}
	fmt.Fprintf(out, "Created database successfully.\n")

	if conv.SpDialect == constants.DIALECT_POSTGRESQL {
		// Update schema separately for PG databases.
		return UpdateDatabase(ctx, adminClient, dbURI, conv, out, driver)
	}
	return nil
}

// UpdateDatabase updates an existing spanner database.
func UpdateDatabase(ctx context.Context, adminClient *database.DatabaseAdminClient, dbURI string, conv *internal.Conv, out *os.File, driver string) error {
	fmt.Fprintf(out, "Updating schema for %s with default permissions ... \n", dbURI)
	// The schema we send to Spanner excludes comments (since Cloud
	// Spanner DDL doesn't accept them), and protects table and col names
	// using backticks (to avoid any issues with Spanner reserved words).
	// Foreign Keys are set to false since we create them post data migration.
	schema := conv.SpSchema.GetDDL(ddl.Config{Comments: false, ProtectIds: true, Tables: true, ForeignKeys: false, SpDialect: conv.SpDialect, Source: driver})
	req := &adminpb.UpdateDatabaseDdlRequest{
		Database:   dbURI,
		Statements: schema,
	}
	// Update queries for postgres as target db return response after more
	// than 1 min for large schemas, therefore, timeout is specified as 5 minutes
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	op, err := adminClient.UpdateDatabaseDdl(ctx, req)
	if err != nil {
		return fmt.Errorf("can't build UpdateDatabaseDdlRequest: %w", utils.AnalyzeError(err, dbURI))
	}
	if err := op.Wait(ctx); err != nil {
		return fmt.Errorf("UpdateDatabaseDdl call failed: %w", utils.AnalyzeError(err, dbURI))
	}
	fmt.Fprintf(out, "Updated schema successfully.\n")
	return nil
}

// UpdateDDLForeignKeys updates the Spanner database with foreign key
// constraints using ALTER TABLE statements.
func UpdateDDLForeignKeys(ctx context.Context, adminClient *database.DatabaseAdminClient, dbURI string, conv *internal.Conv, out *os.File, driver string, migrationType string) error {

	if conv.SpDialect != constants.DIALECT_POSTGRESQL && migrationType == constants.DATAFLOW_MIGRATION {
		//foreign keys were applied as part of CreateDatabase
		return nil
	}

	// The schema we send to Spanner excludes comments (since Cloud
	// Spanner DDL doesn't accept them), and protects table and col names
	// using backticks (to avoid any issues with Spanner reserved words).
	fkStmts := conv.SpSchema.GetDDL(ddl.Config{Comments: false, ProtectIds: true, Tables: false, ForeignKeys: true, SpDialect: conv.SpDialect, Source: driver})
	if len(fkStmts) == 0 {
		return nil
	}
	if len(fkStmts) > 50 {
		fmt.Println(`
Warning: Large number of foreign keys detected. Spanner can take a long amount of 
time to create foreign keys (over 5 mins per batch of Foreign Keys even with no data). 
Harbourbridge does not have control over a single foreign key creation time. The number 
of concurrent Foreign Key Creation Requests sent to spanner can be increased by 
tweaking the MaxWorkers variable (https://github.com/GoogleCloudPlatform/spanner-migration-tool/blob/master/conversion/conversion.go#L89).
However, setting it to a very high value might lead to exceeding the admin quota limit. Spanner migration tool tries to stay under the
admin quota limit by spreading the FK creation requests over time.`)
	}
	msg := fmt.Sprintf("Updating schema of database %s with foreign key constraints ...", dbURI)
	conv.Audit.Progress = *internal.NewProgress(int64(len(fkStmts)), msg, internal.Verbose(), true, int(internal.ForeignKeyUpdateInProgress))

	workers := make(chan int, MaxWorkers)
	for i := 1; i <= MaxWorkers; i++ {
		workers <- i
	}
	var progressMutex sync.Mutex
	progress := int64(0)

	// We dispatch parallel foreign key create requests to ensure the backfill runs in parallel to reduce overall time.
	// This cuts down the time taken to a third (approx) compared to Serial and Batched creation. We also do not want to create
	// too many requests and get throttled due to network or hitting catalog memory limits.
	// Ensure atmost `MaxWorkers` go routines run in parallel that each update the ddl with one foreign key statement.
	for _, fkStmt := range fkStmts {
		workerID := <-workers
		go func(fkStmt string, workerID int) {
			defer func() {
				// Locking the progress reporting otherwise progress results displayed could be in random order.
				progressMutex.Lock()
				progress++
				conv.Audit.Progress.MaybeReport(progress)
				progressMutex.Unlock()
				workers <- workerID
			}()
			internal.VerbosePrintf("Submitting new FK create request: %s\n", fkStmt)
			logger.Log.Debug("Submitting new FK create request", zap.String("fkStmt", fkStmt))

			op, err := adminClient.UpdateDatabaseDdl(ctx, &adminpb.UpdateDatabaseDdlRequest{
				Database:   dbURI,
				Statements: []string{fkStmt},
			})
			if err != nil {
				fmt.Printf("Cannot submit request for create foreign key with statement: %s\n due to error: %s. Skipping this foreign key...\n", fkStmt, err)
				conv.Unexpected(fmt.Sprintf("Can't add foreign key with statement %s: %s", fkStmt, err))
				return
			}
			if err := op.Wait(ctx); err != nil {
				fmt.Printf("Can't add foreign key with statement: %s\n due to error: %s. Skipping this foreign key...\n", fkStmt, err)
				conv.Unexpected(fmt.Sprintf("Can't add foreign key with statement %s: %s", fkStmt, err))
				return
			}
			internal.VerbosePrintln("Updated schema with statement: " + fkStmt)
			logger.Log.Debug("Updated schema with statement", zap.String("fkStmt", fkStmt))
		}(fkStmt, workerID)
		// Send out an FK creation request every second, with total of maxWorkers request being present in a batch.
		time.Sleep(time.Second)
	}
	// Wait for all the goroutines to finish.
	for i := 1; i <= MaxWorkers; i++ {
		<-workers
	}
	conv.Audit.Progress.UpdateProgress("Foreign key update complete.", 100, internal.ForeignKeyUpdateComplete)
	conv.Audit.Progress.Done()
	return nil
}

// WriteSchemaFile writes DDL statements in a file. It includes CREATE TABLE
// statements and ALTER TABLE statements to add foreign keys.
// The parameter name should end with a .txt.
func WriteSchemaFile(conv *internal.Conv, now time.Time, name string, out *os.File, driver string) {
	f, err := os.Create(name)
	if err != nil {
		fmt.Fprintf(out, "Can't create schema file %s: %v\n", name, err)
		return
	}

	// The schema file we write out below is optimized for reading. It includes comments, foreign keys
	// and doesn't add backticks around table and column names. This file is
	// intended for explanatory and documentation purposes, and is not strictly
	// legal Cloud Spanner DDL (Cloud Spanner doesn't currently support comments).
	spDDL := conv.SpSchema.GetDDL(ddl.Config{Comments: true, ProtectIds: false, Tables: true, ForeignKeys: true, SpDialect: conv.SpDialect, Source: driver})
	if len(spDDL) == 0 {
		spDDL = []string{"\n-- Schema is empty -- no tables found\n"}
	}
	l := []string{
		fmt.Sprintf("-- Schema generated %s\n", now.Format("2006-01-02 15:04:05")),
		strings.Join(spDDL, ";\n\n"),
		"\n",
	}
	if _, err := f.WriteString(strings.Join(l, "")); err != nil {
		fmt.Fprintf(out, "Can't write out schema file: %v\n", err)
		return
	}
	fmt.Fprintf(out, "Wrote schema to file '%s'.\n", name)

	// Convert <file_name>.<ext> to <file_name>.ddl.<ext>.
	nameSplit := strings.Split(name, ".")
	nameSplit = append(nameSplit[:len(nameSplit)-1], "ddl", nameSplit[len(nameSplit)-1])
	name = strings.Join(nameSplit, ".")
	f, err = os.Create(name)
	if err != nil {
		fmt.Fprintf(out, "Can't create legal schema ddl file %s: %v\n", name, err)
		return
	}

	// We change 'Comments' to false and 'ProtectIds' to true below to write out a
	// schema file that is a legal Cloud Spanner DDL.
	spDDL = conv.SpSchema.GetDDL(ddl.Config{Comments: false, ProtectIds: true, Tables: true, ForeignKeys: true, SpDialect: conv.SpDialect, Source: driver})
	if len(spDDL) == 0 {
		spDDL = []string{"\n-- Schema is empty -- no tables found\n"}
	}
	l = []string{
		strings.Join(spDDL, ";\n\n"),
		"\n",
	}
	if _, err = f.WriteString(strings.Join(l, "")); err != nil {
		fmt.Fprintf(out, "Can't write out legal schema ddl file: %v\n", err)
		return
	}
	fmt.Fprintf(out, "Wrote legal schema ddl to file '%s'.\n", name)
}

// WriteSessionFile writes conv struct to a file in JSON format.
func WriteSessionFile(conv *internal.Conv, name string, out *os.File) {
	f, err := os.Create(name)
	if err != nil {
		fmt.Fprintf(out, "Can't create session file %s: %v\n", name, err)
		return
	}
	// Session file will basically contain 'conv' struct in JSON format.
	// It contains all the information for schema and data conversion state.
	convJSON, err := json.MarshalIndent(conv, "", " ")
	if err != nil {
		fmt.Fprintf(out, "Can't encode session state to JSON: %v\n", err)
		return
	}
	if _, err := f.Write(convJSON); err != nil {
		fmt.Fprintf(out, "Can't write out session file: %v\n", err)
		return
	}
	fmt.Fprintf(out, "Wrote session to file '%s'.\n", name)
}

// WriteConvGeneratedFiles creates a directory labeled downloads with the current timestamp
// where it writes the sessionfile, report summary and DDLs then returns the directory where it writes.
func WriteConvGeneratedFiles(conv *internal.Conv, dbName string, driver string, BytesRead int64, out *os.File) (string, error) {
	now := time.Now()
	dirPath := "harbour_bridge_output/" + dbName + "/"
	err := os.MkdirAll(dirPath, os.ModePerm)
	if err != nil {
		fmt.Fprintf(out, "Can't create directory %s: %v\n", dirPath, err)
		return "", err
	}
	schemaFileName := dirPath + dbName + "_schema.txt"
	WriteSchemaFile(conv, now, schemaFileName, out, driver)
	reportFileName := dirPath + dbName
	Report(driver, nil, BytesRead, "", conv, reportFileName, dbName, out)
	sessionFileName := dirPath + dbName + ".session.json"
	WriteSessionFile(conv, sessionFileName, out)
	return dirPath, nil
}

// ReadSessionFile reads a session JSON file and
// unmarshal it's content into *internal.Conv.
func ReadSessionFile(conv *internal.Conv, sessionJSON string) error {
	s, err := ioutil.ReadFile(sessionJSON)
	if err != nil {
		return err
	}
	err = json.Unmarshal(s, &conv)
	if err != nil {
		return err
	}
	return nil
}

// WriteBadData prints summary stats about bad rows and writes detailed info
// to file 'name'.
func WriteBadData(bw *writer.BatchWriter, conv *internal.Conv, banner, name string, out *os.File) {
	badConversions := conv.BadRows()
	badWrites := utils.SumMapValues(bw.DroppedRowsByTable())

	badDataStreaming := int64(0)
	if conv.Audit.StreamingStats.Streaming {
		badDataStreaming = getBadStreamingDataCount(conv)
	}

	if badConversions == 0 && badWrites == 0 && badDataStreaming == 0 {
		os.Remove(name) // Cleanup bad-data file from previous run.
		return
	}
	f, err := os.Create(name)
	if err != nil {
		fmt.Fprintf(out, "Can't write out bad data file: %v\n", err)
		return
	}
	f.WriteString(banner)
	maxRows := 100
	if badConversions > 0 {
		l := conv.SampleBadRows(maxRows)
		if int64(len(l)) < badConversions {
			f.WriteString("A sample of rows that generated conversion errors:\n")
		} else {
			f.WriteString("Rows that generated conversion errors:\n")
		}
		for _, r := range l {
			_, err := f.WriteString("  " + r + "\n")
			if err != nil {
				fmt.Fprintf(out, "Can't write out bad data file: %v\n", err)
				return
			}
		}
	}
	if badWrites > 0 {
		l := bw.SampleBadRows(maxRows)
		if int64(len(l)) < badWrites {
			f.WriteString("A sample of rows that successfully converted but couldn't be written to Spanner:\n")
		} else {
			f.WriteString("Rows that successfully converted but couldn't be written to Spanner:\n")
		}
		for _, r := range l {
			_, err := f.WriteString("  " + r + "\n")
			if err != nil {
				fmt.Fprintf(out, "Can't write out bad data file: %v\n", err)
				return
			}
		}
	}
	if badDataStreaming > 0 {
		err = writeBadStreamingData(conv, f)
		if err != nil {
			fmt.Fprintf(out, "Can't write out bad data file: %v\n", err)
			return
		}
	}

	fmt.Fprintf(out, "See file '%s' for details of bad rows\n", name)
}

// getBadStreamingDataCount returns the total sum of bad and dropped records during
// streaming migration process.
func getBadStreamingDataCount(conv *internal.Conv) int64 {
	badDataCount := int64(0)

	for _, x := range conv.Audit.StreamingStats.BadRecords {
		badDataCount += utils.SumMapValues(x)
	}
	for _, x := range conv.Audit.StreamingStats.DroppedRecords {
		badDataCount += utils.SumMapValues(x)
	}
	return badDataCount
}

// writeBadStreamingData writes sample of bad records and dropped records during streaming
// migration process to bad data file.
func writeBadStreamingData(conv *internal.Conv, f *os.File) error {
	f.WriteString("\nBad data encountered during streaming migration:\n\n")

	stats := (conv.Audit.StreamingStats)

	badRecords := int64(0)
	for _, x := range stats.BadRecords {
		badRecords += utils.SumMapValues(x)
	}
	droppedRecords := int64(0)
	for _, x := range stats.DroppedRecords {
		droppedRecords += utils.SumMapValues(x)
	}

	if badRecords > 0 {
		l := stats.SampleBadRecords
		if int64(len(l)) < badRecords {
			f.WriteString("A sample of records that generated conversion errors:\n")
		} else {
			f.WriteString("Records that generated conversion errors:\n")
		}
		for _, r := range l {
			_, err := f.WriteString("  " + r + "\n")
			if err != nil {
				return err
			}
		}
		f.WriteString("\n")
	}
	if droppedRecords > 0 {
		l := stats.SampleBadWrites
		if int64(len(l)) < droppedRecords {
			f.WriteString("A sample of records that successfully converted but couldn't be written to Spanner:\n")
		} else {
			f.WriteString("Records that successfully converted but couldn't be written to Spanner:\n")
		}
		for _, r := range l {
			_, err := f.WriteString("  " + r + "\n")
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// ProcessDump invokes process dump function from a sql package based on driver selected.
func ProcessDump(driver string, conv *internal.Conv, r *internal.Reader) error {
	switch driver {
	case constants.MYSQLDUMP:
		return common.ProcessDbDump(conv, r, mysql.DbDumpImpl{})
	case constants.PGDUMP:
		return common.ProcessDbDump(conv, r, postgres.DbDumpImpl{})
	default:
		return fmt.Errorf("process dump for driver %s not supported", driver)
	}
}

func GetInfoSchema(sourceProfile profiles.SourceProfile, targetProfile profiles.TargetProfile) (common.InfoSchema, error) {
	connectionConfig, err := connectionConfig(sourceProfile)
	if err != nil {
		return nil, err
	}
	driver := sourceProfile.Driver
	switch driver {
	case constants.MYSQL:
		db, err := sql.Open(driver, connectionConfig.(string))
		dbName := getDbNameFromSQLConnectionStr(driver, connectionConfig.(string))
		if err != nil {
			return nil, err
		}
		return mysql.InfoSchemaImpl{
			DbName:        dbName,
			Db:            db,
			SourceProfile: sourceProfile,
			TargetProfile: targetProfile,
		}, nil
	case constants.POSTGRES:
		db, err := sql.Open(driver, connectionConfig.(string))
		if err != nil {
			return nil, err
		}
		temp := false
		return postgres.InfoSchemaImpl{
			Db:             db,
			SourceProfile:  sourceProfile,
			TargetProfile:  targetProfile,
			IsSchemaUnique: &temp, //this is a workaround to set a bool pointer
		}, nil
	case constants.DYNAMODB:
		mySession := session.Must(session.NewSession())
		dydbClient := dydb.New(mySession, connectionConfig.(*aws.Config))
		var dydbStreamsClient *dynamodbstreams.DynamoDBStreams
		if sourceProfile.Conn.Streaming {
			newSession := session.Must(session.NewSession())
			dydbStreamsClient = dynamodbstreams.New(newSession, connectionConfig.(*aws.Config))
		}
		return dynamodb.InfoSchemaImpl{
			DynamoClient:        dydbClient,
			SampleSize:          profiles.GetSchemaSampleSize(sourceProfile),
			DynamoStreamsClient: dydbStreamsClient,
		}, nil
	case constants.SQLSERVER:
		db, err := sql.Open(driver, connectionConfig.(string))
		dbName := getDbNameFromSQLConnectionStr(driver, connectionConfig.(string))
		if err != nil {
			return nil, err
		}
		return sqlserver.InfoSchemaImpl{DbName: dbName, Db: db}, nil
	case constants.ORACLE:
		db, err := sql.Open(driver, connectionConfig.(string))
		dbName := getDbNameFromSQLConnectionStr(driver, connectionConfig.(string))
		if err != nil {
			return nil, err
		}
		return oracle.InfoSchemaImpl{DbName: strings.ToUpper(dbName), Db: db, SourceProfile: sourceProfile, TargetProfile: targetProfile}, nil
	default:
		return nil, fmt.Errorf("driver %s not supported", driver)
	}
}

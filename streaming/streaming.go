// Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package streaming

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strconv"
	"strings"
	"sync"
	"time"

	dataflow "cloud.google.com/go/dataflow/apiv1beta3"
	datastream "cloud.google.com/go/datastream/apiv1"
	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	datastreampb "google.golang.org/genproto/googleapis/cloud/datastream/v1"
	dataflowpb "google.golang.org/genproto/googleapis/dataflow/v1beta3"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	resourcemanager "cloud.google.com/go/resourcemanager/apiv3"
	resourcemanagerpb "cloud.google.com/go/resourcemanager/apiv3/resourcemanagerpb"
	dataflowaccessor "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/dataflow"
	storageacc "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/storage"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/utils"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/profiles"
	"github.com/google/uuid"
	"github.com/googleapis/gax-go/v2"
	"go.uber.org/ratelimit"
	"google.golang.org/grpc/codes"
)

var (
	// Default value for max concurrent backfill tasks in Datastream. Datastream resorts to its default value for 0.
	maxCdcTasks int32 = 5
	// Default value for max concurrent backfill tasks in Datastream.
	maxBackfillTasks int32 = 50
	// Min allowed value for max concurrent backfill/CDC tasks in Datastream. 0 value results in the default value being used and hence, is valid.
	MIN_DATASTREAM_TASK_LIMIT int32 = 0
	// Max allowed value for max concurrent backfill/CDC tasks in Datastream.
	MAX_DATASTREAM_TASK_LIMIT int32 = 50
	// Default value for maxWorkers.
	maxWorkers int32 = 50
	// Default value for NumWorkers.
	numWorkers int32 = 1
	// Max allowed value for maxWorkers and numWorkers.
	MAX_WORKER_LIMIT int32 = 1000
	// Min allowed value for maxWorkers and numWorkers.
	MIN_WORKER_LIMIT int32 = 1
	// Default gcs path of the Dataflow template.
	DEFAULT_TEMPLATE_PATH string = "gs://dataflow-templates-southamerica-west1/2023-09-12-00_RC00/flex/Cloud_Datastream_to_Spanner"

	DEFAULT_DATASTREAM_CLIENT_BACKOFF_BASE_DELAY time.Duration = 1.0 * time.Second
	DEFAULT_DATASTREAM_CLIENT_BACKOFF_MAX_DELAY  time.Duration = 900 * time.Second
	DEFAULT_DATASTREAM_CLIENT_BACKOFF_MULTIPLIER float64       = 1.6
	DEFAULT_DATASTREAM_RETRY_CODES               []codes.Code  = []codes.Code{
		codes.DeadlineExceeded,
		codes.Unavailable,
		codes.ResourceExhausted,
		codes.Unknown,
	}

	// DataStream by default provides 20 API calls per second.
	// Each launch operation calls datastream twice, ignoring `op.wait`, hence keeping it at 10 per second.
	DEFAULT_DATASTREAM_LAUNCH_RATE_PER_SEC int = 10
	// TODO(vardhanvthigle): Caliberate this.
	// Keeping it less for now since the dataFlow launch operation makes outbound calls to various clients.
	// Keeping it at 1 per second will not impact the actual time it takes to provision resources for a large
	// scale migration, as that depends a lot on actual time a batch of 20 (current parallelization in code) tkaes
	// to complete it's provisioning which includes waiting for various operations.
	DEFAULT_DATAFLOW_LAUNCH_RATE_PER_SEC int = 1
	// Rate Limiters
	// A coarse delay based rate limiting for launching datastream.
	DATA_STREAM_RL ratelimit.Limiter = ratelimit.New(DEFAULT_DATASTREAM_LAUNCH_RATE_PER_SEC)

	// A coarse delay based rate limiting for launching DataFlow.
	DATA_FLOW_RL ratelimit.Limiter = ratelimit.New(DEFAULT_DATAFLOW_LAUNCH_RATE_PER_SEC)
)

type SrcConnCfg struct {
	Name     string
	Location string
}

type DstConnCfg struct {
	Name     string
	Location string
	Prefix   string
}

type DatastreamCfg struct {
	StreamId                    string                            `json:"streamId"`
	StreamLocation              string                            `json:"streamLocation"`
	StreamDisplayName           string                            `json:"streamDisplayName"`
	SourceConnectionConfig      SrcConnCfg                        `json:"sourceConnectionConfig"`
	DestinationConnectionConfig DstConnCfg                        `json:"destinationConnectionConfig"`
	Properties                  string                            `json:"properties"`
	SchemaDetails               map[string]internal.SchemaDetails `json:"-"`
	MaxConcurrentBackfillTasks  string                            `json:"maxConcurrentBackfillTasks"`
	MaxConcurrentCdcTasks       string                            `json:"maxConcurrentCdcTasks"`
}

type GcsCfg struct {
	TtlInDays    int64 `json:"ttlInDays"`
	TtlInDaysSet bool  `json:"ttlInDaysSet"`
}

type DataflowCfg struct {
	ProjectId            string            `json:"projectId"`
	JobName              string            `json:"jobName"`
	Location             string            `json:"location"`
	VpcHostProjectId     string            `json:"hostProjectId"`
	Network              string            `json:"network"`
	Subnetwork           string            `json:"subnetwork"`
	MaxWorkers           string            `json:"maxWorkers"`
	NumWorkers           string            `json:"numWorkers"`
	ServiceAccountEmail  string            `json:"serviceAccountEmail"`
	MachineType          string            `json:"machineType"`
	AdditionalUserLabels string            `json:"additionalUserLabels"`
	KmsKeyName           string            `json:"kmsKeyName"`
	GcsTemplatePath      string            `json:"gcsTemplatePath"`
	DbNameToShardIdMap   map[string]string `json:"dbNameToShardIdMap"`
}

type StreamingCfg struct {
	DatastreamCfg DatastreamCfg            `json:"datastreamCfg"`
	GcsCfg        GcsCfg                   `json:"gcsCfg"`
	DataflowCfg   DataflowCfg              `json:"dataflowCfg"`
	TmpDir        string                   `json:"tmpDir"`
	PubsubCfg     internal.PubsubResources `json:"pubsubCfg"`
	DataShardId   string                   `json:"dataShardId"`
}

// Returns the retry error codes and backoff policy to the GCP client retry logic.
func dataStreamGaxRetrier() gax.Retryer {
	return gax.OnCodes(DEFAULT_DATASTREAM_RETRY_CODES, gax.Backoff{
		Initial:    DEFAULT_DATASTREAM_CLIENT_BACKOFF_BASE_DELAY,
		Max:        DEFAULT_DATASTREAM_CLIENT_BACKOFF_MAX_DELAY,
		Multiplier: DEFAULT_DATASTREAM_CLIENT_BACKOFF_MULTIPLIER,
	})
}

// VerifyAndUpdateCfg checks the fields and errors out if certain fields are empty.
// It then auto-populates certain empty fields like StreamId and Dataflow JobName.
func VerifyAndUpdateCfg(streamingCfg *StreamingCfg, dbName string, schemaDetails map[string]internal.SchemaDetails) error {
	dsCfg := streamingCfg.DatastreamCfg
	if dsCfg.StreamLocation == "" {
		return fmt.Errorf("please specify DatastreamCfg.StreamLocation in the streaming config")
	}
	srcCfg := dsCfg.SourceConnectionConfig
	if srcCfg.Name == "" || srcCfg.Location == "" {
		return fmt.Errorf("please specify Name and Location under DatastreamCfg.SourceConnectionConfig in the streaming config")
	}
	dstCfg := dsCfg.DestinationConnectionConfig
	if dstCfg.Name == "" || dstCfg.Location == "" {
		return fmt.Errorf("please specify Name and Location under DatastreamCfg.DestinationConnectionConfig in the streaming config")
	}

	dfCfg := streamingCfg.DataflowCfg
	if dfCfg.Location == "" {
		return fmt.Errorf("please specify the Location under DataflowCfg in the streaming config")
	}

	// If both ID and Display name are empty, generate a new one for both.
	// If either is present, assign it to the other one.
	if dsCfg.StreamId == "" && dsCfg.StreamDisplayName == "" {
		// TODO: Update names to have more info like dbname.
		streamId, err := utils.GenerateName("smt-stream-" + dbName)
		streamId = strings.Replace(streamId, "_", "-", -1)
		if err != nil {
			return fmt.Errorf("error generating stream name: %v", err)
		}
		streamingCfg.DatastreamCfg.StreamId = streamId
		streamingCfg.DatastreamCfg.StreamDisplayName = streamId
	} else if dsCfg.StreamId == "" {
		streamingCfg.DatastreamCfg.StreamId = streamingCfg.DatastreamCfg.StreamDisplayName
	} else if dsCfg.StreamDisplayName == "" {
		streamingCfg.DatastreamCfg.StreamDisplayName = streamingCfg.DatastreamCfg.StreamId
	}

	streamingCfg.DatastreamCfg.SchemaDetails = schemaDetails

	if dsCfg.MaxConcurrentCdcTasks != "" {
		intVal, err := strconv.ParseInt(dsCfg.MaxConcurrentCdcTasks, 10, 64)
		if err != nil {
			return fmt.Errorf("could not parse maxConcurrentCdcTasks parameter %s, please provide a positive integer as input", dsCfg.MaxConcurrentCdcTasks)
		}
		maxCdcTasks = int32(intVal)
		if maxCdcTasks < MIN_DATASTREAM_TASK_LIMIT || maxCdcTasks > MAX_DATASTREAM_TASK_LIMIT {
			return fmt.Errorf("maxConcurrentCdcTasks should lie in the range [%d, %d]", MIN_DATASTREAM_TASK_LIMIT, MAX_DATASTREAM_TASK_LIMIT)
		}
	}
	if dsCfg.MaxConcurrentBackfillTasks != "" {
		intVal, err := strconv.ParseInt(dsCfg.MaxConcurrentBackfillTasks, 10, 64)
		if err != nil {
			return fmt.Errorf("could not parse maxConcurrentBackfillTasks parameter %s, please provide a positive integer as input", dsCfg.MaxConcurrentBackfillTasks)
		}
		maxBackfillTasks = int32(intVal)
		if maxBackfillTasks < MIN_DATASTREAM_TASK_LIMIT || maxBackfillTasks > MAX_DATASTREAM_TASK_LIMIT {
			return fmt.Errorf("maxConcurrentBackfillTasks should lie in the range [%d, %d]", MIN_DATASTREAM_TASK_LIMIT, MAX_DATASTREAM_TASK_LIMIT)
		}
	}

	if dfCfg.JobName == "" {
		// Update names to have more info like dbname.
		jobName, err := utils.GenerateName("smt-dataflow-" + dbName)
		jobName = strings.Replace(jobName, "_", "-", -1)
		if err != nil {
			return fmt.Errorf("error generating stream name: %v", err)
		}
		streamingCfg.DataflowCfg.JobName = jobName
	}

	filePath := streamingCfg.TmpDir
	u, err := utils.ParseGCSFilePath(filePath)
	if err != nil {
		return fmt.Errorf("parseFilePath: unable to parse file path: %v", err)
	}
	// We update the TmpDir in case any '/' were added in ParseGCSFilePath().
	streamingCfg.TmpDir = u.String()
	bucketName := u.Host
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create GCS client")
	}
	defer client.Close()
	bucket := client.Bucket(bucketName)
	_, err = bucket.Attrs(ctx)
	if err != nil {
		return fmt.Errorf("bucket %s does not exist", bucketName)
	}

	// Verify GCS bucket tuning configs.
	if streamingCfg.GcsCfg.TtlInDaysSet {
		ttl := streamingCfg.GcsCfg.TtlInDays
		if ttl <= 0 {
			return fmt.Errorf("ttlInDays should be a positive integer")
		}
	}
	return nil
}

// ReadStreamingConfig reads the file and unmarshalls it into the StreamingCfg struct.
func ReadStreamingConfig(file, dbName string, schemaDetails map[string]internal.SchemaDetails) (StreamingCfg, error) {
	streamingCfg := StreamingCfg{}
	cfgFile, err := ioutil.ReadFile(file)
	if err != nil {
		return streamingCfg, fmt.Errorf("can't read streaming config file due to: %v", err)
	}
	err = json.Unmarshal(cfgFile, &streamingCfg)
	if err != nil {
		return streamingCfg, fmt.Errorf("unable to unmarshall json due to: %v", err)
	}
	err = VerifyAndUpdateCfg(&streamingCfg, dbName, schemaDetails)
	if err != nil {
		return streamingCfg, fmt.Errorf("streaming config is incomplete: %v", err)
	}
	return streamingCfg, nil
}

// dbName is the name of the database to be migrated.
// tabeList is the common list of tables that need to be migrated from each database
func getMysqlSourceStreamConfig(dbList []profiles.LogicalShard, datastreamCfg DatastreamCfg) (*datastreampb.SourceConfig_MysqlSourceConfig, error) {
	schemaDetails := datastreamCfg.SchemaDetails
	mysqlTables := []*datastreampb.MysqlTable{}
	for _, tableList := range schemaDetails {
		for _, table := range tableList.TableDetails {
			includeTable := &datastreampb.MysqlTable{
				Table: table.TableName,
			}
			mysqlTables = append(mysqlTables, includeTable)
		}
	}
	includeDbList := []*datastreampb.MysqlDatabase{}
	for _, db := range dbList {
		//create include db object
		includeDb := &datastreampb.MysqlDatabase{
			Database:    db.DbName,
			MysqlTables: mysqlTables,
		}
		includeDbList = append(includeDbList, includeDb)
	}
	//TODO: Clean up fmt.Printf logs and replace them with zap logger.
	fmt.Printf("Include DB List for datastream: %+v\n", includeDbList)
	mysqlSrcCfg := &datastreampb.MysqlSourceConfig{
		IncludeObjects:             &datastreampb.MysqlRdbms{MysqlDatabases: includeDbList},
		MaxConcurrentBackfillTasks: maxBackfillTasks,
		MaxConcurrentCdcTasks:      maxCdcTasks,
	}
	return &datastreampb.SourceConfig_MysqlSourceConfig{MysqlSourceConfig: mysqlSrcCfg}, nil
}

func getOracleSourceStreamConfig(dbName string, datastreamCfg DatastreamCfg) (*datastreampb.SourceConfig_OracleSourceConfig, error) {
	oracleTables := []*datastreampb.OracleTable{}
	for _, tableList := range datastreamCfg.SchemaDetails {
		for _, table := range tableList.TableDetails {
			includeTable := &datastreampb.OracleTable{
				Table: table.TableName,
			}
			oracleTables = append(oracleTables, includeTable)
		}
	}
	oracledb := &datastreampb.OracleSchema{
		Schema:       dbName,
		OracleTables: oracleTables,
	}
	oracleSrcCfg := &datastreampb.OracleSourceConfig{
		IncludeObjects:             &datastreampb.OracleRdbms{OracleSchemas: []*datastreampb.OracleSchema{oracledb}},
		MaxConcurrentBackfillTasks: maxBackfillTasks,
		MaxConcurrentCdcTasks:      maxCdcTasks,
	}
	return &datastreampb.SourceConfig_OracleSourceConfig{OracleSourceConfig: oracleSrcCfg}, nil
}

func getPostgreSQLSourceStreamConfig(datastreamCfg DatastreamCfg) (*datastreampb.SourceConfig_PostgresqlSourceConfig, error) {
	properties := datastreamCfg.Properties
	params, err := profiles.ParseMap(properties)
	if err != nil {
		return nil, fmt.Errorf("could not parse properties: %v", err)
	}
	postgreSQLSchema := []*datastreampb.PostgresqlSchema{}
	for schema, tableList := range datastreamCfg.SchemaDetails {
		postgreSQLTables := []*datastreampb.PostgresqlTable{}
		for _, table := range tableList.TableDetails {
			var includeTable *datastreampb.PostgresqlTable
			if schema == "public" {
				includeTable = &datastreampb.PostgresqlTable{
					Table: table.TableName,
				}
			} else {
				includeTable = &datastreampb.PostgresqlTable{
					Table: strings.TrimPrefix(table.TableName, schema+"."),
				}
			}
			postgreSQLTables = append(postgreSQLTables, includeTable)
		}
		includeSchema := &datastreampb.PostgresqlSchema{
			Schema:           schema,
			PostgresqlTables: postgreSQLTables,
		}
		postgreSQLSchema = append(postgreSQLSchema, includeSchema)
	}
	replicationSlot, replicationSlotExists := params["replicationSlot"]
	publication, publicationExists := params["publication"]
	if !replicationSlotExists || !publicationExists {
		return nil, fmt.Errorf("replication slot or publication not specified")
	}
	postgresSrcCfg := &datastreampb.PostgresqlSourceConfig{
		IncludeObjects:             &datastreampb.PostgresqlRdbms{PostgresqlSchemas: postgreSQLSchema},
		ReplicationSlot:            replicationSlot,
		Publication:                publication,
		MaxConcurrentBackfillTasks: maxBackfillTasks,
	}
	return &datastreampb.SourceConfig_PostgresqlSourceConfig{PostgresqlSourceConfig: postgresSrcCfg}, nil
}

func getSourceStreamConfig(srcCfg *datastreampb.SourceConfig, sourceProfile profiles.SourceProfile, dbList []profiles.LogicalShard, datastreamCfg DatastreamCfg) error {
	var err error = nil
	switch sourceProfile.Driver {
	case constants.MYSQL:
		// For MySQL, it supports sharded migrations and batching databases in a physical machine into a single
		// Datastream, so dbList is passed.
		srcCfg.SourceStreamConfig, err = getMysqlSourceStreamConfig(dbList, datastreamCfg)
		return err
	case constants.ORACLE:
		// For Oracle, no sharded migrations or db batching support, so the dbList always contains only one element.
		srcCfg.SourceStreamConfig, err = getOracleSourceStreamConfig(dbList[0].DbName, datastreamCfg)
		return err
	case constants.POSTGRES:
		// For Postgres, tables need to be configured at the schema level, which will require more information List<Dbs> and Map<Schema, List<Tables>>
		// instead of List<Dbs> and List<Tables>. Becuase of this we do not configure postgres datastream at individual table level currently.
		srcCfg.SourceStreamConfig, err = getPostgreSQLSourceStreamConfig(datastreamCfg)
		return err
	default:
		return fmt.Errorf("only MySQL, Oracle and PostgreSQL are supported as source streams")
	}
}

func CreatePubsubResources(ctx context.Context, projectID string, datastreamDestinationConnCfg DstConnCfg, dbName string) (*internal.PubsubResources, error) {
	pubsubClient, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("pubsub client can not be created: %v", err)
	}
	defer pubsubClient.Close()

	// Create pubsub topic and subscription
	pubsubCfg, err := createPubsubTopicAndSubscription(ctx, pubsubClient, dbName)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("Could not create pubsub resources. Some permissions missing. Please check https://googlecloudplatform.github.io/spanner-migration-tool/permissions.html for required pubsub permissions. error=%v", err))
		return nil, err
	}

	// Fetch the created target profile and get the target gcs bucket name and path.
	// Then create notification for the target bucket.
	// Creating datastream client to fetch target profile.
	dsClient, err := datastream.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("datastream client can not be created: %v", err)
	}
	defer dsClient.Close()

	bucketName, prefix, err := FetchTargetBucketAndPath(ctx, dsClient, projectID, datastreamDestinationConnCfg)
	if err != nil {
		return nil, err
	}

	// Create pubsub notification on the target gcs path
	storageClient, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("GCS client can not be created: %v", err)
	}
	defer storageClient.Close()

	notificationID, err := createNotificationOnBucket(ctx, storageClient, projectID, pubsubCfg.TopicId, bucketName, prefix)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("Could not create pubsub resources. Some permissions missing. Please check https://googlecloudplatform.github.io/spanner-migration-tool/permissions.html for required pubsub permissions. error=%v", err))
		return nil, err
	}
	pubsubCfg.BucketName = bucketName
	pubsubCfg.NotificationId = notificationID
	logger.Log.Info(fmt.Sprintf("Successfully created pubsub topic id=%s, subscription id=%s, notification for bucket=%s with id=%s.\n", pubsubCfg.TopicId, pubsubCfg.SubscriptionId, bucketName, notificationID))
	return &pubsubCfg, nil
}

func createPubsubTopicAndSubscription(ctx context.Context, pubsubClient *pubsub.Client, dbName string) (internal.PubsubResources, error) {
	pubsubCfg := internal.PubsubResources{}
	// Generate ID
	subscriptionId, err := utils.GenerateName("smt-sub-" + dbName)
	if err != nil {
		return pubsubCfg, fmt.Errorf("error generating pubsub subscription ID: %v", err)
	}
	pubsubCfg.SubscriptionId = subscriptionId

	topicId, err := utils.GenerateName("smt-topic-" + dbName)
	if err != nil {
		return pubsubCfg, fmt.Errorf("error generating pubsub topic ID: %v", err)
	}
	pubsubCfg.TopicId = topicId

	// Create Topic and Subscription
	topicObj, err := pubsubClient.CreateTopic(ctx, pubsubCfg.TopicId)
	if err != nil {
		return pubsubCfg, fmt.Errorf("pubsub topic could not be created: %v", err)
	}

	_, err = pubsubClient.CreateSubscription(ctx, pubsubCfg.SubscriptionId, pubsub.SubscriptionConfig{
		Topic:             topicObj,
		AckDeadline:       time.Minute * 10,
		RetentionDuration: time.Hour * 24 * 7,
	})
	if err != nil {
		return pubsubCfg, fmt.Errorf("pubsub subscription could not be created: %v", err)
	}
	return pubsubCfg, nil
}

// FetchTargetBucketAndPath fetches the bucket and path name from a Datastream destination config.
func FetchTargetBucketAndPath(ctx context.Context, datastreamClient *datastream.Client, projectID string, datastreamDestinationConnCfg DstConnCfg) (string, string, error) {
	if datastreamClient == nil {
		return "", "", fmt.Errorf("datastream client could not be created")
	}
	dstProf := fmt.Sprintf("projects/%s/locations/%s/connectionProfiles/%s", projectID, datastreamDestinationConnCfg.Location, datastreamDestinationConnCfg.Name)
	res, err := datastreamClient.GetConnectionProfile(ctx, &datastreampb.GetConnectionProfileRequest{Name: dstProf})
	if err != nil {
		return "", "", fmt.Errorf("could not get connection profiles: %v", err)
	}
	// Fetch the GCS path from the target connection profile.
	gcsProfile := res.Profile.(*datastreampb.ConnectionProfile_GcsProfile).GcsProfile
	bucketName := gcsProfile.Bucket
	prefix := gcsProfile.RootPath + datastreamDestinationConnCfg.Prefix
	prefix = utils.ConcatDirectoryPath(prefix, "data/")
	return bucketName, prefix, nil
}

func createNotificationOnBucket(ctx context.Context, storageClient *storage.Client, projectID, topicID, bucketName, prefix string) (string, error) {
	notification := storage.Notification{
		TopicID:          topicID,
		TopicProjectID:   projectID,
		PayloadFormat:    storage.JSONPayload,
		ObjectNamePrefix: prefix,
	}

	createdNotification, err := storageClient.Bucket(bucketName).AddNotification(ctx, &notification)
	if err != nil {
		return "", fmt.Errorf("GCS Notification could not be created: %v", err)
	}
	return createdNotification.ID, nil
}

// LaunchStream populates the parameters from the streaming config and triggers a stream on Cloud Datastream.
func LaunchStream(ctx context.Context, sourceProfile profiles.SourceProfile, dbList []profiles.LogicalShard, projectID string, datastreamCfg DatastreamCfg) error {
	projectNumberResource := GetProjectNumberResource(ctx, fmt.Sprintf("projects/%s", projectID))
	fmt.Println("Launching stream ", fmt.Sprintf("%s/locations/%s", projectNumberResource, datastreamCfg.StreamLocation))
	dsClient, err := datastream.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("datastream client can not be created: %v", err)
	}
	defer dsClient.Close()
	// Rate limit this function to match DataStream API Quota.
	DATA_STREAM_RL.Take()
	fmt.Println("Created client...")
	prefix := datastreamCfg.DestinationConnectionConfig.Prefix
	prefix = utils.ConcatDirectoryPath(prefix, "data")

	gcsDstCfg := &datastreampb.GcsDestinationConfig{
		Path:       prefix,
		FileFormat: &datastreampb.GcsDestinationConfig_AvroFileFormat{},
	}
	srcCfg := &datastreampb.SourceConfig{
		SourceConnectionProfile: fmt.Sprintf("%s/locations/%s/connectionProfiles/%s", projectNumberResource, datastreamCfg.SourceConnectionConfig.Location, datastreamCfg.SourceConnectionConfig.Name),
	}
	err = getSourceStreamConfig(srcCfg, sourceProfile, dbList, datastreamCfg)
	if err != nil {
		return fmt.Errorf("could not get source stream config: %v", err)
	}

	dstCfg := &datastreampb.DestinationConfig{
		DestinationConnectionProfile: fmt.Sprintf("%s/locations/%s/connectionProfiles/%s", projectNumberResource, datastreamCfg.DestinationConnectionConfig.Location, datastreamCfg.DestinationConnectionConfig.Name),
		DestinationStreamConfig:      &datastreampb.DestinationConfig_GcsDestinationConfig{GcsDestinationConfig: gcsDstCfg},
	}
	streamInfo := &datastreampb.Stream{
		DisplayName:       datastreamCfg.StreamDisplayName,
		SourceConfig:      srcCfg,
		DestinationConfig: dstCfg,
		State:             datastreampb.Stream_RUNNING,
		BackfillStrategy:  &datastreampb.Stream_BackfillAll{BackfillAll: &datastreampb.Stream_BackfillAllStrategy{}},
	}
	createStreamRequest := &datastreampb.CreateStreamRequest{
		Parent:   fmt.Sprintf("%s/locations/%s", projectNumberResource, datastreamCfg.StreamLocation),
		StreamId: datastreamCfg.StreamId,
		Stream:   streamInfo,
		// Setting a RequestId makes idempotent retries possible.
		RequestId: uuid.New().String(),
	}

	fmt.Println("Created stream request..")

	dsOp, err := dsClient.CreateStream(ctx, createStreamRequest, gax.WithRetry(dataStreamGaxRetrier))
	if err != nil {
		fmt.Printf("cannot create stream: createStreamRequest: %+v\n", createStreamRequest)
		return fmt.Errorf("cannot create stream: %v ", err)
	}

	_, err = dsOp.Wait(ctx)
	if err != nil {
		fmt.Printf("datastream create operation failed: createStreamRequest: %+v\n", createStreamRequest)
		return fmt.Errorf("datastream create operation failed: %v", err)
	}
	fmt.Println("Successfully created stream ", datastreamCfg.StreamId)

	fmt.Print("Setting stream state to RUNNING...")
	streamInfo.Name = fmt.Sprintf("%s/locations/%s/streams/%s", projectNumberResource, datastreamCfg.StreamLocation, datastreamCfg.StreamId)
	updateStreamRequest := &datastreampb.UpdateStreamRequest{
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"state"}},
		Stream:     streamInfo,
		// Setting a RequestId makes idempotent retries possible.
		RequestId: uuid.New().String(),
	}
	upOp, err := dsClient.UpdateStream(ctx, updateStreamRequest, gax.WithRetry(dataStreamGaxRetrier))
	if err != nil {
		return fmt.Errorf("could not create update request: %v", err)
	}
	_, err = upOp.Wait(ctx)
	if err != nil {
		return fmt.Errorf("update stream operation failed: %v", err)
	}
	fmt.Println("Done")
	return nil
}

// LaunchDataflowJob populates the parameters from the streaming config and triggers a Dataflow job.
func LaunchDataflowJob(ctx context.Context, targetProfile profiles.TargetProfile, streamingCfg StreamingCfg, conv *internal.Conv) (internal.DataflowOutput, error) {
	project, instance, dbName, _ := targetProfile.GetResourceIds(ctx, time.Now(), "", nil)
	dataflowCfg := streamingCfg.DataflowCfg
	datastreamCfg := streamingCfg.DatastreamCfg

	// Rate limit this function to match DataFlow createJob Quota.
	DATA_FLOW_RL.Take()

	fmt.Println("Launching dataflow job ", dataflowCfg.JobName, " in ", project, "-", dataflowCfg.Location)

	c, err := dataflow.NewFlexTemplatesClient(ctx)
	if err != nil {
		return internal.DataflowOutput{}, fmt.Errorf("could not create flex template client: %v", err)
	}
	defer c.Close()
	fmt.Println("Created flex template client...")

	//Creating datastream client to fetch the gcs bucket using target profile.
	dsClient, err := datastream.NewClient(ctx)
	if err != nil {
		return internal.DataflowOutput{}, fmt.Errorf("datastream client can not be created: %v", err)
	}
	defer dsClient.Close()

	// Fetch the GCS path from the destination connection profile.
	dstProf := fmt.Sprintf("projects/%s/locations/%s/connectionProfiles/%s", project, datastreamCfg.DestinationConnectionConfig.Location, datastreamCfg.DestinationConnectionConfig.Name)
	res, err := dsClient.GetConnectionProfile(ctx, &datastreampb.GetConnectionProfileRequest{Name: dstProf})
	if err != nil {
		return internal.DataflowOutput{}, fmt.Errorf("could not get connection profiles: %v", err)
	}
	gcsProfile := res.Profile.(*datastreampb.ConnectionProfile_GcsProfile).GcsProfile
	inputFilePattern := "gs://" + gcsProfile.Bucket + gcsProfile.RootPath + datastreamCfg.DestinationConnectionConfig.Prefix
	if inputFilePattern[len(inputFilePattern)-1] != '/' {
		inputFilePattern = inputFilePattern + "/"
	}
	fmt.Println("Reading files from datastream destination ", inputFilePattern)

	// Initiate runtime environment flags and overrides.
	var (
		dataflowProjectId        = project
		dataflowVpcHostProjectId = project
		gcsTemplatePath          = DEFAULT_TEMPLATE_PATH
		dataflowSubnetwork       = ""
		workerIpAddressConfig    = dataflowpb.WorkerIPAddressConfiguration_WORKER_IP_PUBLIC
		dataflowUserLabels       = make(map[string]string)
		machineType              = "n1-standard-2"
	)
	// If project override present, use that otherwise default to Spanner project. Useful when customers want to run Dataflow in separate project.
	if dataflowCfg.ProjectId != "" {
		dataflowProjectId = dataflowCfg.ProjectId
	}
	// If VPC Host project override present, use that otherwise default to Spanner project.
	if dataflowCfg.VpcHostProjectId != "" {
		dataflowVpcHostProjectId = dataflowCfg.VpcHostProjectId
	}
	if dataflowCfg.GcsTemplatePath != "" {
		gcsTemplatePath = dataflowCfg.GcsTemplatePath
	}

	// If either network or subnetwork is specified, set IpConfig to private.
	if dataflowCfg.Network != "" || dataflowCfg.Subnetwork != "" {
		workerIpAddressConfig = dataflowpb.WorkerIPAddressConfiguration_WORKER_IP_PRIVATE
		if dataflowCfg.Subnetwork != "" {
			dataflowSubnetwork = fmt.Sprintf("https://www.googleapis.com/compute/v1/projects/%s/regions/%s/subnetworks/%s", dataflowVpcHostProjectId, dataflowCfg.Location, dataflowCfg.Subnetwork)
		}
	}

	if dataflowCfg.AdditionalUserLabels != "" {
		err = json.Unmarshal([]byte(dataflowCfg.AdditionalUserLabels), &dataflowUserLabels)
		if err != nil {
			return internal.DataflowOutput{}, fmt.Errorf("could not unmarshal AdditionalUserLabels json %s : error = %v", dataflowCfg.AdditionalUserLabels, err)
		}
	}

	if dataflowCfg.MaxWorkers != "" {
		intVal, err := strconv.ParseInt(dataflowCfg.MaxWorkers, 10, 64)
		if err != nil {
			return internal.DataflowOutput{}, fmt.Errorf("could not parse MaxWorkers parameter %s, please provide a positive integer as input", dataflowCfg.MaxWorkers)
		}
		maxWorkers = int32(intVal)
		if maxWorkers < MIN_WORKER_LIMIT || maxWorkers > MAX_WORKER_LIMIT {
			return internal.DataflowOutput{}, fmt.Errorf("maxWorkers should lie in the range [%d, %d]", MIN_WORKER_LIMIT, MAX_WORKER_LIMIT)
		}
	}
	if dataflowCfg.NumWorkers != "" {
		intVal, err := strconv.ParseInt(dataflowCfg.NumWorkers, 10, 64)
		if err != nil {
			return internal.DataflowOutput{}, fmt.Errorf("could not parse NumWorkers parameter %s, please provide a positive integer as input", dataflowCfg.NumWorkers)
		}
		numWorkers = int32(intVal)
		if numWorkers < MIN_WORKER_LIMIT || numWorkers > MAX_WORKER_LIMIT {
			return internal.DataflowOutput{}, fmt.Errorf("numWorkers should lie in the range [%d, %d]", MIN_WORKER_LIMIT, MAX_WORKER_LIMIT)
		}
	}

	if dataflowCfg.MachineType != "" {
		machineType = dataflowCfg.MachineType
	}

	launchParameters := &dataflowpb.LaunchFlexTemplateParameter{
		JobName:  dataflowCfg.JobName,
		Template: &dataflowpb.LaunchFlexTemplateParameter_ContainerSpecGcsPath{ContainerSpecGcsPath: gcsTemplatePath},
		Parameters: map[string]string{
			"inputFilePattern":              utils.ConcatDirectoryPath(inputFilePattern, "data"),
			"streamName":                    fmt.Sprintf("projects/%s/locations/%s/streams/%s", project, datastreamCfg.StreamLocation, datastreamCfg.StreamId),
			"instanceId":                    instance,
			"databaseId":                    dbName,
			"sessionFilePath":               streamingCfg.TmpDir + "session.json",
			"deadLetterQueueDirectory":      inputFilePattern + "dlq",
			"transformationContextFilePath": streamingCfg.TmpDir + "transformationContext.json",
			"gcsPubSubSubscription":         fmt.Sprintf("projects/%s/subscriptions/%s", project, streamingCfg.PubsubCfg.SubscriptionId),
		},
		Environment: &dataflowpb.FlexTemplateRuntimeEnvironment{
			MaxWorkers:            maxWorkers,
			NumWorkers:            numWorkers,
			ServiceAccountEmail:   dataflowCfg.ServiceAccountEmail,
			AutoscalingAlgorithm:  2, // 2 corresponds to AUTOSCALING_ALGORITHM_BASIC
			EnableStreamingEngine: true,
			Network:               dataflowCfg.Network,
			Subnetwork:            dataflowSubnetwork,
			IpConfiguration:       workerIpAddressConfig,
			MachineType:           machineType,
			AdditionalUserLabels:  dataflowUserLabels,
			KmsKeyName:            dataflowCfg.KmsKeyName,
		},
	}
	req := &dataflowpb.LaunchFlexTemplateRequest{
		ProjectId:       dataflowProjectId,
		LaunchParameter: launchParameters,
		Location:        dataflowCfg.Location,
	}
	fmt.Println("Created flex template request body...")

	respDf, err := c.LaunchFlexTemplate(ctx, req)
	if err != nil {
		fmt.Printf("flexTemplateRequest: %+v\n", req)
		return internal.DataflowOutput{}, fmt.Errorf("unable to launch template: %v", err)
	}
	// Refactor to use accessor return value.
	gcloudDfCmd := dataflowaccessor.GetGcloudDataflowCommandFromRequest(req)
	logger.Log.Debug(fmt.Sprintf("\nEquivalent gCloud command for job %s:\n%s\n\n", req.LaunchParameter.JobName, gcloudDfCmd))
	return internal.DataflowOutput{JobID: respDf.Job.Id, GCloudCmd: gcloudDfCmd}, nil
}

func StoreGeneratedResources(conv *internal.Conv, streamingCfg StreamingCfg, dfJobId, gcloudDataflowCmd, project, dataShardId string, gcsBucket internal.GcsResources, dashboardName string) {
	datastreamCfg := streamingCfg.DatastreamCfg
	dataflowCfg := streamingCfg.DataflowCfg
	conv.Audit.StreamingStats.DatastreamResources = internal.DatastreamResources{DatastreamName: datastreamCfg.StreamId, Region: datastreamCfg.StreamLocation}
	conv.Audit.StreamingStats.DataflowResources = internal.DataflowResources{JobId: dfJobId, GcloudCmd: gcloudDataflowCmd, Region: dataflowCfg.Location}
	conv.Audit.StreamingStats.PubsubResources = streamingCfg.PubsubCfg
	conv.Audit.StreamingStats.GcsResources = gcsBucket
	conv.Audit.StreamingStats.MonitoringResources = internal.MonitoringResources{DashboardName: dashboardName}
	if dataShardId != "" {
		var resourceMutex sync.Mutex
		resourceMutex.Lock()
		var shardResources internal.ShardResources
		shardResources.DatastreamResources = internal.DatastreamResources{DatastreamName: datastreamCfg.StreamId, Region: datastreamCfg.StreamLocation}
		shardResources.DataflowResources = internal.DataflowResources{JobId: dfJobId, GcloudCmd: gcloudDataflowCmd, Region: dataflowCfg.Location}
		shardResources.PubsubResources = streamingCfg.PubsubCfg
		shardResources.GcsResources = gcsBucket
		if dashboardName != "" {
			{
				shardResources.MonitoringResources = internal.MonitoringResources{DashboardName: dashboardName}
			}
		}
		conv.Audit.StreamingStats.ShardToShardResourcesMap[dataShardId] = shardResources
		resourceMutex.Unlock()
	}
	fullStreamName := fmt.Sprintf("projects/%s/locations/%s/streams/%s", project, datastreamCfg.StreamLocation, datastreamCfg.StreamId)
	dfJobDetails := fmt.Sprintf("project: %s, location: %s, name: %s, id: %s", project, dataflowCfg.Location, dataflowCfg.JobName, dfJobId)
	logger.Log.Info("\n------------------------------------------\n")
	logger.Log.Info("The Datastream stream: " + fullStreamName + " ,the Dataflow job: " + dfJobDetails +
		" the Pubsub topic: " + streamingCfg.PubsubCfg.TopicId + " ,the subscription: " + streamingCfg.PubsubCfg.SubscriptionId +
		" and the pubsub Notification id:" + streamingCfg.PubsubCfg.NotificationId + " on bucket: " + streamingCfg.PubsubCfg.BucketName +
		" can be cleaned up by using the UI if you have it open. Otherwise, you can use the cleanup CLI command and supply the migrationjobId" +
		" generated by Spanner migration tool. You can find the migrationJobId in the logs or in the 'spannermigrationtool_metadata'" +
		" database in your spanner instance. If migrationJobId is not stored due to an error, you will have to clean up the resources manually.")
}

func CreateStreamingConfig(pl profiles.DataShard) StreamingCfg {
	//create dataflowcfg from pl receiver object
	inputDataflowConfig := pl.DataflowConfig
	dataflowCfg := DataflowCfg{
		ProjectId:            inputDataflowConfig.ProjectId,
		Location:             inputDataflowConfig.Location,
		Network:              inputDataflowConfig.Network,
		VpcHostProjectId:     inputDataflowConfig.VpcHostProjectId,
		Subnetwork:           inputDataflowConfig.Subnetwork,
		MaxWorkers:           inputDataflowConfig.MaxWorkers,
		NumWorkers:           inputDataflowConfig.NumWorkers,
		ServiceAccountEmail:  inputDataflowConfig.ServiceAccountEmail,
		MachineType:          inputDataflowConfig.MachineType,
		AdditionalUserLabels: inputDataflowConfig.AdditionalUserLabels,
		KmsKeyName:           inputDataflowConfig.KmsKeyName,
		GcsTemplatePath:      inputDataflowConfig.GcsTemplatePath,
	}
	//create src and dst datastream from pl receiver object
	datastreamCfg := DatastreamCfg{
		StreamLocation:             pl.StreamLocation,
		MaxConcurrentBackfillTasks: pl.DatastreamConfig.MaxConcurrentBackfillTasks,
		MaxConcurrentCdcTasks:      pl.DatastreamConfig.MaxConcurrentCdcTasks,
	}
	//set src connection profile
	inputSrcConnProfile := pl.SrcConnectionProfile
	srcConnCfg := SrcConnCfg{Location: inputSrcConnProfile.Location, Name: inputSrcConnProfile.Name}
	datastreamCfg.SourceConnectionConfig = srcConnCfg
	//set dst connection profile
	inputDstConnProfile := pl.DstConnectionProfile
	dstConnCfg := DstConnCfg{Name: inputDstConnProfile.Name, Location: inputDstConnProfile.Location}
	datastreamCfg.DestinationConnectionConfig = dstConnCfg

	gcsCfg := GcsCfg{
		TtlInDays:    pl.GcsConfig.TtlInDays,
		TtlInDaysSet: pl.GcsConfig.TtlInDaysSet,
	}
	//create the streamingCfg object
	streamingCfg := StreamingCfg{
		DatastreamCfg: datastreamCfg,
		GcsCfg:        gcsCfg,
		DataflowCfg:   dataflowCfg,
		TmpDir:        pl.TmpDir,
		DataShardId:   pl.DataShardId}
	return streamingCfg
}

// Maps Project-Id to ProjectNumber.
var ProjectNumberResourceCache sync.Map

// Returns a string that encodes the project number like `projects/12345`
func GetProjectNumberResource(ctx context.Context, projectID string) string {
	projectNumberResource, found := ProjectNumberResourceCache.Load(projectID)
	if found {
		return projectNumberResource.(string)
	}

	rmClient, err := resourcemanager.NewProjectsClient(ctx)
	if err != nil {
		logger.Log.Warn(fmt.Sprintf("Could not create resourcemanager client to query project number. Defaulting to ProjectId=%s. error=%v",
			projectID, err))
		return projectID
	}
	defer rmClient.Close()
	req := resourcemanagerpb.GetProjectRequest{Name: projectID}
	project, err := rmClient.GetProject(ctx, &req)
	if err != nil {
		logger.Log.Warn(fmt.Sprintf("Could not query resourcemanager to get project number. Defaulting to ProjectId=%s. error=%v",
			projectID, err))
		return projectID
	}
	projectNumberResource = project.GetName()
	ProjectNumberResourceCache.Store(projectID, projectNumberResource)
	return projectNumberResource.(string)

}

func StartDatastream(ctx context.Context, streamingCfg StreamingCfg, sourceProfile profiles.SourceProfile, targetProfile profiles.TargetProfile, schemaDetails map[string]internal.SchemaDetails) (StreamingCfg, error) {
	driver := sourceProfile.Driver
	var dbList []profiles.LogicalShard
	switch driver {
	case constants.MYSQL:
		dbList = append(dbList, profiles.LogicalShard{DbName: sourceProfile.Conn.Mysql.Db})
	case constants.ORACLE:
		dbList = append(dbList, profiles.LogicalShard{DbName: sourceProfile.Conn.Oracle.User})
	case constants.POSTGRES:
		dbList = append(dbList, profiles.LogicalShard{DbName: streamingCfg.DatastreamCfg.Properties})
	}
	err := LaunchStream(ctx, sourceProfile, dbList, targetProfile.Conn.Sp.Project, streamingCfg.DatastreamCfg)
	if err != nil {
		return streamingCfg, fmt.Errorf("error launching stream: %v", err)
	}
	return streamingCfg, nil
}

func StartDataflow(ctx context.Context, targetProfile profiles.TargetProfile, streamingCfg StreamingCfg, conv *internal.Conv) (internal.DataflowOutput, error) {

	convJSON, err := json.MarshalIndent(conv, "", " ")
	if err != nil {
		return internal.DataflowOutput{}, fmt.Errorf("can't encode session state to JSON: %v", err)
	}
	err = storageacc.WriteDataToGCS(ctx, streamingCfg.TmpDir, "session.json", string(convJSON))
	if err != nil {
		return internal.DataflowOutput{}, fmt.Errorf("error while writing to GCS: %v", err)
	}
	transformationContextMap := map[string]interface{}{
		"SchemaToShardId": streamingCfg.DataflowCfg.DbNameToShardIdMap,
	}
	transformationContext, err := json.Marshal(transformationContextMap)
	if err != nil {
		return internal.DataflowOutput{}, fmt.Errorf("failed to compute transformation context: %s", err.Error())
	}
	err = storageacc.WriteDataToGCS(ctx, streamingCfg.TmpDir, "transformationContext.json", string(transformationContext))
	if err != nil {
		return internal.DataflowOutput{}, fmt.Errorf("error while writing to GCS: %v", err)
	}
	dfOutput, err := LaunchDataflowJob(ctx, targetProfile, streamingCfg, conv)
	if err != nil {
		return internal.DataflowOutput{}, fmt.Errorf("error launching dataflow: %v", err)
	}
	return dfOutput, nil
}

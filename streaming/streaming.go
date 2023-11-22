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
	dashboard "cloud.google.com/go/monitoring/dashboard/apiv1"
	"cloud.google.com/go/monitoring/dashboard/apiv1/dashboardpb"
	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	datastreampb "google.golang.org/genproto/googleapis/cloud/datastream/v1"
	dataflowpb "google.golang.org/genproto/googleapis/dataflow/v1beta3"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/utils"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/profiles"
)

var (
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
	StreamId                    string     `json:"streamId"`
	StreamLocation              string     `json:"streamLocation"`
	StreamDisplayName           string     `json:"streamDisplayName"`
	SourceConnectionConfig      SrcConnCfg `json:"sourceConnectionConfig"`
	DestinationConnectionConfig DstConnCfg `json:"destinationConnectionConfig"`
	Properties                  string     `json:"properties"`
	TableList                   []string   `json:"tableList"`
}

type DataflowCfg struct {
	ProjectId            string            `json:"projectId"`
	JobName              string            `json:"jobName"`
	Location             string            `json:"location"`
	VpcHostProjectId     string            `json:"vpcHostProjectId"`
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
	DatastreamCfg DatastreamCfg      `json:"datastreamCfg"`
	DataflowCfg   DataflowCfg        `json:"dataflowCfg"`
	TmpDir        string             `json:"tmpDir"`
	PubsubCfg     internal.PubsubCfg `json:"pubsubCfg"`
	DataShardId   string             `json:"dataShardId"`
}

// VerifyAndUpdateCfg checks the fields and errors out if certain fields are empty.
// It then auto-populates certain empty fields like StreamId and Dataflow JobName.
func VerifyAndUpdateCfg(streamingCfg *StreamingCfg, dbName string, tableList []string) error {
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

	// Populate the tables to be streamed in the datastreamCfg from the dervied list from session file
	streamingCfg.DatastreamCfg.TableList = append(streamingCfg.DatastreamCfg.TableList, tableList...)

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
	return nil
}

// ReadStreamingConfig reads the file and unmarshalls it into the StreamingCfg struct.
func ReadStreamingConfig(file, dbName string, tableList []string) (StreamingCfg, error) {
	streamingCfg := StreamingCfg{}
	cfgFile, err := ioutil.ReadFile(file)
	if err != nil {
		return streamingCfg, fmt.Errorf("can't read streaming config file due to: %v", err)
	}
	err = json.Unmarshal(cfgFile, &streamingCfg)
	if err != nil {
		return streamingCfg, fmt.Errorf("unable to unmarshall json due to: %v", err)
	}
	err = VerifyAndUpdateCfg(&streamingCfg, dbName, tableList)
	if err != nil {
		return streamingCfg, fmt.Errorf("streaming config is incomplete: %v", err)
	}
	return streamingCfg, nil
}

// dbName is the name of the database to be migrated.
// tabeList is the common list of tables that need to be migrated from each database
func getMysqlSourceStreamConfig(dbList []profiles.LogicalShard, tableList []string) *datastreampb.SourceConfig_MysqlSourceConfig {
	mysqlTables := []*datastreampb.MysqlTable{}
	for _, table := range tableList {
		includeTable := &datastreampb.MysqlTable{
			Table: table,
		}
		mysqlTables = append(mysqlTables, includeTable)
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
		MaxConcurrentBackfillTasks: 50,
	}
	return &datastreampb.SourceConfig_MysqlSourceConfig{MysqlSourceConfig: mysqlSrcCfg}
}

func getOracleSourceStreamConfig(dbName string, tableList []string) *datastreampb.SourceConfig_OracleSourceConfig {
	oracleTables := []*datastreampb.OracleTable{}
	for _, table := range tableList {
		includeTable := &datastreampb.OracleTable{
			Table: table,
		}
		oracleTables = append(oracleTables, includeTable)
	}
	oracledb := &datastreampb.OracleSchema{
		Schema:       dbName,
		OracleTables: oracleTables,
	}
	oracleSrcCfg := &datastreampb.OracleSourceConfig{
		IncludeObjects:             &datastreampb.OracleRdbms{OracleSchemas: []*datastreampb.OracleSchema{oracledb}},
		MaxConcurrentBackfillTasks: 50,
	}
	return &datastreampb.SourceConfig_OracleSourceConfig{OracleSourceConfig: oracleSrcCfg}
}

func getPostgreSQLSourceStreamConfig(properties string) (*datastreampb.SourceConfig_PostgresqlSourceConfig, error) {
	params, err := profiles.ParseMap(properties)
	if err != nil {
		return nil, fmt.Errorf("could not parse properties: %v", err)
	}
	var excludeObjects []*datastreampb.PostgresqlSchema
	for _, s := range []string{"information_schema", "postgres", "pg_catalog", "pg_temp_1", "pg_toast", "pg_toast_temp_1"} {
		excludeObjects = append(excludeObjects, &datastreampb.PostgresqlSchema{
			Schema: s,
		})
	}
	replicationSlot, replicationSlotExists := params["replicationSlot"]
	publication, publicationExists := params["publication"]
	if !replicationSlotExists || !publicationExists {
		return nil, fmt.Errorf("replication slot or publication not specified")
	}
	postgresSrcCfg := &datastreampb.PostgresqlSourceConfig{
		ExcludeObjects:             &datastreampb.PostgresqlRdbms{PostgresqlSchemas: excludeObjects},
		ReplicationSlot:            replicationSlot,
		Publication:                publication,
		MaxConcurrentBackfillTasks: 50,
	}
	return &datastreampb.SourceConfig_PostgresqlSourceConfig{PostgresqlSourceConfig: postgresSrcCfg}, nil
}

func getSourceStreamConfig(srcCfg *datastreampb.SourceConfig, sourceProfile profiles.SourceProfile, dbList []profiles.LogicalShard, datastreamCfg DatastreamCfg) error {
	switch sourceProfile.Driver {
	case constants.MYSQL:
		// For MySQL, it supports sharded migrations and batching databases in a physical machine into a single
		//Datastream, so dbList is passed.
		srcCfg.SourceStreamConfig = getMysqlSourceStreamConfig(dbList, datastreamCfg.TableList)
		return nil
	case constants.ORACLE:
		// For Oracle, no sharded migrations or db batching support, so the dbList always contains only one element.
		srcCfg.SourceStreamConfig = getOracleSourceStreamConfig(dbList[0].DbName, datastreamCfg.TableList)
		return nil
	case constants.POSTGRES:
		// For Postgres, tables need to be configured at the schema level, which will require more information List<Dbs> and Map<Schema, List<Tables>>
		// instead of List<Dbs> and List<Tables>. Becuase of this we do not configure postgres datastream at individual table level currently.
		sourceStreamConfig, err := getPostgreSQLSourceStreamConfig(datastreamCfg.Properties)
		if err == nil {
			srcCfg.SourceStreamConfig = sourceStreamConfig
		}
		return err
	default:
		return fmt.Errorf("only MySQL, Oracle and PostgreSQL are supported as source streams")
	}
}

func CreatePubsubResources(ctx context.Context, projectID string, datastreamDestinationConnCfg DstConnCfg, dbName string) (*internal.PubsubCfg, error) {
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

func createPubsubTopicAndSubscription(ctx context.Context, pubsubClient *pubsub.Client, dbName string) (internal.PubsubCfg, error) {
	pubsubCfg := internal.PubsubCfg{}
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
	prefix = concatDirectoryPath(prefix, "data/")
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

func concatDirectoryPath(basePath, subPath string) string {
	// ensure basPath doesn't start with '/' and ends with '/'
	if basePath == "" || basePath == "/" {
		basePath = ""
	} else {
		if basePath[0] == '/' {
			basePath = basePath[1:]
		}
		if basePath[len(basePath)-1] != '/' {
			basePath = basePath + "/"
		}
	}
	// ensure subPath doesn't start with '/' ends with '/'
	if subPath == "" || subPath == "/" {
		subPath = ""
	} else {
		if subPath[0] == '/' {
			subPath = subPath[1:]
		}
		if subPath[len(subPath)-1] != '/' {
			subPath = subPath + "/"
		}
	}
	path := fmt.Sprintf("%s%s", basePath, subPath)
	return path
}

// LaunchStream populates the parameters from the streaming config and triggers a stream on Cloud Datastream.
func LaunchStream(ctx context.Context, sourceProfile profiles.SourceProfile, dbList []profiles.LogicalShard, projectID string, datastreamCfg DatastreamCfg) error {
	fmt.Println("Launching stream ", fmt.Sprintf("projects/%s/locations/%s", projectID, datastreamCfg.StreamLocation))
	dsClient, err := datastream.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("datastream client can not be created: %v", err)
	}
	defer dsClient.Close()
	fmt.Println("Created client...")
	prefix := datastreamCfg.DestinationConnectionConfig.Prefix
	prefix = concatDirectoryPath(prefix, "data")

	gcsDstCfg := &datastreampb.GcsDestinationConfig{
		Path:       prefix,
		FileFormat: &datastreampb.GcsDestinationConfig_AvroFileFormat{},
	}
	srcCfg := &datastreampb.SourceConfig{
		SourceConnectionProfile: fmt.Sprintf("projects/%s/locations/%s/connectionProfiles/%s", projectID, datastreamCfg.SourceConnectionConfig.Location, datastreamCfg.SourceConnectionConfig.Name),
	}
	err = getSourceStreamConfig(srcCfg, sourceProfile, dbList, datastreamCfg)
	if err != nil {
		return fmt.Errorf("could not get source stream config: %v", err)
	}

	dstCfg := &datastreampb.DestinationConfig{
		DestinationConnectionProfile: fmt.Sprintf("projects/%s/locations/%s/connectionProfiles/%s", projectID, datastreamCfg.DestinationConnectionConfig.Location, datastreamCfg.DestinationConnectionConfig.Name),
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
		Parent:   fmt.Sprintf("projects/%s/locations/%s", projectID, datastreamCfg.StreamLocation),
		StreamId: datastreamCfg.StreamId,
		Stream:   streamInfo,
	}

	fmt.Println("Created stream request..")

	dsOp, err := dsClient.CreateStream(ctx, createStreamRequest)
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
	streamInfo.Name = fmt.Sprintf("projects/%s/locations/%s/streams/%s", projectID, datastreamCfg.StreamLocation, datastreamCfg.StreamId)
	updateStreamRequest := &datastreampb.UpdateStreamRequest{
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"state"}},
		Stream:     streamInfo,
	}
	upOp, err := dsClient.UpdateStream(ctx, updateStreamRequest)
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

func CleanUpStreamingJobs(ctx context.Context, conv *internal.Conv, projectID, region string) error {
	//create clients
	c, err := dataflow.NewJobsV1Beta3Client(ctx)
	if err != nil {
		return fmt.Errorf("could not create job client: %v", err)
	}
	defer c.Close()
	fmt.Println("Created dataflow job client...")
	dsClient, err := datastream.NewClient(ctx)
	fmt.Println("Created datastream client...")
	if err != nil {
		return fmt.Errorf("datastream client can not be created: %v", err)
	}
	defer dsClient.Close()
	pubsubClient, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return fmt.Errorf("pubsub client cannot be created: %v", err)
	}
	defer pubsubClient.Close()

	storageClient, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("storage client cannot be created: %v", err)
	}
	defer storageClient.Close()

	//clean up for single instance migrations
	if conv.Audit.StreamingStats.DataflowJobId != "" {
		CleanupDataflowJob(ctx, c, conv.Audit.StreamingStats.DataflowJobId, projectID, region)
	}
	if conv.Audit.StreamingStats.DataStreamName != "" {
		CleanupDatastream(ctx, dsClient, conv.Audit.StreamingStats.DataStreamName, projectID, region)
	}
	if conv.Audit.StreamingStats.PubsubCfg.TopicId != "" && !conv.IsSharded {
		CleanupPubsubResources(ctx, pubsubClient, storageClient, conv.Audit.StreamingStats.PubsubCfg, projectID)
	}
	if conv.Audit.StreamingStats.MonitoringResources.DashboardName != "" && !conv.IsSharded {
		CleanupMonitoringDashboard(ctx, conv.Audit.StreamingStats.MonitoringResources.DashboardName, projectID)
	}
	if conv.Audit.StreamingStats.AggMonitoringResources.DashboardName != "" && conv.IsSharded {
		CleanupMonitoringDashboard(ctx, conv.Audit.StreamingStats.AggMonitoringResources.DashboardName, projectID)
	}
	if conv.Audit.StreamingStats.AggMonitoringResources.DashboardName != "" && conv.IsSharded {
		CleanupMonitoringDashboard(ctx, conv.Audit.StreamingStats.AggMonitoringResources.DashboardName, projectID)
	}
	// clean up jobs for sharded migrations (with error handling)
	for _, resourceDetails := range conv.Audit.StreamingStats.ShardToDataflowInfoMap {
		dfId := resourceDetails.JobId
		err := CleanupDataflowJob(ctx, c, dfId, projectID, region)
		if err != nil {
			fmt.Printf("Cleanup of the dataflow job: %s was unsuccessful, please clean up the job manually", dfId)
		}
	}
	for _, dsName := range conv.Audit.StreamingStats.ShardToDataStreamNameMap {
		err := CleanupDatastream(ctx, dsClient, dsName, projectID, region)
		if err != nil {
			fmt.Printf("Cleanup of the datastream: %s was unsuccessful, please clean up the stream manually", dsName)
		}
	}
	for _, pubsubCfg := range conv.Audit.StreamingStats.ShardToPubsubIdMap {
		CleanupPubsubResources(ctx, pubsubClient, storageClient, pubsubCfg, projectID)
	}
	for _, monitoringResource := range conv.Audit.StreamingStats.ShardToMonitoringResourcesMap {
		if monitoringResource.DashboardName != "" {
			CleanupMonitoringDashboard(ctx, monitoringResource.DashboardName, projectID)
		}
	}
	fmt.Println("Clean up complete")
	return nil
}

func CleanupPubsubResources(ctx context.Context, pubsubClient *pubsub.Client, storageClient *storage.Client, pubsubCfg internal.PubsubCfg, projectID string) {
	subscription := pubsubClient.Subscription(pubsubCfg.SubscriptionId)

	err := subscription.Delete(ctx)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("Cleanup of the pubsub subscription: %s Failed, please clean up the pubsub subscription manually\n error=%v\n", pubsubCfg.SubscriptionId, err))
	} else {
		logger.Log.Info(fmt.Sprintf("Successfully deleted subscription: %s\n\n", pubsubCfg.SubscriptionId))
	}

	topic := pubsubClient.Topic(pubsubCfg.TopicId)

	err = topic.Delete(ctx)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("Cleanup of the pubsub topic: %s Failed, please clean up the pubsub topic manually\n error=%v\n", pubsubCfg.TopicId, err))
	} else {
		logger.Log.Info(fmt.Sprintf("Successfully deleted topic: %s\n\n", pubsubCfg.TopicId))
	}

	bucket := storageClient.Bucket(pubsubCfg.BucketName)

	if err := bucket.DeleteNotification(ctx, pubsubCfg.NotificationId); err != nil {
		logger.Log.Error(fmt.Sprintf("Cleanup of GCS pubsub notification: %s failed.\n error=%v\n", pubsubCfg.NotificationId, err))
	} else {
		logger.Log.Info(fmt.Sprintf("Successfully deleted GCS pubsub notification: %s\n\n", pubsubCfg.NotificationId))
	}
}

func CleanupMonitoringDashboard(ctx context.Context, dashboardName string, projectID string) {
	client, err := dashboard.NewDashboardsClient(ctx)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("Cleanup of the monitoring dashboard: %s Failed, please clean up the dashboard manually\n error=%v\n", dashboardName, err))
	}
	defer client.Close()
	req := &dashboardpb.DeleteDashboardRequest{
		Name: fmt.Sprintf("projects/%s/dashboards/%s", projectID, dashboardName),
	}
	err = client.DeleteDashboard(ctx, req)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("Cleanup of the monitoring dashboard: %s Failed, please clean up the dashboard manually\n error=%v\n", dashboardName, err))
	} else {
		logger.Log.Info(fmt.Sprintf("Successfully deleted Monitoring Dashboard: %s\n\n", dashboardName))
	}
}

func CleanupDatastream(ctx context.Context, client *datastream.Client, dsName string, projectID, region string) error {
	req := &datastreampb.DeleteStreamRequest{
		Name: fmt.Sprintf("projects/%s/locations/%s/streams/%s", projectID, region, dsName),
	}
	_, err := client.DeleteStream(ctx, req)
	if err != nil {
		return fmt.Errorf("error while deleting datastream job: %v", err)
	}
	return nil
}

func CleanupDataflowJob(ctx context.Context, client *dataflow.JobsV1Beta3Client, dataflowJobId string, projectID, region string) error {
	job := &dataflowpb.Job{
		Id:             dataflowJobId,
		ProjectId:      projectID,
		RequestedState: dataflowpb.JobState_JOB_STATE_CANCELLED,
	}

	dfReq := &dataflowpb.UpdateJobRequest{
		ProjectId: projectID,
		JobId:     dataflowJobId,
		Location:  region,
		Job:       job,
	}
	_, err := client.UpdateJob(ctx, dfReq)
	if err != nil {
		fmt.Println(err)
		return fmt.Errorf("error while cancelling dataflow job: %v", err)
	}
	return nil
}

// LaunchDataflowJob populates the parameters from the streaming config and triggers a Dataflow job.
func LaunchDataflowJob(ctx context.Context, targetProfile profiles.TargetProfile, streamingCfg StreamingCfg, conv *internal.Conv) (internal.DataflowOutput, error) {
	project, instance, dbName, _ := targetProfile.GetResourceIds(ctx, time.Now(), "", nil)
	dataflowCfg := streamingCfg.DataflowCfg
	datastreamCfg := streamingCfg.DatastreamCfg
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

	launchParameters := &dataflowpb.LaunchFlexTemplateParameter{
		JobName:  dataflowCfg.JobName,
		Template: &dataflowpb.LaunchFlexTemplateParameter_ContainerSpecGcsPath{ContainerSpecGcsPath: gcsTemplatePath},
		Parameters: map[string]string{
			"inputFilePattern":              concatDirectoryPath(inputFilePattern, "data"),
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
			MachineType:           dataflowCfg.MachineType,
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
	gcloudDfCmd := utils.GetGcloudDataflowCommand(req)
	logger.Log.Debug(fmt.Sprintf("\nEquivalent gCloud command for job %s:\n%s\n\n", req.LaunchParameter.JobName, gcloudDfCmd))
	return internal.DataflowOutput{JobID: respDf.Job.Id, GCloudCmd: gcloudDfCmd}, nil
}

func StoreGeneratedResources(conv *internal.Conv, streamingCfg StreamingCfg, dfJobId, gcloudDataflowCmd, project, dataShardId string, gcsBucket internal.GcsResources, dashboardName string) {
	datastreamCfg := streamingCfg.DatastreamCfg
	dataflowCfg := streamingCfg.DataflowCfg
	conv.Audit.StreamingStats.DataStreamName = datastreamCfg.StreamId
	conv.Audit.StreamingStats.DataflowJobId = dfJobId
	conv.Audit.StreamingStats.DataflowLocation = streamingCfg.DataflowCfg.Location
	conv.Audit.StreamingStats.DataflowGcloudCmd = gcloudDataflowCmd
	conv.Audit.StreamingStats.PubsubCfg = streamingCfg.PubsubCfg
	conv.Audit.StreamingStats.GcsResources = gcsBucket
	conv.Audit.StreamingStats.MonitoringResources = internal.MonitoringResources{DashboardName: dashboardName}
	if dataShardId != "" {
		var resourceMutex sync.Mutex
		resourceMutex.Lock()
		conv.Audit.StreamingStats.ShardToDataStreamNameMap[dataShardId] = datastreamCfg.StreamId
		conv.Audit.StreamingStats.ShardToDataflowInfoMap[dataShardId] = internal.ShardedDataflowJobResources{JobId: dfJobId, GcloudCmd: gcloudDataflowCmd}
		conv.Audit.StreamingStats.ShardToPubsubIdMap[dataShardId] = streamingCfg.PubsubCfg
		conv.Audit.StreamingStats.ShardToGcsResources[dataShardId] = gcsBucket
		if dashboardName != "" {
			{
				conv.Audit.StreamingStats.ShardToMonitoringResourcesMap[dataShardId] = internal.MonitoringResources{DashboardName: dashboardName}
			}
		}
		resourceMutex.Unlock()
	}
	fullStreamName := fmt.Sprintf("projects/%s/locations/%s/streams/%s", project, datastreamCfg.StreamLocation, datastreamCfg.StreamId)
	dfJobDetails := fmt.Sprintf("project: %s, location: %s, name: %s, id: %s", project, dataflowCfg.Location, dataflowCfg.JobName, dfJobId)
	logger.Log.Info("\n------------------------------------------\n")
	logger.Log.Info("The Datastream job: " + fullStreamName + " ,the Dataflow job: " + dfJobDetails +
		" the Pubsub topic: " + streamingCfg.PubsubCfg.TopicId + " ,the subscription: " + streamingCfg.PubsubCfg.SubscriptionId +
		" and the pubsub Notification id:" + streamingCfg.PubsubCfg.NotificationId + " on bucket: " + streamingCfg.PubsubCfg.BucketName +
		" will have to be manually cleaned up via the UI. Spanner migration tool will not delete them post completion of the migration.")
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
	datastreamCfg := DatastreamCfg{StreamLocation: pl.StreamLocation}
	//set src connection profile
	inputSrcConnProfile := pl.SrcConnectionProfile
	srcConnCfg := SrcConnCfg{Location: inputSrcConnProfile.Location, Name: inputSrcConnProfile.Name}
	datastreamCfg.SourceConnectionConfig = srcConnCfg
	//set dst connection profile
	inputDstConnProfile := pl.DstConnectionProfile
	dstConnCfg := DstConnCfg{Name: inputDstConnProfile.Name, Location: inputDstConnProfile.Location}
	datastreamCfg.DestinationConnectionConfig = dstConnCfg
	//create the streamingCfg object
	streamingCfg := StreamingCfg{DataflowCfg: dataflowCfg, DatastreamCfg: datastreamCfg, TmpDir: pl.TmpDir, DataShardId: pl.DataShardId}
	return streamingCfg
}

func StartDatastream(ctx context.Context, streamingCfg StreamingCfg, sourceProfile profiles.SourceProfile, targetProfile profiles.TargetProfile, tableList []string) (StreamingCfg, error) {
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
	err = utils.WriteToGCS(streamingCfg.TmpDir, "session.json", string(convJSON))
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
	err = utils.WriteToGCS(streamingCfg.TmpDir, "transformationContext.json", string(transformationContext))
	if err != nil {
		return internal.DataflowOutput{}, fmt.Errorf("error while writing to GCS: %v", err)
	}
	dfOutput, err := LaunchDataflowJob(ctx, targetProfile, streamingCfg, conv)
	if err != nil {
		return internal.DataflowOutput{}, fmt.Errorf("error launching dataflow: %v", err)
	}
	return dfOutput, nil
}

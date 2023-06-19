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
	"strings"
	"sync"
	"time"

	dataflow "cloud.google.com/go/dataflow/apiv1beta3"
	datastream "cloud.google.com/go/datastream/apiv1"
	"cloud.google.com/go/storage"
	datastreampb "google.golang.org/genproto/googleapis/cloud/datastream/v1"
	dataflowpb "google.golang.org/genproto/googleapis/dataflow/v1beta3"
	fieldmaskpb "google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/cloudspannerecosystem/harbourbridge/common/constants"
	"github.com/cloudspannerecosystem/harbourbridge/common/utils"
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/profiles"
)

const (
	maxWorkers int32 = 50
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
	StreamId                    string
	StreamLocation              string
	StreamDisplayName           string
	SourceConnectionConfig      SrcConnCfg
	DestinationConnectionConfig DstConnCfg
	Properties                  string
}

type DataflowCfg struct {
	JobName            string
	Location           string
	HostProjectId      string
	Network            string
	Subnetwork         string
	DbNameToShardIdMap map[string]string
}

type StreamingCfg struct {
	DatastreamCfg DatastreamCfg
	DataflowCfg   DataflowCfg
	TmpDir        string
	DataShardId   string
}

// VerifyAndUpdateCfg checks the fields and errors out if certain fields are empty.
// It then auto-populates certain empty fields like StreamId and Dataflow JobName.
func VerifyAndUpdateCfg(streamingCfg *StreamingCfg, dbName string) error {
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
		streamId, err := utils.GenerateName("hb-stream-" + dbName)
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

	if dfCfg.JobName == "" {
		// Update names to have more info like dbname.
		jobName, err := utils.GenerateName("hb-dataflow-" + dbName)
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
func ReadStreamingConfig(file, dbName string) (StreamingCfg, error) {
	streamingCfg := StreamingCfg{}
	cfgFile, err := ioutil.ReadFile(file)
	if err != nil {
		return streamingCfg, fmt.Errorf("can't read streaming config file due to: %v", err)
	}
	err = json.Unmarshal(cfgFile, &streamingCfg)
	if err != nil {
		return streamingCfg, fmt.Errorf("unable to unmarshall json due to: %v", err)
	}
	err = VerifyAndUpdateCfg(&streamingCfg, dbName)
	if err != nil {
		return streamingCfg, fmt.Errorf("streaming config is incomplete: %v", err)
	}
	return streamingCfg, nil
}

func getMysqlSourceStreamConfig(dbList []profiles.LogicalShard) *datastreampb.SourceConfig_MysqlSourceConfig {
	includeDbList := []*datastreampb.MysqlDatabase{}
	for _, db := range dbList {
		//create include db object
		includeDb := &datastreampb.MysqlDatabase{
			Database: db.DbName,
		}
		includeDbList = append(includeDbList, includeDb)
	}
	//TODO: Clean up fmt.Printf logs and replace them with zap logger.
	fmt.Printf("Include DB List for datastream: %+v\n", includeDbList)
	mysqlSrcCfg := &datastreampb.MysqlSourceConfig{
		IncludeObjects: &datastreampb.MysqlRdbms{MysqlDatabases: includeDbList},
	}
	return &datastreampb.SourceConfig_MysqlSourceConfig{MysqlSourceConfig: mysqlSrcCfg}
}

func getOracleSourceStreamConfig(dbName string) *datastreampb.SourceConfig_OracleSourceConfig {
	oracledb := &datastreampb.OracleSchema{
		Schema: dbName,
	}
	oracleSrcCfg := &datastreampb.OracleSourceConfig{
		IncludeObjects: &datastreampb.OracleRdbms{OracleSchemas: []*datastreampb.OracleSchema{oracledb}},
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
		ExcludeObjects:  &datastreampb.PostgresqlRdbms{PostgresqlSchemas: excludeObjects},
		ReplicationSlot: replicationSlot,
		Publication:     publication,
	}
	return &datastreampb.SourceConfig_PostgresqlSourceConfig{PostgresqlSourceConfig: postgresSrcCfg}, nil
}

func getSourceStreamConfig(srcCfg *datastreampb.SourceConfig, driver string, dbList []profiles.LogicalShard, datastreamCfg DatastreamCfg) error {
	switch driver {
	case constants.MYSQL:
		srcCfg.SourceStreamConfig = getMysqlSourceStreamConfig(dbList)
		return nil
	case constants.ORACLE:
		// For Oracle, the User name denotes the name of the schema while the dbName parameter has the SID.
		srcCfg.SourceStreamConfig = getOracleSourceStreamConfig(dbList[0].DbName)
		return nil
	case constants.POSTGRES:
		sourceStreamConfig, err := getPostgreSQLSourceStreamConfig(datastreamCfg.Properties)
		if err == nil {
			srcCfg.SourceStreamConfig = sourceStreamConfig
		}
		return err
	default:
		return fmt.Errorf("only MySQL, Oracle and PostgreSQL are supported as source streams")
	}
}

// LaunchStream populates the parameters from the streaming config and triggers a stream on Cloud Datastream.
func LaunchStream(ctx context.Context, driver string, dbList []profiles.LogicalShard, projectID string, datastreamCfg DatastreamCfg) error {
	fmt.Println("Launching stream ", fmt.Sprintf("projects/%s/locations/%s", projectID, datastreamCfg.StreamLocation))
	dsClient, err := datastream.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("datastream client can not be created: %v", err)
	}
	defer dsClient.Close()
	fmt.Println("Created client...")

	gcsDstCfg := &datastreampb.GcsDestinationConfig{
		Path:       datastreamCfg.DestinationConnectionConfig.Prefix,
		FileFormat: &datastreampb.GcsDestinationConfig_AvroFileFormat{},
	}
	srcCfg := &datastreampb.SourceConfig{
		SourceConnectionProfile: fmt.Sprintf("projects/%s/locations/%s/connectionProfiles/%s", projectID, datastreamCfg.SourceConnectionConfig.Location, datastreamCfg.SourceConnectionConfig.Name),
	}
	err = getSourceStreamConfig(srcCfg, driver, dbList, datastreamCfg)
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

	//clean up for single instance migrations
	if conv.Audit.StreamingStats.DataflowJobId != "" {
		CleanupDataflowJob(ctx, c, conv.Audit.StreamingStats.DataflowJobId, projectID, region)
	}
	if conv.Audit.StreamingStats.DataStreamName != "" {
		CleanupDatastream(ctx, dsClient, conv.Audit.StreamingStats.DataStreamName, projectID, region)
	}
	// clean up jobs for sharded migrations (with error handling)
	for _, dfId := range conv.Audit.StreamingStats.ShardToDataflowJobMap {
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
	fmt.Println("Clean up complete")
	return nil
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
func LaunchDataflowJob(ctx context.Context, targetProfile profiles.TargetProfile, streamingCfg StreamingCfg, conv *internal.Conv) error {
	project, instance, dbName, _ := targetProfile.GetResourceIds(ctx, time.Now(), "", nil)
	dataflowCfg := streamingCfg.DataflowCfg
	datastreamCfg := streamingCfg.DatastreamCfg
	fmt.Println("Launching dataflow job ", dataflowCfg.JobName, " in ", project, "-", dataflowCfg.Location)

	c, err := dataflow.NewFlexTemplatesClient(ctx)
	if err != nil {
		return fmt.Errorf("could not create flex template client: %v", err)
	}
	defer c.Close()
	fmt.Println("Created flex template client...")

	//Creating datastream client to fetch the gcs bucket using target profile.
	dsClient, err := datastream.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("datastream client can not be created: %v", err)
	}
	defer dsClient.Close()

	// Fetch the GCS path from the destination connection profile.
	dstProf := fmt.Sprintf("projects/%s/locations/%s/connectionProfiles/%s", project, datastreamCfg.DestinationConnectionConfig.Location, datastreamCfg.DestinationConnectionConfig.Name)
	res, err := dsClient.GetConnectionProfile(ctx, &datastreampb.GetConnectionProfileRequest{Name: dstProf})
	if err != nil {
		return fmt.Errorf("could not get connection profiles: %v", err)
	}
	gcsProfile := res.Profile.(*datastreampb.ConnectionProfile_GcsProfile).GcsProfile
	inputFilePattern := "gs://" + gcsProfile.Bucket + gcsProfile.RootPath + datastreamCfg.DestinationConnectionConfig.Prefix
	if inputFilePattern[len(inputFilePattern)-1] != '/' {
		inputFilePattern = inputFilePattern + "/"
	}
	fmt.Println("Reading files from datastream destination ", inputFilePattern)
	var dataflowHostProjectId string
	if dataflowCfg.HostProjectId == "" {
		dataflowHostProjectId, _ = utils.GetProject()
	} else {
		dataflowHostProjectId = dataflowCfg.HostProjectId
	}
	dataflowSubnetwork := ""
	if dataflowCfg.Network != "" {
		if dataflowCfg.Subnetwork == "" {
			return fmt.Errorf("if network is specified, subnetwork cannot be empty")
		} else {
			dataflowSubnetwork = fmt.Sprintf("https://www.googleapis.com/compute/v1/projects/%s/regions/%s/subnetworks/%s", dataflowHostProjectId, dataflowCfg.Location, dataflowCfg.Subnetwork)
		}
	}

	launchParameters := createLaunchParameters(dataflowCfg, inputFilePattern, project, datastreamCfg, instance, dbName, streamingCfg, dataflowSubnetwork)

	req := &dataflowpb.LaunchFlexTemplateRequest{
		ProjectId:       project,
		LaunchParameter: launchParameters,
		Location:        dataflowCfg.Location,
	}
	fmt.Println("Created flex template request body...")

	respDf, err := c.LaunchFlexTemplate(ctx, req)
	if err != nil {
		fmt.Printf("flexTemplateRequest: %+v\n", req)
		return fmt.Errorf("unable to launch template: %v", err)
	}
	storeGeneratedResources(conv, datastreamCfg, respDf, project, streamingCfg.DataShardId)
	return nil
}

func storeGeneratedResources(conv *internal.Conv, datastreamCfg DatastreamCfg, respDf *dataflowpb.LaunchFlexTemplateResponse, project string, dataShardId string) {
	conv.Audit.StreamingStats.DataStreamName = datastreamCfg.StreamId
	conv.Audit.StreamingStats.DataflowJobId = respDf.Job.Id
	if dataShardId != "" {
		var resourceMutex sync.Mutex
		resourceMutex.Lock()
		conv.Audit.StreamingStats.ShardToDataStreamNameMap[dataShardId] = datastreamCfg.StreamId
		conv.Audit.StreamingStats.ShardToDataflowJobMap[dataShardId] = respDf.Job.Id
		resourceMutex.Unlock()
	}
	fullStreamName := fmt.Sprintf("projects/%s/locations/%s/streams/%s", project, datastreamCfg.StreamLocation, datastreamCfg.StreamId)
	dfJobDetails := fmt.Sprintf("project: %s, location: %s, name: %s, id: %s", project, respDf.Job.Location, respDf.Job.Name, respDf.Job.Id)
	fmt.Println("\n------------------------------------------\n" +
		"The Datastream job: " + fullStreamName + "and the Dataflow job: " + dfJobDetails +
		" will have to be manually cleaned up via the UI. HarbourBridge will not delete them post completion of the migration.")
}

func createLaunchParameters(dataflowCfg DataflowCfg, inputFilePattern string, project string, datastreamCfg DatastreamCfg, instance string, dbName string, streamingCfg StreamingCfg, dataflowSubnetwork string) *dataflowpb.LaunchFlexTemplateParameter {
	return &dataflowpb.LaunchFlexTemplateParameter{
		JobName:  dataflowCfg.JobName,
		Template: &dataflowpb.LaunchFlexTemplateParameter_ContainerSpecGcsPath{ContainerSpecGcsPath: "gs://khajanchi-gsql/images/datastream-to-spanner-image-spec.json"},
		Parameters: map[string]string{
			"inputFilePattern":              inputFilePattern,
			"streamName":                    fmt.Sprintf("projects/%s/locations/%s/streams/%s", project, datastreamCfg.StreamLocation, datastreamCfg.StreamId),
			"instanceId":                    instance,
			"databaseId":                    dbName,
			"sessionFilePath":               streamingCfg.TmpDir + "session.json",
			"deadLetterQueueDirectory":      inputFilePattern + "dlq",
			"transformationContextFilePath": streamingCfg.TmpDir + "transformationContext.json",
		},
		Environment: &dataflowpb.FlexTemplateRuntimeEnvironment{
			MaxWorkers:            maxWorkers,
			AutoscalingAlgorithm:  2, // 2 corresponds to AUTOSCALING_ALGORITHM_BASIC
			EnableStreamingEngine: true,
			Network:               dataflowCfg.Network,
			Subnetwork:            dataflowSubnetwork,
		},
	}
}

func getStreamingConfig(sourceProfile profiles.SourceProfile, targetProfile profiles.TargetProfile) (StreamingCfg, error) {
	switch sourceProfile.Conn.Ty {
	case profiles.SourceProfileConnectionTypeMySQL:
		return ReadStreamingConfig(sourceProfile.Conn.Mysql.StreamingConfig, targetProfile.Conn.Sp.Dbname)
	case profiles.SourceProfileConnectionTypeOracle:
		return ReadStreamingConfig(sourceProfile.Conn.Oracle.StreamingConfig, targetProfile.Conn.Sp.Dbname)
	case profiles.SourceProfileConnectionTypePostgreSQL:
		return ReadStreamingConfig(sourceProfile.Conn.Pg.StreamingConfig, targetProfile.Conn.Sp.Dbname)
	default:
		return StreamingCfg{}, fmt.Errorf("only MySQL, Oracle and PostgreSQL are supported as source streams")
	}
}

func CreateStreamingConfig(pl profiles.DataShard) StreamingCfg {
	//create dataflowcfg from pl receiver object
	inputDataflowConfig := pl.DataflowConfig
	dataflowCfg := DataflowCfg{Location: inputDataflowConfig.Location,
		Network:       inputDataflowConfig.Network,
		HostProjectId: inputDataflowConfig.HostProjectId,
		Subnetwork:    inputDataflowConfig.Subnetwork}
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

func StartDatastream(ctx context.Context, sourceProfile profiles.SourceProfile, targetProfile profiles.TargetProfile) (StreamingCfg, error) {
	streamingCfg, err := getStreamingConfig(sourceProfile, targetProfile)
	if err != nil {
		return streamingCfg, fmt.Errorf("error reading streaming config: %v", err)
	}
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
	err = LaunchStream(ctx, driver, dbList, targetProfile.Conn.Sp.Project, streamingCfg.DatastreamCfg)
	if err != nil {
		return streamingCfg, fmt.Errorf("error launching stream: %v", err)
	}
	return streamingCfg, nil
}

func StartDataflow(ctx context.Context, targetProfile profiles.TargetProfile, streamingCfg StreamingCfg, conv *internal.Conv) error {

	convJSON, err := json.MarshalIndent(conv, "", " ")
	if err != nil {
		return fmt.Errorf("can't encode session state to JSON: %v", err)
	}
	err = utils.WriteToGCS(streamingCfg.TmpDir, "session.json", string(convJSON))
	if err != nil {
		return fmt.Errorf("error while writing to GCS: %v", err)
	}
	transformationContextMap := map[string]interface{}{
		"SchemaToShardId": streamingCfg.DataflowCfg.DbNameToShardIdMap,
	}
	transformationContext, err := json.Marshal(transformationContextMap)
	if err != nil {
		return fmt.Errorf("failed to compute transformation context: %s", err.Error())
	}
	err = utils.WriteToGCS(streamingCfg.TmpDir, "transformationContext.json", string(transformationContext))
	if err != nil {
		return fmt.Errorf("error while writing to GCS: %v", err)
	}
	err = LaunchDataflowJob(ctx, targetProfile, streamingCfg, conv)
	if err != nil {
		return fmt.Errorf("error launching dataflow: %v", err)
	}
	return nil
}

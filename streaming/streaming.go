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
	JobName  string
	Location string
}

type StreamingCfg struct {
	DatastreamCfg DatastreamCfg
	DataflowCfg   DataflowCfg
	TmpDir        string
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

func getMysqlSourceStreamConfig(dbName string) *datastreampb.SourceConfig_MysqlSourceConfig {
	mydb := &datastreampb.MysqlDatabase{
		Database: dbName,
	}
	mysqlSrcCfg := &datastreampb.MysqlSourceConfig{
		IncludeObjects: &datastreampb.MysqlRdbms{MysqlDatabases: []*datastreampb.MysqlDatabase{mydb}},
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
	infoschemadb := &datastreampb.PostgresqlSchema{
		Schema: "infoschema",
	}
	pgcatalogdb := &datastreampb.PostgresqlSchema{
		Schema: "pg_catalog",
	}
	pg_toast := &datastreampb.PostgresqlSchema{
		Schema: "pg_toast",
	}
	postgres := &datastreampb.PostgresqlSchema{
		Schema: "postgres",
	}
	pg_temp_1 := &datastreampb.PostgresqlSchema{
		Schema: "pg_temp_1",
	}
	pg_toast_temp_1 := &datastreampb.PostgresqlSchema{
		Schema: "pg_toast_temp_1",
	}
	replicationSlot, replicationSlotExists := params["replicationSlot"]
	publication, publicationExists := params["publication"]
	if !replicationSlotExists || !publicationExists {
		return nil, fmt.Errorf("replication slot or publication not specified")
	}
	postgresSrcCfg := &datastreampb.PostgresqlSourceConfig{
		ExcludeObjects:  &datastreampb.PostgresqlRdbms{PostgresqlSchemas: []*datastreampb.PostgresqlSchema{infoschemadb, pgcatalogdb, pg_toast, postgres, pg_temp_1, pg_toast_temp_1}},
		ReplicationSlot: replicationSlot,
		Publication:     publication,
	}
	return &datastreampb.SourceConfig_PostgresqlSourceConfig{PostgresqlSourceConfig: postgresSrcCfg}, nil
}

func getSourceStreamConfig(srcCfg *datastreampb.SourceConfig, sourceProfile profiles.SourceProfile, datastreamCfg DatastreamCfg) error {
	switch sourceProfile.Driver {
	case constants.MYSQL:
		srcCfg.SourceStreamConfig = getMysqlSourceStreamConfig(sourceProfile.Conn.Mysql.Db)
		return nil
	case constants.ORACLE:
		// For Oracle, the User name denotes the name of the schema while the dbName parameter has the SID.
		srcCfg.SourceStreamConfig = getOracleSourceStreamConfig(sourceProfile.Conn.Oracle.User)
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
func LaunchStream(ctx context.Context, sourceProfile profiles.SourceProfile, projectID string, datastreamCfg DatastreamCfg) error {
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
	err = getSourceStreamConfig(srcCfg, sourceProfile, datastreamCfg)
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
	c, err := dataflow.NewJobsV1Beta3Client(ctx)
	if err != nil {
		return fmt.Errorf("could not create job client: %v", err)
	}
	defer c.Close()
	fmt.Println("Created dataflow job client...")

	job := &dataflowpb.Job{
		Id:             conv.Audit.StreamingStats.DataflowJobId,
		ProjectId:      projectID,
		RequestedState: dataflowpb.JobState_JOB_STATE_CANCELLED,
	}

	dfReq := &dataflowpb.UpdateJobRequest{
		ProjectId: projectID,
		JobId:     conv.Audit.StreamingStats.DataflowJobId,
		Location:  region,
		Job:       job,
	}
	_, err = c.UpdateJob(ctx, dfReq)
	if err != nil {
		fmt.Println(err)
		return fmt.Errorf("error while cancelling dataflow job: %v", err)
	}
	dsClient, err := datastream.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("datastream client can not be created: %v", err)
	}
	defer dsClient.Close()
	req := &datastreampb.DeleteStreamRequest{
		Name: fmt.Sprintf("projects/%s/locations/%s/streams/%s", projectID, region, conv.Audit.StreamingStats.DataStreamName),
	}
	_, err = dsClient.DeleteStream(ctx, req)
	if err != nil {
		return fmt.Errorf("error while deleting datastream job: %v", err)
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

	launchParameter := &dataflowpb.LaunchFlexTemplateParameter{
		JobName:  dataflowCfg.JobName,
		Template: &dataflowpb.LaunchFlexTemplateParameter_ContainerSpecGcsPath{ContainerSpecGcsPath: "gs://dataflow-templates/latest/flex/Cloud_Datastream_to_Spanner"},
		Parameters: map[string]string{
			"inputFilePattern":         inputFilePattern,
			"streamName":               fmt.Sprintf("projects/%s/locations/%s/streams/%s", project, datastreamCfg.StreamLocation, datastreamCfg.StreamId),
			"instanceId":               instance,
			"databaseId":               dbName,
			"sessionFilePath":          streamingCfg.TmpDir + "session.json",
			"deadLetterQueueDirectory": inputFilePattern + "dlq",
		},
		Environment: &dataflowpb.FlexTemplateRuntimeEnvironment{
			MaxWorkers:            maxWorkers,
			AutoscalingAlgorithm:  2, // 2 corresponds to AUTOSCALING_ALGORITHM_BASIC
			EnableStreamingEngine: true,
		},
	}

	req := &dataflowpb.LaunchFlexTemplateRequest{
		ProjectId:       project,
		LaunchParameter: launchParameter,
		Location:        dataflowCfg.Location,
	}
	fmt.Println("Created flex template request body...")

	respDf, err := c.LaunchFlexTemplate(ctx, req)
	if err != nil {
		fmt.Printf("flexTemplateRequest: %+v\n", req)
		return fmt.Errorf("unable to launch template: %v", err)
	}
	conv.Audit.StreamingStats.DataStreamName = datastreamCfg.StreamId
	conv.Audit.StreamingStats.DataflowJobId = respDf.Job.Id
	fullStreamName := fmt.Sprintf("projects/%s/locations/%s/streams/%s", project, datastreamCfg.StreamLocation, datastreamCfg.StreamId)
	dfJobDetails := fmt.Sprintf("project: %s, location: %s, name: %s, id: %s", project, respDf.Job.Location, respDf.Job.Name, respDf.Job.Id)
	fmt.Println("\n------------------------------------------\n" +
		"The Datastream job: " + fullStreamName + "and the Dataflow job: " + dfJobDetails +
		" will have to be manually cleaned up via the UI. HarbourBridge will not delete them post completion of the migration.")
	return nil
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

func StartDatastream(ctx context.Context, sourceProfile profiles.SourceProfile, targetProfile profiles.TargetProfile) (StreamingCfg, error) {
	streamingCfg, err := getStreamingConfig(sourceProfile, targetProfile)
	if err != nil {
		return streamingCfg, fmt.Errorf("error reading streaming config: %v", err)
	}

	err = LaunchStream(ctx, sourceProfile, targetProfile.Conn.Sp.Project, streamingCfg.DatastreamCfg)
	if err != nil {
		return streamingCfg, fmt.Errorf("error launching stream: %v", err)
	}
	return streamingCfg, nil
}

func StartDataflow(ctx context.Context, sourceProfile profiles.SourceProfile, targetProfile profiles.TargetProfile, streamingCfg StreamingCfg, conv *internal.Conv) error {

	convJSON, err := json.MarshalIndent(conv, "", " ")
	if err != nil {
		return fmt.Errorf("can't encode session state to JSON: %v", err)
	}
	err = utils.WriteToGCS(streamingCfg.TmpDir, "session.json", string(convJSON))
	if err != nil {
		return fmt.Errorf("error while writing to GCS: %v", err)
	}
	err = LaunchDataflowJob(ctx, targetProfile, streamingCfg, conv)
	if err != nil {
		return fmt.Errorf("error launching dataflow: %v", err)
	}
	return nil
}

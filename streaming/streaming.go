// Copyright 2022 Google LLC
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
package streaming

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"time"

	dataflow "cloud.google.com/go/dataflow/apiv1beta3"
	datastream "cloud.google.com/go/datastream/apiv1alpha1"
	datastreampb "google.golang.org/genproto/googleapis/cloud/datastream/v1alpha1"
	dataflowpb "google.golang.org/genproto/googleapis/dataflow/v1beta3"
	fieldmaskpb "google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/cloudspannerecosystem/harbourbridge/common/utils"
	"github.com/cloudspannerecosystem/harbourbridge/profiles"
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
}

type DataflowCfg struct {
	JobName  string
	Location string
}

type StreamingCfg struct {
	DatastreamCfg DatastreamCfg
	DataflowCfg   DataflowCfg
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
		if err != nil {
			return fmt.Errorf("error generating stream name: %v", err)
		}
		streamingCfg.DataflowCfg.JobName = jobName
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

// LaunchStream populates the parameters from the streaming config and triggers a stream on Cloud Datastream.
func LaunchStream(ctx context.Context, dbName, projectID string, datastreamCfg DatastreamCfg) error {
	fmt.Println("Launching stream...")
	dsClient, err := datastream.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("datastream client can not be created: %v", err)
	}
	defer dsClient.Close()
	fmt.Println("Created client...")

	mydb := &datastreampb.MysqlDatabase{
		DatabaseName: dbName,
	}
	mysqlSrcCfg := &datastreampb.MysqlSourceConfig{
		Allowlist: &datastreampb.MysqlRdbms{MysqlDatabases: []*datastreampb.MysqlDatabase{mydb}},
	}
	gcsDstCfg := &datastreampb.GcsDestinationConfig{
		Path:       datastreamCfg.DestinationConnectionConfig.Prefix,
		FileFormat: &datastreampb.GcsDestinationConfig_AvroFileFormat{},
	}
	srcCfg := &datastreampb.SourceConfig{
		SourceConnectionProfileName: fmt.Sprintf("projects/%s/locations/%s/connectionProfiles/%s", projectID, datastreamCfg.SourceConnectionConfig.Location, datastreamCfg.SourceConnectionConfig.Name),
		SourceStreamConfig:          &datastreampb.SourceConfig_MysqlSourceConfig{MysqlSourceConfig: mysqlSrcCfg},
	}
	dstCfg := &datastreampb.DestinationConfig{
		DestinationConnectionProfileName: fmt.Sprintf("projects/%s/locations/%s/connectionProfiles/%s", projectID, datastreamCfg.DestinationConnectionConfig.Location, datastreamCfg.DestinationConnectionConfig.Name),
		DestinationStreamConfig:          &datastreampb.DestinationConfig_GcsDestinationConfig{GcsDestinationConfig: gcsDstCfg},
	}
	streamInfo := &datastreampb.Stream{
		DisplayName:       datastreamCfg.StreamDisplayName,
		SourceConfig:      srcCfg,
		DestinationConfig: dstCfg,
		State:             datastreampb.Stream_RUNNING,
		BackfillStrategy:  &datastreampb.Stream_BackfillNone{BackfillNone: &datastreampb.Stream_BackfillNoneStrategy{}},
	}
	createStreamRequest := &datastreampb.CreateStreamRequest{
		Parent:   fmt.Sprintf("projects/%s/locations/%s", projectID, datastreamCfg.StreamLocation),
		StreamId: datastreamCfg.StreamId,
		Stream:   streamInfo,
	}

	fmt.Println("Created stream request..")

	dsOp, err := dsClient.CreateStream(ctx, createStreamRequest)
	if err != nil {
		fmt.Printf("createStreamRequest: %+v\n", createStreamRequest)
		return fmt.Errorf("cannot create stream: %v ", err)
	}

	_, err = dsOp.Wait(ctx)
	if err != nil {
		fmt.Printf("createStreamRequest: %+v\n", createStreamRequest)
		return fmt.Errorf("datastream create operation failed: %v", err)
	}
	fmt.Println("Successfully created stream")

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
	fmt.Println("Stream state set to RUNNING...")
	return nil
}

// LaunchDataflowJob populates the parameters from the streaming config and triggers a Dataflow job.
func LaunchDataflowJob(ctx context.Context, targetProfile profiles.TargetProfile, datastreamCfg DatastreamCfg, dataflowCfg DataflowCfg) error {
	fmt.Println("Launching dataflow job...")
	project, instance, dbName, _ := targetProfile.GetResourceIds(ctx, time.Now(), "", nil)

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

	launchParameter := &dataflowpb.LaunchFlexTemplateParameter{
		JobName:  dataflowCfg.JobName,
		Template: &dataflowpb.LaunchFlexTemplateParameter_ContainerSpecGcsPath{ContainerSpecGcsPath: "gs://dataflow-templates/latest/flex/Cloud_Datastream_to_Spanner"},
		Parameters: map[string]string{
			"inputFilePattern": "gs://" + gcsProfile.BucketName + gcsProfile.RootPath + datastreamCfg.DestinationConnectionConfig.Prefix,
			"streamName":       fmt.Sprintf("projects/%s/locations/%s/streams/%s", project, datastreamCfg.StreamLocation, datastreamCfg.StreamId),
			"instanceId":       instance,
			"databaseId":       dbName,
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
	fullStreamName := fmt.Sprintf("projects/%s/locations/%s/streams/%s", project, datastreamCfg.StreamLocation, datastreamCfg.StreamId)
	dfJobDetails := fmt.Sprintf("project: %s, location: %s, name: %s, id: %s", project, respDf.Job.Location, respDf.Job.Name, respDf.Job.Id)
	fmt.Println("\n------------------------------------------\n" +
		"The Datastream job: " + fullStreamName + "and the Dataflow job: " + dfJobDetails +
		" will have to be manually cleaned up via he UI. HarbourBridge will not delete them post completion of the migration.")
	return nil
}

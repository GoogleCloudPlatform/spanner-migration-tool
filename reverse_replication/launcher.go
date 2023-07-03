package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	dataflow "cloud.google.com/go/dataflow/apiv1beta3"
	"cloud.google.com/go/dataflow/apiv1beta3/dataflowpb"
	"cloud.google.com/go/pubsub"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/storage"
	adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
)

var (
	projectId            string
	dataflowRegion       string
	jobNamePrefix        string
	changeStreamName     string
	instanceId           string
	dbName               string
	metadataInstance     string
	metadataDatabase     string
	pubSubDataTopicId    string
	pubSubEndpoint       string
	sourceShardsFilePath string
	sessionFilePath      string
)

func setupGlobalFlags() {
	flag.StringVar(&projectId, "projectId", "", "projectId")
	flag.StringVar(&dataflowRegion, "dataflowRegion", "", "region for dataflow jobs")
	flag.StringVar(&jobNamePrefix, "jobNamePrefix", "reverse-rep", "job name prefix for the dataflow jobs")
	flag.StringVar(&changeStreamName, "changeStreamName", "", "change stream name")
	flag.StringVar(&instanceId, "instanceId", "", "spanner instance id")
	flag.StringVar(&dbName, "dbName", "", "spanner database name")
	flag.StringVar(&metadataInstance, "metadataInstance", "", "spanner instance name to store changestream metadata")
	flag.StringVar(&metadataDatabase, "metadataDatabase", "", "spanner database name to store changestream metadata")
	flag.StringVar(&pubSubDataTopicId, "pubSubDataTopicId", "", "pub/sub data topic id, should be of the form projects/my-project/topics/my-topic")
	flag.StringVar(&pubSubEndpoint, "pubSubEndpoint", "", "pub/sub endpoint, defaults to same endpoint as the dataflow region.")
	flag.StringVar(&sourceShardsFilePath, "sourceShardsFilePath", "", "gcs file path for file containing shard info")
	flag.StringVar(&sessionFilePath, "sessionFilePath", "", "gcs file path for session file generated via HarbourBridge")
}

func prechecks() error {
	if projectId == "" {
		return fmt.Errorf("please specify a valid projectId")
	}
	if dataflowRegion == "" {
		return fmt.Errorf("please specify a valid dataflowRegion")
	}
	if changeStreamName == "" {
		return fmt.Errorf("please specify a valid changeStreamName")
	}
	if instanceId == "" {
		return fmt.Errorf("please specify a valid instanceId")
	}
	if dbName == "" {
		return fmt.Errorf("please specify a valid dbName")
	}
	if metadataInstance == "" {
		return fmt.Errorf("please specify a valid metadataInstance")
	}
	if metadataDatabase == "" {
		return fmt.Errorf("please specify a valid metadataDatabase")
	}
	if pubSubDataTopicId == "" {
		return fmt.Errorf("please specify a valid pubSubDataTopicId of the form project/project-name/topics/topic-name")
	}
	if sourceShardsFilePath == "" {
		return fmt.Errorf("please specify a valid sourceShardsFilePath")
	}
	if sessionFilePath == "" {
		return fmt.Errorf("please specify a valid sessionFilePath")
	}
	match, _ := regexp.MatchString("projects/.*/topics/.*", pubSubDataTopicId)
	if !match {
		return fmt.Errorf("please specify a valid pubSubDataTopicId of the form project/project-name/topics/topic-name")
	}
	if pubSubEndpoint == "" {
		pubSubEndpoint = fmt.Sprintf("https://%s-pubsub.googleapis.com:443", dataflowRegion)
	}
	return nil
}

func main() {
	ORDERING_TEMPLATE := "gs://deepchowdhury-gsql/templates/flex/Spanner_Change_Streams_to_Sink"
	WRITER_TEMPLATE := "gs://deepchowdhury-gsql/templates/flex/Ordered_Changestream_Buffer_to_Sourcedb"
	//WRITER_TEMPLATE := "gs://aks-test-revrep/images/ordered-changestream-buffer-to-sourcedb-image-spec.json"
	setupGlobalFlags()
	flag.Parse()

	err := prechecks()
	if err != nil {
		fmt.Println("incorrect arguments passed:", err)
		return
	}
	ctx := context.Background()
	adminClient, _ := database.NewDatabaseAdminClient(ctx)
	op, err := adminClient.UpdateDatabaseDdl(ctx, &adminpb.UpdateDatabaseDdlRequest{
		Database:   fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectId, instanceId, dbName),
		Statements: []string{fmt.Sprintf("CREATE CHANGE STREAM %s FOR ALL OPTIONS (value_capture_type = 'NEW_ROW')", changeStreamName)},
	})
	if err != nil {
		fmt.Printf("Cannot submit request create change stream request: %v\n", err)
		return
	}
	if err := op.Wait(ctx); err != nil {
		if !strings.Contains(err.Error(), "Duplicate name in schema") {
			fmt.Printf("Could not update database ddl: %v\n", err)
			return
		} else {
			fmt.Printf("changestream with name '%s' already exists...skipping changestream creation\n", changeStreamName)
		}
	} else {
		fmt.Println("Created changestream ", changeStreamName)
	}

	gcsclient, _ := storage.NewClient(ctx)
	u, _ := url.Parse(sourceShardsFilePath)
	rc, _ := gcsclient.Bucket(u.Host).Object(u.Path[1:]).NewReader(ctx)
	bArr, _ := ioutil.ReadAll(rc)
	rc.Close()
	var data []interface{}
	json.Unmarshal(bArr, &data)
	arr := []string{}
	for i := 0; i < len(data); i++ {
		arr = append(arr, data[i].(map[string]interface{})["logicalShardId"].(string))
	}

	topicName := strings.Split(pubSubDataTopicId, "/")[3]
	client, err := pubsub.NewClient(ctx, strings.Split(pubSubDataTopicId, "/")[1])
	if err != nil {
		fmt.Println(err)
	}
	defer client.Close()
	_, err = client.CreateTopic(ctx, topicName)
	if err != nil {
		if !(strings.Contains(err.Error(), "AlreadyExists")) {
			fmt.Printf("could not create topic: %v\n", err)
			return
		} else {
			fmt.Printf("topic '%s' already exists, skipping creation...\n", topicName)
		}
	} else {
		fmt.Println("Created topic ", pubSubDataTopicId)
	}

	wg := &sync.WaitGroup{}
	for i := 0; i < len(arr); i++ {
		wg.Add(1)
		go func(shardId string) {
			defer wg.Done()
			_, err := client.CreateSubscription(ctx, shardId, pubsub.SubscriptionConfig{
				Topic:                 client.Topic(topicName),
				AckDeadline:           600 * time.Second,
				EnableMessageOrdering: true,
				Filter:                fmt.Sprintf("attributes.shardId=\"%s\"", shardId),
			})
			if err != nil {
				if !(strings.Contains(err.Error(), "AlreadyExists")) {
					fmt.Printf("could not create subscription: %v\n", err)
				} else {
					fmt.Printf("subscription '%s' already exists, skipping creation...\n", shardId)
				}
				return
			}
			fmt.Println("Created sub: ", shardId)
		}(arr[i])
	}
	wg.Wait()

	c, err := dataflow.NewFlexTemplatesClient(ctx)
	if err != nil {
		fmt.Printf("could not create flex template client: %v\n", err)
		return
	}
	defer c.Close()

	launchParameters := &dataflowpb.LaunchFlexTemplateParameter{
		JobName:  fmt.Sprintf("%s-ordering", jobNamePrefix),
		Template: &dataflowpb.LaunchFlexTemplateParameter_ContainerSpecGcsPath{ContainerSpecGcsPath: ORDERING_TEMPLATE},
		Parameters: map[string]string{
			"changeStreamName":   changeStreamName,
			"instanceId":         instanceId,
			"databaseId":         dbName,
			"spannerProjectId":   projectId,
			"metadataInstance":   metadataInstance,
			"metadataDatabase":   metadataDatabase,
			"startTimestamp":     "",
			"incrementInterval":  "10",
			"sinkType":           "pubsub",
			"pubSubDataTopicId":  pubSubDataTopicId,
			"pubSubErrorTopicId": pubSubDataTopicId,
			"pubSubEndpoint":     pubSubEndpoint,
			"sessionFilePath":    sessionFilePath,
		},
		Environment: &dataflowpb.FlexTemplateRuntimeEnvironment{
			NumWorkers:            5,
			AdditionalExperiments: []string{"use_runner_v2"},
			MachineType:           "n2-standard-4",
		},
	}

	req := &dataflowpb.LaunchFlexTemplateRequest{
		ProjectId:       projectId,
		LaunchParameter: launchParameters,
		Location:        dataflowRegion,
	}
	fmt.Printf("\nGCLOUD CMD FOR ORDERING JOB:\n%s\n\n", getGcloudCommand(req, ORDERING_TEMPLATE))

	_, err = c.LaunchFlexTemplate(ctx, req)
	if err != nil {
		fmt.Printf("unable to launch ordering job: %v \n REQUEST BODY: %+v\n", err, req)
		return
	}
	fmt.Println("Launched ordering job: ", fmt.Sprintf("%s-ordering", jobNamePrefix))

	launchParameters = &dataflowpb.LaunchFlexTemplateParameter{
		JobName:  fmt.Sprintf("%s-writer", jobNamePrefix),
		Template: &dataflowpb.LaunchFlexTemplateParameter_ContainerSpecGcsPath{ContainerSpecGcsPath: WRITER_TEMPLATE},
		Parameters: map[string]string{
			"sourceShardsFilePath": sourceShardsFilePath,
			"sessionFilePath":      sessionFilePath,
			"bufferType":           "pubsub",
			"pubSubProjectId":      projectId,
		},
		Environment: &dataflowpb.FlexTemplateRuntimeEnvironment{
			NumWorkers: 5,
			//	AdditionalExperiments: []string{"use_runner_v2"},
			MachineType: "n2-standard-4",
		},
	}
	req = &dataflowpb.LaunchFlexTemplateRequest{
		ProjectId:       projectId,
		LaunchParameter: launchParameters,
		Location:        dataflowRegion,
	}
	fmt.Printf("\nGCLOUD CMD FOR WRITER JOB:\n%s\n\n", getGcloudCommand(req, WRITER_TEMPLATE))

	_, err = c.LaunchFlexTemplate(ctx, req)
	if err != nil {
		fmt.Printf("unable to launch writer job: %v \n REQUEST BODY: %+v\n", err, req)
		return
	}
	fmt.Println("Launched writer job: ", fmt.Sprintf("%s-writer", jobNamePrefix))

}

func getGcloudCommand(req *dataflowpb.LaunchFlexTemplateRequest, templatePath string) string {
	lp := req.LaunchParameter
	params := ""
	for k, v := range lp.Parameters {
		params = params + k + "=" + v + ","
	}
	params = strings.TrimSuffix(params, ",")
	cmd := fmt.Sprintf("gcloud beta dataflow flex-template run %s --project=%s --region=%s --template-file-gcs-location=%s --parameters %s --num-workers=%d --worker-machine-type=%s",
		lp.JobName, req.ProjectId, req.Location, templatePath, params, lp.Environment.NumWorkers, lp.Environment.MachineType)
	if lp.Environment.AdditionalExperiments != nil {
		exps := lp.Environment.AdditionalExperiments
		experiments := strings.Join(exps[:], ",")
		cmd += " --additional-experiments=" + experiments
	}
	return cmd
}

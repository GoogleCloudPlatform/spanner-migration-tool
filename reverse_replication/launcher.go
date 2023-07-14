package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/url"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/spanner"

	dataflow "cloud.google.com/go/dataflow/apiv1beta3"

	"cloud.google.com/go/dataflow/apiv1beta3/dataflowpb"
	"cloud.google.com/go/pubsub"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
	adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
)

/*
	TODO: Add modes for running such as:
		Launch only the Dataflow jobs and skip the ChangeStream and PubSub
		Launch only the Ordering Dataflow job
		Launch only the Writer Dataflow job etc.
*/

var (
	projectId            string
	dataflowRegion       string
	jobNamePrefix        string
	changeStreamName     string
	instanceId           string
	dbName               string
	metadataInstance     string
	metadataDatabase     string
	startTimestamp       string
	pubSubDataTopicId    string
	pubSubEndpoint       string
	sourceShardsFilePath string
	sessionFilePath      string
	machineType          string
	orderingWorkers      int
	writerWorkers        int
)

const (
	ALREADY_EXISTS_ERROR = "code = AlreadyExists"
)

func setupGlobalFlags() {
	flag.StringVar(&projectId, "projectId", "", "projectId")
	flag.StringVar(&dataflowRegion, "dataflowRegion", "", "region for dataflow jobs")
	flag.StringVar(&jobNamePrefix, "jobNamePrefix", "reverse-rep", "job name prefix for the dataflow jobs, defaults to reverse-rep")
	flag.StringVar(&changeStreamName, "changeStreamName", "reverseReplicationStream", "change stream name, defaults to reverseReplicationStream")
	flag.StringVar(&instanceId, "instanceId", "", "spanner instance id")
	flag.StringVar(&dbName, "dbName", "", "spanner database name")
	flag.StringVar(&metadataInstance, "metadataInstance", "", "spanner instance name to store changestream metadata, defaults to target Spanner instance")
	flag.StringVar(&metadataDatabase, "metadataDatabase", "change-stream-metadata", "spanner database name to store changestream metadata, defaults to change-stream-metadata")
	flag.StringVar(&startTimestamp, "startTimestamp", "", "timestamp from which the changestream should start reading changes in RFC 3339 format, defaults to empty string which is equivalent to the current timestamp.")
	flag.StringVar(&pubSubDataTopicId, "pubSubDataTopicId", "", "id pub/sub data topic, pre-existing topics should use the same project as Spanner.")
	flag.StringVar(&pubSubEndpoint, "pubSubEndpoint", "", "pub/sub endpoint, defaults to same endpoint as the dataflow region.")
	flag.StringVar(&sourceShardsFilePath, "sourceShardsFilePath", "", "gcs file path for file containing shard info")
	flag.StringVar(&sessionFilePath, "sessionFilePath", "", "gcs file path for session file generated via Spanner migration tool")
	flag.StringVar(&machineType, "machineType", "n2-standard-4", "dataflow worker machine type, defaults to n2-standard-4")
	flag.IntVar(&orderingWorkers, "orderingWorkers", 5, "number of workers for ordering job")
	flag.IntVar(&writerWorkers, "writerWorkers", 5, "number of workers for writer job")
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
		metadataInstance = instanceId
		fmt.Println("metadataInstance not provided, defaulting to target spanner instance id: ", metadataInstance)
	}
	if metadataDatabase == "" {
		metadataDatabase = "change-stream-metadata"
		fmt.Println("metadataDatabase not provided, defaulting to: ", metadataDatabase)
	}
	if pubSubDataTopicId == "" {
		pubSubDataTopicId = "reverse-replication"
		fmt.Println("pubSubDataTopicId not provided, defaulting to ", pubSubDataTopicId)
	}
	if sourceShardsFilePath == "" {
		return fmt.Errorf("please specify a valid sourceShardsFilePath")
	}
	if sessionFilePath == "" {
		return fmt.Errorf("please specify a valid sessionFilePath")
	}
	if machineType == "" {
		machineType = "n2-standard-4"
		fmt.Println("machineType not provided, defaulting to: ", machineType)
	}
	if pubSubEndpoint == "" {
		pubSubEndpoint = fmt.Sprintf("%s-pubsub.googleapis.com:443", dataflowRegion)
	}
	return nil
}

func main() {
	fmt.Println("Setting up reverse replication pipeline...")
	ORDERING_TEMPLATE := "gs://dataflow-templates-southamerica-west1/2023-07-04-00_RC00/flex/Spanner_Change_Streams_to_Sink"
	WRITER_TEMPLATE := "gs://dataflow-templates-southamerica-west1/2023-07-04-00_RC00/flex/Ordered_Changestream_Buffer_to_Sourcedb"

	setupGlobalFlags()
	flag.Parse()

	err := prechecks()
	if err != nil {
		fmt.Println("incorrect arguments passed:", err)
		return
	}

	dbUri := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectId, instanceId, dbName)

	ctx := context.Background()
	adminClient, _ := database.NewDatabaseAdminClient(ctx)
	spClient, err := spanner.NewClient(ctx, dbUri)

	err = validateOrCreateChangeStream(ctx, adminClient, spClient, dbUri)
	if err != nil {
		fmt.Println("Error in validating/creating changestream:", err)
		return
	}
	createDbReq := &adminpb.CreateDatabaseRequest{
		Parent:          fmt.Sprintf("projects/%s/instances/%s", projectId, metadataInstance),
		CreateStatement: fmt.Sprintf("CREATE DATABASE `%s`", metadataDatabase),
	}

	createDbOp, err := adminClient.CreateDatabase(ctx, createDbReq)
	if err != nil {
		if !strings.Contains(err.Error(), ALREADY_EXISTS_ERROR) {
			fmt.Printf("Cannot submit create database request for metadata db: %v\n", err)
			return
		} else {
			fmt.Printf("metadata db %s already exists...skipping creation\n", fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectId, metadataInstance, metadataDatabase))
		}
	} else {
		if _, err := createDbOp.Wait(ctx); err != nil {
			if !strings.Contains(err.Error(), ALREADY_EXISTS_ERROR) {
				fmt.Printf("create database request failed for metadata db: %v\n", err)
				return
			} else {
				fmt.Printf("metadata db %s already exists...skipping creation\n", fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectId, metadataInstance, metadataDatabase))
			}
		} else {
			fmt.Println("Created metadata db", fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectId, metadataInstance, metadataDatabase))
		}
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

	pubSubDataTopicUri := fmt.Sprintf("projects/%s/topics/%s", projectId, pubSubDataTopicId)
	topicName := pubSubDataTopicId
	client, err := pubsub.NewClient(ctx, projectId)
	if err != nil {
		fmt.Println(err)
	}
	defer client.Close()
	_, err = client.CreateTopic(ctx, topicName)
	if err != nil {
		if !(strings.Contains(err.Error(), ALREADY_EXISTS_ERROR)) {
			fmt.Printf("could not create topic: %v\n", err)
			return
		} else {
			fmt.Printf("topic '%s' already exists, skipping creation...\n", topicName)
		}
	} else {
		fmt.Println("Created topic ", pubSubDataTopicUri)
	}
	subError := false
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
				if !(strings.Contains(err.Error(), ALREADY_EXISTS_ERROR)) {
					fmt.Printf("could not create subscription: %v\n", err)
					subError = true
					return
				} else {
					err := verifySubscription(ctx, client, shardId)
					if err != nil {
						fmt.Printf("subscription '%s' already exists, but is configured incorrectly: %v\n", shardId, err)
						subError = true
						return
					}
					fmt.Printf("subscription '%s' already exists, skipping creation\n", shardId)
				}
				return
			}
			fmt.Println("Created Pub/Sub subscription: ", shardId)
		}(arr[i])
	}
	wg.Wait()
	if subError {
		fmt.Printf("error in creating/validating subscriptions\n")
		return
	}

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
			"startTimestamp":     startTimestamp,
			"incrementInterval":  "10",
			"sinkType":           "pubsub",
			"pubSubDataTopicId":  pubSubDataTopicUri,
			"pubSubErrorTopicId": pubSubDataTopicUri,
			"pubSubEndpoint":     pubSubEndpoint,
			"sessionFilePath":    sessionFilePath,
		},
		Environment: &dataflowpb.FlexTemplateRuntimeEnvironment{
			NumWorkers:            int32(orderingWorkers),
			AdditionalExperiments: []string{"use_runner_v2"},
			MachineType:           machineType,
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
			NumWorkers:            int32(writerWorkers),
			AdditionalExperiments: []string{"use_runner_v2"},
			MachineType:           machineType,
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

func verifySubscription(ctx context.Context, client *pubsub.Client, subName string) error {
	subscription := client.Subscription(subName)
	subCfg, err := subscription.Config(ctx)
	if err != nil {
		return fmt.Errorf("cannot fetch config for subscription %s", subName)
	}
	topicUri := subCfg.Topic.String()
	if topicUri != fmt.Sprintf("projects/%s/topics/%s", projectId, pubSubDataTopicId) {
		return fmt.Errorf("pubSubDataTopicId provided was %s, but existing subscription %s receives from %s. Please change pubSubDataTopicId or delete the subscription", pubSubDataTopicId, subName, topicUri)
	}
	if !subCfg.EnableMessageOrdering {
		return fmt.Errorf("existing subscription %s has EnableMessageOrdering set to false. Pleasse update or delete the subscription", subName)
	}
	if !(strings.Contains(subCfg.Filter, fmt.Sprintf("attributes.shardId=\"%s\"", subName))) {
		return fmt.Errorf("existing subscription %s does not have the correct filter. Please delete the subscription", subName)
	}
	return nil
}

func validateOrCreateChangeStream(ctx context.Context, adminClient *database.DatabaseAdminClient, spClient *spanner.Client, dbUri string) error {
	q := `SELECT * FROM information_schema.change_streams`
	stmt := spanner.Statement{
		SQL: q,
	}
	iter := spClient.Single().Query(ctx, stmt)
	defer iter.Stop()
	var cs_catalog, cs_schema, cs_name string
	var coversAll bool
	csExists := false
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("couldn't read row from change_streams table: %w", err)
		}
		err = row.Columns(&cs_catalog, &cs_schema, &cs_name, &coversAll)
		if err != nil {
			return fmt.Errorf("can't scan row from change_streams table: %v", err)
		}
		if cs_name == changeStreamName {
			csExists = true
			fmt.Printf("Found changestream %s\n", changeStreamName)
			break
		}
	}
	if !csExists {
		fmt.Printf("changestream %s not found\n", changeStreamName)
		err := createChangeStream(ctx, adminClient, dbUri)
		if err != nil {
			return fmt.Errorf("could not create changestream: %v", err)
		}
		return nil
	}
	q = `SELECT option_value FROM information_schema.change_stream_options WHERE change_stream_name = @p1 AND option_name = 'value_capture_type'`
	stmt = spanner.Statement{
		SQL: q,
		Params: map[string]interface{}{
			"p1": changeStreamName,
		},
	}
	iter = spClient.Single().Query(ctx, stmt)
	defer iter.Stop()
	var option_value string
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("couldn't read row from change_stream_options table: %w", err)
		}
		err = row.Columns(&option_value)
		if err != nil {
			return fmt.Errorf("can't scan row from change_stream_options table: %v", err)
		}
		if option_value != "NEW_ROW" {
			return fmt.Errorf("VALUE_CAPTURE_TYPE for changestream %s is not NEW_ROW. Please update the changestream option or create a new one", changeStreamName)
		}
	}
	if !coversAll {
		fmt.Printf("\nWARNING: watching definition for the existing changestream %s is not 'ALL'."+
			" This means only specific tables and columns are tracked."+
			" Only the tables and columns watched by this changestream will get reverse replicated.\n\n", changeStreamName)
	}
	fmt.Println("Skipping changestream creation ...")
	return nil
}

func createChangeStream(ctx context.Context, adminClient *database.DatabaseAdminClient, dbUri string) error {
	fmt.Println("Creating changestream")
	op, err := adminClient.UpdateDatabaseDdl(ctx, &adminpb.UpdateDatabaseDdlRequest{
		Database: dbUri,
		// TODO: create change stream for only the tables present in Spanner.
		Statements: []string{fmt.Sprintf("CREATE CHANGE STREAM %s FOR ALL OPTIONS (value_capture_type = 'NEW_ROW')", changeStreamName)},
	})
	if err != nil {
		return fmt.Errorf("Cannot submit request create change stream request: %v\n", err)
	}
	if err := op.Wait(ctx); err != nil {
		return fmt.Errorf("Could not update database ddl: %v\n", err)
	} else {
		fmt.Println("Successfully created changestream", changeStreamName)
	}
	return nil
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

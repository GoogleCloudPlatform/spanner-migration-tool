package main

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/utils"

	dataflow "cloud.google.com/go/dataflow/apiv1beta3"
	"cloud.google.com/go/storage"

	"cloud.google.com/go/dataflow/apiv1beta3/dataflowpb"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"google.golang.org/api/iterator"
	adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
)

var (
	projectId                      string
	dataflowRegion                 string
	jobNamePrefix                  string
	changeStreamName               string
	instanceId                     string
	dbName                         string
	metadataInstance               string
	metadataDatabase               string
	startTimestamp                 string
	sourceShardsFilePath           string
	sessionFilePath                string
	machineType                    string
	vpcNetwork                     string
	vpcSubnetwork                  string
	vpcHostProjectId               string
	serviceAccountEmail            string
	readerWorkers                  int
	writerWorkers                  int
	windowDuration                 string
	gcsPath                        string
	filtrationMode                 string
	metadataTableSuffix            string
	sourceDbTimezoneOffset         string
	writerRunMode                  string
	readerRunMode                  string
	readerShardingCustomJarPath    string
	readerShardingCustomClassName  string
	readerShardingCustomParameters string
	readerSkipDirectoryName        string
	spannerReaderTemplateLocation  string
	sourceWriterTemplateLocation   string
	jobsToLaunch                   string
	skipChangeStreamCreation       bool
	skipMetadataDatabaseCreation   bool
	networkTags                    string
	runIdentifier                  string
	readerMaxWorkers               int
)

const (
	ALREADY_EXISTS_ERROR = "code = AlreadyExists"
)

func setupGlobalFlags() {
	flag.StringVar(&projectId, "projectId", "", "ProjectId.")
	flag.StringVar(&dataflowRegion, "dataflowRegion", "", "Region for dataflow jobs.")
	flag.StringVar(&jobNamePrefix, "jobNamePrefix", "smt-reverse-replication", "Job name prefix for the dataflow jobs, defaults to reverse-rep. Automatically converted to lower case due to Dataflow name constraints.")
	flag.StringVar(&changeStreamName, "changeStreamName", "reverseReplicationStream", "Change stream name, defaults to reverseReplicationStream.")
	flag.StringVar(&instanceId, "instanceId", "", "Spanner instance id.")
	flag.StringVar(&dbName, "dbName", "", "Spanner database name.")
	flag.StringVar(&metadataInstance, "metadataInstance", "", "Spanner instance name to store changestream metadata, defaults to target Spanner instance.")
	flag.StringVar(&metadataDatabase, "metadataDatabase", "rev_repl_metadata", "spanner database name to store changestream metadata, defaults to change-stream-metadata.")
	flag.StringVar(&startTimestamp, "startTimestamp", "", "Timestamp from which the changestream should start reading changes in RFC 3339 format, defaults to empty string which is equivalent to the current timestamp.")
	flag.StringVar(&windowDuration, "windowDuration", "10s", "The window duration/size in which change stream data will be written to Cloud Storage. Defaults to 10 seconds.")
	flag.StringVar(&gcsPath, "gcsPath", "", "A pre-created GCS directory where the change stream data resides.")
	flag.StringVar(&filtrationMode, "filtrationMode", "forward_migration", "The flag to decide whether or not to filter the forward migrated data.Defaults to forward_migration.")
	flag.StringVar(&metadataTableSuffix, "metadataTableSuffix", "", "The suffix to apply when creating metadata tables.Helpful in case of multiple runs.Default is no suffix.")
	flag.StringVar(&readerSkipDirectoryName, "readerSkipDirectoryName", "skip", "Records skipped from reverse replication are written to this directory. Defaults to: skip.")
	flag.StringVar(&sourceShardsFilePath, "sourceShardsFilePath", "", "Gcs file path for file containing shard info.")
	flag.StringVar(&sessionFilePath, "sessionFilePath", "", "Gcs file path for session file generated via Spanner migration tool.")
	flag.StringVar(&sourceDbTimezoneOffset, "sourceDbTimezoneOffset", "+00:00", "The timezone offset with respect to UTC for the source database.Defaults to +00:00.")
	flag.StringVar(&readerRunMode, "readerRunMode", "regular", "Whether the reader from Spanner job runs in regular or resume mode. Default is regular.")
	flag.StringVar(&writerRunMode, "writerRunMode", "regular", "Whether the writer to source job runs in regular,reprocess,resumeFailed,resumeSuccess or resumeAll mode. Default is regular.")
	flag.StringVar(&machineType, "machineType", "n2-standard-4", "Dataflow worker machine type, defaults to n2-standard-4.")
	flag.StringVar(&vpcNetwork, "vpcNetwork", "", "Name of the VPC network to be used for the dataflow jobs.")
	flag.StringVar(&vpcSubnetwork, "vpcSubnetwork", "", "Name of the VPC subnetwork to be used for the dataflow jobs. Subnet should exist in the same region as the 'dataflowRegion' parameter.")
	flag.StringVar(&vpcHostProjectId, "vpcHostProjectId", "", "Project ID hosting the subnetwork. If unspecified, the 'projectId' parameter value will be used for subnetwork.")
	flag.StringVar(&serviceAccountEmail, "serviceAccountEmail", "", "The email address of the service account to run the job as.")
	flag.IntVar(&readerWorkers, "readerWorkers", 5, "Number of workers for reader job.")
	flag.IntVar(&writerWorkers, "writerWorkers", 5, "Number of workers for writer job.")
	flag.StringVar(&spannerReaderTemplateLocation, "spannerReaderTemplateLocation", "gs://dataflow-templates-us-east7/2024-03-06-00_RC00/flex/Spanner_Change_Streams_to_Sharded_File_Sink", "The dataflow template location for the Spanner reader job.")
	flag.StringVar(&sourceWriterTemplateLocation, "sourceWriterTemplateLocation", "gs://dataflow-templates-us-east7/2024-03-06-00_RC00/flex/GCS_to_Sourcedb", "The dataflow template location for the Source writer job.")
	flag.StringVar(&jobsToLaunch, "jobsToLaunch", "both", "Whether to launch the spanner reader job or the source writer job or both. Default is both. Support values are both,reader,writer.")
	flag.BoolVar(&skipChangeStreamCreation, "skipChangeStreamCreation", false, "Whether to skip the change stream creation. Default is false.")
	flag.BoolVar(&skipMetadataDatabaseCreation, "skipMetadataDatabaseCreation", false, "Whether to skip Metadata database creation.Default is false.")
	flag.StringVar(&networkTags, "networkTags", "", "Network tags addded to the Dataflow jobs worker and launcher VMs.")
	flag.StringVar(&readerShardingCustomClassName, "readerShardingCustomClassName", "", "The fully qualified custom class name for sharding logic.")
	flag.StringVar(&readerShardingCustomJarPath, "readerShardingCustomJarPath", "", "The GCS path to custom jar for sharding logic.")
	flag.StringVar(&runIdentifier, "runIdentifier", "", "The run identifier for the Dataflow jobs.")
	flag.StringVar(&readerShardingCustomParameters, "readerShardingCustomParameters", "", "Any custom parameters to be supplied to custom sharding class.")
	flag.IntVar(&readerMaxWorkers, "readerMaxWorkers", 20, "Number of max workers for reader job.")

}

func prechecks() error {
	if projectId == "" {
		return fmt.Errorf("please specify a valid projectId")
	}
	if dataflowRegion == "" {
		return fmt.Errorf("please specify a valid dataflowRegion")
	}
	if jobNamePrefix == "" {
		return fmt.Errorf("please specify a non-empty jobNamePrefix")
	} else {
		// Capital letters not allowed in Dataflow job names.
		jobNamePrefix = strings.ToLower(jobNamePrefix)
	}
	if gcsPath == "" {
		return fmt.Errorf("please specify a non-empty gcsPath")
	} else if !strings.HasPrefix(gcsPath, "gs://") {
		return fmt.Errorf("please specify a valid GCS path for gcsPath, like gs://<>")
	}
	if changeStreamName == "" {
		return fmt.Errorf("please specify a valid changeStreamName")
	} else {
		changeStreamName = strings.ReplaceAll(changeStreamName, "-", "_")
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
		metadataDatabase = "rev_repl_metadata"
		fmt.Println("metadataDatabase not provided, defaulting to: ", metadataDatabase)
	}

	if sourceShardsFilePath == "" {
		return fmt.Errorf("please specify a valid sourceShardsFilePath")
	} else if !strings.HasPrefix(sourceShardsFilePath, "gs://") {
		return fmt.Errorf("please specify a valid GCS path for sourceShardsFilePath, like gs://<>")
	}

	if sessionFilePath == "" {
		return fmt.Errorf("please specify a valid sessionFilePath")
	} else if !strings.HasPrefix(sessionFilePath, "gs://") {
		return fmt.Errorf("please specify a valid GCS path for sessionFilePath, like gs://<>")
	}

	if machineType == "" {
		machineType = "n2-standard-4"
		fmt.Println("machineType not provided, defaulting to: ", machineType)
	}

	if vpcHostProjectId == "" {
		vpcHostProjectId = projectId
	}

	if readerShardingCustomJarPath != "" && readerShardingCustomClassName == "" {
		return fmt.Errorf("When supplying readerShardingCustomJarPath value, the readerShardingCustomClassName should also be supplied ")
	}

	if readerShardingCustomClassName != "" && readerShardingCustomJarPath == "" {
		return fmt.Errorf("When supplying readerShardingCustomClassName value, the readerShardingCustomJarPath should also be supplied ")
	}

	if readerShardingCustomJarPath != "" && !strings.HasPrefix(readerShardingCustomJarPath, "gs://") {
		return fmt.Errorf("please specify a valid GCS path for readerShardingCustomJarPath, like gs://<>")
	}

	return nil
}

func main() {
	fmt.Println("Setting up reverse replication pipeline...")

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

	client, err := storage.NewClient(ctx)
	if err != nil {
		fmt.Println("failed to create GCS client")
		return
	}
	defer client.Close()
	gcsBucketPath := strings.ReplaceAll(gcsPath, "gs://", "")
	splitPaths := strings.Split(gcsBucketPath, "/")
	gcsBucketName := splitPaths[0]
	bucket := client.Bucket(gcsBucketName)
	_, err = bucket.Attrs(ctx)
	if err != nil {
		fmt.Println("GCS Path does not exist, please create before running reverse replication:", gcsBucketName)
		return
	}

	if !skipChangeStreamCreation {

		err = validateOrCreateChangeStream(ctx, adminClient, spClient, dbUri)
		if err != nil {
			fmt.Println("Error in validating/creating changestream:", err)
			return
		}
	}

	if !skipMetadataDatabaseCreation {
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
	}

	c, err := dataflow.NewFlexTemplatesClient(ctx)
	if err != nil {
		fmt.Printf("could not create flex template client: %v\n", err)
		return
	}
	defer c.Close()

	// If custom network is not selected, use public IP. Typical for internal testing flow.
	workerIpAddressConfig := dataflowpb.WorkerIPAddressConfiguration_WORKER_IP_PUBLIC
	if vpcNetwork != "" || vpcSubnetwork != "" {
		workerIpAddressConfig = dataflowpb.WorkerIPAddressConfiguration_WORKER_IP_PRIVATE
		// If subnetwork is not provided, assume network has auto subnet configuration.
		if vpcSubnetwork != "" {
			vpcSubnetwork = fmt.Sprintf("https://www.googleapis.com/compute/v1/projects/%s/regions/%s/subnetworks/%s", vpcHostProjectId, dataflowRegion, vpcSubnetwork)
		}
	}

	runId := ""
	if runIdentifier != "" {
		runId = runIdentifier
	} else {
		runId = time.Now().UTC().Format(time.RFC3339)
		runId = strings.ReplaceAll(runId, ":", "-")
		runId = strings.ToLower(runId)
	}

	if jobsToLaunch == "both" || jobsToLaunch == "reader" {
		var additionalExpr []string

		if networkTags == "" {
			additionalExpr = []string{"use_runner_v2"}
		} else {
			additionalExpr = []string{"use_runner_v2", "use_network_tags=" + networkTags, "use_network_tags_for_flex_templates=" + networkTags}
		}

		readerParams := map[string]string{
			"changeStreamName":     changeStreamName,
			"instanceId":           instanceId,
			"databaseId":           dbName,
			"spannerProjectId":     projectId,
			"metadataInstance":     metadataInstance,
			"metadataDatabase":     metadataDatabase,
			"startTimestamp":       startTimestamp,
			"sessionFilePath":      sessionFilePath,
			"windowDuration":       windowDuration,
			"gcsOutputDirectory":   gcsPath,
			"filtrationMode":       filtrationMode,
			"sourceShardsFilePath": sourceShardsFilePath,
			"metadataTableSuffix":  metadataTableSuffix,
			"skipDirectoryName":    readerSkipDirectoryName,
			"runIdentifier":        runId,
			"runMode":              readerRunMode,
		}
		if readerShardingCustomJarPath != "" {
			readerParams["shardingCustomJarPath"] = readerShardingCustomJarPath //cant send empty since it expects GCS format
			readerParams["shardingCustomClassName"] = readerShardingCustomClassName
			readerParams["shardingCustomParameters"] = readerShardingCustomParameters
		}
		launchParameters := &dataflowpb.LaunchFlexTemplateParameter{
			JobName:    fmt.Sprintf("%s-reader-%s-%s", jobNamePrefix, runId, utils.GenerateHashStr()),
			Template:   &dataflowpb.LaunchFlexTemplateParameter_ContainerSpecGcsPath{ContainerSpecGcsPath: spannerReaderTemplateLocation},
			Parameters: readerParams,
			Environment: &dataflowpb.FlexTemplateRuntimeEnvironment{
				NumWorkers:            int32(readerWorkers),
				AdditionalExperiments: additionalExpr,
				MachineType:           machineType,
				Network:               vpcNetwork,
				Subnetwork:            vpcSubnetwork,
				IpConfiguration:       workerIpAddressConfig,
				ServiceAccountEmail:   serviceAccountEmail,
				MaxWorkers:            int32(readerMaxWorkers),
			},
		}

		req := &dataflowpb.LaunchFlexTemplateRequest{
			ProjectId:       projectId,
			LaunchParameter: launchParameters,
			Location:        dataflowRegion,
		}
		fmt.Printf("\nGCLOUD CMD FOR READER JOB:\n%s\n\n", getGcloudCommand(req))

		readerJobResponse, err := c.LaunchFlexTemplate(ctx, req)
		if err != nil {
			fmt.Printf("unable to launch reader job: %v \n REQUEST BODY: %+v\n", err, req)
			return
		}
		fmt.Println("Launched reader job: ", readerJobResponse.Job)
	}

	if jobsToLaunch == "both" || jobsToLaunch == "writer" {

		var additionalExpr []string

		if networkTags != "" {

			additionalExpr = []string{"use_network_tags=" + networkTags, "use_network_tags_for_flex_templates=" + networkTags}
		}

		launchParameters := &dataflowpb.LaunchFlexTemplateParameter{
			JobName:  fmt.Sprintf("%s-writer-%s-%s", jobNamePrefix, runId, utils.GenerateHashStr()),
			Template: &dataflowpb.LaunchFlexTemplateParameter_ContainerSpecGcsPath{ContainerSpecGcsPath: sourceWriterTemplateLocation},
			Parameters: map[string]string{
				"sourceShardsFilePath":   sourceShardsFilePath,
				"sessionFilePath":        sessionFilePath,
				"sourceDbTimezoneOffset": sourceDbTimezoneOffset,
				"metadataTableSuffix":    metadataTableSuffix,
				"GCSInputDirectoryPath":  gcsPath,
				"spannerProjectId":       projectId,
				"metadataInstance":       metadataInstance,
				"metadataDatabase":       metadataDatabase,
				"runMode":                writerRunMode,
				"runIdentifier":          runId,
			},
			Environment: &dataflowpb.FlexTemplateRuntimeEnvironment{
				NumWorkers:            int32(writerWorkers),
				AdditionalExperiments: additionalExpr,
				MachineType:           machineType,
				Network:               vpcNetwork,
				Subnetwork:            vpcSubnetwork,
				IpConfiguration:       workerIpAddressConfig,
				ServiceAccountEmail:   serviceAccountEmail,
			},
		}
		req := &dataflowpb.LaunchFlexTemplateRequest{
			ProjectId:       projectId,
			LaunchParameter: launchParameters,
			Location:        dataflowRegion,
		}
		fmt.Printf("\nGCLOUD CMD FOR WRITER JOB:\n%s\n\n", getGcloudCommand(req))

		writerJobResponse, err := c.LaunchFlexTemplate(ctx, req)
		if err != nil {
			fmt.Printf("unable to launch writer job: %v \n REQUEST BODY: %+v\n", err, req)
			return
		}

		fmt.Println("Launched writer job: ", writerJobResponse.Job)
	}

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

func getGcloudCommand(req *dataflowpb.LaunchFlexTemplateRequest) string {
	lp := req.LaunchParameter
	params := ""
	for k, v := range lp.Parameters {
		params = params + k + "=" + v + ","
	}
	params = strings.TrimSuffix(params, ",")
	cmd := fmt.Sprintf("gcloud dataflow flex-template run %s --project=%s --region=%s --template-file-gcs-location=%s --parameters %s --num-workers=%d --worker-machine-type=%s",
		lp.JobName, req.ProjectId, req.Location, lp.GetContainerSpecGcsPath(), params, lp.Environment.NumWorkers, lp.Environment.MachineType)
	if lp.Environment.AdditionalExperiments != nil {
		exps := lp.Environment.AdditionalExperiments
		experiments := strings.Join(exps[:], ",")
		cmd += " --additional-experiments=" + experiments
	}
	return cmd
}

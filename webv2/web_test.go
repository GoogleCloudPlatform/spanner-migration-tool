package webv2

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/profiles"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/streaming"
	helpers "github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/helpers"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/session"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/types"
	"github.com/stretchr/testify/assert"
)

func TestCreateStreamingCfgFile(t *testing.T) {
	// Mock data
	sessionState := &session.SessionState{
		Region:   "us-central1",
		Bucket:   "my-bucket",
		RootPath: "/",
		Driver:   "postgres",
	}

	targetDetails := types.TargetDetails{
		SourceConnectionProfileName: "source-profile",
		TargetConnectionProfileName: "target-profile",
		ReplicationSlot:             "replication-slot",
		Publication:                 "publication",
	}

	datastreamConfig := profiles.DatastreamConfig{
		MaxConcurrentBackfillTasks: "5",
		MaxConcurrentCdcTasks:      "10",
	}

	dataflowConfig := profiles.DataflowConfig{
		ProjectId:            "project-id",
		Location:             "europe-west1",
		Network:              "network",
		Subnetwork:           "subnetwork",
		MaxWorkers:           "10",
		NumWorkers:           "5",
		ServiceAccountEmail:  "service-account-email",
		VpcHostProjectId:     "vpc-host-project-id",
		MachineType:          "machine-type",
		AdditionalUserLabels: "",
		KmsKeyName:           "kms-key-name",
		GcsTemplatePath:      "gcs-template-path",
		CustomJarPath:        "custom-jar-path",
		CustomClassName:      "custom-class-name",
		CustomParameter:      "custom-parameter",
	}

	gcsConfig := profiles.GcsConfig{
		TtlInDays:    7,
		TtlInDaysSet: true,
	}

	details := types.MigrationDetails{
		TargetDetails:    targetDetails,
		DatastreamConfig: datastreamConfig,
		DataflowConfig:   dataflowConfig,
		GcsConfig:        gcsConfig,
	}

	fileName := "test_config.json"

	// Execute function
	err := createStreamingCfgFile(sessionState, details, fileName)
	assert.NoError(t, err, "Expected no error when creating streaming config file")

	// Read and verify file
	fileContent, err := ioutil.ReadFile(fileName)
	assert.NoError(t, err, "Expected no error when reading streaming config file")

	var cfg streaming.StreamingCfg
	err = json.Unmarshal(fileContent, &cfg)
	assert.NoError(t, err, "Expected no error when unmarshalling streaming config file")

	// Expected data
	expectedCfg := streaming.StreamingCfg{
		DatastreamCfg: streaming.DatastreamCfg{
			StreamId:          "",
			StreamLocation:    sessionState.Region,
			StreamDisplayName: "",
			SourceConnectionConfig: streaming.SrcConnCfg{
				Name:     targetDetails.SourceConnectionProfileName,
				Location: sessionState.Region,
			},
			DestinationConnectionConfig: streaming.DstConnCfg{
				Name:     targetDetails.TargetConnectionProfileName,
				Location: sessionState.Region,
			},
			MaxConcurrentBackfillTasks: datastreamConfig.MaxConcurrentBackfillTasks,
			MaxConcurrentCdcTasks:      datastreamConfig.MaxConcurrentCdcTasks,
		},
		GcsCfg: streaming.GcsCfg{
			TtlInDays:    gcsConfig.TtlInDays,
			TtlInDaysSet: gcsConfig.TtlInDaysSet,
		},
		DataflowCfg: streaming.DataflowCfg{
			ProjectId:            dataflowConfig.ProjectId,
			JobName:              "",
			Location:             dataflowConfig.Location,
			Network:              dataflowConfig.Network,
			Subnetwork:           dataflowConfig.Subnetwork,
			MaxWorkers:           dataflowConfig.MaxWorkers,
			NumWorkers:           dataflowConfig.NumWorkers,
			ServiceAccountEmail:  dataflowConfig.ServiceAccountEmail,
			VpcHostProjectId:     dataflowConfig.VpcHostProjectId,
			MachineType:          dataflowConfig.MachineType,
			AdditionalUserLabels: dataflowConfig.AdditionalUserLabels,
			KmsKeyName:           dataflowConfig.KmsKeyName,
			GcsTemplatePath:      dataflowConfig.GcsTemplatePath,
			CustomJarPath:        dataflowConfig.CustomJarPath,
			CustomClassName:      dataflowConfig.CustomClassName,
			CustomParameter:      dataflowConfig.CustomParameter,
		},
		TmpDir: "gs://" + sessionState.Bucket + sessionState.RootPath,
	}

	expectedDatabaseType, _ := helpers.GetSourceDatabaseFromDriver(sessionState.Driver)
	if expectedDatabaseType == constants.POSTGRES {
		expectedCfg.DatastreamCfg.Properties = fmt.Sprintf("replicationSlot=%v,publication=%v", targetDetails.ReplicationSlot, targetDetails.Publication)
	}

	assert.Equal(t, expectedCfg, cfg, "The streaming configuration should match the expected configuration")

	// Clean up
	os.Remove(fileName)
}

package webv2

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	ca "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/cassandra"
	cc "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/clients/cassandra"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
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

func TestValidateCassandraConnection(t *testing.T) {
	originalGetOrCreateClient := ca.GetOrCreateClient
	defer func() {
		ca.GetOrCreateClient = originalGetOrCreateClient
	}()

	config := types.DriverConfig{
		Host: "127.0.0.1", Port: "9042", Database: "test_keyspace", DataCenter: "dc1", User: "user", Password: "pass",
	}

	t.Run("Success", func(t *testing.T) {
		mockMetadata := new(cc.MockKeyspaceMetadata)
		mockClient := new(cc.MockCassandraCluster)
		mockClient.On("KeyspaceMetadata", "test_keyspace").Return(mockMetadata, nil).Once()
		mockClient.On("Close").Return().Once()

		ca.GetOrCreateClient = func(contactPoints []string, port int, keyspace, datacenter, user, password string) (cc.CassandraClusterInterface, error) {
			return mockClient, nil
		}

		metadata, err := validateCassandraConnection(config)
		assert.NoError(t, err)
		assert.Equal(t, mockMetadata, metadata)
		mockClient.AssertExpectations(t)
	})

	t.Run("Failure", func(t *testing.T) {
		expectedErr := errors.New("client creation failed")
		ca.GetOrCreateClient = func(contactPoints []string, port int, keyspace, datacenter, user, password string) (cc.CassandraClusterInterface, error) {
			return nil, expectedErr
		}

		metadata, err := validateCassandraConnection(config)
		assert.Nil(t, metadata)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Cassandra connection error")
		assert.ErrorIs(t, err, expectedErr)
	})
}

func TestDatabaseConnectionCassandra(t *testing.T) {
	originalGetOrCreateClient := ca.GetOrCreateClient
	defer func() {
		ca.GetOrCreateClient = originalGetOrCreateClient
	}()

	config := types.DriverConfig{
		Driver:     constants.CASSANDRA,
		Host:       "127.0.0.1",
		Port:       "9042",
		Database:   "test_keyspace",
		DataCenter: "dc1",
		User:       "user",
		Password:   "pass",
	}
	configJSON, _ := json.Marshal(config)

	t.Run("Success", func(t *testing.T) {
		mockMetadata := new(cc.MockKeyspaceMetadata)
		mockClient := new(cc.MockCassandraCluster)
		mockClient.On("KeyspaceMetadata", "test_keyspace").Return(mockMetadata, nil).Once()
		mockClient.On("Close").Return().Once()

		ca.GetOrCreateClient = func(contactPoints []string, port int, keyspace, datacenter, user, password string) (cc.CassandraClusterInterface, error) {
			return mockClient, nil
		}

		req, _ := http.NewRequest("POST", "/connect", bytes.NewBuffer(configJSON))
		rr := httptest.NewRecorder()
		handler := http.HandlerFunc(databaseConnection)
		handler.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusOK, rr.Code)

		sessionState := session.GetSessionState()
		assert.Equal(t, constants.CASSANDRA, sessionState.Driver)
		assert.Equal(t, "test_keyspace", sessionState.DbName)
		assert.Equal(t, mockMetadata, sessionState.KeyspaceMetadata)
		mockClient.AssertExpectations(t)
	})

	t.Run("Failure", func(t *testing.T) {
		expectedErr := errors.New("client creation failed")
		ca.GetOrCreateClient = func(contactPoints []string, port int, keyspace, datacenter, user, password string) (cc.CassandraClusterInterface, error) {
			return nil, expectedErr
		}

		req, _ := http.NewRequest("POST", "/connect", bytes.NewBuffer(configJSON))
		rr := httptest.NewRecorder()
		handler := http.HandlerFunc(databaseConnection)
		handler.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusInternalServerError, rr.Code)
		assert.Contains(t, rr.Body.String(), "Cassandra connection error")
	})
}

func TestSetSourceDBDetailsForDirectConnectCassandra(t *testing.T) {
	originalGetOrCreateClient := ca.GetOrCreateClient
	defer func() {
		ca.GetOrCreateClient = originalGetOrCreateClient
	}()

	config := types.DriverConfig{
		Driver:     constants.CASSANDRA,
		Host:       "127.0.0.1",
		Port:       "9042",
		Database:   "test_keyspace",
		DataCenter: "dc1",
		User:       "user",
		Password:   "pass",
	}
	configJSON, _ := json.Marshal(config)

	t.Run("Success", func(t *testing.T) {
		mockMetadata := new(cc.MockKeyspaceMetadata)
		mockClient := new(cc.MockCassandraCluster)
		mockClient.On("KeyspaceMetadata", "test_keyspace").Return(mockMetadata, nil).Once()
		mockClient.On("Close").Return().Once()

		ca.GetOrCreateClient = func(contactPoints []string, port int, keyspace, datacenter, user, password string) (cc.CassandraClusterInterface, error) {
			return mockClient, nil
		}

		req, _ := http.NewRequest("POST", "/SetSourceDBDetailsForDirectConnect", bytes.NewBuffer(configJSON))
		rr := httptest.NewRecorder()
		handler := http.HandlerFunc(setSourceDBDetailsForDirectConnect)
		handler.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusOK, rr.Code)

		sessionState := session.GetSessionState()
		assert.Equal(t, "test_keyspace", sessionState.DbName)
		assert.Equal(t, mockMetadata, sessionState.KeyspaceMetadata)
		assert.Equal(t, "127.0.0.1", sessionState.SourceDBConnDetails.Host)
		mockClient.AssertExpectations(t)
	})

	t.Run("Failure", func(t *testing.T) {
		expectedErr := errors.New("client creation failed")
		ca.GetOrCreateClient = func(contactPoints []string, port int, keyspace, datacenter, user, password string) (cc.CassandraClusterInterface, error) {
			return nil, expectedErr
		}

		req, _ := http.NewRequest("POST", "/SetSourceDBDetailsForDirectConnect", bytes.NewBuffer(configJSON))
		rr := httptest.NewRecorder()
		handler := http.HandlerFunc(setSourceDBDetailsForDirectConnect)
		handler.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusInternalServerError, rr.Code)
		assert.Contains(t, rr.Body.String(), "Cassandra connection error")
	})
}

func TestGetSourceAndTargetProfiles_Cassandra(t *testing.T) {
	sessionState := session.GetSessionState()
	sessionState.Conv = internal.MakeConv()
	sessionState.Driver = constants.CASSANDRA
	sessionState.DbName = "test_keyspace"
	sessionState.Dialect = constants.DIALECT_GOOGLESQL
	sessionState.SourceDBConnDetails = session.SourceDBConnDetails{
			Host:           "127.0.0.1",
			Port:           "9042",
			User:           "user",
			Password:       "pass",
			DataCenter:     "dc1",
			ConnectionType: helpers.DIRECT_CONNECT_MODE,
	}
	sessionState.SpannerProjectId = "test-project"
	sessionState.SpannerInstanceID = "test-instance"
	
	details := types.MigrationDetails{
		TargetDetails: types.TargetDetails{
			TargetDB: "test-spanner-db",
		},
		MigrationMode: helpers.SCHEMA_ONLY,
	}

	sourceProfile, _, _, _, err := getSourceAndTargetProfiles(sessionState, details)

	assert.NoError(t, err)
	assert.Equal(t, constants.CASSANDRA, sourceProfile.Driver)
	assert.Equal(t, "127.0.0.1", sourceProfile.Conn.Cassandra.Host)
	assert.Equal(t, "9042", sourceProfile.Conn.Cassandra.Port)
	assert.Equal(t, "user", sourceProfile.Conn.Cassandra.User)
	assert.Equal(t, "pass", sourceProfile.Conn.Cassandra.Pwd)
	assert.Equal(t, "test_keyspace", sourceProfile.Conn.Cassandra.Keyspace)
	assert.Equal(t, "dc1", sourceProfile.Conn.Cassandra.DataCenter)
}
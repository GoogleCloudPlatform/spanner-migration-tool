package profile

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"

	datastream "cloud.google.com/go/datastream/apiv1"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/utils"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/conversion"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/profiles"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/streaming"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/helpers"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/session"
	"google.golang.org/api/iterator"
	datastreampb "google.golang.org/genproto/googleapis/cloud/datastream/v1"
)

type shardedDataflowConfig struct {
	MigrationProfile profiles.SourceProfileConfig
}

func ListConnectionProfiles(w http.ResponseWriter, r *http.Request) {
	ctx := context.Background()
	dsClient, err := datastream.NewClient(ctx)
	if err != nil {
		http.Error(w, fmt.Sprintf("datastream client can not be created: %v", err), http.StatusBadRequest)
	}
	defer dsClient.Close()
	sessionState := session.GetSessionState()
	sessionState.Conv.ConvLock.Lock()
	defer sessionState.Conv.ConvLock.Unlock()
	source := r.FormValue("source") == "true"
	if !source {
		sessionState.Conv.Audit.MigrationRequestId, _ = utils.GenerateName("smt-job")
		sessionState.Conv.Audit.MigrationRequestId = strings.Replace(sessionState.Conv.Audit.MigrationRequestId, "_", "-", -1)
		sessionState.Bucket = strings.ToLower(sessionState.Conv.Audit.MigrationRequestId) + "/"
	}
	databaseType, err := helpers.GetSourceDatabaseFromDriver(sessionState.Driver)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error while getting source database: %v", err), http.StatusBadRequest)
		return
	}
	var connectionProfileList []connectionProfile
	req := &datastreampb.ListConnectionProfilesRequest{
		Parent: fmt.Sprintf("projects/%s/locations/%s", sessionState.GCPProjectID, sessionState.Region),
	}
	it := dsClient.ListConnectionProfiles(ctx, req)
	for {
		resp, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			http.Error(w, fmt.Sprintf("Error while getting list of connection profiles: %v", err), http.StatusBadRequest)
			return
		}
		if source && databaseType == constants.MYSQL && resp.GetMysqlProfile().GetHostname() != "" {
			connectionProfileList = append(connectionProfileList, connectionProfile{Name: resp.GetName(), DisplayName: resp.GetDisplayName()})
		} else if source && databaseType == constants.ORACLE && resp.GetOracleProfile().GetHostname() != "" {
			connectionProfileList = append(connectionProfileList, connectionProfile{Name: resp.GetName(), DisplayName: resp.GetDisplayName()})
		} else if source && databaseType == constants.POSTGRES && resp.GetPostgresqlProfile().GetHostname() != "" {
			connectionProfileList = append(connectionProfileList, connectionProfile{Name: resp.GetName(), DisplayName: resp.GetDisplayName()})
		} else if !source && resp.GetGcsProfile().GetBucket() != "" {
			connectionProfileList = append(connectionProfileList, connectionProfile{Name: resp.GetName(), DisplayName: resp.GetDisplayName()})
		}
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(connectionProfileList)
}

func GetStaticIps(w http.ResponseWriter, r *http.Request) {
	ctx := context.Background()
	dsClient, err := datastream.NewClient(ctx)
	if err != nil {
		http.Error(w, fmt.Sprintf("datastream client can not be created: %v", err), http.StatusBadRequest)
	}
	defer dsClient.Close()
	sessionState := session.GetSessionState()
	req := &datastreampb.FetchStaticIpsRequest{
		Name: fmt.Sprintf("projects/%s/locations/%s", sessionState.GCPProjectID, sessionState.Region),
	}
	it := dsClient.FetchStaticIps(ctx, req)
	var staticIpList []string
	for {
		resp, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			http.Error(w, fmt.Sprintf("Error while fetching static Ips: %v", err), http.StatusBadRequest)
			return
		}
		staticIpList = append(staticIpList, resp)
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(staticIpList)
}

func CreateConnectionProfile(w http.ResponseWriter, r *http.Request) {
	log.Println("request started", "method", r.Method, "path", r.URL.Path)
	reqBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Println("request's body Read Error")
		http.Error(w, fmt.Sprintf("Body Read Error : %v", err), http.StatusInternalServerError)
	}

	details := connectionProfileReqV2{}
	err = json.Unmarshal(reqBody, &details)
	if err != nil {
		log.Println("request's Body parse error")
		http.Error(w, fmt.Sprintf("Request Body parse error : %v", err), http.StatusBadRequest)
		return
	}
	ctx := context.Background()
	dsClient, err := datastream.NewClient(ctx)
	if err != nil {
		http.Error(w, fmt.Sprintf("datastream client can not be created: %v", err), http.StatusBadRequest)
	}
	defer dsClient.Close()
	sessionState := session.GetSessionState()
	sessionState.Conv.ConvLock.Lock()
	defer sessionState.Conv.ConvLock.Unlock()
	databaseType, err := helpers.GetSourceDatabaseFromDriver(sessionState.Driver)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error while getting source database: %v", err), http.StatusBadRequest)
		return
	}

	if sessionState.IsSharded {
		if databaseType != constants.MYSQL {
			http.Error(w, fmt.Sprintf("this database type is not currently implemented for sharded migrations: %v", err), http.StatusBadRequest)
			return
		}
		resGenerator := conversion.ResourceGenerationStruct{}
		req := conversion.ConnectionProfileReq{
			ConnectionProfile: conversion.ConnectionProfile{
				ProjectId: sessionState.GCPProjectID,
				Id: details.Id,
				ValidateOnly: details.ValidateOnly,
				IsSource: details.IsSource,
				Host: details.Host,
				Port: details.Port,
				Password: details.Password,
				User: details.User,
				Region: sessionState.Region,
			},
			Ctx: ctx,
		}
		mutex := &sync.Mutex{}
		result := resGenerator.PrepareMinimalDowntimeResources(&req, mutex)
		if result.Err != nil {
			http.Error(w, fmt.Sprintf("Resource generation failed: %v", err), http.StatusBadRequest)
			return
		}
		return
	}

	req := &datastreampb.CreateConnectionProfileRequest{
		Parent:              fmt.Sprintf("projects/%s/locations/%s", sessionState.GCPProjectID, sessionState.Region),
		ConnectionProfileId: details.Id,
		ConnectionProfile: &datastreampb.ConnectionProfile{
			DisplayName:  details.Id,
			Connectivity: &datastreampb.ConnectionProfile_StaticServiceIpConnectivity{},
		},
		ValidateOnly: details.ValidateOnly,
	}
	var bucketName string
	if !details.IsSource {
		bucketName = strings.ToLower(sessionState.Conv.Audit.MigrationRequestId)
		err = utils.CreateGCSBucket(bucketName, sessionState.GCPProjectID, sessionState.Region)
		if err != nil {
			http.Error(w, fmt.Sprintf("Error while creating bucket: %v", err), http.StatusBadRequest)
			return
		}
	}
	setConnectionProfileFromSessionState(details.IsSource, *sessionState, req, databaseType)

	op, err := dsClient.CreateConnectionProfile(ctx, req)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error while creating connection profile: %v", err), http.StatusBadRequest)
		return
	}
	_, err = op.Wait(ctx)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error while creating connection profile: %v", err), http.StatusBadRequest)
		return
	}
}

func VerifyJsonConfiguration(w http.ResponseWriter, r *http.Request) {
	reqBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("Body Read Error : %v", err), http.StatusInternalServerError)
		return
	}

	var srcConfig shardedDataflowConfig
	err = json.Unmarshal(reqBody, &srcConfig)
	if err != nil {
		http.Error(w, fmt.Sprintf("Body Read Error : %v", err), http.StatusInternalServerError)
		return
	}

	resGenerator := conversion.ResourceGenerationStruct{}
	ctx := context.Background()
	sessionState := session.GetSessionState()
	sourceProfileConfig := srcConfig.MigrationProfile
	sourceProfile := profiles.SourceProfile{Ty: profiles.SourceProfileTypeConfig, Config: sourceProfileConfig}
	err = resGenerator.ValidateResourceGeneration(ctx, sessionState.GCPProjectID, sessionState.SpannerInstanceID, sourceProfile, sessionState.Conv)
	if err != nil {
		http.Error(w, fmt.Sprintf("Body Read Error : %v", err), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}
func setConnectionProfileFromSessionState(isSource bool, sessionState session.SessionState, req *datastreampb.CreateConnectionProfileRequest, databaseType string) {
	if isSource {
		port, _ := strconv.ParseInt((sessionState.SourceDBConnDetails.Port), 10, 32)
		if databaseType == constants.MYSQL {
			req.ConnectionProfile.Profile = &datastreampb.ConnectionProfile_MysqlProfile{
				MysqlProfile: &datastreampb.MysqlProfile{
					Hostname: sessionState.SourceDBConnDetails.Host,
					Port:     int32(port),
					Username: sessionState.SourceDBConnDetails.User,
					Password: sessionState.SourceDBConnDetails.Password,
				},
			}
		} else if databaseType == constants.ORACLE {
			req.ConnectionProfile.Profile = &datastreampb.ConnectionProfile_OracleProfile{
				OracleProfile: &datastreampb.OracleProfile{
					Hostname: sessionState.SourceDBConnDetails.Host,
					Port:     int32(port),
					Username: sessionState.SourceDBConnDetails.User,
					Password: sessionState.SourceDBConnDetails.Password,
				},
			}
		} else if databaseType == constants.POSTGRES {
			req.ConnectionProfile.Profile = &datastreampb.ConnectionProfile_PostgresqlProfile{
				PostgresqlProfile: &datastreampb.PostgresqlProfile{
					Hostname: sessionState.SourceDBConnDetails.Host,
					Port:     int32(port),
					Username: sessionState.SourceDBConnDetails.User,
					Password: sessionState.SourceDBConnDetails.Password,
					Database: sessionState.DbName,
				},
			}

		}
	} else {
		req.ConnectionProfile.Profile = &datastreampb.ConnectionProfile_GcsProfile{
			GcsProfile: &datastreampb.GcsProfile{
				Bucket:   strings.ToLower(sessionState.Conv.Audit.MigrationRequestId),
				RootPath: "/",
			},
		}
	}

}

// Cleanup streaming jobs API assumes defaults while performing cleanup.
// The underlying backend library exposes more hooks which can are not yet implemented on the UI, and are only available via the CLI.
func CleanUpStreamingJobs(w http.ResponseWriter, r *http.Request) {
	ctx := context.Background()
	sessionState := session.GetSessionState()
	sessionState.Conv.ConvLock.Lock()
	defer sessionState.Conv.ConvLock.Unlock()
	jobCleanupOptions := streaming.JobCleanupOptions{
		Datastream: true,
		Dataflow:   true,
		Pubsub:     true,
		Monitoring: true,
	}
	streaming.InitiateJobCleanup(ctx, sessionState.Conv.Audit.MigrationRequestId, nil, jobCleanupOptions, sessionState.GCPProjectID, sessionState.SpannerInstanceID)
}

type connectionProfileReq struct {
	Id           string
	ValidateOnly bool
	IsSource     bool
}

type connectionProfileReqV2 struct {
	Id           string
	ValidateOnly bool
	IsSource     bool
	Host         string
	Port         string
	Password     string
	User         string
}

type connectionProfile struct {
	Name        string
	DisplayName string
}

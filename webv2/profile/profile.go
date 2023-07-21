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

	datastream "cloud.google.com/go/datastream/apiv1"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/utils"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/streaming"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/helpers"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/webv2/session"
	"github.com/google/uuid"
	"google.golang.org/api/iterator"
	datastreampb "google.golang.org/genproto/googleapis/cloud/datastream/v1"
)

func GetBucket(project, location, profileName string) (string, string, error) {
	ctx := context.Background()
	dsClient, err := datastream.NewClient(ctx)
	if err != nil {
		return "", "", fmt.Errorf("datastream client can not be created: %v", err)
	}
	defer dsClient.Close()
	// Fetch the GCS path from the destination connection profile.
	dstProf := fmt.Sprintf("projects/%s/locations/%s/connectionProfiles/%s", project, location, profileName)
	res, err := dsClient.GetConnectionProfile(ctx, &datastreampb.GetConnectionProfileRequest{Name: dstProf})
	if err != nil {
		return "", "", fmt.Errorf("could not get connection profile: %v", err)
	}
	gcsProfile := res.Profile.(*datastreampb.ConnectionProfile_GcsProfile).GcsProfile
	return gcsProfile.GetBucket(), gcsProfile.GetRootPath(), nil
}

func ListConnectionProfiles(w http.ResponseWriter, r *http.Request) {
	ctx := context.Background()
	dsClient, err := datastream.NewClient(ctx)
	if err != nil {
		http.Error(w, fmt.Sprintf("datastream client can not be created: %v", err), http.StatusBadRequest)
	}
	defer dsClient.Close()
	sessionState := session.GetSessionState()
	source := r.FormValue("source") == "true"
	if !source {
		sessionState.Conv.Audit.MigrationRequestId = "HB-" + uuid.New().String()
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
	databaseType, err := helpers.GetSourceDatabaseFromDriver(sessionState.Driver)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error while getting source database: %v", err), http.StatusBadRequest)
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

		if sessionState.IsSharded {
			bucketName = strings.ToLower(sessionState.Conv.Audit.MigrationRequestId + "-" + details.Id)
		} else {
			bucketName = strings.ToLower(sessionState.Conv.Audit.MigrationRequestId)
		}
		err = utils.CreateGCSBucket(bucketName, sessionState.GCPProjectID)
		if err != nil {
			http.Error(w, fmt.Sprintf("Error while creating bucket: %v", err), http.StatusBadRequest)
			return
		}
	}
	if sessionState.IsSharded {
		setConnectionProfileFromRequest(details, bucketName, req, databaseType)
	} else {
		setConnectionProfileFromSessionState(details.IsSource, *sessionState, req, databaseType)
	}

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

func setConnectionProfileFromRequest(details connectionProfileReqV2, bucketName string, req *datastreampb.CreateConnectionProfileRequest, databaseType string) error {
	if details.IsSource {
		port, _ := strconv.ParseInt((details.Port), 10, 32)
		if databaseType == constants.MYSQL {
			req.ConnectionProfile.Profile = &datastreampb.ConnectionProfile_MysqlProfile{
				MysqlProfile: &datastreampb.MysqlProfile{
					Hostname: details.Host,
					Port:     int32(port),
					Username: details.User,
					Password: details.Password,
				},
			}
			return nil
		} else {
			return fmt.Errorf("this database type is not currently implemented for sharded migrations")
		}
	} else {
		req.ConnectionProfile.Profile = &datastreampb.ConnectionProfile_GcsProfile{
			GcsProfile: &datastreampb.GcsProfile{
				Bucket:   bucketName,
				RootPath: "/",
			},
		}
		return nil
	}
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

func CleanUpStreamingJobs(w http.ResponseWriter, r *http.Request) {
	ctx := context.Background()
	sessionState := session.GetSessionState()
	err := streaming.CleanUpStreamingJobs(ctx, sessionState.Conv, sessionState.GCPProjectID, sessionState.Region)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error while cleaning up streaming jobs: %v", err), http.StatusBadRequest)
	}
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

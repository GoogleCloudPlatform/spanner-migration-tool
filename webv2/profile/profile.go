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
	"github.com/cloudspannerecosystem/harbourbridge/common/constants"
	"github.com/cloudspannerecosystem/harbourbridge/common/utils"
	"github.com/cloudspannerecosystem/harbourbridge/webv2/helpers"
	"github.com/cloudspannerecosystem/harbourbridge/webv2/session"
	"github.com/google/uuid"
	"google.golang.org/api/iterator"
	datastreampb "google.golang.org/genproto/googleapis/cloud/datastream/v1"
)

func GetBucket(project, location, profileName string) (string, error) {
	ctx := context.Background()
	dsClient, err := datastream.NewClient(ctx)
	if err != nil {
		return "", fmt.Errorf("datastream client can not be created: %v", err)
	}
	defer dsClient.Close()
	// Fetch the GCS path from the destination connection profile.
	dstProf := fmt.Sprintf("projects/%s/locations/%s/connectionProfiles/%s", project, location, profileName)
	res, err := dsClient.GetConnectionProfile(ctx, &datastreampb.GetConnectionProfileRequest{Name: dstProf})
	if err != nil {
		return "", fmt.Errorf("could not get connection profile: %v", err)
	}
	gcsProfile := res.Profile.(*datastreampb.ConnectionProfile_GcsProfile).GcsProfile
	return "gs://" + gcsProfile.GetBucket() + gcsProfile.GetRootPath(), nil
}

func ListConnectionProfiles(w http.ResponseWriter, r *http.Request) {
	ctx := context.Background()
	dsClient, err := datastream.NewClient(ctx)
	if err != nil {
		http.Error(w, fmt.Sprintf("datastream client can not be created: %v", err), http.StatusBadRequest)
	}
	defer dsClient.Close()
	sessionState := session.GetSessionState()
	region := r.FormValue("region")
	source := r.FormValue("source") == "true"
	databaseType, err := helpers.GetSourceDatabaseFromDriver(sessionState.Driver)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error while getting source database: %v", err), http.StatusBadRequest)
		return
	}
	var connectionProfileList []connectionProfile
	req := &datastreampb.ListConnectionProfilesRequest{
		Parent: fmt.Sprintf("projects/%s/locations/%s", sessionState.GCPProjectID, region),
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
	region := r.FormValue("region")
	req := &datastreampb.FetchStaticIpsRequest{
		Name: fmt.Sprintf("projects/%s/locations/%s", sessionState.GCPProjectID, region),
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

	details := connectionProfileReq{}
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
	sessionState.Conv.Audit.MigrationRequestId = "HB-" + uuid.New().String()
	databaseType, err := helpers.GetSourceDatabaseFromDriver(sessionState.Driver)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error while getting source database: %v", err), http.StatusBadRequest)
		return
	}
	req := &datastreampb.CreateConnectionProfileRequest{
		Parent:              fmt.Sprintf("projects/%s/locations/%s", sessionState.GCPProjectID, details.Region),
		ConnectionProfileId: details.Id,
		ConnectionProfile: &datastreampb.ConnectionProfile{
			DisplayName:  details.Id,
			Connectivity: &datastreampb.ConnectionProfile_StaticServiceIpConnectivity{},
		},
		ValidateOnly: details.ValidateOnly,
	}
	if !details.IsSource {
		err = utils.CreateGCSBucket(strings.ToLower(sessionState.Conv.Audit.MigrationRequestId), sessionState.GCPProjectID)
		if err != nil {
			http.Error(w, fmt.Sprintf("Error while creating bucket: %v", err), http.StatusBadRequest)
			return
		}
	}
	setConnectionProfile(details.IsSource, *sessionState, req, databaseType)
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

func setConnectionProfile(isSource bool, sessionState session.SessionState, req *datastreampb.CreateConnectionProfileRequest, databaseType string) {
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

type connectionProfileReq struct {
	Id           string
	Region       string
	ValidateOnly bool
	IsSource     bool
}

type connectionProfile struct {
	Name        string
	DisplayName string
}

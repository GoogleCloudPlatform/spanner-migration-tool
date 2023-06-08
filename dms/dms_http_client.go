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
package dms

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	dms "cloud.google.com/go/clouddms/apiv1"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
)

// API Objects
type ConnectionProfile struct {
	// projects/{project}/locations/{location}/connectionProfiles/{connectionProfile}.
	Name        string                    `json:"name,omitempty"`
	Labels      map[string]string         `json:"labels,omitempty"`
	State       ConnectionProfile_State   `json:"state,omitempty"`
	DisplayName string                    `json:"display_name,omitempty"`
	Spanner     *SpannerConnectionProfile `json:"spanner,omitempty"`
	MySQL       *MySqlConnectionProfile   `json:"mysql,omitempty"`
	Error       *status.Status            `json:"error,omitempty"`
}

type SpannerConnectionProfile struct {
	// projects/{project}/instances/{instance}/databases/{database}
	Database string `json:"database,omitempty"`
}

type MySqlConnectionProfile struct {
	Host                        string               `json:"host,omitempty"`
	Port                        int32                `json:"port,omitempty"`
	Username                    string               `json:"username,omitempty"`
	Password                    string               `json:"password,omitempty"`
	PrivateConn                 *PrivateConnectivity `json:"privateConnectivity,omitempty"`
	StaticServiceIpConnectivity *StaticConnectivity  `json:"staticServiceIpConnectivity,omitempty"`
}

type PrivateConnectivity struct {
	// projects/{project}/locations/{location}/privateConnections/{privateConnection}
	PrivateConnection string `json:"private_connection,omitempty"`
}

type StaticConnectivity struct {
}

type ConnectionProfile_State string

const (
	ConnectionProfile_STATE_UNSPECIFIED ConnectionProfile_State = "STATE_UNSPECIFIED"
	ConnectionProfile_DRAFT             ConnectionProfile_State = "DRAFT"
	ConnectionProfile_CREATING          ConnectionProfile_State = "CREATING"
	ConnectionProfile_READY             ConnectionProfile_State = "READY"
	ConnectionProfile_UPDATING          ConnectionProfile_State = "UPDATING"
	ConnectionProfile_DELETING          ConnectionProfile_State = "DELETING"
	ConnectionProfile_DELETED           ConnectionProfile_State = "DELETED"
	ConnectionProfile_FAILED            ConnectionProfile_State = "FAILED"
)

// This resource represents a long-running operation
type Operation struct {
	Name     string     `json:"name,omitempty"`
	Metadata *Metadata  `json:"metadata,omitempty"`
	Done     bool       `json:"done,omitempty"`
	Error    *Status    `json:"error,omitempty"`
	Response *anypb.Any `json:"response,omitempty"`
}

type Metadata struct {
	Target string `json:"target,omitempty"`
}

type Status struct {
	Code    int32  `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
	Status  string `json:"status,omitempty"`
}

type ConversionWorkspace struct {
	Name           string       `json:"name,omitempty"`
	Source         DBEngineInfo `json:"source,omitempty"`
	Destination    DBEngineInfo `json:"destination,omitempty"`
	GlobalSettings settings     `json:"globalSettings"`
}

type settings struct {
	V2 string `json:"v2"`
}

type DBEngineInfo struct {
	Engine DBEngine `json:"engine,omitempty"`
}

type DBEngine string

const (
	MYSQL      DBEngine = "MYSQL"
	SPANNER    DBEngine = "SPANNER"
	ORACLE     DBEngine = "ORACLE"
	POSTGRESQL DBEngine = "POSTGRESQL"
)

// API Objects end

// DMSHttpClient
type dmsHttpClient struct {
	httpClient *http.Client
	dmsClient  *dms.DataMigrationClient
}

func NewDmsHttpClient(ctx context.Context, dmsClient *dms.DataMigrationClient) (*dmsHttpClient, error) {
	if dmsClient == nil {
		return nil, fmt.Errorf("dmsClient is nil")
	}

	c, err := createHTTPClient()
	if err != nil {
		return nil, err
	}

	return &dmsHttpClient{
		httpClient: c,
		dmsClient:  dmsClient,
	}, nil
}

// createConnectionProfile REST API
func (d *dmsHttpClient) callCreateConnectionProfile(ctx context.Context, project string, location string, connectionProfileID string, connectionProfile *ConnectionProfile, testConnectivityOnly bool) error {

	connectionProfileURL := "https://datamigration.googleapis.com/v1/projects/%s/locations/%s/connectionProfiles?connectionProfileId=%s"

	connectionProfileURL = fmt.Sprintf(connectionProfileURL, project, location, connectionProfileID)

	if testConnectivityOnly {
		connectionProfileURL = connectionProfileURL + "&validateOnly=true"
	}

	request, err := json.MarshalIndent(connectionProfile, "", " ")

	if err != nil {
		return fmt.Errorf("Could not marshal dms connectionProfile json: %v", err)
	}

	fmt.Printf("Request body:%v\n", string(request))
	resp, err := d.httpClient.Post(connectionProfileURL, "application/json", bytes.NewBuffer(request))

	if err != nil {
		return fmt.Errorf("Error calling createConnectionProfile API, err=%v", err)
	}

	// read response body
	body, err := ioutil.ReadAll(resp.Body)
	// close response body
	defer resp.Body.Close()
	if err != nil {
		return fmt.Errorf("Error reading response body, err=%v", err)
	}
	fmt.Printf("Response Body, body=%s\n", string(body))

	var op Operation
	err = json.Unmarshal(body, &op)

	if err != nil {
		return fmt.Errorf("Could not unmarshal operation object, err=%v", err)
	}
	if op.Error != nil {
		fmt.Printf("Error while creating connection profile, resp=%s", string(body))
		return fmt.Errorf("Could not create connection profile, err=%v", op.Error.Message)
	}

	return d.WaitForConnectionProfileOperation(ctx, op)
}

func (d *dmsHttpClient) WaitForConnectionProfileOperation(ctx context.Context, op Operation) error {

	lrop := d.dmsClient.CreateConnectionProfileOperation(op.Name)
	_, err := lrop.Wait(context.Background())

	if err != nil {
		return fmt.Errorf("Error creating Connection profile, err=%v", err)
	}

	// fmt.Printf("Successfull, lrop=%v\n, c=%v\n", lrop, c)
	return nil
}

// createConversionWorkspace REST API
func (d *dmsHttpClient) callCreateConversionWorkspace(ctx context.Context, project, location, conversionProfileID string, conversionWorkspace *ConversionWorkspace) error {
	conversionWorkspaceURL := "https://datamigration.googleapis.com/v1/projects/%s/locations/%s/conversionWorkspaces?conversionWorkspaceId=%s"

	conversionWorkspaceURL = fmt.Sprintf(conversionWorkspaceURL, project, location, conversionProfileID)

	request, err := json.MarshalIndent(conversionWorkspace, "", " ")

	if err != nil {
		return fmt.Errorf("Could not marshal dms conversionWorkspace json: %v", err)
	}
	// fmt.Printf("Request body=%v\n", string(request))
	resp, err := d.httpClient.Post(conversionWorkspaceURL, "application/json", bytes.NewBuffer(request))

	if err != nil {
		return fmt.Errorf("Error calling create conversionWorkspace API, err=%v", err)
	}

	// read response body
	body, err := ioutil.ReadAll(resp.Body)
	// close response body
	defer resp.Body.Close()
	if err != nil {
		return fmt.Errorf("Error reading response body, err=%v", err)
	}
	// fmt.Printf("Response Body, body=%s\n", string(body))

	var op Operation
	err = json.Unmarshal(body, &op)

	if err != nil {
		return fmt.Errorf("Could not unmarshal operation object, err=%v", err)
	}
	if op.Error != nil {
		fmt.Printf("Error while creating conversion workspace , resp=%s", string(body))
		return fmt.Errorf("Could not create conversion workspace, err=%v", op.Error.Message)
	}

	return d.WaitForConversionWorkspaceOperation(ctx, op)
}

func (d *dmsHttpClient) WaitForConversionWorkspaceOperation(ctx context.Context, op Operation) error {

	lrop := d.dmsClient.CreateConversionWorkspaceOperation(op.Name)
	_, err := lrop.Wait(context.Background())

	if err != nil {
		return fmt.Errorf("Error creating Conversion workspace, err=%v", err)
	}

	// fmt.Printf("Successfull, lrop=%v\n, c=%v\n", lrop, c)
	return nil
}

// createHTTPClient creates http client with application default credentials
func createHTTPClient() (*http.Client, error) {
	client, err := google.DefaultClient(oauth2.NoContext,
		"https://www.googleapis.com/auth/cloud-platform")

	if err != nil {
		return nil, fmt.Errorf("Error creating http client with default authentication: %v", err)
	}
	fmt.Println("Created http client.")

	return client, err
}

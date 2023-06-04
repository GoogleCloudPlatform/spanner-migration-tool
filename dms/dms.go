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
	"context"
	"fmt"

	dms "cloud.google.com/go/clouddms/apiv1"
	"cloud.google.com/go/clouddms/apiv1/clouddmspb"
)

const (
	maxWorkers int32 = 50
)

type SrcConnCfg struct {
	ConnectionProfileID string
	Location            string
	Project             string
	MySQLCfg            MySQLConnCfg
}

type DstConnCfg struct {
	ConnectionProfileID string
	Location            string
	Project             string
	SpannerCfg          SpannerConnCfg
}

type MySQLConnCfg struct {
	Host           string
	Port           int64
	Username       string
	Password       string
	PrivateConnCfg PrivateConnectionCfg
}

type PrivateConnectionCfg struct {
	// projects/{project}/locations/{location}/privateConnections/{privateConnectionID}
	PrivateConnection string
}

type SpannerConnCfg struct {
	Project  string
	Instance string
	Database string
}

type DMSJobCfg struct {
	ID       string
	Project  string
	Location string
	// projects/{project}/locations/{location}/connectionProfiles/{connectionProfileID}
	SourceConnProfileID      string
	DestinationConnProfileID string
	// projects/{project}/locations/{location}/conversionWorkspaces/{conversionWorkspace}
	ConversionWorkspaceID       string
	ConversionWorkspaceCommitID string
}

type ConversionWorkspaceCfg struct {
	ID          string
	Project     string
	Location    string
	SessionFile SessionFileCfg
}

type SessionFileCfg struct {
	FileName    string
	FileContent string
}

// CreateDMSJob creates a DMS Job.
func CreateDMSJob(ctx context.Context, job DMSJobCfg) error {
	dmsClient, err := dms.NewDataMigrationClient(ctx)
	if err != nil {
		return fmt.Errorf("dms client can not be created: %v", err)
	}
	defer dmsClient.Close()
	fmt.Println("Created dms client...")

	parent := "projects/%s/locations/%s"
	parent = fmt.Sprintf(parent, job.Project, job.Location)
	name := "projects/%s/locations/%s/migrationJobs/%s"
	name = fmt.Sprintf(name, job.Project, job.Location, job.ID)

	req := &clouddmspb.CreateMigrationJobRequest{
		Parent:         parent,
		MigrationJobId: job.ID,
		MigrationJob: &clouddmspb.MigrationJob{
			Name:        name,
			Type:        clouddmspb.MigrationJob_CONTINUOUS,
			Source:      job.SourceConnProfileID,
			Destination: job.DestinationConnProfileID,
			ConversionWorkspace: &clouddmspb.ConversionWorkspaceInfo{
				Name:     job.ConversionWorkspaceID,
				CommitId: job.ConversionWorkspaceCommitID,
			},
			Connectivity:        nil,
			SourceDatabase:      nil,
			DestinationDatabase: nil,
		},
	}

	op, err := dmsClient.CreateMigrationJob(ctx, req)
	if err != nil {
		return fmt.Errorf("dms migration job could not be created: %v", err)
	}
	dmsJob, err := op.Wait(ctx)
	if err != nil {
		return fmt.Errorf("dms migration job could not be created (after waiting): %v", err)
	}
	fmt.Printf("Created dms job. JobId=%v", dmsJob.Name)
	return nil
}

// CreateMySQLConnectionProfile creates a MySQL connection profile in DMS
func CreateMySQLConnectionProfile(ctx context.Context, sourceConnCfg SrcConnCfg) error {
	name := "projects/%s/locations/%s/connectionProfiles/%s"

	name = fmt.Sprintf(name, sourceConnCfg.Project, sourceConnCfg.Location, sourceConnCfg.ConnectionProfileID)

	var privateConnection *PrivateConnectivity
	privateConnection = nil
	if sourceConnCfg.MySQLCfg.PrivateConnCfg.PrivateConnection != "" {
		privateConnection = &PrivateConnectivity{
			PrivateConnection: sourceConnCfg.MySQLCfg.PrivateConnCfg.PrivateConnection,
		}
	}

	req := ConnectionProfile{
		Name:        name,
		DisplayName: sourceConnCfg.ConnectionProfileID,
		MySQL: &MySqlConnectionProfile{
			Host:        sourceConnCfg.MySQLCfg.Host,
			Port:        int32(sourceConnCfg.MySQLCfg.Port),
			Username:    sourceConnCfg.MySQLCfg.Username,
			Password:    sourceConnCfg.MySQLCfg.Password,
			PrivateConn: privateConnection,
		},
	}
	dmsClient, err := dms.NewDataMigrationClient(ctx)
	if err != nil {
		return fmt.Errorf("dms client can not be created: %v", err)
	}
	defer dmsClient.Close()
	fmt.Println("Created dms client...")

	client, err := NewDmsHttpClient(ctx, dmsClient)
	if err != nil {
		return err
	}

	err = client.createConnectionProfile(ctx, sourceConnCfg.Project, sourceConnCfg.Location, sourceConnCfg.ConnectionProfileID, &req)
	if err != nil {
		return err
	}
	fmt.Printf("Created MySQL connection profile. Id=%v", sourceConnCfg.ConnectionProfileID)
	return nil
}

// CreateSpannerConnectionProfile creates a Spanner connection profile in DMS
func CreateSpannerConnectionProfile(ctx context.Context, destinationConfig DstConnCfg) error {
	name := "projects/%s/locations/%s/connectionProfiles/%s"
	name = fmt.Sprintf(name, destinationConfig.Project, destinationConfig.Location, destinationConfig.ConnectionProfileID)
	spannerDB := "projects/%s/instances/%s/databases/%s"
	spannerDB = fmt.Sprintf(spannerDB, destinationConfig.SpannerCfg.Project, destinationConfig.SpannerCfg.Instance, destinationConfig.SpannerCfg.Database)
	req := ConnectionProfile{
		Name:        name,
		DisplayName: destinationConfig.ConnectionProfileID,
		Spanner:     &SpannerConnectionProfile{Database: spannerDB},
	}

	dmsClient, err := dms.NewDataMigrationClient(ctx)
	if err != nil {
		return fmt.Errorf("dms client can not be created: %v", err)
	}
	defer dmsClient.Close()
	fmt.Println("Created dms client...")

	client, err := NewDmsHttpClient(ctx, dmsClient)
	if err != nil {
		return err
	}
	return client.createConnectionProfile(ctx, destinationConfig.Project, destinationConfig.Location, destinationConfig.ConnectionProfileID, &req)
}

// CreateConversionWorkspace creates a conversion workspace and uploads session file
func CreateConversionWorkspace(ctx context.Context, workspaceCfg ConversionWorkspaceCfg) (string, error) {
	dmsClient, err := dms.NewDataMigrationClient(ctx)
	if err != nil {
		return "", fmt.Errorf("dms client can not be created: %v", err)
	}
	defer dmsClient.Close()
	fmt.Println("Created dms client...")

	client, err := NewDmsHttpClient(ctx, dmsClient)
	if err != nil {
		return "", err
	}

	err = createWorkspace(ctx, client, workspaceCfg)
	if err != nil {
		return "", err
	}

	return importSessionFile(ctx, dmsClient, workspaceCfg)
}

func createWorkspace(ctx context.Context, client *dmsHttpClient, workspaceCfg ConversionWorkspaceCfg) error {
	name := "projects/%s/locations/%s/conversionWorkspaces/%s"
	name = fmt.Sprintf(name, workspaceCfg.Project, workspaceCfg.Location, workspaceCfg.ID)
	req := ConversionWorkspace{
		Name:        name,
		Source:      DBEngineInfo{Engine: MYSQL},
		Destination: DBEngineInfo{Engine: SPANNER},
		// GlobalSettings: settings{V2: "true"},
	}
	return client.createConversionWorkspace(ctx, workspaceCfg.Project, workspaceCfg.Location, workspaceCfg.ID, &req)
}

func importSessionFile(ctx context.Context, dmsClient *dms.DataMigrationClient, workspaceCfg ConversionWorkspaceCfg) (string, error) {
	name := "projects/%s/locations/%s/conversionWorkspaces/%s"
	name = fmt.Sprintf(name, workspaceCfg.Project, workspaceCfg.Location, workspaceCfg.ID)

	importReq := &clouddmspb.ImportMappingRulesRequest{
		Parent:      name,
		RulesFormat: clouddmspb.ImportRulesFileFormat_IMPORT_RULES_FILE_FORMAT_HARBOUR_BRIDGE_SESSION_FILE,
		AutoCommit:  true,
		RulesFiles: []*clouddmspb.ImportMappingRulesRequest_RulesFile{
			{
				RulesSourceFilename: workspaceCfg.SessionFile.FileName,
				RulesContent:        workspaceCfg.SessionFile.FileContent,
			},
		},
	}

	importOp, err := dmsClient.ImportMappingRules(ctx, importReq)
	if err != nil {
		return "", fmt.Errorf("Could not create mapping rules from Harbourbridge session, err=%v", err)
	}
	conversionWorkspace, err := importOp.Wait(ctx)
	if err != nil {
		return "", fmt.Errorf("Could not create mapping rules from Harbourbridge session (after waiting): %v", err)
	}
	return conversionWorkspace.LatestCommitId, nil
}

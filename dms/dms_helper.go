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
	"google.golang.org/api/iterator"
)

const (
	maxWorkers int32 = 50

	ConnectionProfileResourceFormat   = "projects/%s/locations/%s/connectionProfiles/%s"
	ConversionWorkspaceResourceFormat = "projects/%s/locations/%s/conversionWorkspaces/%s"
	MigrationJobResourceFormat        = "projects/%s/locations/%s/migrationJobs/%s"
	SpannerDatabaseResourceFormat     = "projects/%s/instances/%s/databases/%s"
	LocationResourceFormat            = "projects/%s/locations/%s"
	PrivateConnectionResourceFormat   = "projects/%s/locations/%s/privateConnections/%s"
)

type SrcConnCfg struct {
	ConnectionProfileID ResourceIdentifier
	MySQLCfg            MySQLConnCfg
}

type DstConnCfg struct {
	ConnectionProfileID ResourceIdentifier
	SpannerCfg          SpannerConnCfg
}

type ResourceIdentifier struct {
	Project  string
	Location string
	ID       string
}

type MySQLConnCfg struct {
	Host           string
	Port           int64
	Username       string
	Password       string
	PrivateConnCfg PrivateConnectionCfg
}

type PrivateConnectionCfg struct {
	PrivateConnection ResourceIdentifier
}

type SpannerConnCfg struct {
	Project  string
	Instance string
	Database string
}

type DMSJobCfg struct {
	JobID                       ResourceIdentifier
	SourceConnProfileID         ResourceIdentifier
	DestinationConnProfileID    ResourceIdentifier
	ConversionWorkspaceID       ResourceIdentifier
	ConversionWorkspaceCommitID string
}

type ConversionWorkspaceCfg struct {
	ConversionWorkspaceID     ResourceIdentifier
	SessionFile               SessionFileCfg
	SourceConnectionProfileID ResourceIdentifier
}

type SessionFileCfg struct {
	FileName    string
	FileContent string
}

// createDMSJob creates a DMS Job.
func createDMSJob(ctx context.Context, job DMSJobCfg) error {
	dmsClient, err := dms.NewDataMigrationClient(ctx)
	if err != nil {
		return fmt.Errorf("dms client can not be created: %v", err)
	}
	defer dmsClient.Close()
	fmt.Println("Created dms client...")

	parent := fmt.Sprintf(LocationResourceFormat, job.JobID.Project, job.JobID.Location)
	name := migrationJobID(job.JobID)

	req := &clouddmspb.CreateMigrationJobRequest{
		Parent:         parent,
		MigrationJobId: job.JobID.ID,
		MigrationJob: &clouddmspb.MigrationJob{
			Name:        name,
			Type:        clouddmspb.MigrationJob_CONTINUOUS,
			Source:      connectionProfileID(job.SourceConnProfileID),
			Destination: connectionProfileID(job.DestinationConnProfileID),
			ConversionWorkspace: &clouddmspb.ConversionWorkspaceInfo{
				Name:     conversionWorkspaceID(job.ConversionWorkspaceID),
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

// launchDMSJob creates a DMS Job.
func launchDMSJob(ctx context.Context, job DMSJobCfg) error {
	dmsClient, err := dms.NewDataMigrationClient(ctx)
	if err != nil {
		return fmt.Errorf("dms client can not be created: %v", err)
	}
	defer dmsClient.Close()
	fmt.Println("Created dms client...")

	name := migrationJobID(job.JobID)

	req := &clouddmspb.StartMigrationJobRequest{
		Name: name,
	}

	op, err := dmsClient.StartMigrationJob(ctx, req)
	if err != nil {
		return fmt.Errorf("dms migration job could not be started: %v", err)
	}
	dmsJob, err := op.Wait(ctx)
	if err != nil {
		return fmt.Errorf("dms migration job could not be started (after waiting): %v", err)
	}
	fmt.Printf("Launched dms job. JobId=%v", dmsJob.Name)
	return nil
}

// createMySQLConnectionProfile creates a MySQL connection profile in DMS
func createMySQLConnectionProfile(ctx context.Context, sourceConnCfg SrcConnCfg, testConnectivityOnly bool) error {

	name := connectionProfileID(sourceConnCfg.ConnectionProfileID)

	var privateConnection *PrivateConnectivity
	privateConnection = nil
	var staticConnectivity *StaticConnectivity
	staticConnectivity = nil
	if sourceConnCfg.MySQLCfg.PrivateConnCfg.PrivateConnection.ID != "" {
		privateConnection = &PrivateConnectivity{
			PrivateConnection: privateConnectionID(sourceConnCfg.MySQLCfg.PrivateConnCfg.PrivateConnection),
		}
	} else {
		staticConnectivity = &StaticConnectivity{}
	}

	req := ConnectionProfile{
		Name:        name,
		DisplayName: sourceConnCfg.ConnectionProfileID.ID,
		MySQL: &MySqlConnectionProfile{
			Host:                        sourceConnCfg.MySQLCfg.Host,
			Port:                        int32(sourceConnCfg.MySQLCfg.Port),
			Username:                    sourceConnCfg.MySQLCfg.Username,
			Password:                    sourceConnCfg.MySQLCfg.Password,
			PrivateConn:                 privateConnection,
			StaticServiceIpConnectivity: staticConnectivity,
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

	err = client.callCreateConnectionProfile(ctx, sourceConnCfg.ConnectionProfileID.Project, sourceConnCfg.ConnectionProfileID.Location, sourceConnCfg.ConnectionProfileID.ID, &req, testConnectivityOnly)
	if err != nil {
		return err
	}
	fmt.Printf("Created MySQL connection profile. Id=%v", sourceConnCfg.ConnectionProfileID)
	return nil
}

// createSpannerConnectionProfile creates a Spanner connection profile in DMS
func createSpannerConnectionProfile(ctx context.Context, destinationConfig DstConnCfg, testConnectivityOnly bool) error {
	name := connectionProfileID(destinationConfig.ConnectionProfileID)
	spannerDB := fmt.Sprintf(SpannerDatabaseResourceFormat, destinationConfig.SpannerCfg.Project, destinationConfig.SpannerCfg.Instance, destinationConfig.SpannerCfg.Database)
	req := ConnectionProfile{
		Name:        name,
		DisplayName: destinationConfig.ConnectionProfileID.ID,
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
	return client.callCreateConnectionProfile(ctx, destinationConfig.ConnectionProfileID.Project, destinationConfig.ConnectionProfileID.Location, destinationConfig.ConnectionProfileID.ID, &req, testConnectivityOnly)
}

func DoesConnectionProfileExist(ctx context.Context, project, location, connectionProfileID string) (bool, error) {
	name := "projects/%s/locations/%s/connectionProfiles/%s"
	name = fmt.Sprintf(name, project, location, connectionProfileID)

	dmsClient, err := dms.NewDataMigrationClient(ctx)
	if err != nil {
		return false, fmt.Errorf("dms client can not be created: %v", err)
	}
	defer dmsClient.Close()
	fmt.Println("Created dms client...")

	req := &clouddmspb.GetConnectionProfileRequest{Name: name}
	conn, err := dmsClient.GetConnectionProfile(ctx, req)
	if err != nil {
		return false, err
	}
	return conn.Name == name, nil
}

// createConversionWorkspace creates a conversion workspace and uploads session file
func createConversionWorkspace(ctx context.Context, workspaceCfg ConversionWorkspaceCfg) (string, error) {
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

	err = seedWorkspace(ctx, dmsClient, workspaceCfg)
	if err != nil {
		return "", err
	}

	return importSessionFile(ctx, dmsClient, workspaceCfg)
}

func createWorkspace(ctx context.Context, client *dmsHttpClient, workspaceCfg ConversionWorkspaceCfg) error {
	name := conversionWorkspaceID(workspaceCfg.ConversionWorkspaceID)
	req := ConversionWorkspace{
		Name:        name,
		Source:      DBEngineInfo{Engine: MYSQL},
		Destination: DBEngineInfo{Engine: SPANNER},
		// GlobalSettings: settings{V2: "true"},
	}
	return client.callCreateConversionWorkspace(ctx, workspaceCfg.ConversionWorkspaceID.Project, workspaceCfg.ConversionWorkspaceID.Location, workspaceCfg.ConversionWorkspaceID.ID, &req)
}

func seedWorkspace(ctx context.Context, dmsClient *dms.DataMigrationClient, workspaceCfg ConversionWorkspaceCfg) error {
	name := conversionWorkspaceID(workspaceCfg.ConversionWorkspaceID)

	seedReq := &clouddmspb.SeedConversionWorkspaceRequest{
		Name:       name,
		AutoCommit: true,
		SeedFrom:   &clouddmspb.SeedConversionWorkspaceRequest_SourceConnectionProfile{SourceConnectionProfile: connectionProfileID(workspaceCfg.SourceConnectionProfileID)},
	}

	seedOp, err := dmsClient.SeedConversionWorkspace(ctx, seedReq)
	if err != nil {
		return fmt.Errorf("Could not seed Conversion workspace, err=%v", err)
	}
	_, err = seedOp.Wait(ctx)
	if err != nil {
		return fmt.Errorf("Could not seed Conversion workspace, err=%v", err)
	}
	return nil
}

func importSessionFile(ctx context.Context, dmsClient *dms.DataMigrationClient, workspaceCfg ConversionWorkspaceCfg) (string, error) {
	name := conversionWorkspaceID(workspaceCfg.ConversionWorkspaceID)

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

func fetchStaticIps(ctx context.Context, project, location string) ([]string, error) {
	dmsClient, err := dms.NewDataMigrationClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("dms client can not be created: %v", err)
	}
	defer dmsClient.Close()
	fmt.Println("Created dms client...")

	name := fmt.Sprintf(LocationResourceFormat, project, location)
	iter := dmsClient.FetchStaticIps(ctx, &clouddmspb.FetchStaticIpsRequest{Name: name})

	result := []string{}
	var iterErr error
	for iterErr == nil {
		ip, err := iter.Next()
		result = append(result, ip)
		iterErr = err
	}
	if iterErr != iterator.Done {
		return nil, iterErr
	}
	return result, nil
}

func connectionProfileID(resource ResourceIdentifier) string {
	return fmt.Sprintf(ConnectionProfileResourceFormat, resource.Project, resource.Location, resource.ID)
}

func conversionWorkspaceID(resource ResourceIdentifier) string {
	return fmt.Sprintf(ConversionWorkspaceResourceFormat, resource.Project, resource.Location, resource.ID)
}

func migrationJobID(resource ResourceIdentifier) string {
	return fmt.Sprintf(MigrationJobResourceFormat, resource.Project, resource.Location, resource.ID)
}

func privateConnectionID(resource ResourceIdentifier) string {
	return fmt.Sprintf(PrivateConnectionResourceFormat, resource.Project, resource.Location, resource.ID)
}

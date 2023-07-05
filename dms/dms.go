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
	"encoding/json"
	"fmt"
	"sync"

	"github.com/cloudspannerecosystem/harbourbridge/common/utils"
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/profiles"
)

type DMSCfg struct {
	DataShard profiles.DMSDataShard
	DMSJobCfg DMSJobCfg
}

// Test MySQL Connectivity
func TestMySQLConnectionProfile(ctx context.Context, srcConn SrcConnCfg) error {
	return createMySQLConnectionProfile(ctx, srcConn, true)
}

// Create MySQL Connection Profile
func CreateMySQLConnectionProfile(ctx context.Context, srcConn SrcConnCfg) error {
	return createMySQLConnectionProfile(ctx, srcConn, false)
}

// Test Spanner Connectivity
func TestSpannerConnectionProfile(ctx context.Context, destConn DstConnCfg) error {
	return createSpannerConnectionProfile(ctx, destConn, true)
}

// Create Spanner Connection Profile
func CreateSpannerConnectionProfile(ctx context.Context, destConn DstConnCfg) error {
	return createSpannerConnectionProfile(ctx, destConn, false)
}

func CreateDMSConfig(pl profiles.DMSDataShard, destination profiles.TargetProfileConnectionSpanner, conversionWorkspace ResourceIdentifier, commitId string) (*DMSCfg, error) {
	// validate
	if pl.SrcConnectionProfile.Name == "" {
		return nil, fmt.Errorf("please specify DMSDataShard.SrcConnectionProfile.Name in the config")
	}
	if pl.SrcConnectionProfile.Location == "" {
		return nil, fmt.Errorf("please specify DMSDataShard.SrcConnectionProfile.Location in the config")
	}
	if pl.DMSConfig.JobLocation == "" {
		return nil, fmt.Errorf("please specify DMSDataShard.DMSConfig.JobLocation in the config")
	}
	// create dmsCfg from pl receiver object
	jobId, err := utils.GenerateName("hb-dms-job")
	if err != nil {
		return nil, err
	}

	inputDmsConfig := pl.DMSConfig
	dmsJobCfg := DMSJobCfg{
		JobID: ResourceIdentifier{
			Location: inputDmsConfig.JobLocation,
			Project:  destination.Project,
			ID:       jobId,
		},
	}
	//create src and dst connection profile objects from pl receiver object
	inputSrcConnProfile := pl.SrcConnectionProfile
	dmsJobCfg.SourceConnProfileID = ResourceIdentifier{
		ID:       inputSrcConnProfile.Name,
		Location: inputSrcConnProfile.Location,
		Project:  destination.Project,
	}
	dstConnectionProfileId, err := utils.GenerateName("hb-spanner")
	if err != nil {
		return nil, err
	}
	//set dst connection profile
	dmsJobCfg.DestinationConnProfileID = ResourceIdentifier{
		ID:       dstConnectionProfileId,
		Location: inputSrcConnProfile.Location,
		Project:  destination.Project,
	}
	// Set conversion workspace details
	dmsJobCfg.ConversionWorkspaceID = conversionWorkspace
	dmsJobCfg.ConversionWorkspaceCommitID = commitId
	return &DMSCfg{DataShard: pl, DMSJobCfg: dmsJobCfg}, nil
}

type ConvWithShardIdMapping struct {
	*internal.Conv
	DatabaseName string `json:"DatabaseName"`
}

func CreateConversionWorkspace(ctx context.Context, schemaDBName string, dataShard *profiles.DMSDataShard, project string, conv *internal.Conv) (*ResourceIdentifier, string, error) {
	workspaceId, err := utils.GenerateName("hb-dms")
	if err != nil {
		return nil, "", err
	}
	filename, err := utils.GenerateName("hb-session")
	if err != nil {
		return nil, "", err
	}
	convWithMapping := ConvWithShardIdMapping{
		Conv:         conv,
		DatabaseName: schemaDBName,
	}
	convJSON, err := json.MarshalIndent(convWithMapping, "", " ")

	if err != nil {
		return nil, "", err
	}
	workspaceCfg := ConversionWorkspaceCfg{
		ConversionWorkspaceID: ResourceIdentifier{Project: project, Location: dataShard.SrcConnectionProfile.Location, ID: workspaceId},
		SessionFile: SessionFileCfg{
			FileName:    filename,
			FileContent: string(convJSON),
		},
		SourceConnectionProfileID: ResourceIdentifier{Project: project, Location: dataShard.SrcConnectionProfile.Location, ID: dataShard.SrcConnectionProfile.Name},
	}
	commitId, err := createConversionWorkspace(ctx, workspaceCfg)
	return &workspaceCfg.ConversionWorkspaceID, commitId, err
}

// CreateAndLaunchDMSJob
func CreateAndLaunchDMSJob(ctx context.Context, dmsCfg DMSCfg, targetProfile profiles.TargetProfileConnectionSpanner, conv *internal.Conv) error {
	err := createDMSJob(ctx, dmsCfg.DMSJobCfg)
	if err != nil {
		return err
	}
	storeGeneratedResources(conv, dmsCfg, dmsCfg.DataShard.DataShardId)
	return launchDMSJob(ctx, dmsCfg.DMSJobCfg)
}

func storeGeneratedResources(conv *internal.Conv, dmsCfg DMSCfg, dataShardId string) {
	conv.Audit.StreamingStats.DMSJobId = dmsCfg.DMSJobCfg.JobID.ID
	conv.Audit.StreamingStats.ConversionWorkspaceName = dmsCfg.DMSJobCfg.ConversionWorkspaceID.ID
	if dataShardId != "" {
		var resourceMutex sync.Mutex
		resourceMutex.Lock()
		conv.Audit.StreamingStats.ShardToDMSJobMap[dataShardId] = dmsCfg.DMSJobCfg.JobID.ID
		resourceMutex.Unlock()
	}
	dmsJobName := fmt.Sprintf("projects/%s/locations/%s/migrationJobs/%s", dmsCfg.DMSJobCfg.JobID.Project, dmsCfg.DMSJobCfg.JobID.Location, dmsCfg.DMSJobCfg.JobID.ID)
	fmt.Println("\n------------------------------------------\n" +
		"The DMS job: " + dmsJobName + 
		" will have to be manually cleaned up via the UI. HarbourBridge will not delete them post completion of the migration.")
}

// Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"context"
	"fmt"
	"time"

	sp "cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"github.com/cloudspannerecosystem/harbourbridge/common/utils"
	"github.com/cloudspannerecosystem/harbourbridge/profiles"
)

// CreateDatabaseClient creates new database client and admin client.
func CreateDatabaseClient(ctx context.Context, targetProfile profiles.TargetProfile, driver string, ioHelper utils.IOStreams) (*database.DatabaseAdminClient, *sp.Client, string, error) {
	project, instance, dbName, err := targetProfile.GetResourceIds(ctx, time.Now(), driver, ioHelper.Out)
	if err != nil {
		return nil, nil, "", err
	}
	fmt.Println("Using Google Cloud project:", project)
	fmt.Println("Using Cloud Spanner instance:", instance)
	utils.PrintPermissionsWarning(driver, ioHelper.Out)

	dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", project, instance, dbName)
	adminClient, err := utils.NewDatabaseAdminClient(ctx)
	if err != nil {
		err = fmt.Errorf("can't create admin client: %v", utils.AnalyzeError(err, dbURI))
		return nil, nil, dbURI, err
	}
	client, err := utils.GetClient(ctx, dbURI)
	if err != nil {
		err = fmt.Errorf("can't create client for db %s: %v", dbURI, err)
		return adminClient, nil, dbURI, err
	}
	return adminClient, client, dbURI, nil
}

// PrepareMigrationPrerequisites creates source and target profiles, opens a new IOStream and generates the database name.
func PrepareMigrationPrerequisites(sourceProfileString, targetProfileString, source string) (profiles.SourceProfile, profiles.TargetProfile, utils.IOStreams, string, error) {
	sourceProfile, err := profiles.NewSourceProfile(sourceProfileString, source)
	if err != nil {
		return profiles.SourceProfile{}, profiles.TargetProfile{}, utils.IOStreams{}, "", err
	}
	sourceProfile.Driver, err = sourceProfile.ToLegacyDriver(source)
	if err != nil {
		return profiles.SourceProfile{}, profiles.TargetProfile{}, utils.IOStreams{}, "", err
	}

	targetProfile, err := profiles.NewTargetProfile(targetProfileString)
	if err != nil {
		return sourceProfile, profiles.TargetProfile{}, utils.IOStreams{}, "", err
	}
	targetProfile.TargetDb = targetProfile.ToLegacyTargetDb()

	dumpFilePath := ""
	if sourceProfile.Ty == profiles.SourceProfileTypeFile && (sourceProfile.File.Format == "" || sourceProfile.File.Format == "dump") {
		dumpFilePath = sourceProfile.File.Path
	}
	ioHelper := utils.NewIOStreams(sourceProfile.Driver, dumpFilePath)
	if ioHelper.SeekableIn != nil {
		defer ioHelper.In.Close()
	}

	dbName, err := utils.GetDatabaseName(sourceProfile.Driver, time.Now())
	if err != nil {
		err = fmt.Errorf("can't generate database name for prefix: %v", err)
		return sourceProfile, targetProfile, ioHelper, "", err
	}
	return sourceProfile, targetProfile, ioHelper, dbName, nil
}

// Copyright 2023 Google LLC
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

// Package utils contains common helper functions used across multiple other packages.
// Utils should not import any Spanner migration tool packages.
package utils

import (
	"context"
	"fmt"

	datastream "cloud.google.com/go/datastream/apiv1"
	"cloud.google.com/go/datastream/apiv1/datastreampb"
)

// FetchTargetBucketAndPath fetches the bucket and path name from a Datastream destination config.
func FetchTargetBucketAndPath(ctx context.Context, datastreamClient *datastream.Client, projectID string, datastreamDestinationConnCfg DstConnCfg) (string, string, error) {
	if datastreamClient == nil {
		return "", "", fmt.Errorf("datastream client could not be created")
	}
	dstProf := fmt.Sprintf("projects/%s/locations/%s/connectionProfiles/%s", projectID, datastreamDestinationConnCfg.Location, datastreamDestinationConnCfg.Name)
	res, err := datastreamClient.GetConnectionProfile(ctx, &datastreampb.GetConnectionProfileRequest{Name: dstProf})
	if err != nil {
		return "", "", fmt.Errorf("could not get connection profiles: %v", err)
	}
	// Fetch the GCS path from the target connection profile.
	gcsProfile := res.Profile.(*datastreampb.ConnectionProfile_GcsProfile).GcsProfile
	bucketName := gcsProfile.Bucket
	prefix := gcsProfile.RootPath + datastreamDestinationConnCfg.Prefix
	prefix = ConcatDirectoryPath(prefix, "data/")
	return bucketName, prefix, nil
}

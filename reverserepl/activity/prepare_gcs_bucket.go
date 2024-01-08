// Copyright 2024 Google LLC
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
package activity

import (
	"context"
	"fmt"
	"strings"

	storageacc "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/storage"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	resource "github.com/GoogleCloudPlatform/spanner-migration-tool/reverserepl/resource"
)

type PrepareGcsBucketInput struct {
	SmtJobId               string
	SmtBucketName          string
	SpannerProjectId       string
	SpannerLocation        string
	SessionFilePath        string
	SourceConnectionConfig string
	IsSMTBucketRequired    bool
}

type PrepareGcsBucketOutput struct {
	SessionFilePath        string
	SourceConnectionConfig string
}

type PrepareGcsBucket struct {
	Input *PrepareGcsBucketInput
}

// This creates a GCS bucket if based on flag input. It subsequently uploads local files to the bucket.
func (p *PrepareGcsBucket) Transaction(ctx context.Context) error {
	input := p.Input
	if input.IsSMTBucketRequired {
		err := resource.CreateBucketSMTResource(ctx, input.SmtJobId, input.SmtBucketName, input.SpannerProjectId, input.SpannerLocation, nil, 45)
		if err != nil {
			return err
		}
		logger.Log.Info(fmt.Sprintf("Created bucket: %s", input.SmtBucketName))
		if !strings.HasPrefix(input.SessionFilePath, constants.GCS_FILE_PREFIX) {
			err := storageacc.UploadLocalFileToGCS(ctx, fmt.Sprintf("%s%s/", constants.GCS_FILE_PREFIX, input.SmtBucketName), "session.json", input.SessionFilePath)
			if err != nil {
				return fmt.Errorf("could not upload session file to GCS: %v", err)
			}
			logger.Log.Debug(fmt.Sprintf("Uploaded local session file: %s to bucket %s", input.SessionFilePath, input.SmtBucketName))
		}
		if !strings.HasPrefix(input.SourceConnectionConfig, constants.GCS_FILE_PREFIX) {
			err := storageacc.UploadLocalFileToGCS(ctx, fmt.Sprintf("%s%s/", constants.GCS_FILE_PREFIX, input.SmtBucketName), "source-connection-config.json", input.SourceConnectionConfig)
			if err != nil {
				return fmt.Errorf("could not upload source connection config file to GCS: %v", err)
			}
			logger.Log.Debug(fmt.Sprintf("Uploaded local source connection config : %s to bucket %s", input.SourceConnectionConfig, input.SmtBucketName))
		}
	}
	return nil
}

func (p *PrepareGcsBucket) Compensation(ctx context.Context) error {
	return nil
}

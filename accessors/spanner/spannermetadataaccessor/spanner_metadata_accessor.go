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
package spannermetadataaccessor

import (
	"context"
	"fmt"

	"cloud.google.com/go/spanner"
	spannermetadataclient "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/spanner/spannermetadataaccessor/clients"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"google.golang.org/api/iterator"
)

type SpannerMetadataAccessor interface {
	// IsSpannerSupportedDefaultStatement checks if the given statement is supported by Spanner.
	IsSpannerSupportedDefaultStatement(SpProjectId string, SpInstanceId string, statement string, coldatatype string) bool
}

type SpannerMetadataAccessorImpl struct{}

func (spm *SpannerMetadataAccessorImpl) IsSpannerSupportedDefaultStatement(SpProjectId string, SpInstanceId string, statement string, coldatatype string) bool {
	db := getSpannerMetadataDbUri(SpProjectId, SpInstanceId)
	if SpProjectId == "" || SpInstanceId == "" {
		return false
	}

	ctx := context.Background()
	spmClient, err := spannermetadataclient.GetOrCreateClient(ctx, db)
	if err != nil {
		return false
	}

	if spmClient == nil {
		return false
	}
	stmt := spanner.Statement{
		SQL: "SELECT CAST(" + statement + " AS " + coldatatype + ") AS statementValue",
	}
	iter := spmClient.Single().Query(ctx, stmt)
	defer iter.Stop()
	for {
		_, err := iter.Next()
		if err == iterator.Done {
			return true
		}
		if err != nil {
			return false
		}

	}

}

func getSpannerMetadataDbUri(projectId string, instanceId string) string {
	return fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectId, instanceId, constants.METADATA_DB)
}

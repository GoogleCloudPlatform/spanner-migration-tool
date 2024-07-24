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
	// IsSpannerSupportedStatement checks whether the statement from Source database is supported by Spanner or not.
	IsSpannerSupportedStatement(SpProjectId string, SpInstanceId string, defaultval string, columntype string) bool
	// fires query to spanner to cast statement based on spanner column type.
	isValidSpannerStatement(db string, defaultval string, ty string) error
}

type SpannerMetadataAccessorImpl struct{}

func (spm *SpannerMetadataAccessorImpl) IsSpannerSupportedStatement(SpProjectId string, SpInstanceId string, statement string, columntype string) bool {
	db := getSpannerUri(SpProjectId, SpInstanceId)
	if SpProjectId == "" || SpInstanceId == "" {
		return false
	}
	err := spm.isValidSpannerStatement(db, statement, columntype)
	if err != nil {
		return false
	} else {
		return true
	}
}
func (spm *SpannerMetadataAccessorImpl) isValidSpannerStatement(db string, statement string, ty string) error {
	ctx := context.Background()
	spmClient, err := spannermetadataclient.GetOrCreateClient(ctx, db)
	if err != nil {
		return err
	}

	if spmClient == nil {
		return fmt.Errorf("Client is nil")
	}
	stmt := spanner.Statement{
		SQL: "SELECT CAST(" + statement + " AS " + ty + ") AS ConvertedDefaultval",
	}
	iter := spmClient.Single().Query(ctx, stmt)
	defer iter.Stop()
	for {
		_, err := iter.Next()
		if err == iterator.Done {
			return nil
		}
		if err != nil {
			return err
		}

	}
}

func getSpannerUri(projectId string, instanceId string) string {
	return fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectId, instanceId, constants.METADATA_DB)
}

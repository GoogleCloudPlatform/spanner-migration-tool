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

	"cloud.google.com/go/spanner"
	spanneraccessor "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/spanner"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/dao"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
)

type CreateSmtJobEntryInput struct {
	SmtJobId         string
	JobName          string
	SpannerProjectId string
	InstanceId       string
	DatabaseId       string
	JobData          string
}

type CreateSmtJobEntry struct {
	Input *CreateSmtJobEntryInput
}

// This creates an entry in the SMT job table.
func (p *CreateSmtJobEntry) Transaction(ctx context.Context) error {
	input := p.Input
	dialect, err := spanneraccessor.GetDatabaseDialect(ctx, fmt.Sprintf("projects/%s/instances/%s/databases/%s", input.SpannerProjectId, input.InstanceId, input.DatabaseId))
	if err != nil {
		return fmt.Errorf("could not fetch database dialect: %v", err)
	}
	logger.Log.Debug(fmt.Sprintf("found database dialect: %s", dialect))
	jobData := spanner.NullJSON{Valid: true, Value: input.JobData}
	err = dao.InsertSMTJobEntry(ctx, input.SmtJobId, input.JobName, constants.REVERSE_REPLICATION_JOB_TYPE, dialect, input.DatabaseId, jobData)
	if err != nil {
		return err
	}
	logger.Log.Debug("Created entry SMT Job entry")
	return nil
}

func (p *CreateSmtJobEntry) Compensation(ctx context.Context) error {
	return nil
}

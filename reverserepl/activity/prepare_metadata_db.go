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

	spanneracc "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/spanner"
	resource "github.com/GoogleCloudPlatform/spanner-migration-tool/reverserepl/resource"
)

type PrepareMetadataDbInput struct {
	SmtJobId string
	DbURI    string
}

type PrepareMetadataDbOutput struct {
	Exists  bool
	Created bool
}

type PrepareMetadataDb struct {
	Input  *PrepareMetadataDbInput
	Output *PrepareMetadataDbOutput
}

// Creates a metadata db for reverse replication if one is not already present.
func (p *PrepareMetadataDb) Transaction(ctx context.Context) error {
	input := p.Input
	dbExists, err := spanneracc.CheckExistingDb(ctx, input.DbURI)
	if err != nil {
		return err
	}
	if dbExists {
		p.Output.Exists = true
		return nil
	}
	resource.CreateMetadataDbSMTResource(ctx, input.SmtJobId, input.DbURI)
	p.Output.Created = true
	return nil
}

func (p *PrepareMetadataDb) Compensation(ctx context.Context) error {
	return nil
}

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

	spanneracc "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/spanner"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	resource "github.com/GoogleCloudPlatform/spanner-migration-tool/reverserepl/resource"
)

type PrepareChangeStreamInput struct {
	SmtJobId         string
	ChangeStreamName string
	DbURI            string
}

type PrepareChangeStreamOutput struct {
	Exists                     bool
	ExistsWithIncorrectOptions bool
	Created                    bool
}

type PrepareChangeStream struct {
	Input  *PrepareChangeStreamInput
	Output *PrepareChangeStreamOutput
}

// This checks is a valid change stream exists or not. If not, it creates one on the provided DbURI.
func (p *PrepareChangeStream) Transaction(ctx context.Context) error {
	input := p.Input
	csExists, err := spanneracc.CheckIfChangeStreamExists(ctx, input.ChangeStreamName, input.DbURI)
	if err != nil {
		return err
	}
	if csExists {
		err = spanneracc.ValidateChangeStreamOptions(ctx, input.ChangeStreamName, input.DbURI)
		if err != nil {
			p.Output.ExistsWithIncorrectOptions = true
			return fmt.Errorf("invalid change stream option found: %v", err)
		}
		logger.Log.Info(fmt.Sprintf("change stream %s already exists for %s, skipping creation", input.ChangeStreamName, input.DbURI))
		p.Output.Exists = true
		return nil
	}
	err = resource.CreateChangeStreamSMTResource(ctx, input.SmtJobId, input.ChangeStreamName, input.DbURI)
	if err != nil {
		return fmt.Errorf("could not create change stream resource: %v", err)
	}
	logger.Log.Info(fmt.Sprintf("Created change stream %s for %s", input.ChangeStreamName, input.DbURI))
	p.Output.Created = true
	return nil
}

func (p *PrepareChangeStream) Compensation(ctx context.Context) error {
	return nil
}

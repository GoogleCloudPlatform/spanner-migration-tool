// Copyright 2026 Google LLC
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
package secretmanageraccessor

import (
	"context"
	"fmt"

	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	secretmanagerclient "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/clients/secretmanager"
)

type SecretManagerAccessor interface {
	GetSecret(ctx context.Context, secretId string) (string, error)
}

type SecretManagerAccessorImpl struct {
	Client secretmanagerclient.SecretManagerClient
}

func (sma *SecretManagerAccessorImpl) GetSecret(ctx context.Context, secretId string) (string, error) {
	req := &secretmanagerpb.AccessSecretVersionRequest{
		Name: secretId,
	}

	result, err := sma.Client.AccessSecretVersion(ctx, req)
	if err != nil {
		return "", fmt.Errorf("failed to access secret version: %v", err)
	}

	return string(result.Payload.Data), nil
}

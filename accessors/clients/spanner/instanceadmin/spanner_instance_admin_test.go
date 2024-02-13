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
package spinstanceadmin

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"

	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"google.golang.org/api/option"
)

func init() {
	logger.Log = zap.NewNop()
}

func TestMain(m *testing.M) {
	res := m.Run()
	os.Exit(res)
}

func resetTest() {
	instanceAdminClient = nil
	once = sync.Once{}
}

func TestGetOrCreateClient_Basic(t *testing.T) {
	resetTest()
	ctx := context.Background()
	oldFunc := newInstanceAdminClient
	defer func() { newInstanceAdminClient = oldFunc }()
	newInstanceAdminClient = func(ctx context.Context, opts ...option.ClientOption) (*instance.InstanceAdminClient, error) {
		return &instance.InstanceAdminClient{}, nil
	}
	c, err := GetOrCreateClient(ctx)
	assert.NotNil(t, c)
	assert.Nil(t, err)
}

func TestGetOrCreateClient_OnlyOnceViaSync(t *testing.T) {
	resetTest()
	ctx := context.Background()
	oldFunc := newInstanceAdminClient
	defer func() { newInstanceAdminClient = oldFunc }()

	newInstanceAdminClient = func(ctx context.Context, opts ...option.ClientOption) (*instance.InstanceAdminClient, error) {
		return &instance.InstanceAdminClient{}, nil
	}
	c, err := GetOrCreateClient(ctx)
	assert.NotNil(t, c)
	assert.Nil(t, err)
	// Explicitly set the client to nil. Running GetOrCreateClient should not create a
	// new client since sync would already be executed.
	instanceAdminClient = nil
	newInstanceAdminClient = func(ctx context.Context, opts ...option.ClientOption) (*instance.InstanceAdminClient, error) {
		return nil, fmt.Errorf("test error")
	}
	c, err = GetOrCreateClient(ctx)
	assert.Nil(t, c)
	assert.Nil(t, err)
}

func TestGetOrCreateClient_OnlyOnceViaIf(t *testing.T) {
	resetTest()
	ctx := context.Background()
	oldFunc := newInstanceAdminClient
	defer func() { newInstanceAdminClient = oldFunc }()

	newInstanceAdminClient = func(ctx context.Context, opts ...option.ClientOption) (*instance.InstanceAdminClient, error) {
		return &instance.InstanceAdminClient{}, nil
	}
	oldC, err := GetOrCreateClient(ctx)
	assert.NotNil(t, oldC)
	assert.Nil(t, err)

	// Explicitly reset once. Running GetOrCreateClient should not create a
	// new client the if condition should prevent it.
	once = sync.Once{}
	newInstanceAdminClient = func(ctx context.Context, opts ...option.ClientOption) (*instance.InstanceAdminClient, error) {
		return nil, fmt.Errorf("test error")
	}
	newC, err := GetOrCreateClient(ctx)
	assert.Equal(t, oldC, newC)
	assert.Nil(t, err)
}

func TestGetOrCreateClient_Error(t *testing.T) {
	resetTest()
	ctx := context.Background()
	oldFunc := newInstanceAdminClient
	defer func() { newInstanceAdminClient = oldFunc }()

	newInstanceAdminClient = func(ctx context.Context, opts ...option.ClientOption) (*instance.InstanceAdminClient, error) {
		return nil, fmt.Errorf("test error")
	}
	c, err := GetOrCreateClient(ctx)
	assert.Nil(t, c)
	assert.NotNil(t, err)
}

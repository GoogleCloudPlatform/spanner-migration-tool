package secretmanagerclient

import (
	"context"
	"fmt"
	"testing"

	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	googleapis "github.com/googleapis/gax-go/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/api/option"
)

// MockSecretManagerClient is a mock of SecretManagerClient interface
type MockSecretManagerClient struct {
	mock.Mock
}

func (m *MockSecretManagerClient) AccessSecretVersion(ctx context.Context, req *secretmanagerpb.AccessSecretVersionRequest, opts ...googleapis.CallOption) (*secretmanagerpb.AccessSecretVersionResponse, error) {
	args := m.Called(ctx, req, opts)
	return args.Get(0).(*secretmanagerpb.AccessSecretVersionResponse), args.Error(1)
}

func (m *MockSecretManagerClient) Close() error {
	args := m.Called()
	return args.Error(0)
}

func TestGetOrCreateClient(t *testing.T) {
	callCount := 0
	creator := func(ctx context.Context, opts ...option.ClientOption) (*secretmanager.Client, error) {
			callCount++
			return &secretmanager.Client{}, nil
	}

	factory := &SecretManagerClientFactory{
			creator: creator,
	}

	ctx := context.Background()
	client1, err := factory.GetOrCreateClient(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, client1)

	// Call again, should return same instance and not increment callCount
	client2, err := factory.GetOrCreateClient(ctx)
	assert.NoError(t, err)
	assert.Equal(t, client1, client2)
	assert.Equal(t, 1, callCount)
}

func TestGetOrCreateClient_Error(t *testing.T) {

	expectedErr := fmt.Errorf("client creation failed")
	creator := func(ctx context.Context, opts ...option.ClientOption) (*secretmanager.Client, error) {
		return nil, expectedErr
	}

	factory := &SecretManagerClientFactory{
			creator: creator,
	}

	ctx := context.Background()
	client, err := factory.GetOrCreateClient(ctx)
	assert.Error(t, err)
	assert.Nil(t, client)
	assert.Contains(t, err.Error(), expectedErr.Error())
}

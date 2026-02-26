package secretmanageraccessor

import (
	"context"
	"fmt"
	"testing"

	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	googleapis "github.com/googleapis/gax-go/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
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

func TestGetSecret(t *testing.T) {
	mockClient := new(MockSecretManagerClient)
	accessor := &SecretManagerAccessorImpl{Client: mockClient}
	ctx := context.Background()
	secretId := "projects/my-project/secrets/my-secret/versions/latest"
	expectedPwd := "my-password"

	mockClient.On("AccessSecretVersion", ctx, mock.MatchedBy(func(req *secretmanagerpb.AccessSecretVersionRequest) bool {
		return req.Name == secretId
	}), mock.Anything).Return(&secretmanagerpb.AccessSecretVersionResponse{
		Payload: &secretmanagerpb.SecretPayload{
			Data: []byte(expectedPwd),
		},
	}, nil)

	pwd, err := accessor.GetSecret(ctx, secretId)
	assert.NoError(t, err)
	assert.Equal(t, expectedPwd, pwd)
	mockClient.AssertExpectations(t)
}

func TestGetSecret_Error(t *testing.T) {
	mockClient := new(MockSecretManagerClient)
	accessor := &SecretManagerAccessorImpl{Client: mockClient}
	ctx := context.Background()
	secretId := "projects/my-project/secrets/my-secret/versions/latest"

	mockClient.On("AccessSecretVersion", ctx, mock.MatchedBy(func(req *secretmanagerpb.AccessSecretVersionRequest) bool {
		return req.Name == secretId
	}), mock.Anything).Return(&secretmanagerpb.AccessSecretVersionResponse{}, fmt.Errorf("client error"))

	_, err := accessor.GetSecret(ctx, secretId)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to access secret version")
	mockClient.AssertExpectations(t)
}

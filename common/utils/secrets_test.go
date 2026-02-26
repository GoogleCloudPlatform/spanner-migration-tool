package utils

import (
	"context"
	"fmt"
	"testing"

	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	secretmanagerclient "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/clients/secretmanager"
	googleapis "github.com/googleapis/gax-go/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

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

func TestFetchPasswordFromSecretManager(t *testing.T) {
	mockClient := new(MockSecretManagerClient)

	// Override the client creation function
	oldNewClient := secretmanagerclient.NewSecretManagerClient
	secretmanagerclient.NewSecretManagerClient = func(ctx context.Context) (secretmanagerclient.SecretManagerClient, error) {
		return mockClient, nil
	}
	defer func() { secretmanagerclient.NewSecretManagerClient = oldNewClient }()

	testCases := []struct {
		name           string
		secretId       string
		mockResponse   *secretmanagerpb.AccessSecretVersionResponse
		mockError      error
		expectedSecret string
		expectedPwd    string
		expectError    bool
	}{
		{
			name:     "Success with version",
			secretId: "projects/my-project/secrets/my-secret/versions/1",
			mockResponse: &secretmanagerpb.AccessSecretVersionResponse{
				Payload: &secretmanagerpb.SecretPayload{
					Data: []byte("password123"),
				},
			},
			expectedSecret: "projects/my-project/secrets/my-secret/versions/1",
			expectedPwd:    "password123",
			expectError:    false,
		},
		{
			name:     "Success without version (appends latest)",
			secretId: "projects/my-project/secrets/my-secret",
			mockResponse: &secretmanagerpb.AccessSecretVersionResponse{
				Payload: &secretmanagerpb.SecretPayload{
					Data: []byte("password456"),
				},
			},
			expectedSecret: "projects/my-project/secrets/my-secret/versions/latest",
			expectedPwd:    "password456",
			expectError:    false,
		},
		{
			name:        "Client error",
			secretId:    "projects/my-project/secrets/my-secret",
			mockError:   fmt.Errorf("client error"),
			expectError: true,
		},
		{
			name:     "Success with trailing slash (appends latest)",
			secretId: "projects/my-project/secrets/my-secret/",
			mockResponse: &secretmanagerpb.AccessSecretVersionResponse{
				Payload: &secretmanagerpb.SecretPayload{
					Data: []byte("password789"),
				},
			},
			expectedSecret: "projects/my-project/secrets/my-secret/versions/latest",
			expectedPwd:    "password789",
			expectError:    false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if !tc.expectError {
				mockClient.On("AccessSecretVersion", mock.Anything, mock.MatchedBy(func(req *secretmanagerpb.AccessSecretVersionRequest) bool {
					return req.Name == tc.expectedSecret
				}), mock.Anything).Return(tc.mockResponse, nil).Once()
			} else if tc.mockError != nil {
				mockClient.On("AccessSecretVersion", mock.Anything, mock.Anything, mock.Anything).Return(&secretmanagerpb.AccessSecretVersionResponse{}, tc.mockError).Once()
			}

			secretId, pwd, err := FetchPasswordFromSecretManager(tc.secretId)

			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedSecret, secretId)
				assert.Equal(t, tc.expectedPwd, pwd)
			}
		})
	}
}

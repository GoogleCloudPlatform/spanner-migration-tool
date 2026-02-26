package utils

import (
	"context"
	"fmt"
	"strings"

	secretmanagerclient "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/clients/secretmanager"
	secretmanageraccessor "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/secretmanager"
)

// FetchPasswordFromSecretManager fetches the password from Secret Manager.
// It returns the resolved secret ID (with version if added), the password, and any error.
func FetchPasswordFromSecretManager(secretId string) (string, string, error) {
	secretId = strings.TrimSuffix(secretId, "/")
	if !strings.Contains(secretId, "/versions/") {
		secretId += "/versions/latest"
	}
	ctx := context.Background()
	smc, err := secretmanagerclient.NewSecretManagerClient(ctx)
	if err != nil {
		return secretId, "", fmt.Errorf("failed to create secret manager client: %v", err)
	}

	sma := &secretmanageraccessor.SecretManagerAccessorImpl{Client: smc}
	pwd, err := sma.GetSecret(ctx, secretId)
	if err != nil {
		return secretId, "", fmt.Errorf("failed to fetch password from secret manager: %v", err)
	}
	return secretId, pwd, nil
}

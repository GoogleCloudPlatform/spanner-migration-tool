package secretmanagerclient

import (
	"context"
	"fmt"
	"sync"

	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	googleapis "github.com/googleapis/gax-go/v2"
	"google.golang.org/api/option"
)

type SecretManagerClient interface {
    AccessSecretVersion(ctx context.Context, req *secretmanagerpb.AccessSecretVersionRequest, opts ...googleapis.CallOption) (*secretmanagerpb.AccessSecretVersionResponse, error)
    Close() error
}

// ClientCreator is a function type for creating a Secret Manager client.
type ClientCreator func(ctx context.Context, opts ...option.ClientOption) (*secretmanager.Client, error)

// SecretManagerClientFactory manages the creation of Secret Manager clients.
type SecretManagerClientFactory struct {
    client  *secretmanager.Client
    creator ClientCreator
    once    sync.Once
}

var defaultFactory = &SecretManagerClientFactory{
    creator: secretmanager.NewClient,
}


func (f *SecretManagerClientFactory) GetOrCreateClient(ctx context.Context) (*secretmanager.Client, error) {
    var err error
    f.once.Do(func() {
        f.client, err = f.creator(ctx)
    })
    if err != nil {
        return nil, fmt.Errorf("failed to create secret manager client: %v", err)
    }
    return f.client, nil
}


var NewSecretManagerClient = func(ctx context.Context) (SecretManagerClient, error) {
	return defaultFactory.GetOrCreateClient(ctx)
}

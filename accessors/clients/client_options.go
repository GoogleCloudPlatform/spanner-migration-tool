package clients

import (
	"golang.org/x/oauth2"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"os"
)

func FetchSpannerClientOptions() []option.ClientOption {
	var clientOptions []option.ClientOption
	if endpoint := os.Getenv("SPANNER_API_ENDPOINT"); endpoint != "" {
		clientOptions = append(clientOptions, option.WithEndpoint(endpoint))
	} else if emulatorHost := os.Getenv("SPANNER_EMULATOR_HOST"); emulatorHost != "" && os.Getenv("SPANNER_OMNI") == "true" {
		clientOptions = append(clientOptions, option.WithEndpoint(emulatorHost))
	} else if os.Getenv("SPANNER_OMNI") == "true" {
		clientOptions = append(clientOptions, option.WithEndpoint("localhost:15000"))
	}


	if os.Getenv("SPANNER_OMNI") == "true" {
		clientOptions = append(clientOptions, option.WithoutAuthentication())
		clientOptions = append(clientOptions, option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())))
	}

	authOption := fetchAuthClientOptions()
	if authOption != nil {
		clientOptions = append(clientOptions, authOption)
	}
	return clientOptions
}


func FetchStorageClientOptions() []option.ClientOption {
	var clientOptions []option.ClientOption
	authOption := fetchAuthClientOptions()
	if authOption != nil {
		clientOptions = append(clientOptions, authOption)
	}
	return clientOptions
}

func fetchAuthClientOptions() option.ClientOption {

	if gcloudAuthPlugin := os.Getenv("GCLOUD_AUTH_PLUGIN"); gcloudAuthPlugin == "true" {
		// Wrap the token with cloud.google.com/go/auth.Credentials.
		tokenFromSource := option.WithTokenSource(oauth2.StaticTokenSource(&oauth2.Token{
			AccessToken: os.Getenv("GCLOUD_AUTH_ACCESS_TOKEN"),
		}))

		return tokenFromSource
	}
	return nil
}

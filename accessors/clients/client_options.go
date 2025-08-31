package clients

import (
	"golang.org/x/oauth2"
	"google.golang.org/api/option"
	"os"
)

func FetchSpannerClientOptions() []option.ClientOption {
	var clientOptions []option.ClientOption
	if endpoint := os.Getenv("SPANNER_API_ENDPOINT"); endpoint != "" {
		clientOptions = append(clientOptions, option.WithEndpoint(endpoint))
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

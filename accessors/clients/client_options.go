package clients

import (
	"golang.org/x/oauth2"
	"google.golang.org/api/option"
	"os"
)

func FetchSpannerClientOptions() ([]option.ClientOption, error) {
	var clientOptions []option.ClientOption
	if endpoint := os.Getenv("SPANNER_API_ENDPOINT"); endpoint != "" {
		clientOptions = append(clientOptions, option.WithEndpoint(endpoint))
	}

	if gcloudAuthPlugin := os.Getenv("GCLOUD_AUTH_PLUGIN"); gcloudAuthPlugin == "true" {
		// Wrap the token with cloud.google.com/go/auth.Credentials.

		tokenFromSource := option.WithTokenSource(oauth2.StaticTokenSource(&oauth2.Token{
			AccessToken: os.Getenv("GCLOUD_AUTH_ACCESS_TOKEN"),
		}))

		clientOptions = append(clientOptions, tokenFromSource)
	}
	return clientOptions, nil
}

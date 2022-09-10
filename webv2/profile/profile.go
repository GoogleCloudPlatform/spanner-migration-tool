package profile

import (
	"context"
	"fmt"
	"net/http"

	datastream "cloud.google.com/go/datastream/apiv1"
	"google.golang.org/api/iterator"
	datastreampb "google.golang.org/genproto/googleapis/cloud/datastream/v1"
)

func GetRegions(w http.ResponseWriter, r *http.Request) {
	ctx := context.Background()
	dsClient, err := datastream.NewClient(ctx)
	if err != nil {
		http.Error(w, fmt.Sprintf("datastream client can not be created: %v", err), http.StatusBadRequest)
	}
	defer dsClient.Close()
	fmt.Println("Created client...")

	req := &datastreampb.ListConnectionProfilesRequest{
		Parent: fmt.Sprintf("projects/%s/locations/%s", "span-cloud-testing", "us-central1"),
	}
	it := dsClient.ListConnectionProfiles(ctx, req)
	for {
		resp, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			fmt.Println(err)
		}
		if resp.GetMysqlProfile().GetHostname() != "" {
			fmt.Println(resp.Name)
		}

	}
}

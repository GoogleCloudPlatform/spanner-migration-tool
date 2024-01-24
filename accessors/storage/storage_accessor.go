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
package storageaccessor

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"cloud.google.com/go/storage"
	storageclient "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/clients/storage"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/utils"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"google.golang.org/api/googleapi"
)

type StorageAccessor interface {
	CreateGCSBucket(ctx context.Context, bucketName, projectID, location string) error
	CreateGCSBucketWithLifecycle(ctx context.Context, bucketName, projectID, location string, matchesPrefix []string, ttl int64) error
	EnableBucketLifecycleDeleteRule(ctx context.Context, bucketName string, matchesPrefix []string, ttl int64) error
	UploadLocalFileToGCS(ctx context.Context, filePath, fileName, localFilePath string) error
	WriteDataToGCS(ctx context.Context, filePath, fileName, data string) error
	ReadGcsFile(ctx context.Context, filePath string) (string, error)
	ReadAnyFile(ctx context.Context, filePath string) (string, error)
}

type StorageAccessorImpl struct{}

func (sa *StorageAccessorImpl) CreateGCSBucket(ctx context.Context, bucketName, projectID, location string) error {
	return sa.createGCSBucketUtil(ctx, bucketName, projectID, location, nil, 0)
}

func (sa *StorageAccessorImpl) CreateGCSBucketWithLifecycle(ctx context.Context, bucketName, projectID, location string, matchesPrefix []string, ttl int64) error {
	return sa.createGCSBucketUtil(ctx, bucketName, projectID, location, matchesPrefix, ttl)
}

func (sa *StorageAccessorImpl) createGCSBucketUtil(ctx context.Context, bucketName, projectID, location string, matchesPrefix []string, ttl int64) error {
	client, err := storageclient.GetOrCreateClient(ctx)
	if err != nil {
		return err
	}
	bucket := client.Bucket(bucketName)
	attrs := storage.BucketAttrs{
		Location: location,
	}
	if ttl > 0 {
		attrs.Lifecycle = storage.Lifecycle{
			Rules: []storage.LifecycleRule{
				{
					Action: storage.LifecycleAction{Type: "Delete"},
					Condition: storage.LifecycleCondition{
						AgeInDays: ttl,
						// The prefixes should not contain the bucket names and starting slash.
						// For object gs://my_bucket/pictures/paris_2022.jpg,
						// you would use a condition such as "matchesPrefix":["pictures/paris_"].
						MatchesPrefix: matchesPrefix,
					},
				},
			},
		}
	}

	if err := bucket.Create(ctx, projectID, &attrs); err != nil {
		if e, ok := err.(*googleapi.Error); ok {
			// Ignoring the bucket already exists error.
			if e.Code != 409 {
				return fmt.Errorf("failed to create bucket: %v", err)
			} else {
				fmt.Printf("Using the existing bucket: %v \n", bucketName)
			}
		} else {
			return fmt.Errorf("failed to create bucket: %v", err)
		}

	} else {
		logger.Log.Info(fmt.Sprintf("Created new GCS bucket: %v\n", bucketName))
	}
	return nil
}

// Applies the bucket lifecycle with delete rule. Only accepts the Age and
// prefix rule conditions as it is only used for the Datastream destination
// bucket currently.
func (sa *StorageAccessorImpl) EnableBucketLifecycleDeleteRule(ctx context.Context, bucketName string, matchesPrefix []string, ttl int64) error {
	client, err := storageclient.GetOrCreateClient(ctx)
	if err != nil {
		return fmt.Errorf("could not create client while enabling lifecycle: %w", err)
	}

	for i, str := range matchesPrefix {
		matchesPrefix[i] = strings.TrimPrefix(str, "/")
	}
	bucket := client.Bucket(bucketName)
	bucketAttrsToUpdate := storage.BucketAttrsToUpdate{
		Lifecycle: &storage.Lifecycle{
			Rules: []storage.LifecycleRule{
				{
					Action: storage.LifecycleAction{Type: "Delete"},
					Condition: storage.LifecycleCondition{
						AgeInDays: ttl,
						// The prefixes should not contain the bucket names and starting slash.
						// For object gs://my_bucket/pictures/paris_2022.jpg,
						// you would use a condition such as "matchesPrefix":["pictures/paris_"].
						MatchesPrefix: matchesPrefix,
					},
				},
			},
		},
	}

	attrs, err := bucket.Update(ctx, bucketAttrsToUpdate)
	if err != nil {
		return fmt.Errorf("could not bucket with lifecycle: %w", err)
	}
	logger.Log.Info(fmt.Sprintf("Added lifecycle rule to bucket %v\n. Rule Action: %v\t Rule Condition: %v\n",
		bucketName, attrs.Lifecycle.Rules[0].Action, attrs.Lifecycle.Rules[0].Condition))
	return nil
}

// UploadLocalFileToGCS uploads an object.
func (sa *StorageAccessorImpl) UploadLocalFileToGCS(ctx context.Context, filePath, fileName, localFilePath string) error {
	data, err := os.ReadFile(localFilePath)
	if err != nil {
		return fmt.Errorf("could not read file %s: %w", localFilePath, err)
	}
	return sa.WriteDataToGCS(ctx, filePath, fileName, string(data))
}

func (sa *StorageAccessorImpl) WriteDataToGCS(ctx context.Context, filePath, fileName, data string) error {
	client, err := storageclient.GetOrCreateClient(ctx)
	if err != nil {
		return fmt.Errorf("could not create client while uploading to GCS: %w", err)
	}

	u, err := utils.ParseGCSFilePath(filePath)
	if err != nil {
		return fmt.Errorf("parseFilePath: unable to parse file path: %v", err)
	}
	bucketName := u.Host
	bucket := client.Bucket(bucketName)
	obj := bucket.Object(u.Path[1:] + fileName)

	w := obj.NewWriter(ctx)
	logger.Log.Info(fmt.Sprintf("Writing data to %s", filePath))
	n, err := fmt.Fprint(w, data)
	if err != nil {
		fmt.Printf("Failed to write to Cloud Storage: %s", filePath)
		return err
	}
	logger.Log.Info(fmt.Sprintf("Wrote %d bytes to GCS", n))

	if err := w.Close(); err != nil {
		fmt.Printf("Failed to close GCS file: %s", filePath)
		return err
	}
	return nil
}

func (sa *StorageAccessorImpl) ReadGcsFile(ctx context.Context, filePath string) (string, error) {
	client, err := storageclient.GetOrCreateClient(ctx)
	if err != nil {
		return "", fmt.Errorf("could not create client: %w", err)
	}

	u, err := utils.ParseGCSFilePath(filePath)
	if err != nil {
		return "", fmt.Errorf("unable to parse file path: %v", err)
	}
	bucketName := u.Host
	bucket := client.Bucket(bucketName)
	obj := bucket.Object(u.Path[1:])

	rc, err := obj.NewReader(ctx)
	if err != nil {
		return "", err
	}
	defer rc.Close()
	buf := new(strings.Builder)
	logger.Log.Info(fmt.Sprintf("Reading from %s", filePath))
	n, err := io.Copy(buf, rc)
	if err != nil {
		return "", err
	}
	logger.Log.Info(fmt.Sprintf("Read %d bytes", n))
	return buf.String(), nil
}

func (sa *StorageAccessorImpl) ReadAnyFile(ctx context.Context, filePath string) (string, error) {
	if strings.HasPrefix(filePath, constants.GCS_FILE_PREFIX) {
		return sa.ReadGcsFile(ctx, filePath)
	}
	buf, err := os.ReadFile(filePath)
	if err != nil {
		return "", err
	}
	return string(buf), nil
}

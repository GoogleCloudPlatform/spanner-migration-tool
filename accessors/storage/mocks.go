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

	storageclient "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/clients/storage"
)

type StorageAccessorMock struct {
	CreateGCSBucketMock                func(ctx context.Context, sc storageclient.StorageClient, bucketName, projectID, location string, ttl int64, matchesPrefix []string) error
	ApplyBucketLifecycleDeleteRuleMock func(ctx context.Context, sc storageclient.StorageClient, bucketName string, matchesPrefix []string, ttl int64) error
	UploadLocalFileToGCSMock           func(ctx context.Context, sc storageclient.StorageClient, filePath, fileName, localFilePath string) error
	WriteDataToGCSMock                 func(ctx context.Context, sc storageclient.StorageClient, filePath, fileName, data string) error
	ReadGcsFileMock                    func(ctx context.Context, sc storageclient.StorageClient, filePath string) (string, error)
	ReadAnyFileMock                    func(ctx context.Context, sc storageclient.StorageClient, filePath string) (string, error)
}

func (sam *StorageAccessorMock) CreateGCSBucket(ctx context.Context, sc storageclient.StorageClient, bucketName, projectID, location string, ttl int64, matchesPrefix []string) error {
	return sam.CreateGCSBucketMock(ctx, sc, bucketName, projectID, location, ttl, matchesPrefix)
}

func (sam *StorageAccessorMock) ApplyBucketLifecycleDeleteRule(ctx context.Context, sc storageclient.StorageClient, bucketName string, matchesPrefix []string, ttl int64) error {
	return sam.ApplyBucketLifecycleDeleteRuleMock(ctx, sc, bucketName, matchesPrefix, ttl)
}

func (sam *StorageAccessorMock) UploadLocalFileToGCS(ctx context.Context, sc storageclient.StorageClient, filePath, fileName, localFilePath string) error {
	return sam.UploadLocalFileToGCSMock(ctx, sc, filePath, fileName, localFilePath)
}

func (sam *StorageAccessorMock) WriteDataToGCS(ctx context.Context, sc storageclient.StorageClient, filePath, fileName, data string) error {
	return sam.WriteDataToGCSMock(ctx, sc, filePath, fileName, data)
}

func (sam *StorageAccessorMock) ReadGcsFile(ctx context.Context, sc storageclient.StorageClient, filePath string) (string, error) {
	return sam.ReadGcsFileMock(ctx, sc, filePath)
}

func (sam *StorageAccessorMock) ReadAnyFile(ctx context.Context, sc storageclient.StorageClient, filePath string) (string, error) {
	return sam.ReadAnyFileMock(ctx, sc, filePath)
}

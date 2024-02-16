// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a
package storageaccessor

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"cloud.google.com/go/storage"
	storageclient "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/clients/storage"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"google.golang.org/api/googleapi"
)

func init() {
	logger.Log = zap.NewNop()
}

func TestMain(m *testing.M) {
	res := m.Run()
	os.Exit(res)
}

func TestStorageAccessorImpl_CreateGCSBucket(t *testing.T) {
	testCases := []struct {
		name        string
		scm         storageclient.StorageClientMock
		expectError bool
	}{
		{
			name: "Basic",
			scm: storageclient.StorageClientMock{
				BucketMock: func(name string) storageclient.BucketHandle {
					return &storageclient.BucketHandleMock{
						CreateMock: func(ctx context.Context, projectID string, attrs *storage.BucketAttrs) (err error) {
							return nil
						},
					}
				},
			},
			expectError: false,
		},
		{
			name: "random error",
			scm: storageclient.StorageClientMock{
				BucketMock: func(name string) storageclient.BucketHandle {
					return &storageclient.BucketHandleMock{
						CreateMock: func(ctx context.Context, projectID string, attrs *storage.BucketAttrs) (err error) {
							return fmt.Errorf("random error")
						},
					}
				},
			},
			expectError: true,
		},
		{
			name: "Bucket already exists",
			scm: storageclient.StorageClientMock{
				BucketMock: func(name string) storageclient.BucketHandle {
					return &storageclient.BucketHandleMock{
						CreateMock: func(ctx context.Context, projectID string, attrs *storage.BucketAttrs) (err error) {
							return &googleapi.Error{Code: 409}
						},
					}
				},
			},
			expectError: false,
		},
		{
			name: "Other google api error",
			scm: storageclient.StorageClientMock{
				BucketMock: func(name string) storageclient.BucketHandle {
					return &storageclient.BucketHandleMock{
						CreateMock: func(ctx context.Context, projectID string, attrs *storage.BucketAttrs) (err error) {
							return &googleapi.Error{Code: 100}
						},
					}
				},
			},
			expectError: true,
		},
	}
	ctx := context.Background()
	sa := StorageAccessorImpl{}
	for _, tc := range testCases {
		err := sa.CreateGCSBucket(ctx, &tc.scm, StorageBucketMetadata{
			BucketName:    "test-bucket",
			ProjectID:     "test-project",
			Location:      "india2",
			Ttl:           1,
			MatchesPrefix: nil,
		})
		assert.Equal(t, tc.expectError, err != nil, tc.name)
	}
}

func TestStorageAccessorImpl_ApplyBucketLifecycleDeleteRule(t *testing.T) {
	testCases := []struct {
		name          string
		scm           storageclient.StorageClientMock
		ttl           int64
		matchesPrefix []string
		expectError   bool
	}{
		{
			name: "Basic",
			scm: storageclient.StorageClientMock{
				BucketMock: func(name string) storageclient.BucketHandle {
					return &storageclient.BucketHandleMock{
						UpdateMock: func(ctx context.Context, uattrs storage.BucketAttrsToUpdate) (attrs *storage.BucketAttrs, err error) {
							if strings.HasPrefix(uattrs.Lifecycle.Rules[0].Condition.MatchesPrefix[0], "/") {
								return nil, fmt.Errorf("test error")
							}
							return &storage.BucketAttrs{Lifecycle: storage.Lifecycle{Rules: []storage.LifecycleRule{
								{
									Action: storage.LifecycleAction{Type: "Delete"},
									Condition: storage.LifecycleCondition{
										AgeInDays:     5,
										MatchesPrefix: []string{},
									},
								},
							}}}, nil
						},
					}
				},
			},
			ttl:           5,
			matchesPrefix: []string{"test"},
			expectError:   false,
		},
		{
			name: "Update error",
			scm: storageclient.StorageClientMock{
				BucketMock: func(name string) storageclient.BucketHandle {
					return &storageclient.BucketHandleMock{
						UpdateMock: func(ctx context.Context, uattrs storage.BucketAttrsToUpdate) (attrs *storage.BucketAttrs, err error) {
							return nil, fmt.Errorf("test error")
						},
					}
				},
			},
			ttl:           5,
			matchesPrefix: []string{"test"},
			expectError:   true,
		},
		{
			name: "Prefix '/' gets removed",
			scm: storageclient.StorageClientMock{
				BucketMock: func(name string) storageclient.BucketHandle {
					return &storageclient.BucketHandleMock{
						UpdateMock: func(ctx context.Context, uattrs storage.BucketAttrsToUpdate) (attrs *storage.BucketAttrs, err error) {
							if strings.HasPrefix(uattrs.Lifecycle.Rules[0].Condition.MatchesPrefix[0], "/") {
								return nil, fmt.Errorf("test error")
							}
							return &storage.BucketAttrs{Lifecycle: storage.Lifecycle{Rules: []storage.LifecycleRule{
								{
									Action: storage.LifecycleAction{Type: "Delete"},
									Condition: storage.LifecycleCondition{
										AgeInDays:     5,
										MatchesPrefix: []string{},
									},
								},
							}}}, nil
						},
					}
				},
			},
			ttl:           5,
			matchesPrefix: []string{"/test"},
			expectError:   false,
		},
	}
	ctx := context.Background()
	sa := StorageAccessorImpl{}
	for _, tc := range testCases {
		err := sa.ApplyBucketLifecycleDeleteRule(ctx, &tc.scm, StorageBucketMetadata{
			BucketName:    "test-bucket",
			Ttl:           tc.ttl,
			MatchesPrefix: tc.matchesPrefix,
		})
		assert.Equal(t, tc.expectError, err != nil, tc.name)
	}
}

func TestStorageAccessorImpl_WriteDataToGCS(t *testing.T) {
	testCases := []struct {
		name        string
		scm         storageclient.StorageClientMock
		filePath    string
		fileName    string
		data        string
		expectError bool
	}{
		{
			name: "Basic",
			scm: storageclient.StorageClientMock{
				BucketMock: func(name string) storageclient.BucketHandle {
					return &storageclient.BucketHandleMock{
						ObjectMock: func(name string) storageclient.ObjectHandle {
							return &storageclient.ObjectHandleMock{
								NewWriterMock: func(ctx context.Context) io.WriteCloser {
									return &storageclient.WriterMock{
										WriteMock: func(p []byte) (n int, err error) { return len(p), nil },
										CloseMock: func() error { return nil },
									}
								},
							}
						},
					}
				},
			},
			filePath:    "gs://bucket/path",
			fileName:    "test-file",
			data:        "abcd",
			expectError: false,
		},
		{
			name: "File parsing error",
			scm: storageclient.StorageClientMock{
				BucketMock: func(name string) storageclient.BucketHandle {
					return &storageclient.BucketHandleMock{
						ObjectMock: func(name string) storageclient.ObjectHandle {
							return &storageclient.ObjectHandleMock{
								NewWriterMock: func(ctx context.Context) io.WriteCloser {
									return &storageclient.WriterMock{
										WriteMock: func(p []byte) (n int, err error) { return len(p), nil },
										CloseMock: func() error { return nil },
									}
								},
							}
						},
					}
				},
			},
			filePath:    "://bucket/path",
			fileName:    "test-file",
			data:        "abcd",
			expectError: true,
		},
		{
			name: "Write error",
			scm: storageclient.StorageClientMock{
				BucketMock: func(name string) storageclient.BucketHandle {
					return &storageclient.BucketHandleMock{
						ObjectMock: func(name string) storageclient.ObjectHandle {
							return &storageclient.ObjectHandleMock{
								NewWriterMock: func(ctx context.Context) io.WriteCloser {
									return &storageclient.WriterMock{
										WriteMock: func(p []byte) (n int, err error) { return 0, fmt.Errorf("test-error") },
										CloseMock: func() error { return nil },
									}
								},
							}
						},
					}
				},
			},
			filePath:    "gs://bucket/path",
			fileName:    "test-file",
			data:        "abcd",
			expectError: true,
		},
		{
			name: "Close error",
			scm: storageclient.StorageClientMock{
				BucketMock: func(name string) storageclient.BucketHandle {
					return &storageclient.BucketHandleMock{
						ObjectMock: func(name string) storageclient.ObjectHandle {
							return &storageclient.ObjectHandleMock{
								NewWriterMock: func(ctx context.Context) io.WriteCloser {
									return &storageclient.WriterMock{
										WriteMock: func(p []byte) (n int, err error) { return len(p), nil },
										CloseMock: func() error { return fmt.Errorf("test error") },
									}
								},
							}
						},
					}
				},
			},
			filePath:    "gs://bucket/path",
			fileName:    "test-file",
			data:        "abcd",
			expectError: true,
		},
	}
	ctx := context.Background()
	sa := StorageAccessorImpl{}
	for _, tc := range testCases {
		err := sa.WriteDataToGCS(ctx, &tc.scm, tc.filePath, tc.fileName, tc.data)
		assert.Equal(t, tc.expectError, err != nil, tc.name)
	}
}

func TestStorageAccessorImpl_ReadGcsFile(t *testing.T) {
	testCases := []struct {
		name        string
		scm         storageclient.StorageClientMock
		filePath    string
		expectError bool
		want        string
	}{
		{
			name: "Basic",
			scm: storageclient.StorageClientMock{
				BucketMock: func(name string) storageclient.BucketHandle {
					return &storageclient.BucketHandleMock{
						ObjectMock: func(name string) storageclient.ObjectHandle {
							return &storageclient.ObjectHandleMock{
								NewReaderMock: func(ctx context.Context) (io.ReadCloser, error) {
									return &storageclient.ReaderMock{
										ReadMock: func(p []byte) (n int, err error) {
											copy(p, "hello")
											return 5, io.EOF
										},
										CloseMock: func() error { return nil },
									}, nil
								},
							}
						},
					}
				},
			},
			filePath:    "gs://bucket/path",
			expectError: false,
			want:        "hello",
		},
		{
			name:        "Parse error",
			scm:         storageclient.StorageClientMock{},
			filePath:    "://bucket/path",
			expectError: true,
			want:        "",
		},
		{
			name: "New reader error",
			scm: storageclient.StorageClientMock{
				BucketMock: func(name string) storageclient.BucketHandle {
					return &storageclient.BucketHandleMock{
						ObjectMock: func(name string) storageclient.ObjectHandle {
							return &storageclient.ObjectHandleMock{
								NewReaderMock: func(ctx context.Context) (io.ReadCloser, error) {
									return nil, fmt.Errorf("test error")
								},
							}
						},
					}
				},
			},
			filePath:    "gs://bucket/path",
			expectError: true,
			want:        "",
		},
		{
			name: "Read error",
			scm: storageclient.StorageClientMock{
				BucketMock: func(name string) storageclient.BucketHandle {
					return &storageclient.BucketHandleMock{
						ObjectMock: func(name string) storageclient.ObjectHandle {
							return &storageclient.ObjectHandleMock{
								NewReaderMock: func(ctx context.Context) (io.ReadCloser, error) {
									return &storageclient.ReaderMock{
										ReadMock: func(p []byte) (n int, err error) {
											return 0, fmt.Errorf("test error")
										},
										CloseMock: func() error { return nil },
									}, nil
								},
							}
						},
					}
				},
			},
			filePath:    "gs://bucket/path",
			expectError: true,
			want:        "",
		},
	}
	ctx := context.Background()
	sa := StorageAccessorImpl{}
	for _, tc := range testCases {
		got, err := sa.ReadGcsFile(ctx, &tc.scm, tc.filePath)
		assert.Equal(t, tc.expectError, err != nil, tc.name)
		assert.Equal(t, tc.want, got, tc.name)
	}
}

func TestStorageAccessorImpl_DeleteGCSBucket(t *testing.T) {
	testCases := []struct {
		name        string
		scm         storageclient.StorageClientMock
		expectError bool
	}{
		{
			name: "Basic",
			scm: storageclient.StorageClientMock{
				BucketMock: func(name string) storageclient.BucketHandle {
					return &storageclient.BucketHandleMock{
						DeleteMock: func(ctx context.Context) error {
							return nil
						},
					}
				},
			},
			expectError: false,
		},
		{
			name: "Error",
			scm: storageclient.StorageClientMock{
				BucketMock: func(name string) storageclient.BucketHandle {
					return &storageclient.BucketHandleMock{
						DeleteMock: func(ctx context.Context) error {
							return fmt.Errorf("error")
						},
					}
				},
			},
			expectError: true,
		},
	}
	ctx := context.Background()
	sa := StorageAccessorImpl{}
	for _, tc := range testCases {
		err := sa.DeleteGCSBucket(ctx, &tc.scm, StorageBucketMetadata{
			BucketName:    "test-bucket",
		})
		assert.Equal(t, tc.expectError, err != nil, tc.name)
	}
}
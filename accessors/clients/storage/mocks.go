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
package storageclient

import (
	"context"
	"io"

	"cloud.google.com/go/storage"
)

// Mock that implements the StorageClient interface.
// Pass in unit tests where StorageClient is an input parameter.
type StorageClientMock struct {
	BucketMock func(name string) BucketHandle
}

func (scm *StorageClientMock) Bucket(name string) BucketHandle {
	return scm.BucketMock(name)
}

// Mock that implements the BucketHandle interface.
// Pass in unit tests where BucketHandle is an input parameter.
type BucketHandleMock struct {
	CreateMock func(ctx context.Context, projectID string, attrs *storage.BucketAttrs) (err error)
	UpdateMock func(ctx context.Context, uattrs storage.BucketAttrsToUpdate) (attrs *storage.BucketAttrs, err error)
	ObjectMock func(name string) ObjectHandle
}

func (b *BucketHandleMock) Create(ctx context.Context, projectID string, attrs *storage.BucketAttrs) (err error) {
	return b.CreateMock(ctx, projectID, attrs)
}

func (b *BucketHandleMock) Update(ctx context.Context, uattrs storage.BucketAttrsToUpdate) (attrs *storage.BucketAttrs, err error) {
	return b.UpdateMock(ctx, uattrs)
}

func (b *BucketHandleMock) Object(name string) ObjectHandle {
	return b.ObjectMock(name)
}

// Mock that implements the ObjectHandle interface.
// Pass in unit tests where ObjectHandle is an input parameter.
type ObjectHandleMock struct {
	NewWriterMock func(ctx context.Context) io.WriteCloser
	NewReaderMock func(ctx context.Context) (io.ReadCloser, error)
}

func (o *ObjectHandleMock) NewWriter(ctx context.Context) io.WriteCloser {
	return o.NewWriterMock(ctx)
}

func (o *ObjectHandleMock) NewReader(ctx context.Context) (io.ReadCloser, error) {
	return o.NewReaderMock(ctx)
}

// Mock that implements the io.WriteCloser interface.
// Pass in unit tests where io.WriteCloser is an input parameter.
type WriterMock struct {
	WriteMock func(p []byte) (n int, err error)
	CloseMock func() error
}

func (w *WriterMock) Write(p []byte) (n int, err error) {
	return w.WriteMock(p)
}

func (w *WriterMock) Close() error {
	return w.CloseMock()
}

// Mock that implements the io.ReadCloser interface.
// Pass in unit tests where io.ReadCloser is an input parameter.
type ReaderMock struct {
	ReadMock  func(p []byte) (n int, err error)
	CloseMock func() error
}

func (r *ReaderMock) Read(p []byte) (n int, err error) {
	return r.ReadMock(p)
}

func (r *ReaderMock) Close() error {
	return r.CloseMock()
}

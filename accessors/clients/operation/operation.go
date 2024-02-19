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
package operation

import (
	"context"

	"github.com/googleapis/gax-go/v2"
)

// Generic interface for mocking long running operations like CreateStreamOperation, UpdateStreamOperation etc.
type Operation[T any] interface {
	Wait(ctx context.Context, opts ...gax.CallOption) (*T, error)
}

// Wrapping the operation interace in a struct helps us stick to the golang idiom of not returning an interface.
type OperationWrapper[T any] struct {
	elem Operation[T]
}

func (o *OperationWrapper[T]) Wait(ctx context.Context, opts ...gax.CallOption) (*T, error) {
	return o.elem.Wait(ctx, opts...)
}

func NewOperationWrapper[T any](elem Operation[T]) OperationWrapper[T] {
	return OperationWrapper[T]{elem}
}

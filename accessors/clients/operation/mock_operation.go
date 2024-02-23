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
	"time"

	"github.com/googleapis/gax-go/v2"
)

type MockOperation[T any] struct {
	RetVal *T
	RetErr error
	Delay  time.Duration
}

func (m MockOperation[T]) Wait(ctx context.Context, opts ...gax.CallOption) (*T, error) {
	// As per golang docs, a 0 or -ve delay makes sleep return immediately.
	time.Sleep(m.Delay)
	return m.RetVal, m.RetErr
}

type MockNilOperation struct {
	RetErr error
	Delay  time.Duration
}

func (m *MockNilOperation) Wait(ctx context.Context, opts ...gax.CallOption) error {
	// As per golang docs, a 0 or -ve delay makes sleep return immediately.
	time.Sleep(m.Delay)
	return m.RetErr
}

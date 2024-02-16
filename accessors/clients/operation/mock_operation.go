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

func (m *MockNilOperation) Wait(ctx context.Context, opts ...gax.CallOption) (error) {
	// As per golang docs, a 0 or -ve delay makes sleep return immediately.
	time.Sleep(m.Delay)
	return m.RetErr
}

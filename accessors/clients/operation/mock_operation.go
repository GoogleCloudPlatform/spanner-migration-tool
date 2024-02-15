package operation

import (
	"context"
	"time"

	"github.com/googleapis/gax-go/v2"
)

type MockOperation[T any] struct {
	retVal *T
	retErr error
	delay  time.Duration
}

func (m MockOperation[T]) Wait(ctx context.Context, opts ...gax.CallOption) (*T, error) {
	// As per golang docs, a 0 or -ve delay makes sleep return immediately.
	time.Sleep(m.delay)
	return m.retVal, m.retErr
}
